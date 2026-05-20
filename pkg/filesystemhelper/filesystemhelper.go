package filesystemhelper

import (
	"io/fs"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/clickhouse"
	"github.com/Altinity/clickhouse-backup/v2/pkg/common"
	"github.com/Altinity/clickhouse-backup/v2/pkg/metadata"
	"github.com/Altinity/clickhouse-backup/v2/pkg/utils"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

var (
	uid       *int
	gid       *int
	chownLock sync.Mutex
)

// Chown - set permission on path to clickhouse user
// This is necessary that the ClickHouse will be able to read parts files on restore
func Chown(fPath string, ch *clickhouse.ClickHouse, disks []clickhouse.Disk, recursive bool) error {
	var (
		dataPath string
		err      error
	)
	if os.Getuid() != 0 {
		return nil
	}
	chownLock.Lock()
	if uid == nil {
		if dataPath, err = ch.GetDefaultPath(disks); err != nil {
			return errors.WithMessage(err, "Chown GetDefaultPath")
		}
		info, err := os.Stat(dataPath)
		if err != nil {
			return errors.WithMessage(err, "Chown os.Stat")
		}
		stat := info.Sys().(*syscall.Stat_t)
		intUid := int(stat.Uid)
		intGid := int(stat.Gid)
		uid = &intUid
		gid = &intGid
	}
	chownLock.Unlock()
	if !recursive {
		return os.Chown(fPath, *uid, *gid)
	}
	return filepath.WalkDir(fPath, func(fPath string, f fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		return os.Chown(fPath, *uid, *gid)
	})
}

func Mkdir(name string, ch *clickhouse.ClickHouse, disks []clickhouse.Disk) error {
	if err := os.MkdirAll(name, 0750); err != nil && !os.IsExist(err) {
		return errors.WithMessage(err, "Mkdir MkdirAll")
	}
	if err := Chown(name, ch, disks, false); err != nil {
		return errors.WithMessage(err, "Mkdir Chown")
	}
	return nil
}

func MkdirAll(path string, ch *clickhouse.ClickHouse, disks []clickhouse.Disk) error {
	// Fast path: if we can tell whether path is a directory or file, stop with success or error.
	dir, err := os.Stat(path)
	if err == nil {
		if dir.IsDir() {
			return nil
		}
		return &os.PathError{Op: "mkdir", Path: path, Err: syscall.ENOTDIR}
	}

	// Slow path: make sure parent exists and then call Mkdir for path.
	i := len(path)
	for i > 0 && os.IsPathSeparator(path[i-1]) { // Skip trailing path separator.
		i--
	}

	j := i
	for j > 0 && !os.IsPathSeparator(path[j-1]) { // Scan backward over element.
		j--
	}

	if j > 1 {
		// Create parent.
		err = MkdirAll(path[:j-1], ch, disks)
		if err != nil {
			return errors.WithMessage(err, "MkdirAll create parent")
		}
	}

	// Parent now exists; invoke Mkdir and use its result.
	err = Mkdir(path, ch, disks)
	if err != nil {
		// Handle arguments like "foo/." by
		// double-checking that directory doesn't exist.
		dir, err1 := os.Lstat(path)
		if err1 == nil && dir.IsDir() {
			return nil
		}
		return err
	}
	return nil
}

// HardlinkBackupPartsToStorage - copy partitions for specific table to detached folder
func HardlinkBackupPartsToStorage(backupName string, backupTable metadata.TableMetadata, disks []clickhouse.Disk, diskMap map[string]string, tableDataPaths, skipProjections []string, ch *clickhouse.ClickHouse, toDetached bool) error {
	start := time.Now()
	dstDataPaths := clickhouse.GetDisksByPaths(disks, tableDataPaths)
	dbAndTableDir := path.Join(common.TablePathEncode(backupTable.Database), common.TablePathEncode(backupTable.Table))
	if !toDetached {
		for backupDiskName := range backupTable.Parts {
			dstParentDir, dstParentDirExists := dstDataPaths[backupDiskName]
			if dstParentDirExists {
				// avoid to restore to non-empty to avoid attach in already dropped partitions, corner case
				existsFiles, err := os.ReadDir(dstParentDir)
				if err != nil && !os.IsNotExist(err) {
					return errors.WithMessage(err, "HardlinkBackupPartsToStorage ReadDir")
				}
				for _, f := range existsFiles {
					if f.Name() != "detached" && !strings.HasSuffix(f.Name(), ".txt") {
						return errors.Errorf("%s contains exists data %v, we can't restore directly via ATTACH TABLE, use `clickhouse->restore_as_attach=false` in your config", dstParentDir, existsFiles)
					}
				}
			}
		}
	}
	for backupDiskName := range backupTable.Parts {
		for _, part := range backupTable.Parts[backupDiskName] {
			// Use a per-iteration variable to avoid corrupting backupDiskName
			// across inner loop iterations when RebalancedDisk differs per part
			activeDisk := backupDiskName
			dstParentDir, dstParentDirExists := dstDataPaths[activeDisk]
			if !dstParentDirExists && part.RebalancedDisk == "" {
				return errors.Errorf("dstDataPaths=%#v, not contains %s", dstDataPaths, activeDisk)
			}
			if part.RebalancedDisk != "" {
				activeDisk = part.RebalancedDisk
				dstParentDir, dstParentDirExists = dstDataPaths[part.RebalancedDisk]
				if !dstParentDirExists {
					// ClickHouse creates store/{uuid}/detached/ on ALL disks of the
					// storage policy at table load time (MergeTreeData constructor).
					// Build the path directly so hardlinks stay on the same filesystem.
					rebalancedDiskPath, hasDisk := diskMap[part.RebalancedDisk]
					if !hasDisk {
						return errors.Errorf("rebalanced disk %s not found in diskMap", part.RebalancedDisk)
					}
					if backupTable.UUID == "" {
						return errors.Errorf("table UUID is empty, can't build store path for rebalanced disk %s", part.RebalancedDisk)
					}
					dstParentDir = filepath.Join(rebalancedDiskPath, "store", backupTable.UUID[:3], backupTable.UUID)
					dstParentDirExists = true
				}
			}
			backupDiskPath := diskMap[activeDisk]
			if toDetached {
				dstParentDir = filepath.Join(dstParentDir, "detached")
			}
			dstPartPath := filepath.Join(dstParentDir, part.Name)
			info, err := os.Stat(dstPartPath)
			if err != nil {
				if os.IsNotExist(err) {
					log.Debug().Msgf("MkDirAll %s", dstPartPath)
					if mkdirErr := MkdirAll(dstPartPath, ch, disks); mkdirErr != nil {
						log.Warn().Msgf("error during Mkdir %+v", mkdirErr)
					}
				} else {
					return err
				}
			} else if !info.IsDir() {
				return errors.Errorf("'%s' should be directory or absent", dstPartPath)
			}
			// activeDisk is the rebalanced disk name when RebalancedDisk is set,
			// matching the directory structure created by download
			srcPartPath := path.Join(backupDiskPath, "backup", backupName, "shadow", dbAndTableDir, activeDisk, part.Name)
			if err := filepath.Walk(srcPartPath, func(filePath string, info os.FileInfo, err error) error {
				if err != nil {
					return err
				}
				// fix https://github.com/Altinity/clickhouse-backup/issues/826
				if strings.Contains(info.Name(), "frozen_metadata.txt") {
					return nil
				}
				filename := strings.Trim(strings.TrimPrefix(filePath, srcPartPath), "/")
				// https://github.com/Altinity/clickhouse-backup/issues/861
				if IsSkipProjections(skipProjections, path.Join(backupTable.Database, backupTable.Table, part.Name, filename)) {
					return nil
				}
				dstFilePath := filepath.Join(dstPartPath, filename)
				if info.IsDir() {
					// https://github.com/Altinity/clickhouse-backup/issues/861
					if IsSkipProjections(skipProjections, path.Join(backupTable.Database, backupTable.Table, part.Name, filename)+"/") {
						return nil
					}
					log.Debug().Msgf("MkDir %s", dstFilePath)
					return Mkdir(dstFilePath, ch, disks)
				}
				if !info.Mode().IsRegular() {
					log.Debug().Msgf("'%s' is not a regular file, skipping.", filePath)
					return nil
				}
				log.Debug().Msgf("Link %s -> %s", filePath, dstFilePath)
				if err := os.Link(filePath, dstFilePath); err != nil {
					if !os.IsExist(err) {
						return errors.Wrapf(err, "failed to create hard link '%s' -> '%s'", filePath, dstFilePath)
					}
				}
				return Chown(dstFilePath, ch, disks, false)
			}); err != nil {
				return errors.Wrapf(err, "error during filepath.Walk for part '%s'", part.Name)
			}
		}
	}
	log.Debug().Str("operation", "HardlinkBackupPartsToStorage").Str("duration", utils.HumanizeDuration(time.Since(start))).Msg("done")
	return nil
}

func IsPartInPartition(partName string, partitionsBackupMap common.EmptyMap) bool {
	partitionId := strings.Split(partName, "_")[0]
	if _, exists := partitionsBackupMap[partitionId]; exists {
		return true
	}
	for pattern := range partitionsBackupMap {
		if matched, err := filepath.Match(pattern, partitionId); err == nil && matched {
			return true
		} else if err != nil {
			log.Warn().Msgf("error filepath.Match(%s, %s) error: %v", pattern, partitionId, err)
			log.Debug().Msgf("%s not found in %s, file will filtered", partitionId, partitionsBackupMap)
			return false
		}
	}
	return false
}

func IsFileInPartition(disk, fileName string, partitionsBackupMap common.EmptyMap) bool {
	fileName = strings.TrimPrefix(fileName, disk+"_")
	fileName = strings.Split(fileName, "_")[0]
	if strings.Contains(fileName, "%") {
		decodedFileName, err := url.QueryUnescape(fileName)
		if err != nil {
			log.Warn().Msgf("error decoding %s: %v", fileName, err)
			log.Debug().Msgf("%s not found in %s, file will filtered", fileName, partitionsBackupMap)
			return false
		}
		fileName = decodedFileName
	}
	if _, exists := partitionsBackupMap[fileName]; exists {
		return true
	}
	for pattern := range partitionsBackupMap {
		if matched, err := filepath.Match(pattern, fileName); err == nil && matched {
			return true
		} else if err != nil {
			log.Warn().Msgf("error filepath.Match(%s, %s) error: %v", pattern, fileName, err)
			log.Debug().Msgf("%s not found in %s, file will filtered", fileName, partitionsBackupMap)
			return false
		}
	}
	return false
}

func MoveShadowToBackup(shadowPath, backupPartsPath string, partitionsBackupMap common.EmptyMap, table *clickhouse.Table, tableDiffFromRemote metadata.TableMetadata, disk clickhouse.Disk, skipProjections []string, version int) ([]metadata.Part, int64, map[string]uint64, error) {
	size := int64(0)
	parts := make([]metadata.Part, 0)
	checksums := make(map[string]uint64)
	walkErr := filepath.Walk(shadowPath, func(filePath string, info os.FileInfo, err error) error {
		// fix https://github.com/Altinity/clickhouse-backup/issues/826
		if strings.Contains(info.Name(), "frozen_metadata.txt") {
			return nil
		}

		// possible relative path
		// store / 1f9 / 1f9dc899-0de9-41f8-b95c-26c1f0d67d93 / 20181023_2_2_0 / checksums.txt
		// store / 1f9 / 1f9dc899-0de9-41f8-b95c-26c1f0d67d93 / 20181023_2_2_0 / x.proj / checksums.txt
		// data / database / table / 20181023_2_2_0 / checksums.txt
		// data / database / table / 20181023_2_2_0 / x.proj / checksums.txt
		relativePath := strings.Trim(strings.TrimPrefix(filePath, shadowPath), "/")
		pathParts := strings.SplitN(relativePath, "/", 4)
		if len(pathParts) != 4 {
			return nil
		}

		// https://github.com/Altinity/clickhouse-backup/issues/861
		if IsSkipProjections(skipProjections, path.Join(table.Database, table.Name, path.Join(pathParts[3:]...))) {
			return nil
		}

		if len(partitionsBackupMap) != 0 && !IsPartInPartition(pathParts[3], partitionsBackupMap) {
			return nil
		}
		var isRequiredPartFound, partExists bool
		if tableDiffFromRemote.Database != "" && tableDiffFromRemote.Table != "" && len(tableDiffFromRemote.Parts) > 0 && len(tableDiffFromRemote.Parts[disk.Name]) > 0 {
			parts, isRequiredPartFound, partExists = addRequiredPartIfNotExists(parts, pathParts[3], tableDiffFromRemote, disk)
			if isRequiredPartFound && !partExists {
				c, checksumErr := common.CalculateChecksum(filePath, "checksums.txt")
				if checksumErr != nil {
					return errors.Wrapf(checksumErr, "common.CalculateChecksum(isRequiredPartFound=true)")
				}
				checksums[pathParts[3]] = c
			}
			if isRequiredPartFound {
				return nil
			}
		}
		dstFilePath := filepath.Join(backupPartsPath, pathParts[3])
		if info.IsDir() {
			// skip detached directory - it is not a data part
			if pathParts[3] == "detached" {
				return filepath.SkipDir
			}
			if !strings.HasSuffix(pathParts[3], ".proj") && !isRequiredPartFound && !partExists {
				parts = append(parts, metadata.Part{
					Name: pathParts[3],
				})
				c, checksumErr := common.CalculateChecksum(filePath, "checksums.txt")
				if checksumErr != nil {
					return errors.Wrapf(checksumErr, "common.CalculateChecksum")
				}
				checksums[pathParts[3]] = c
			}
			return os.MkdirAll(dstFilePath, 0750)
		}
		if !info.Mode().IsRegular() {
			log.Debug().Msgf("'%s' is not a regular file, skipping", filePath)
			return nil
		}
		size += info.Size()
		if version < 21004000 {
			return os.Rename(filePath, dstFilePath)
		} else {
			return os.Link(filePath, dstFilePath)
		}
	})
	if walkErr != nil {
		return nil, 0, nil, walkErr
	}
	// https://github.com/ClickHouse/ClickHouse/issues/71009
	metadata.SortPartsByMinBlock(parts)
	return parts, size, checksums, nil
}

func IsSkipProjections(skipProjections []string, relativePath string) bool {
	if skipProjections == nil || len(skipProjections) == 0 {
		return false
	}
	log.Debug().Msgf("try IsSkipProjections, skipProjections=%v, relativePath=%s", skipProjections, relativePath)

	matchPattenFinal := func(dbPattern string, tablePattern string, projectionPattern string, relativePath string) bool {
		finalPattern := path.Join(dbPattern, tablePattern, "*", projectionPattern+".proj", "*")
		if strings.HasSuffix(relativePath, ".proj") {
			finalPattern = path.Join(dbPattern, tablePattern, "*", projectionPattern+".proj")
		}
		if isMatched, err := filepath.Match(finalPattern, relativePath); isMatched {
			return isMatched
		} else if err != nil {
			log.Warn().Msgf("filepath.Match(%s, %s) return error: %v", finalPattern, relativePath, err)
		} else {
			log.Debug().Msgf("IsSkipProjections not matched %s->%s", finalPattern, relativePath)
		}
		return false
	}
	if strings.Contains(relativePath, ".proj/") || strings.HasSuffix(relativePath, ".proj") {
		dbPattern := "*"
		tablePattern := "*"
		projectionPattern := "*"
		isWildCardPattern := true
		for _, skipPatterns := range skipProjections {
			for _, tableAndProjectionPatterns := range strings.Split(skipPatterns, ",") {
				dbPattern = "*"
				tablePattern = "*"
				projectionPattern = "*"
				tableAndProjectionPattern := strings.SplitN(tableAndProjectionPatterns, ":", 2)
				if len(tableAndProjectionPattern) == 2 {
					projectionPattern = tableAndProjectionPattern[1]
					isWildCardPattern = false
				}
				dbAndTablePattern := strings.SplitN(tableAndProjectionPattern[0], ".", 2)
				if len(dbAndTablePattern) == 2 {
					dbPattern = dbAndTablePattern[0]
					tablePattern = dbAndTablePattern[1]
					isWildCardPattern = false
				} else {
					tablePattern = dbAndTablePattern[0]
					isWildCardPattern = false
				}
				if isMatched := matchPattenFinal(dbPattern, tablePattern, projectionPattern, relativePath); isMatched {
					return true
				}
			}
		}
		if isWildCardPattern {
			if isMatched := matchPattenFinal(dbPattern, tablePattern, projectionPattern, relativePath); isMatched {
				return true
			}
		}
	}
	return false
}

func addRequiredPartIfNotExists(parts []metadata.Part, relativePath string, tableDiffFromRemote metadata.TableMetadata, disk clickhouse.Disk) ([]metadata.Part, bool, bool) {
	isRequiredPartFound := false
	exists := false
	for _, diffPart := range tableDiffFromRemote.Parts[disk.Name] {
		if diffPart.Name == relativePath || strings.HasPrefix(relativePath, diffPart.Name+"/") {
			isRequiredPartFound = true
			break
		}
	}
	if isRequiredPartFound {
		for _, p := range parts {
			if p.Name == relativePath || strings.HasPrefix(relativePath, p.Name+"/") {
				exists = true
				break
			}
		}
		if !exists {
			parts = append(parts, metadata.Part{
				Name:     relativePath,
				Required: true,
			})
		}
	}

	return parts, isRequiredPartFound, exists
}

func IsDuplicatedParts(part1, part2 string) error {
	p1, err := os.Open(part1)
	if err != nil {
		return errors.WithMessage(err, "IsDuplicatedParts open part1")
	}
	defer func() {
		if err = p1.Close(); err != nil {
			log.Warn().Msgf("Can't close %s", part1)
		}
	}()
	p2, err := os.Open(part2)
	if err != nil {
		return errors.WithMessage(err, "IsDuplicatedParts open part2")
	}
	defer func() {
		if err = p2.Close(); err != nil {
			log.Warn().Msgf("Can't close %s", part2)
		}
	}()
	pf1, err := p1.Readdirnames(-1)
	if err != nil {
		return errors.WithMessage(err, "IsDuplicatedParts Readdirnames part1")
	}
	pf2, err := p2.Readdirnames(-1)
	if err != nil {
		return errors.WithMessage(err, "IsDuplicatedParts Readdirnames part2")
	}
	if len(pf1) != len(pf2) {
		return errors.New("files count in parts is different")
	}
	for _, f := range pf1 {
		part1File, err := os.Stat(path.Join(part1, f))
		if err != nil {
			return errors.WithMessage(err, "IsDuplicatedParts stat part1 file")
		}
		part2File, err := os.Stat(path.Join(part2, f))
		if err != nil {
			return errors.WithMessage(err, "IsDuplicatedParts stat part2 file")
		}
		if !os.SameFile(part1File, part2File) {
			return errors.Errorf("file '%s' is different", f)
		}
	}
	return nil
}
