package backup

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"github.com/Altinity/clickhouse-backup/v2/pkg/clickhouse"
	"github.com/Altinity/clickhouse-backup/v2/pkg/common"
	"github.com/Altinity/clickhouse-backup/v2/pkg/metadata"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

// livePartRow - a row of system.parts whose data directory can be hardlinked instead of downloading a part
type livePartRow struct {
	Name     string `ch:"name"`
	Path     string `ch:"path"`
	Disk     string `ch:"disk_name"`
	Database string `ch:"database"`
	Table    string `ch:"table"`
	Hash     string `ch:"hash"`
}

// fetchLivePartsByHash reads system.parts once per table instead of once per part and groups the live
// parts by hash_of_all_files, so hardlinkByHashOfAllFiles resolves its candidate from memory. The
// snapshot is read-only after the build and safe for the concurrent part goroutines,
// https://github.com/Altinity/clickhouse-backup/issues/1457
func (b *Backuper) fetchLivePartsByHash(ctx context.Context, table metadata.TableMetadata) (map[string][]livePartRow, error) {
	seen := make(common.EmptyMap, len(table.HashOfAllFiles))
	hashes := make([]string, 0, len(table.HashOfAllFiles))
	for _, hash := range table.HashOfAllFiles {
		hash = strings.ToLower(hash)
		if _, exists := seen[hash]; exists {
			continue
		}
		seen[hash] = struct{}{}
		hashes = append(hashes, hash)
	}
	result := make(map[string][]livePartRow, len(hashes))
	if len(hashes) == 0 {
		return result, nil
	}
	const chunkSize = 1000
	for chunkStart := 0; chunkStart < len(hashes); chunkStart += chunkSize {
		chunk := hashes[chunkStart:min(chunkStart+chunkSize, len(hashes))]
		var rows []livePartRow
		q := fmt.Sprintf("SELECT name, path, disk_name, database, `table`, lower(hash_of_all_files) AS hash FROM system.parts WHERE active AND lower(hash_of_all_files) IN (%s)", strings.TrimSuffix(strings.Repeat("?,", len(chunk)), ","))
		args := make([]interface{}, len(chunk))
		for i, hash := range chunk {
			args[i] = hash
		}
		if err := b.ch.SelectContext(ctx, &rows, q, args...); err != nil {
			return nil, errors.Wrap(err, "SELECT hash_of_all_files FROM system.parts")
		}
		for _, row := range rows {
			result[row.Hash] = append(result[row.Hash], row)
		}
	}
	log.Debug().Msgf("fetchLivePartsByHash: %s.%s resolved %d of %d hashes from system.parts", table.Database, table.Table, len(result), len(hashes))
	return result, nil
}

// localPartIndexKey addresses a shadow part directory inside local backups the same way
// findLocalPartWithSameChecksum globs for it, by table, disk and part name
type localPartIndexKey struct {
	database string
	table    string
	diskName string
	partName string
}

// localPartIndex replaces the per-part filepath.Glob over every local backup shadow directory
// with a single pass over local backup table metadata,
// https://github.com/Altinity/clickhouse-backup/issues/1457
type localPartIndex struct {
	paths map[localPartIndexKey][]string
	// complete is false when at least one local backup could not be indexed, in that case
	// the index is only a subset of what the glob would find and callers must keep globbing
	complete bool
}

// lookup returns existing shadow part directories in the same order filepath.Glob would return them,
// os.Stat keeps the index semantics identical to the glob, which never returns missing directories
func (idx *localPartIndex) lookup(database, table, diskName, partName string) []string {
	candidates := idx.paths[localPartIndexKey{database: database, table: table, diskName: diskName, partName: partName}]
	existing := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		if _, err := os.Stat(candidate); err == nil {
			existing = append(existing, candidate)
		}
	}
	return existing
}

// buildLocalPartIndex walks table metadata of every local backup and maps each part to the shadow
// directory it occupies on a local disk, so hardlinkIfLocalPartExistsAndChecksumEqual can find
// hardlink candidates without listing all local backups per part,
// https://github.com/Altinity/clickhouse-backup/issues/1457
func (b *Backuper) buildLocalPartIndex(localBackups []LocalBackup, disks []clickhouse.Disk) *localPartIndex {
	idx := &localPartIndex{paths: map[localPartIndexKey][]string{}, complete: true}
	diskPathByName := make(map[string]string, len(disks))
	for _, disk := range disks {
		// the glob searches non backup disks only, IsBackup disks hold embedded backups with another layout
		if disk.IsBackup {
			continue
		}
		diskPathByName[disk.Name] = disk.Path
	}
	backupNames := make([]string, 0, len(localBackups))
	for _, localBackup := range localBackups {
		if localBackup.Broken != "" {
			log.Warn().Msgf("buildLocalPartIndex: local backup %s is broken: %s, hardlink candidates will be searched by glob", localBackup.BackupName, localBackup.Broken)
			idx.complete = false
			continue
		}
		// embedded backups live on an IsBackup disk with a different layout, the glob never matches them
		if strings.Contains(localBackup.Tags, "embedded") {
			continue
		}
		backupNames = append(backupNames, localBackup.BackupName)
	}
	// filepath.Glob returns sorted matches, keep the same candidate order
	sort.Strings(backupNames)
	for _, backupName := range backupNames {
		metadataPath := path.Join(b.DefaultDataPath, "backup", backupName, "metadata")
		if _, statErr := os.Stat(metadataPath); os.IsNotExist(statErr) {
			// `--rbac-only` and `--configs-only` backups contain no table metadata and no shadow parts
			continue
		}
		if walkErr := filepath.Walk(metadataPath, func(filePath string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() || !strings.HasSuffix(filePath, ".json") {
				return nil
			}
			var tm metadata.TableMetadata
			if _, loadErr := tm.Load(filePath); loadErr != nil {
				return loadErr
			}
			dbAndTableDir := path.Join(common.TablePathEncode(tm.Database), common.TablePathEncode(tm.Table))
			for diskName, parts := range tm.Parts {
				for _, part := range parts {
					partDiskName := diskName
					if part.RebalancedDisk != "" {
						partDiskName = part.RebalancedDisk
					}
					diskPath, diskExists := diskPathByName[partDiskName]
					if !diskExists {
						continue
					}
					key := localPartIndexKey{database: tm.Database, table: tm.Table, diskName: partDiskName, partName: part.Name}
					idx.paths[key] = append(idx.paths[key], path.Join(diskPath, "backup", backupName, "shadow", dbAndTableDir, partDiskName, part.Name))
				}
			}
			return nil
		}); walkErr != nil {
			log.Warn().Err(walkErr).Msgf("buildLocalPartIndex: can't index %s, hardlink candidates will be searched by glob", metadataPath)
			idx.complete = false
		}
	}
	log.Debug().Msgf("buildLocalPartIndex: indexed %d parts from %d local backups, complete=%v", len(idx.paths), len(backupNames), idx.complete)
	return idx
}
