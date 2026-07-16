package backup

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/clickhouse"
	"github.com/Altinity/clickhouse-backup/v2/pkg/common"
	"github.com/Altinity/clickhouse-backup/v2/pkg/filesystemhelper"
	"github.com/Altinity/clickhouse-backup/v2/pkg/metadata"
	"github.com/Altinity/clickhouse-backup/v2/pkg/pidlock"
	"github.com/Altinity/clickhouse-backup/v2/pkg/status"
	"github.com/Altinity/clickhouse-backup/v2/pkg/utils"

	recursiveCopy "github.com/otiai10/copy"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

// livePart - active data part of a table from system.parts
type livePart struct {
	Name     string `ch:"name"`
	DiskName string `ch:"disk_name"`
	Path     string `ch:"path"`
}

// partMove - one planned part relocation inside the local backup shadow
type partMove struct {
	PartName string
	// SrcDisk - key in TableMetadata.Parts where the part is registered
	SrcDisk string
	// PhysicalSrcDisk - disk where the part files really are, differs from SrcDisk
	// when Part.RebalancedDisk redirect was set by a previous `download` rebalance
	PhysicalSrcDisk string
	DstDisk         string
	// HardlinkFromLive - hardlink from the live part directory (same filesystem as DstDisk)
	// instead of copying from the source shadow
	HardlinkFromLive bool
	LivePartPath     string
	Size             uint64
	Reason           string
}

// rebalanceDisksInfo - lookups derived from live system.disks, shared by all tables,
// effectiveFree is reduced while planning Rule 3 copies so several parts don't overbook one disk
type rebalanceDisksInfo struct {
	liveTypes     map[string]string   // live disk name -> disk type
	objectDisks   map[string]struct{} // live disks with object or encrypted-over-object type
	skipDisks     map[string]struct{} // live disks matched by clickhouse->skip_disks / skip_disk_types
	effectiveFree map[string]uint64   // max(0, unreserved_space - keep_free_space)
}

// Rebalance - move data parts of a local backup between disks, https://github.com/Altinity/clickhouse-backup/issues/1024
// Rule 1: the live part from system.parts is on the same disk as in the backup - do nothing;
// Rule 2: the live part is on another usable disk - hardlink it into the backup shadow on that disk;
// Rule 3: the part is not in system.parts and its backup disk is invalid (absent in system.disks or
// not in the table storage policy) - copy it to the least used policy disk of the same type;
// parts on object disks are skipped, metadata in table.json / metadata.json is rewritten accordingly
func (b *Backuper) Rebalance(backupName string, tablePattern string, dryRun bool, commandId int) error {
	backupName = utils.CleanBackupNameRE.ReplaceAllString(backupName, "")
	if backupName == "" {
		return errors.New("backup name must be defined")
	}
	if pidCheckErr := pidlock.CheckAndCreatePidFile(backupName, "rebalance"); pidCheckErr != nil {
		return pidCheckErr
	}
	defer pidlock.RemovePidFile(backupName)

	ctx, cancel, err := status.Current.GetContextWithCancel(commandId)
	if err != nil {
		return errors.Wrap(err, "status.Current.GetContextWithCancel")
	}
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()

	startRebalance := time.Now()
	if err = b.ch.Connect(); err != nil {
		return errors.Wrap(err, "can't connect to clickhouse")
	}
	defer b.ch.Close()

	disks, err := b.ch.GetDisks(ctx, true)
	if err != nil {
		return errors.Wrap(err, "b.ch.GetDisks")
	}
	if b.DefaultDataPath, err = b.ch.GetDefaultPath(disks); err != nil {
		return ErrUnknownClickhouseDataPath
	}
	diskMap := map[string]string{}
	for _, disk := range disks {
		diskMap[disk.Name] = disk.Path
	}
	b.DiskToPathMap = diskMap

	localBackup, disks, err := b.getLocalBackup(ctx, backupName, disks)
	if err != nil {
		return errors.Wrap(err, "getLocalBackup")
	}
	if localBackup.Broken != "" {
		return errors.Errorf("can't rebalance broken backup %s: %s", backupName, localBackup.Broken)
	}
	if strings.Contains(localBackup.Tags, "embedded") {
		return errors.Errorf("rebalance not supported for embedded backup %s", backupName)
	}

	info, err := b.buildRebalanceDisksInfo(ctx, disks)
	if err != nil {
		return err
	}

	metadataPath := path.Join(b.DefaultDataPath, "backup", backupName, "metadata")
	tableMetadataList, _, err := b.getTableListByPatternLocal(ctx, metadataPath, tablePattern, false, nil)
	if err != nil {
		return errors.Wrap(err, "getTableListByPatternLocal")
	}

	var hardlinkedParts, copiedParts, skippedParts int
	var movedBytes uint64
	dstDisksUsed := map[string]struct{}{}
	for _, tm := range tableMetadataList {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if len(tm.Parts) == 0 {
			continue
		}
		moves, planErr := b.planTableRebalance(ctx, tm, localBackup, disks, info, backupName)
		if planErr != nil {
			return planErr
		}
		if len(moves) == 0 {
			continue
		}
		tm.LocalFile = path.Join(metadataPath, common.TablePathEncode(tm.Database), common.TablePathEncode(tm.Table)+".json")
		dbAndTableDir := path.Join(common.TablePathEncode(tm.Database), common.TablePathEncode(tm.Table))
		tableChanged := false
		for _, move := range moves {
			action := "copy"
			if move.HardlinkFromLive {
				action = "hardlink"
			}
			logFields := map[string]interface{}{
				"backup": backupName, "operation": "rebalance",
				"table": fmt.Sprintf("`%s`.`%s`", tm.Database, tm.Table), "part": move.PartName,
				"src_disk": move.SrcDisk, "dst_disk": move.DstDisk,
				"action": action, "size": utils.FormatBytes(move.Size), "reason": move.Reason,
			}
			if dryRun {
				log.Info().Fields(logFields).Msg("would move part")
				if move.HardlinkFromLive {
					hardlinkedParts++
				} else {
					copiedParts++
				}
				movedBytes += move.Size
				continue
			}
			moved, moveErr := b.executePartMove(backupName, dbAndTableDir, localBackup, disks, move)
			if moveErr != nil {
				return moveErr
			}
			if !moved {
				skippedParts++
				continue
			}
			applyMoveToTableMetadata(tm, move)
			tableChanged = true
			dstDisksUsed[move.DstDisk] = struct{}{}
			if move.HardlinkFromLive {
				hardlinkedParts++
			} else {
				copiedParts++
			}
			movedBytes += move.Size
			log.Info().Fields(logFields).Msg("part moved")
		}
		if tableChanged {
			if _, saveErr := tm.Save(tm.LocalFile, false); saveErr != nil {
				return errors.Wrapf(saveErr, "can't save %s", tm.LocalFile)
			}
			if chownErr := filesystemhelper.Chown(tm.LocalFile, b.ch, disks, false); chownErr != nil {
				log.Warn().Msgf("can't chown %s error: %v", tm.LocalFile, chownErr)
			}
		}
	}
	if !dryRun && len(dstDisksUsed) > 0 {
		if localBackup.Disks == nil {
			localBackup.Disks = map[string]string{}
		}
		if localBackup.DiskTypes == nil {
			localBackup.DiskTypes = map[string]string{}
		}
		for d := range dstDisksUsed {
			localBackup.Disks[d] = b.DiskToPathMap[d]
			localBackup.DiskTypes[d] = info.liveTypes[d]
		}
		backupMetaFile := path.Join(b.DefaultDataPath, "backup", backupName, "metadata.json")
		if err = localBackup.BackupMetadata.Save(backupMetaFile); err != nil {
			return errors.Wrapf(err, "can't save %s", backupMetaFile)
		}
		if chownErr := filesystemhelper.Chown(backupMetaFile, b.ch, disks, false); chownErr != nil {
			log.Warn().Msgf("can't chown %s error: %v", backupMetaFile, chownErr)
		}
	}
	log.Info().Fields(map[string]interface{}{
		"backup":      backupName,
		"operation":   "rebalance",
		"dry_run":     dryRun,
		"hardlinked":  hardlinkedParts,
		"copied":      copiedParts,
		"skipped":     skippedParts,
		"moved_bytes": utils.FormatBytes(movedBytes),
		"duration":    utils.HumanizeDuration(time.Since(startRebalance)),
	}).Msg("done")
	return nil
}

// buildRebalanceDisksInfo - probe feature columns once and read effective free space from system.disks
func (b *Backuper) buildRebalanceDisksInfo(ctx context.Context, disks []clickhouse.Disk) (*rebalanceDisksInfo, error) {
	probe := make([]struct {
		HasDiskName   uint64 `ch:"has_disk_name"`
		HasUnreserved uint64 `ch:"has_unreserved_space"`
		HasKeepFree   uint64 `ch:"has_keep_free_space"`
	}, 0)
	probeQuery := "SELECT countIf(table='parts' AND name='disk_name') AS has_disk_name, " +
		"countIf(table='disks' AND name='unreserved_space') AS has_unreserved_space, " +
		"countIf(table='disks' AND name='keep_free_space') AS has_keep_free_space " +
		"FROM system.columns WHERE database='system' AND table IN ('parts','disks')"
	if err := b.ch.SelectContext(ctx, &probe, probeQuery); err != nil {
		return nil, errors.Wrap(err, "can't probe system.columns")
	}
	if len(probe) == 0 || probe[0].HasDiskName == 0 {
		return nil, errors.New("rebalance requires `system.parts.disk_name`, please upgrade ClickHouse to 19.15+")
	}
	unreservedExpr := "free_space"
	if probe[0].HasUnreserved > 0 {
		unreservedExpr = "unreserved_space"
	}
	keepFreeExpr := "toUInt64(0)"
	if probe[0].HasKeepFree > 0 {
		keepFreeExpr = "keep_free_space"
	}
	diskSpaces := make([]struct {
		Name       string `ch:"name"`
		Unreserved uint64 `ch:"unreserved_space"`
		KeepFree   uint64 `ch:"keep_free_space"`
	}, 0)
	spaceQuery := fmt.Sprintf("SELECT name, %s AS unreserved_space, %s AS keep_free_space FROM system.disks", unreservedExpr, keepFreeExpr)
	if err := b.ch.SelectContext(ctx, &diskSpaces, spaceQuery); err != nil {
		return nil, errors.Wrap(err, "can't get free space from system.disks")
	}
	info := &rebalanceDisksInfo{
		liveTypes:     map[string]string{},
		objectDisks:   map[string]struct{}{},
		skipDisks:     map[string]struct{}{},
		effectiveFree: map[string]uint64{},
	}
	for _, d := range disks {
		if d.IsBackup {
			continue
		}
		info.liveTypes[d.Name] = d.Type
		if b.isDiskTypeObject(d.Type) || b.isDiskTypeEncryptedObject(d, disks) {
			info.objectDisks[d.Name] = struct{}{}
		}
		if b.shouldSkipByDiskNameOrType(d) {
			info.skipDisks[d.Name] = struct{}{}
		}
	}
	for _, s := range diskSpaces {
		if _, exists := info.liveTypes[s.Name]; !exists {
			continue
		}
		if s.Unreserved > s.KeepFree {
			info.effectiveFree[s.Name] = s.Unreserved - s.KeepFree
		} else {
			info.effectiveFree[s.Name] = 0
		}
	}
	return info, nil
}

// planTableRebalance - collect live parts, resolve the table storage policy and compute part moves for one table
func (b *Backuper) planTableRebalance(ctx context.Context, tm *metadata.TableMetadata, localBackup *LocalBackup, disks []clickhouse.Disk, info *rebalanceDisksInfo, backupName string) ([]partMove, error) {
	liveParts, err := b.getLiveParts(ctx, tm.Database, tm.Table)
	if err != nil {
		return nil, err
	}
	policyName := b.ch.ExtractStoragePolicy(tm.Query)
	policyDisks := map[string]struct{}{}
	for _, d := range disks {
		if d.IsBackup {
			continue
		}
		for _, p := range d.StoragePolicies {
			if p == policyName {
				policyDisks[d.Name] = struct{}{}
				break
			}
		}
	}
	backupObjectDisks := map[string]struct{}{}
	for srcDisk := range tm.Parts {
		hasBlobs, objErr := b.partOnObjectDisk(srcDisk, localBackup.DiskTypes, disks)
		if objErr != nil {
			return nil, objErr
		}
		if hasBlobs {
			backupObjectDisks[srcDisk] = struct{}{}
		}
	}
	dbAndTableDir := path.Join(common.TablePathEncode(tm.Database), common.TablePathEncode(tm.Table))
	resolvePartSize := func(physicalSrcDisk, partName string) uint64 {
		srcDiskPath, exists := b.DiskToPathMap[physicalSrcDisk]
		if !exists {
			srcDiskPath = localBackup.Disks[physicalSrcDisk]
		}
		var size uint64
		partPath := path.Join(srcDiskPath, "backup", backupName, "shadow", dbAndTableDir, physicalSrcDisk, partName)
		if walkErr := filepath.Walk(partPath, func(_ string, fInfo os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if fInfo.Mode().IsRegular() {
				size += uint64(fInfo.Size())
			}
			return nil
		}); walkErr != nil {
			log.Warn().Msgf("can't calculate size of %s error: %v", partPath, walkErr)
		}
		return size
	}
	return computeTableRebalancePlan(tm, localBackup.DiskTypes, liveParts, policyName, policyDisks, backupObjectDisks, info, resolvePartSize)
}

// getLiveParts - active parts of the table from system.parts keyed by part name
func (b *Backuper) getLiveParts(ctx context.Context, database, table string) (map[string]livePart, error) {
	parts := make([]livePart, 0)
	query := "SELECT name, disk_name, path FROM system.parts WHERE active AND database=? AND table=?"
	if err := b.ch.SelectContext(ctx, &parts, query, database, table); err != nil {
		return nil, errors.Wrapf(err, "can't get active parts for `%s`.`%s` from system.parts", database, table)
	}
	result := make(map[string]livePart, len(parts))
	for _, p := range parts {
		result[p.Name] = p
	}
	return result, nil
}

// computeTableRebalancePlan - pure planning logic, mutates info.effectiveFree for Rule 3 allocations,
// resolvePartSize is called only for parts created before Part.Size was introduced (#1268)
func computeTableRebalancePlan(tm *metadata.TableMetadata, backupDiskTypes map[string]string, liveParts map[string]livePart, policyName string, policyDisks map[string]struct{}, backupObjectDisks map[string]struct{}, info *rebalanceDisksInfo, resolvePartSize func(physicalSrcDisk, partName string) uint64) ([]partMove, error) {
	srcDisks := make([]string, 0, len(tm.Parts))
	for srcDisk := range tm.Parts {
		srcDisks = append(srcDisks, srcDisk)
	}
	sort.Strings(srcDisks)
	var moves []partMove
	for _, srcDisk := range srcDisks {
		if _, isObject := backupObjectDisks[srcDisk]; isObject {
			continue
		}
		if _, skip := info.skipDisks[srcDisk]; skip {
			continue
		}
		for _, p := range tm.Parts[srcDisk] {
			if p.Required {
				continue
			}
			physicalSrcDisk := srcDisk
			if p.RebalancedDisk != "" {
				physicalSrcDisk = p.RebalancedDisk
			}
			live, isLive := liveParts[p.Name]
			// Rule 1 - the live part is already on the same disk
			if isLive && live.DiskName == physicalSrcDisk {
				continue
			}
			if isLive {
				_, dstObject := info.objectDisks[live.DiskName]
				_, dstSkip := info.skipDisks[live.DiskName]
				_, dstInPolicy := policyDisks[live.DiskName]
				if !dstObject && !dstSkip && dstInPolicy {
					// Rule 2 - align to system.parts, hardlink from the live part directory
					moves = append(moves, partMove{
						PartName: p.Name, SrcDisk: srcDisk, PhysicalSrcDisk: physicalSrcDisk,
						DstDisk: live.DiskName, HardlinkFromLive: true, LivePartPath: live.Path,
						Size: p.Size, Reason: "align_to_system_parts",
					})
					continue
				}
				// the live copy is unusable as a hardlink source, fall through to the Rule 3 validity check
			}
			_, srcLive := info.liveTypes[physicalSrcDisk]
			_, srcInPolicy := policyDisks[physicalSrcDisk]
			if srcLive && srcInPolicy {
				continue
			}
			// Rule 3 - the backup disk is invalid, copy to the least used policy disk of the same type
			size := p.Size
			if size == 0 {
				size = resolvePartSize(physicalSrcDisk, p.Name)
			}
			srcDiskType := backupDiskTypes[srcDisk]
			if srcDiskType == "" {
				srcDiskType = "local"
			}
			dstDisk, err := chooseRebalanceDstDisk(srcDiskType, size, policyDisks, info)
			if err != nil {
				return nil, errors.Wrapf(err, "can't rebalance `%s`.`%s` part %s from disk %s with storage policy %s", tm.Database, tm.Table, p.Name, srcDisk, policyName)
			}
			info.effectiveFree[dstDisk] -= size
			moves = append(moves, partMove{
				PartName: p.Name, SrcDisk: srcDisk, PhysicalSrcDisk: physicalSrcDisk,
				DstDisk: dstDisk, Size: size, Reason: "invalid_backup_disk",
			})
		}
	}
	return moves, nil
}

// chooseRebalanceDstDisk - the policy disk of the same type with maximum effective free space,
// ties are broken by name to keep planning deterministic
func chooseRebalanceDstDisk(diskType string, size uint64, policyDisks map[string]struct{}, info *rebalanceDisksInfo) (string, error) {
	bestDisk := ""
	var bestFree uint64
	for d := range policyDisks {
		if info.liveTypes[d] != diskType {
			continue
		}
		if _, isObject := info.objectDisks[d]; isObject {
			continue
		}
		if _, skip := info.skipDisks[d]; skip {
			continue
		}
		free := info.effectiveFree[d]
		if free < size {
			continue
		}
		if free > bestFree || (free == bestFree && (bestDisk == "" || d < bestDisk)) {
			bestDisk, bestFree = d, free
		}
	}
	if bestDisk == "" {
		return "", errors.Errorf("no disk with type `%s` and %s free space (unreserved_space - keep_free_space) found", diskType, utils.FormatBytes(size))
	}
	return bestDisk, nil
}

// executePartMove - physically move one part between disks inside the backup shadow,
// returns moved=false without an error when the live hardlink source disappeared (concurrent merge),
// the part stays intact on the source disk in that case
func (b *Backuper) executePartMove(backupName, dbAndTableDir string, localBackup *LocalBackup, disks []clickhouse.Disk, move partMove) (bool, error) {
	srcDiskPath, srcExists := b.DiskToPathMap[move.PhysicalSrcDisk]
	if !srcExists {
		srcDiskPath, srcExists = localBackup.Disks[move.PhysicalSrcDisk]
	}
	if !srcExists {
		return false, errors.Errorf("can't resolve path for disk %s, add it to `clickhouse->disk_mapping`", move.PhysicalSrcDisk)
	}
	srcShadow := path.Join(srcDiskPath, "backup", backupName, "shadow", dbAndTableDir, move.PhysicalSrcDisk, move.PartName)
	if _, statErr := os.Stat(srcShadow); statErr != nil {
		return false, errors.Wrapf(statErr, "can't stat source part path %s", srcShadow)
	}
	dstDiskPath, dstExists := b.DiskToPathMap[move.DstDisk]
	if !dstExists {
		return false, errors.Errorf("disk %s not found in system.disks", move.DstDisk)
	}
	dstShadow := path.Join(dstDiskPath, "backup", backupName, "shadow", dbAndTableDir, move.DstDisk, move.PartName)
	// leftover from an interrupted rebalance
	if _, statErr := os.Stat(dstShadow); statErr == nil {
		if removeErr := os.RemoveAll(dstShadow); removeErr != nil {
			return false, errors.Wrapf(removeErr, "can't remove incomplete %s", dstShadow)
		}
	}
	if move.HardlinkFromLive {
		if linkErr := b.makePartHardlinks(move.LivePartPath, dstShadow); linkErr != nil {
			// the live part could be merged away between the system.parts query and now,
			// the source shadow is intact, keep the part on the original disk
			log.Warn().Msgf("can't hardlink live part %s to %s, keep part on disk %s: %v", move.LivePartPath, dstShadow, move.SrcDisk, linkErr)
			if removeErr := os.RemoveAll(dstShadow); removeErr != nil {
				log.Warn().Msgf("can't remove incomplete %s error: %v", dstShadow, removeErr)
			}
			return false, nil
		}
	} else {
		if copyErr := recursiveCopy.Copy(srcShadow, dstShadow, recursiveCopy.Options{
			OnDirExists: func(src, dst string) recursiveCopy.DirExistsAction {
				return recursiveCopy.Replace
			},
		}); copyErr != nil {
			return false, errors.Wrapf(copyErr, "can't copy %s -> %s", srcShadow, dstShadow)
		}
	}
	if chownErr := filesystemhelper.Chown(dstShadow, b.ch, disks, true); chownErr != nil {
		log.Warn().Msgf("can't chown %s error: %v", dstShadow, chownErr)
	}
	if removeErr := os.RemoveAll(srcShadow); removeErr != nil {
		return false, errors.Wrapf(removeErr, "can't remove source part path %s", srcShadow)
	}
	// best-effort cleanup of the emptied per-disk shadow directory
	_ = os.Remove(path.Dir(srcShadow))
	return true, nil
}

// applyMoveToTableMetadata - move the part entry between Parts[disk] keys, keep per-disk sizes and
// archive file lists consistent, drop empty keys so `list` doesn't show dead disks
func applyMoveToTableMetadata(tm *metadata.TableMetadata, move partMove) {
	srcParts := tm.Parts[move.SrcDisk]
	for i, p := range srcParts {
		if p.Name == move.PartName {
			tm.Parts[move.SrcDisk] = append(srcParts[:i], srcParts[i+1:]...)
			p.RebalancedDisk = ""
			tm.Parts[move.DstDisk] = append(tm.Parts[move.DstDisk], p)
			break
		}
	}
	metadata.SortPartsByMinBlock(tm.Parts[move.DstDisk])
	if tm.Size == nil {
		tm.Size = map[string]int64{}
	}
	tm.Size[move.SrcDisk] -= int64(move.Size)
	tm.Size[move.DstDisk] += int64(move.Size)
	// Files exists only for backups downloaded in a compressed format, entries look like <disk>_<encodedPart>.<ext>
	if len(tm.Files[move.SrcDisk]) > 0 {
		archivePrefix := move.SrcDisk + "_" + common.TablePathEncode(move.PartName) + "."
		remainFiles := make([]string, 0, len(tm.Files[move.SrcDisk]))
		for _, f := range tm.Files[move.SrcDisk] {
			if strings.HasPrefix(f, archivePrefix) {
				tm.Files[move.DstDisk] = append(tm.Files[move.DstDisk], move.DstDisk+strings.TrimPrefix(f, move.SrcDisk))
			} else {
				remainFiles = append(remainFiles, f)
			}
		}
		tm.Files[move.SrcDisk] = remainFiles
	}
	if len(tm.Parts[move.SrcDisk]) == 0 {
		delete(tm.Parts, move.SrcDisk)
		delete(tm.Size, move.SrcDisk)
		if len(tm.Files[move.SrcDisk]) == 0 {
			delete(tm.Files, move.SrcDisk)
		}
	}
}
