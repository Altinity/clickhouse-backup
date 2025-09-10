package backup

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path"
	"regexp"
	"strings"

	"github.com/Altinity/clickhouse-backup/v2/pkg/common"
	"github.com/Altinity/clickhouse-backup/v2/pkg/metadata"
	"github.com/Altinity/clickhouse-backup/v2/pkg/utils"
	"github.com/eapache/go-resiliency/retrier"

	"github.com/Altinity/clickhouse-backup/v2/pkg/clickhouse"
	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/Altinity/clickhouse-backup/v2/pkg/resumable"
	"github.com/Altinity/clickhouse-backup/v2/pkg/storage"
	"github.com/rs/zerolog/log"
)

const DirectoryFormat = "directory"

var errShardOperationUnsupported = errors.New("sharded operations are not supported")

// versioner is an interface for determining the version of Clickhouse
type versioner interface {
	CanShardOperation(ctx context.Context) error
}

type BackuperOpt func(*Backuper)

type Backuper struct {
	cfg                    *config.Config
	ch                     *clickhouse.ClickHouse
	vers                   versioner
	bs                     backupSharder
	dst                    *storage.BackupDestination
	DiskToPathMap          map[string]string
	DefaultDataPath        string
	EmbeddedBackupDataPath string
	isEmbedded             bool
	resume                 bool
	resumableState         *resumable.State
}

func NewBackuper(cfg *config.Config, opts ...BackuperOpt) *Backuper {
	ch := &clickhouse.ClickHouse{
		Config: &cfg.ClickHouse,
	}
	b := &Backuper{
		cfg:  cfg,
		ch:   ch,
		vers: ch,
		bs:   nil,
	}
	for _, opt := range opts {
		opt(b)
	}
	return b
}

// Classify need to log retries
func (b *Backuper) Classify(err error) retrier.Action {
	if err == nil {
		return retrier.Succeed
	}
	log.Warn().Err(err).Msgf("Will wait near %s and retry", common.AddRandomJitter(b.cfg.General.RetriesDuration, b.cfg.General.RetriesJitter))
	return retrier.Retry
}

func WithVersioner(v versioner) BackuperOpt {
	return func(b *Backuper) {
		b.vers = v
	}
}

func WithBackupSharder(s backupSharder) BackuperOpt {
	return func(b *Backuper) {
		b.bs = s
	}
}

func (b *Backuper) initDisksPathsAndBackupDestination(ctx context.Context, disks []clickhouse.Disk, backupName string) error {
	var err error
	if disks == nil {
		disks, err = b.ch.GetDisks(ctx, true)
		if err != nil {
			return err
		}
	}
	b.DefaultDataPath, err = b.ch.GetDefaultPath(disks)
	if err != nil {
		return ErrUnknownClickhouseDataPath
	}
	diskMap := map[string]string{}
	for _, disk := range disks {
		diskMap[disk.Name] = disk.Path
		if b.cfg.ClickHouse.UseEmbeddedBackupRestore && (disk.IsBackup || disk.Name == b.cfg.ClickHouse.EmbeddedBackupDisk) {
			b.EmbeddedBackupDataPath = disk.Path
		}
	}
	if b.cfg.ClickHouse.UseEmbeddedBackupRestore && b.EmbeddedBackupDataPath == "" {
		b.EmbeddedBackupDataPath = b.DefaultDataPath
	}
	b.DiskToPathMap = diskMap
	if b.cfg.General.RemoteStorage != "none" && b.cfg.General.RemoteStorage != "custom" {
		if err = b.CalculateMaxSize(ctx); err != nil {
			return err
		}
		b.dst, err = storage.NewBackupDestination(ctx, b.cfg, b.ch, backupName)
		if err != nil {
			return err
		}
		if err := b.dst.Connect(ctx); err != nil {
			return fmt.Errorf("can't connect to %s: %v", b.dst.Kind(), err)
		}
	}
	return nil
}

// CalculateMaxSize https://github.com/Altinity/clickhouse-backup/issues/404
func (b *Backuper) CalculateMaxSize(ctx context.Context) error {
	maxFileSize, err := b.ch.CalculateMaxFileSize(ctx, b.cfg)
	if err != nil {
		return err
	}
	if b.cfg.General.MaxFileSize > 0 && b.cfg.General.MaxFileSize < maxFileSize {
		log.Warn().Msgf("MAX_FILE_SIZE=%d is less than actual %d, please remove general->max_file_size section from your config", b.cfg.General.MaxFileSize, maxFileSize)
	}
	if b.cfg.General.MaxFileSize <= 0 || b.cfg.General.MaxFileSize < maxFileSize {
		b.cfg.General.MaxFileSize = maxFileSize
	}
	return nil
}

func (b *Backuper) getLocalBackupDataPathForTable(backupName string, disk string, dbAndTablePath string) string {
	backupPath := path.Join(b.DiskToPathMap[disk], "backup", backupName, "shadow", dbAndTablePath, disk)
	if b.isEmbedded {
		backupPath = path.Join(b.DiskToPathMap[disk], backupName, "data", dbAndTablePath)
	}
	return backupPath
}

// populateBackupShardField populates the BackupShard field for a slice of Table structs
func (b *Backuper) populateBackupShardField(ctx context.Context, tables []clickhouse.Table) error {
	// By default, have all fields populated to full backup unless the table is to be skipped
	for i := range tables {
		tables[i].BackupType = clickhouse.ShardBackupFull
		if tables[i].Skip {
			tables[i].BackupType = clickhouse.ShardBackupNone
		}
	}
	if !doesShard(b.cfg.General.ShardedOperationMode) {
		return nil
	}
	if err := b.vers.CanShardOperation(ctx); err != nil {
		return err
	}

	if b.bs == nil {
		// Parse shard config here to avoid error return in NewBackuper
		shardFunc, err := shardFuncByName(b.cfg.General.ShardedOperationMode)
		if err != nil {
			return fmt.Errorf("could not determine shards for tables: %w", err)
		}
		b.bs = newReplicaDeterminer(b.ch, shardFunc)
	}
	assignment, err := b.bs.determineShards(ctx)
	if err != nil {
		return err
	}
	for i, t := range tables {
		if t.Skip {
			continue
		}
		fullBackup, err := assignment.inShard(t.Database, t.Name)
		if err != nil {
			return err
		}
		if !fullBackup {
			tables[i].BackupType = clickhouse.ShardBackupSchema
		}
	}
	return nil
}

func (b *Backuper) isDiskTypeObject(diskType string) bool {
	return diskType == "s3" || diskType == "azure_blob_storage" || diskType == "azure"
}

func (b *Backuper) isDiskTypeEncryptedObject(disk clickhouse.Disk, disks []clickhouse.Disk) bool {
	if disk.Type != "encrypted" {
		return false
	}
	underlyingIdx := -1
	underlyingPath := ""
	for i, d := range disks {
		if d.Name != disk.Name && strings.HasPrefix(disk.Path, d.Path) && b.isDiskTypeObject(d.Type) {
			if d.Path > underlyingPath {
				underlyingIdx = i
				underlyingPath = d.Path
			}
		}
	}
	return underlyingIdx >= 0
}

// getEmbeddedRestoreSettings - different with getEmbeddedBackupSettings, cause https://github.com/ClickHouse/ClickHouse/issues/69053
func (b *Backuper) getEmbeddedRestoreSettings(version int) []string {
	settings := make([]string, 0)
	if (b.cfg.General.RemoteStorage == "s3" || b.cfg.General.RemoteStorage == "gcs") && version >= 23007000 {
		settings = append(settings, "allow_s3_native_copy=1")
		if err := b.ch.Query("SET s3_request_timeout_ms=600000"); err != nil {
			log.Fatal().Msgf("SET s3_request_timeout_ms=600000 error: %v", err)
		}

	}
	if (b.cfg.General.RemoteStorage == "s3" || b.cfg.General.RemoteStorage == "gcs") && version >= 23011000 {
		if err := b.ch.Query("SET s3_use_adaptive_timeouts=0"); err != nil {
			log.Fatal().Msgf("SET s3_use_adaptive_timeouts=0 error: %v", err)
		}
	}
	return settings
}

func (b *Backuper) getEmbeddedBackupSettings(version int) []string {
	settings := make([]string, 0)
	if (b.cfg.General.RemoteStorage == "s3" || b.cfg.General.RemoteStorage == "gcs") && version >= 23007000 {
		settings = append(settings, "allow_s3_native_copy=1")
		if err := b.ch.Query("SET s3_request_timeout_ms=600000"); err != nil {
			log.Fatal().Msgf("SET s3_request_timeout_ms=600000 error: %v", err)
		}

	}
	if (b.cfg.General.RemoteStorage == "s3" || b.cfg.General.RemoteStorage == "gcs") && version >= 23011000 {
		if err := b.ch.Query("SET s3_use_adaptive_timeouts=0"); err != nil {
			log.Fatal().Msgf("SET s3_use_adaptive_timeouts=0 error: %v", err)
		}
	}
	if b.cfg.General.RemoteStorage == "azblob" && version >= 24005000 && b.cfg.ClickHouse.EmbeddedBackupDisk == "" {
		settings = append(settings, "allow_azure_native_copy=1")
	}
	return settings
}

func (b *Backuper) getEmbeddedBackupLocation(ctx context.Context, backupName string) (string, error) {
	if b.cfg.ClickHouse.EmbeddedBackupDisk != "" {
		return fmt.Sprintf("Disk('%s','%s')", b.cfg.ClickHouse.EmbeddedBackupDisk, backupName), nil
	}

	if err := b.applyMacrosToObjectDiskPath(ctx); err != nil {
		return "", err
	}
	if b.cfg.General.RemoteStorage == "s3" {
		s3Endpoint, err := b.ch.ApplyMacros(ctx, b.buildEmbeddedLocationS3())
		if err != nil {
			return "", err
		}
		if b.cfg.S3.AccessKey != "" {
			return fmt.Sprintf("S3('%s/%s/','%s','%s')", s3Endpoint, backupName, b.cfg.S3.AccessKey, b.cfg.S3.SecretKey), nil
		}
		if os.Getenv("AWS_ACCESS_KEY_ID") != "" {
			return fmt.Sprintf("S3('%s/%s/','%s','%s')", s3Endpoint, backupName, os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY")), nil
		}
		return "", fmt.Errorf("provide s3->access_key and s3->secret_key in config to allow embedded backup without `clickhouse->embedded_backup_disk`")
	}
	if b.cfg.General.RemoteStorage == "gcs" {
		gcsEndpoint, err := b.ch.ApplyMacros(ctx, b.buildEmbeddedLocationGCS())
		if err != nil {
			return "", err
		}
		if b.cfg.GCS.EmbeddedAccessKey != "" {
			return fmt.Sprintf("S3('%s/%s/','%s','%s')", gcsEndpoint, backupName, b.cfg.GCS.EmbeddedAccessKey, b.cfg.GCS.EmbeddedSecretKey), nil
		}
		if os.Getenv("AWS_ACCESS_KEY_ID") != "" {
			return fmt.Sprintf("S3('%s/%s/','%s','%s')", gcsEndpoint, backupName, os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY")), nil
		}
		return "", fmt.Errorf("provide gcs->embedded_access_key and gcs->embedded_secret_key in config to allow embedded backup without `clickhouse->embedded_backup_disk`")
	}
	if b.cfg.General.RemoteStorage == "azblob" {
		azblobEndpoint, err := b.ch.ApplyMacros(ctx, b.buildEmbeddedLocationAZBLOB())
		if err != nil {
			return "", err
		}
		if b.cfg.AzureBlob.Container != "" {
			return fmt.Sprintf("AzureBlobStorage('%s','%s','%s/%s/')", azblobEndpoint, b.cfg.AzureBlob.Container, b.cfg.AzureBlob.ObjectDiskPath, backupName), nil
		}
		return "", fmt.Errorf("provide azblob->container and azblob->account_name, azblob->account_key in config to allow embedded backup without `clickhouse->embedded_backup_disk`")
	}
	return "", fmt.Errorf("empty clickhouse->embedded_backup_disk and invalid general->remote_storage: %s", b.cfg.General.RemoteStorage)
}

func (b *Backuper) applyMacrosToObjectDiskPath(ctx context.Context) error {
	var err error
	if b.cfg.General.RemoteStorage == "s3" {
		b.cfg.S3.ObjectDiskPath, err = b.ch.ApplyMacros(ctx, b.cfg.S3.ObjectDiskPath)
	} else if b.cfg.General.RemoteStorage == "gcs" {
		b.cfg.GCS.ObjectDiskPath, err = b.ch.ApplyMacros(ctx, b.cfg.GCS.ObjectDiskPath)
	} else if b.cfg.General.RemoteStorage == "azblob" {
		b.cfg.AzureBlob.ObjectDiskPath, err = b.ch.ApplyMacros(ctx, b.cfg.AzureBlob.ObjectDiskPath)
	} else if b.cfg.General.RemoteStorage == "ftp" {
		b.cfg.FTP.ObjectDiskPath, err = b.ch.ApplyMacros(ctx, b.cfg.FTP.ObjectDiskPath)
	} else if b.cfg.General.RemoteStorage == "sftp" {
		b.cfg.SFTP.ObjectDiskPath, err = b.ch.ApplyMacros(ctx, b.cfg.SFTP.ObjectDiskPath)
	} else if b.cfg.General.RemoteStorage == "cos" {
		b.cfg.COS.ObjectDiskPath, err = b.ch.ApplyMacros(ctx, b.cfg.COS.ObjectDiskPath)
	}
	return err
}

func (b *Backuper) buildEmbeddedLocationS3() string {
	s3backupURL := url.URL{}
	s3backupURL.Scheme = "https"
	if strings.HasPrefix(b.cfg.S3.Endpoint, "http") {
		newUrl, _ := s3backupURL.Parse(b.cfg.S3.Endpoint)
		s3backupURL = *newUrl
		s3backupURL.Path = path.Join(b.cfg.S3.Bucket, b.cfg.S3.ObjectDiskPath)
	} else {
		s3backupURL.Host = b.cfg.S3.Endpoint
		s3backupURL.Path = path.Join(b.cfg.S3.Bucket, b.cfg.S3.ObjectDiskPath)
	}
	if b.cfg.S3.DisableSSL {
		s3backupURL.Scheme = "http"
	}
	if s3backupURL.Host == "" && b.cfg.S3.Region != "" && b.cfg.S3.ForcePathStyle {
		s3backupURL.Host = "s3." + b.cfg.S3.Region + ".amazonaws.com"
		s3backupURL.Path = path.Join(b.cfg.S3.Bucket, b.cfg.S3.ObjectDiskPath)
	}
	if s3backupURL.Host == "" && b.cfg.S3.Bucket != "" && !b.cfg.S3.ForcePathStyle {
		s3backupURL.Host = b.cfg.S3.Bucket + "." + "s3." + b.cfg.S3.Region + ".amazonaws.com"
		s3backupURL.Path = b.cfg.S3.ObjectDiskPath
	}
	return s3backupURL.String()
}

func (b *Backuper) buildEmbeddedLocationGCS() string {
	gcsBackupURL := url.URL{}
	gcsBackupURL.Scheme = "https"
	if b.cfg.GCS.ForceHttp {
		gcsBackupURL.Scheme = "http"
	}
	if b.cfg.GCS.Endpoint != "" {
		if !strings.HasPrefix(b.cfg.GCS.Endpoint, "http") {
			gcsBackupURL.Host = b.cfg.GCS.Endpoint
		} else {
			newUrl, _ := gcsBackupURL.Parse(b.cfg.GCS.Endpoint)
			gcsBackupURL = *newUrl
		}
	}
	if gcsBackupURL.Host == "" {
		gcsBackupURL.Host = "storage.googleapis.com"
	}
	gcsBackupURL.Path = path.Join(b.cfg.GCS.Bucket, b.cfg.GCS.ObjectDiskPath)
	return gcsBackupURL.String()
}

func (b *Backuper) buildEmbeddedLocationAZBLOB() string {
	azblobBackupURL := url.URL{}
	azblobBackupURL.Scheme = b.cfg.AzureBlob.EndpointSchema
	// https://github.com/Altinity/clickhouse-backup/issues/1031
	if b.cfg.AzureBlob.EndpointSuffix == "core.windows.net" {
		azblobBackupURL.Host = b.cfg.AzureBlob.AccountName + ".blob." + b.cfg.AzureBlob.EndpointSuffix
	} else {
		azblobBackupURL.Host = b.cfg.AzureBlob.EndpointSuffix
		azblobBackupURL.Path = b.cfg.AzureBlob.AccountName
	}
	return fmt.Sprintf("DefaultEndpointsProtocol=%s;AccountName=%s;AccountKey=%s;BlobEndpoint=%s;", b.cfg.AzureBlob.EndpointSchema, b.cfg.AzureBlob.AccountName, b.cfg.AzureBlob.AccountKey, azblobBackupURL.String())
}

func (b *Backuper) getObjectDiskPath() (string, error) {
	if b.cfg.General.RemoteStorage == "s3" {
		return b.cfg.S3.ObjectDiskPath, nil
	} else if b.cfg.General.RemoteStorage == "azblob" {
		return b.cfg.AzureBlob.ObjectDiskPath, nil
	} else if b.cfg.General.RemoteStorage == "gcs" {
		return b.cfg.GCS.ObjectDiskPath, nil
	} else if b.cfg.General.RemoteStorage == "cos" {
		return b.cfg.COS.ObjectDiskPath, nil
	} else if b.cfg.General.RemoteStorage == "ftp" {
		return b.cfg.FTP.ObjectDiskPath, nil
	} else if b.cfg.General.RemoteStorage == "sftp" {
		return b.cfg.SFTP.ObjectDiskPath, nil
	} else {
		return "", fmt.Errorf("cleanBackupObjectDisks: requesst object disks path but have unsupported remote_storage: %s", b.cfg.General.RemoteStorage)
	}
}

func (b *Backuper) getTablesDiffFromLocal(ctx context.Context, diffFrom string, tablePattern string) (tablesForUploadFromDiff map[metadata.TableTitle]metadata.TableMetadata, err error) {
	tablesForUploadFromDiff = make(map[metadata.TableTitle]metadata.TableMetadata)
	diffFromBackup, err := b.ReadBackupMetadataLocal(ctx, diffFrom)
	if err != nil {
		return nil, err
	}
	if len(diffFromBackup.Tables) != 0 {
		metadataPath := path.Join(b.DefaultDataPath, "backup", diffFrom, "metadata")
		// empty partitions, because we don't want filter
		diffTablesList, _, err := b.getTableListByPatternLocal(ctx, metadataPath, tablePattern, false, []string{})
		if err != nil {
			return nil, err
		}
		for _, t := range diffTablesList {
			tablesForUploadFromDiff[metadata.TableTitle{
				Database: t.Database,
				Table:    t.Table,
			}] = *t
		}
	}
	return tablesForUploadFromDiff, nil
}

func (b *Backuper) getTablesDiffFromRemote(ctx context.Context, diffFromRemote string, tablePattern string) (tablesForUploadFromDiff map[metadata.TableTitle]metadata.TableMetadata, err error) {
	tablesForUploadFromDiff = make(map[metadata.TableTitle]metadata.TableMetadata)
	backupList, err := b.dst.BackupList(ctx, true, diffFromRemote)
	if err != nil {
		return nil, fmt.Errorf("b.dst.BackupList return error: %v", err)
	}
	var diffRemoteMetadata *metadata.BackupMetadata
	for _, backup := range backupList {
		if backup.BackupName == diffFromRemote {
			diffRemoteMetadata = &backup.BackupMetadata
			break
		}
	}
	if diffRemoteMetadata == nil {
		return nil, fmt.Errorf("%s not found on remote storage", diffFromRemote)
	}

	if len(diffRemoteMetadata.Tables) != 0 {
		diffTablesList, tableListErr := getTableListByPatternRemote(ctx, b, diffRemoteMetadata, tablePattern, false)
		if tableListErr != nil {
			return nil, fmt.Errorf("getTableListByPatternRemote return error: %v", tableListErr)
		}
		for _, t := range diffTablesList {
			tablesForUploadFromDiff[metadata.TableTitle{
				Database: t.Database,
				Table:    t.Table,
			}] = *t
		}
	}
	return tablesForUploadFromDiff, nil
}

func (b *Backuper) GetLocalDataSize(ctx context.Context) (float64, error) {
	localDataSize := float64(0)
	err := b.ch.SelectSingleRow(ctx, &localDataSize, "SELECT value FROM system.asynchronous_metrics WHERE metric='TotalBytesOfMergeTreeTables'")
	return localDataSize, err
}

func (b *Backuper) GetStateDir() string {
	if b.isEmbedded && b.EmbeddedBackupDataPath != "" {
		return b.EmbeddedBackupDataPath
	}
	return b.DefaultDataPath
}

func (b *Backuper) adjustResumeFlag(resume bool) {
	if !resume && b.cfg.General.UseResumableState {
		resume = true
	}
	b.resume = resume
}

// CheckDisksUsage - https://github.com/Altinity/clickhouse-backup/issues/878
func (b *Backuper) CheckDisksUsage(backup storage.Backup, disks []clickhouse.Disk, isResumeExists bool, tablePattern string) error {
	if tablePattern != "" && tablePattern != "*.*" && tablePattern != "*" {
		return nil
	}
	freeSize := uint64(0)
	for _, d := range disks {
		freeSize += d.FreeSpace
	}
	if freeSize <= backup.CompressedSize || freeSize <= backup.DataSize {
		requiredSize := backup.CompressedSize
		if backup.DataSize > backup.CompressedSize {
			requiredSize = backup.DataSize
		}
		errMsg := fmt.Sprintf("%s requires %s free space, but total free space is %s", backup.BackupName, utils.FormatBytes(requiredSize), utils.FormatBytes(freeSize))
		if !isResumeExists {
			return errors.New(errMsg)
		}
		log.Warn().Msg(errMsg)
	}
	return nil
}

// filterPartsAndFilesByDisk - https://github.com/Altinity/clickhouse-backup/issues/908
func (b *Backuper) filterPartsAndFilesByDisk(tables ListOfTables, disks []clickhouse.Disk) {
	for i, table := range tables {
		//skipped table
		if table == nil {
			continue
		}
		filteredParts := make(map[string][]metadata.Part)
		for diskName := range tables[i].Parts {
			if b.shouldDiskNameSkipByNameOrType(diskName, disks) {
				log.Warn().Str("database", table.Database).Str("table", table.Table).Str("disk.Name", diskName).Msg("skipped")
				continue
			}
			filteredParts[diskName] = tables[i].Parts[diskName]
		}
		tables[i].Parts = filteredParts

		filteredFiles := make(map[string][]string)
		for diskName := range tables[i].Files {
			if b.shouldDiskNameSkipByNameOrType(diskName, disks) {
				log.Warn().Str("database", table.Database).Str("table", table.Table).Str("disk.Name", diskName).Msg("skipped")
				continue
			}
			filteredFiles[diskName] = tables[i].Files[diskName]
		}
		tables[i].Files = filteredFiles
	}
}

// https://github.com/Altinity/clickhouse-backup/issues/1127
var dbEngineRE = regexp.MustCompile(`(?m)ENGINE\s*=\s*(\w+\([^)]*\))`)

func (b *Backuper) prepareDatabaseEnginesMap(databases []metadata.DatabasesMeta) map[string]string {
	databaseEngines := make(map[string]string)
	for _, database := range databases {
		engine := database.Engine
		// old clickhouse versions doesn't contain engine_full
		if matches := dbEngineRE.FindAllStringSubmatch(database.Query, 1); matches != nil && len(matches) > 0 {
			engine = matches[0][1]
		}
		// https://github.com/Altinity/clickhouse-backup/issues/1146
		if targetDB, isMapped := b.cfg.General.RestoreDatabaseMapping[database.Name]; isMapped {
			databaseEngines[targetDB] = engine
		}
		databaseEngines[database.Name] = engine
	}
	return databaseEngines
}

func (b *Backuper) calculateChecksum(disk *clickhouse.Disk, partName string) (uint64, error) {
	checksumsFilePath := path.Join(disk.Path, partName, "checksums.txt")
	content, err := os.ReadFile(checksumsFilePath)
	if err != nil {
		return 0, fmt.Errorf("could not read %s: %w", checksumsFilePath, err)
	}

	hash := sha256.Sum256(content)
	return binary.LittleEndian.Uint64(hash[:8]), nil
}
