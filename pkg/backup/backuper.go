package backup

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path"
	"strings"

	"github.com/Altinity/clickhouse-backup/v2/pkg/clickhouse"
	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/Altinity/clickhouse-backup/v2/pkg/resumable"
	"github.com/Altinity/clickhouse-backup/v2/pkg/storage"

	apexLog "github.com/apex/log"
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
	log                    *apexLog.Entry
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
		Log:    apexLog.WithField("logger", "clickhouse"),
	}
	b := &Backuper{
		cfg:  cfg,
		ch:   ch,
		vers: ch,
		bs:   nil,
		log:  apexLog.WithField("logger", "backuper"),
	}
	for _, opt := range opts {
		opt(b)
	}
	return b
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

func (b *Backuper) init(ctx context.Context, disks []clickhouse.Disk, backupName string) error {
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
	b.DiskToPathMap = diskMap
	if b.cfg.General.RemoteStorage != "none" && b.cfg.General.RemoteStorage != "custom" {
		b.dst, err = storage.NewBackupDestination(ctx, b.cfg, b.ch, true, backupName)
		if err != nil {
			return err
		}
		if err := b.dst.Connect(ctx); err != nil {
			return fmt.Errorf("can't connect to %s: %v", b.dst.Kind(), err)
		}
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
	return diskType == "s3" || diskType == "azure_blob_storage"
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

func (b *Backuper) getEmbeddedBackupDestination(ctx context.Context, backupName string) (string, error) {
	if b.cfg.ClickHouse.EmbeddedBackupDisk != "" {
		return fmt.Sprintf("Disk('%s','%s')", b.cfg.ClickHouse.EmbeddedBackupDisk, backupName), nil
	}
	if b.cfg.General.RemoteStorage == "s3" {
		s3Endpoint, err := b.ch.ApplyMacros(ctx, b.buildS3DestinationEndpoint())
		if err != nil {
			return "", err
		}
		if b.cfg.S3.AssumeRoleARN != "" || (b.cfg.S3.AccessKey == "" && os.Getenv("AWS_ACCESS_KEY_ID") != "") {
			return fmt.Sprintf("S3('%s/%s')", s3Endpoint, backupName), nil
		}

	}
	if b.cfg.General.RemoteStorage == "gcs" {

	}
	if b.cfg.General.RemoteStorage == "azblob" {

	}
	return "", fmt.Errorf("empty clickhouse->embedded_backup_disk and invalid general->remote_storage: %s", b.cfg.General.RemoteStorage)
}

func (b *Backuper) buildS3DestinationEndpoint() string {
	url := url.URL{}
	url.Scheme = "https"
	if b.cfg.S3.DisableSSL {
		url.Scheme = "http"
	}
	url.Host = b.cfg.S3.Endpoint
	if url.Host == "" && b.cfg.S3.Region != "" && !b.cfg.S3.ForcePathStyle {
		url.Host = "s3." + b.cfg.S3.Region + ".amazonaws.com"
		url.Path = path.Join(b.cfg.S3.Bucket, b.cfg.S3.ObjectDiskPath)
	}
	if url.Host == "" && b.cfg.S3.Bucket != "" && b.cfg.S3.ForcePathStyle {
		url.Host = b.cfg.S3.Bucket + "." + "s3.amazonaws.com"
		url.Path = b.cfg.S3.ObjectDiskPath
	}
	return url.String()
}
