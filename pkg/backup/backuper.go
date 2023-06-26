package backup

import (
	"context"
	"fmt"
	"github.com/Altinity/clickhouse-backup/pkg/clickhouse"
	"github.com/Altinity/clickhouse-backup/pkg/config"
	"github.com/Altinity/clickhouse-backup/pkg/resumable"
	"github.com/Altinity/clickhouse-backup/pkg/storage"
	apexLog "github.com/apex/log"
	"path"
)

type Backuper struct {
	cfg                    *config.Config
	ch                     *clickhouse.ClickHouse
	dst                    *storage.BackupDestination
	log                    *apexLog.Entry
	DiskToPathMap          map[string]string
	DefaultDataPath        string
	EmbeddedBackupDataPath string
	isEmbedded             bool
	resume                 bool
	resumableState         *resumable.State
}

func NewBackuper(cfg *config.Config) *Backuper {
	ch := &clickhouse.ClickHouse{
		Config: &cfg.ClickHouse,
		Log:    apexLog.WithField("logger", "clickhouse"),
	}
	return &Backuper{
		cfg: cfg,
		ch:  ch,
		log: apexLog.WithField("logger", "backuper"),
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
