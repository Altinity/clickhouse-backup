package backup

import (
	"context"
	"encoding/xml"
	"fmt"
	"github.com/Altinity/clickhouse-backup/pkg/clickhouse"
	"github.com/Altinity/clickhouse-backup/pkg/common"
	"github.com/Altinity/clickhouse-backup/pkg/config"
	"github.com/Altinity/clickhouse-backup/pkg/metadata"
	"github.com/Altinity/clickhouse-backup/pkg/resumable"
	"github.com/Altinity/clickhouse-backup/pkg/storage"
	apexLog "github.com/apex/log"
	recursiveCopy "github.com/otiai10/copy"
	"google.golang.org/genproto/googleapis/cloud/paymentgateway/issuerswitch/v1"
	"io/ioutil"
	"path"
	"os"
	"strings"
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
		disks, err = b.ch.GetDisks(ctx)
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

type embeddedBackupConfig struct {
	Version uint8 `xml:"version"`
	DeduplicateFiles uint8 `xml:"deduplicate_files"`
	Timestamp string                     `xml:"timestamp"`
	UUID string `xml:"uuid"`
	Contents embeddedBackupContent `xml:"contents"`
}

type embeddedBackupContent struct {
	Files []embeddedBackupContentItem `xml:"file"`
}

type embeddedBackupContentItem struct {
	Name string `xml:"name"`
	Size uint64 `xml:"size"`
	Checksum string `xml:"checksum"`
	DataFile string `xml:"data_file,omitempty"`
}
func (b *Backuper) createEmbeddedBackupConfigForRestore(backupName string, metadataPath string, tablesForRestore ListOfTables, partitionsIdMap map[metadata.TableTitle]common.EmptyMap) error {
	embeddedBackupConfigFile := path.Join(metadataPath, backupName, ".backup")
	origEmbeddedBackupConfigFile := embeddedBackupConfigFile + ".orig"
	if err := recursiveCopy.Copy(embeddedBackupConfigFile, origEmbeddedBackupConfigFile); err != nil {
		return err
	}
	var embeddedBackup embeddedBackupConfig
	var err error
	var xmlContent []byte
	if xmlContent, err = os.ReadFile(embeddedBackupConfigFile); err != nil {
		return err
	}
	if err = xml.Unmarshal(xmlContent, &embeddedBackup); err != nil {
		return err
	}
	for _, t := range tablesForRestore {
		for _, f := range embeddedBackup.Contents.Files {
			if strings.HasPrefix(f.Name
			if idMap, exists := partitionsIdMap[metadata.TableTitle{Database: t.Database, Table: t.Table}]; exists && len(idMap) > 0 {

			}
		}
	}
	if xmlContent, err = xml.Marshal(embeddedBackup); err != nil {
		return err
	}
	if err = os.WriteFile(embeddedBackupConfigFile, xmlContent, 0644); err != nil {
		return err
	}

	return nil
}

func (b *Backuper) restoreEmbeddedBackupConfigForRestore(name string, metadataPath string, restore ListOfTables, idMap map[metadata.TableTitle]common.EmptyMap) error {
	embeddedBackupConfigFile := path.Join(metadataPath, ".backup")
	origEmbeddedBackupConfigFile := embeddedBackupConfigFile + ".orig"
	if err := recursiveCopy.Copy(embeddedBackupConfigFile, origEmbeddedBackupConfigFile); err != nil {
		return err
	}
	return nil
}
