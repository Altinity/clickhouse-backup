package backup

import (
	"context"
	"encoding/xml"
	"fmt"
	"github.com/Altinity/clickhouse-backup/pkg/clickhouse"
	"github.com/Altinity/clickhouse-backup/pkg/common"
	"github.com/Altinity/clickhouse-backup/pkg/config"
	"github.com/Altinity/clickhouse-backup/pkg/filesystemhelper"
	"github.com/Altinity/clickhouse-backup/pkg/metadata"
	"github.com/Altinity/clickhouse-backup/pkg/resumable"
	"github.com/Altinity/clickhouse-backup/pkg/storage"
	"github.com/Altinity/clickhouse-backup/pkg/storage/object_disk"
	apexLog "github.com/apex/log"
	recursiveCopy "github.com/otiai10/copy"
	"os"
	"path"
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

type embeddedXMLConfig struct {
	XMLName          xml.Name              `xml:"config"`
	Version          uint8                 `xml:"version"`
	DeduplicateFiles uint8                 `xml:"deduplicate_files"`
	Timestamp        string                `xml:"timestamp"`
	UUID             string                `xml:"uuid"`
	Contents         embeddedBackupContent `xml:"contents"`
}

type embeddedBackupContent struct {
	Files []embeddedBackupContentItem `xml:"file"`
}

type embeddedBackupContentItem struct {
	Name     string `xml:"name"`
	Size     uint64 `xml:"size"`
	Checksum string `xml:"checksum"`
	DataFile string `xml:"data_file,omitempty"`
}

// filterEmbeddedBackupConfig - add to remote key to .suffix, rewrite .backup, filter XML
func (b *Backuper) filterEmbeddedBackupConfig(ctx context.Context, backupName, embeddedBackupConfigFile, suffix string, tables ListOfTables, partitionsIdMap map[metadata.TableTitle]common.EmptyMap, disks []clickhouse.Disk) error {
	var err error
	var xmlContent []byte
	var hardLinks common.EmptyMap
	if xmlContent, err = object_disk.ReadFileContent(ctx, b.ch, b.cfg, b.cfg.ClickHouse.EmbeddedBackupDisk, embeddedBackupConfigFile); err != nil {
		return err
	}
	if xmlContent, hardLinks, err = b.filterEmbeddedXMLContent(xmlContent, tables, partitionsIdMap); err != nil {
		return err
	}
	filteredMetadata, err := object_disk.ReadMetadataFromFile(embeddedBackupConfigFile)
	filteredMetadata.TotalSize = int64(len(xmlContent))
	// expect only one remote storage object
	for i := range filteredMetadata.StorageObjects {
		filteredMetadata.StorageObjects[i].ObjectRelativePath = filteredMetadata.StorageObjects[i].ObjectRelativePath + suffix
		filteredMetadata.StorageObjects[i].ObjectSize = filteredMetadata.TotalSize
	}
	if err := object_disk.WriteMetadataToFile(filteredMetadata, embeddedBackupConfigFile); err != nil {
		return err
	}
	// write filtered XML to new Key
	if err = object_disk.WriteFileContent(ctx, b.ch, b.cfg, b.cfg.ClickHouse.EmbeddedBackupDisk, embeddedBackupConfigFile, xmlContent); err != nil {
		return err
	}
	// upload or download hardLinks
	for linkFile := range hardLinks {
		localFile := path.Join(path.Dir(embeddedBackupConfigFile), linkFile)
		linkFileParts := strings.Split(strings.Trim(linkFile, "/"), "/")
		remoteFile := path.Join(backupName, "shadow", linkFileParts[1], linkFileParts[2], b.cfg.ClickHouse.EmbeddedBackupDisk)
		remoteFileParts := []string{remoteFile}
		remoteFileParts = append(remoteFileParts, linkFileParts[3:]...)
		remoteFile = path.Join(remoteFileParts...)
		switch suffix {
		case ".download":
			if _, err = os.Stat(localFile); err == nil {
				break
			}
			apexLog.Infof("%s embedded hardlink reference %s", suffix, remoteFile)
			downloadDir := path.Dir(localFile)
			if err = os.MkdirAll(downloadDir, 0755); err != nil {
				return err
			}
			if err = filesystemhelper.Chown(downloadDir, b.ch, disks, false); err != nil {
				return err
			}
			if err = b.downloadSingleBackupFile(ctx, remoteFile, localFile, disks); err != nil {
				return err
			}
		case ".upload":
			if _, err = b.dst.StatFile(ctx, remoteFile); err == nil {
				break
			}
			apexLog.Infof("%s embedded hardlink reference %s", suffix, remoteFile)
			if err = b.uploadSingleBackupFile(ctx, localFile, remoteFile); err != nil {
				return err
			}
		}
	}
	return nil
}

func (b *Backuper) filterEmbeddedXMLContent(xmlContent []byte, tablesList ListOfTables, partitionsIdMap map[metadata.TableTitle]common.EmptyMap) ([]byte, common.EmptyMap, error) {
	var err error
	var xmlConfig embeddedXMLConfig
	hardLinks := make(map[string]string, 0)
	hardLinksUniq := make(common.EmptyMap, 0)
	if err = xml.Unmarshal(xmlContent, &xmlConfig); err != nil {
		return nil, nil, err
	}
	// we need hardlink on RESTORE, later files in `.backup` have data_file link to the earlier files
	for _, f := range xmlConfig.Contents.Files {
		if f.DataFile != "" {
			apexLog.Debugf("found hardlink %s->%s in embedded `.backup`", f.Name, f.DataFile)
			hardLinks[f.Name] = f.DataFile
		}
	}

	toBeFilterIdx := map[int]struct{}{}
	for _, t := range tablesList {
		for i := 0; i < len(xmlConfig.Contents.Files); i++ {
			f := xmlConfig.Contents.Files[i]
			dataPrefix := fmt.Sprintf("data/%s/%s", common.TablePathEncode(t.Database), common.TablePathEncode(t.Table))
			if idMap, exists := partitionsIdMap[metadata.TableTitle{Database: t.Database, Table: t.Table}]; exists && len(idMap) > 0 && strings.HasPrefix(f.Name, dataPrefix) {
				found := false
				filePartitionId := strings.Split(f.Name[len(dataPrefix)+1:], "_")[0]
				_, found = idMap[filePartitionId]
				// we need hardlinks during RESTORE
				if !found {
					for _, dst := range hardLinks {
						if dst == f.Name {
							found = true
							break
						}
					}
				}
				if !found {
					// also filter dataFile, if other hardLink is not present
					if dataFile, hardLinkExist := hardLinks[f.Name]; hardLinkExist {
						delete(hardLinks, f.Name)
						apexLog.Debugf("delete hardlink %s->%s in embedded `.backup`", f.Name, dataFile)
						otherLinks := false
						for _, dst := range hardLinks {
							if dst == dataFile {
								otherLinks = true
								apexLog.Debugf("found other hardlink %s->%s in embedded `.backup`", f.Name, dataFile)
							}
						}
						if !otherLinks {
							for j := 0; j < len(xmlConfig.Contents.Files); j++ {
								if xmlConfig.Contents.Files[j].Name == dataFile {
									toBeFilterIdx[j] = struct{}{}
								}
							}

						}
					}
					toBeFilterIdx[i] = struct{}{}
				}
			}
		}
	}
	if len(toBeFilterIdx) > 0 {
		toBeFilterLen := len(xmlConfig.Contents.Files) - len(toBeFilterIdx)
		filteredFiles := make([]embeddedBackupContentItem, toBeFilterLen)
		j := 0
		for i := 0; i < len(xmlConfig.Contents.Files); i++ {
			if _, isFiltered := toBeFilterIdx[i]; !isFiltered {
				filteredFiles[j] = xmlConfig.Contents.Files[i]
				j++
			}
		}
		if j < toBeFilterLen {
			apexLog.Fatalf("filtered %d expected %d", j, toBeFilterLen)
		}
		xmlConfig.Contents.Files = filteredFiles
		if xmlContent, err = xml.Marshal(xmlConfig); err != nil {
			return nil, nil, err
		}
	}

	for _, linkFile := range hardLinks {
		hardLinksUniq[linkFile] = struct{}{}
	}
	return xmlContent, hardLinksUniq, nil
}

func (b *Backuper) restoreEmbeddedBackupConfigToOrig(ctx context.Context, embeddedBackupConfig, suffix string, deleteFilteredContent bool) error {
	restoredMetadata, err := object_disk.ReadMetadataFromFile(embeddedBackupConfig)
	if err != nil {
		return err
	}
	totalSize := int64(0)
	for i := range restoredMetadata.StorageObjects {
		if !strings.HasSuffix(restoredMetadata.StorageObjects[i].ObjectRelativePath, suffix) {
			return fmt.Errorf("%s: remote path `%s` doens't contain `%s`", embeddedBackupConfig, restoredMetadata.StorageObjects[i].ObjectRelativePath, suffix)
		}
		restoredMetadata.StorageObjects[i].ObjectRelativePath = strings.TrimSuffix(restoredMetadata.StorageObjects[i].ObjectRelativePath, suffix)
		fileSize, err := object_disk.GetFileSize(ctx, b.ch, b.cfg, b.cfg.ClickHouse.EmbeddedBackupDisk, restoredMetadata.StorageObjects[i].ObjectRelativePath)
		if err != nil {
			return err
		}
		restoredMetadata.StorageObjects[i].ObjectSize = fileSize
		totalSize += fileSize
	}
	restoredMetadata.TotalSize = totalSize

	if !deleteFilteredContent {
		return object_disk.WriteMetadataToFile(restoredMetadata, embeddedBackupConfig)
	}

	filteredBackupConfig := embeddedBackupConfig + ".filtered"
	if err = recursiveCopy.Copy(embeddedBackupConfig, filteredBackupConfig); err != nil {
		return err
	}
	if err = object_disk.DeleteFileWithContent(ctx, b.ch, b.cfg, b.cfg.ClickHouse.EmbeddedBackupDisk, filteredBackupConfig); err != nil {
		return err
	}
	return object_disk.WriteMetadataToFile(restoredMetadata, embeddedBackupConfig)
}
