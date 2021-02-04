package backup

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"

	"github.com/AlexAkulov/clickhouse-backup/config"
	"github.com/AlexAkulov/clickhouse-backup/pkg/clickhouse"
	"github.com/AlexAkulov/clickhouse-backup/pkg/metadata"
	"github.com/AlexAkulov/clickhouse-backup/pkg/new_storage"

	"github.com/apex/log"
)

func Download(cfg config.Config, backupName string, tablePattern string, schemaOnly bool) error {
	if cfg.General.RemoteStorage == "none" {
		fmt.Println("Download aborted: RemoteStorage set to \"none\"")
		return nil
	}
	if backupName == "" {
		PrintRemoteBackups(cfg, "all")
		return fmt.Errorf("select backup for download")
	}

	ch := &clickhouse.ClickHouse{
		Config: &cfg.ClickHouse,
	}
	if err := ch.Connect(); err != nil {
		return fmt.Errorf("can't connect to clickhouse: %v", err)
	}
	defer ch.Close()
	defaultDataPath, err := ch.GetDefaultPath()
	if err != nil {
		return err
	}
	bd, err := new_storage.NewBackupDestination(cfg)
	if err != nil {
		return err
	}
	disks, err := ch.GetDisks()
	if err != nil {
		return err
	}
	diskMap := map[string]string{}
	for _, disk := range disks {
		diskMap[disk.Name] = disk.Path
	}
	if err := bd.Connect(); err != nil {
		return err
	}
	backupMetafileReader, err := bd.GetFileReader(path.Join(backupName, "metadata.json"))
	if err != nil {
		return err
	}
	tbBody, err := ioutil.ReadAll(backupMetafileReader)
	if err != nil {
		return err
	}
	var backupMetadata metadata.BackupMetadata
	if err := json.Unmarshal(tbBody, &backupMetadata); err != nil {
		return err
	}
	tablesForDownload := parseTablePatternForDownload(backupMetadata.Tables, tablePattern)

	for _, t := range tablesForDownload {
		remoteTableMetadata := path.Join(backupName, "metadata", clickhouse.TablePathEncode(t.Database), fmt.Sprintf("%s.json", clickhouse.TablePathEncode(t.Table)))
		tmReader, err := bd.GetFileReader(remoteTableMetadata)
		if err != nil {
			return err
		}
		tmBody, err := ioutil.ReadAll(tmReader)
		if err != nil {
			return err
		}
		var tableMetadata metadata.TableMetadata
		if err := json.Unmarshal(tmBody, &tableMetadata); err != nil {
			return err
		}
		// save metadata
		metadataBase := path.Join(defaultDataPath, "backup", backupName, "metadata", clickhouse.TablePathEncode(t.Database))
		if err := os.MkdirAll(metadataBase, 0750); err != nil {
			return err
		}
		metadataLocalFile := path.Join(metadataBase, fmt.Sprintf("%s.json", clickhouse.TablePathEncode(t.Table)))
		if err := ioutil.WriteFile(metadataLocalFile, tmBody, 0640); err != nil {
			return err
		}
		if schemaOnly {
			continue
		}
		// download data
		for disk := range tableMetadata.Files {
			uuid := path.Join(clickhouse.TablePathEncode(tableMetadata.Database), clickhouse.TablePathEncode(tableMetadata.Table))
			if tableMetadata.UUID != "" {
				uuid = path.Join(tableMetadata.UUID[0:3], tableMetadata.UUID)
			}
			tableLocalDir := path.Join(diskMap[disk], "backup", backupName, "shadow", uuid)

			for _, archiveFile := range tableMetadata.Files[disk] {
				tableRemoteFile := path.Join(backupName, "shadow", clickhouse.TablePathEncode(t.Database), clickhouse.TablePathEncode(t.Table), archiveFile)
				log.Infof("tableRemoteFile: %s", tableRemoteFile)
				if err := bd.CompressedStreamDownload(tableRemoteFile, tableLocalDir); err != nil {
					return err
				}
			}
		}
	}
	// TODO: merge with exists tables
	// backupMetadata.Tables = tablesForDownload
	backupMetafileLocalPath := path.Join(defaultDataPath, "backup", backupName, "metadata.json")
	if err := ioutil.WriteFile(backupMetafileLocalPath, tbBody, 0640); err != nil {
		return err
	}

	log.Info("  Done.")
	return nil
}
