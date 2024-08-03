package metadata

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"
)

type BackupMetadata struct {
	BackupName              string            `json:"backup_name"`
	Disks                   map[string]string `json:"disks"`      // "default": "/var/lib/clickhouse"
	DiskTypes               map[string]string `json:"disk_types"` // "default": "local"
	ClickhouseBackupVersion string            `json:"version"`
	CreationDate            time.Time         `json:"creation_date"`
	Tags                    string            `json:"tags,omitempty"` // "regular,embedded"
	ClickHouseVersion       string            `json:"clickhouse_version,omitempty"`
	DataSize                uint64            `json:"data_size,omitempty"`
	ObjectDiskSize          uint64            `json:"object_disk_size,omitempty"`
	MetadataSize            uint64            `json:"metadata_size"`
	RBACSize                uint64            `json:"rbac_size,omitempty"`
	ConfigSize              uint64            `json:"config_size,omitempty"`
	CompressedSize          uint64            `json:"compressed_size,omitempty"`
	Databases               []DatabasesMeta   `json:"databases,omitempty"`
	Tables                  []TableTitle      `json:"tables"`
	Functions               []FunctionsMeta   `json:"functions"`
	DataFormat              string            `json:"data_format"`
	RequiredBackup          string            `json:"required_backup,omitempty"`
}

func (b *BackupMetadata) GetFullSize() uint64 {
	size := b.MetadataSize + b.ConfigSize + b.RBACSize
	if strings.Contains(b.Tags, "embedded") {
		size += b.DataSize + b.CompressedSize
	} else {
		if b.CompressedSize > 0 {
			size += b.CompressedSize + b.ObjectDiskSize
		} else {
			size += b.DataSize + b.ObjectDiskSize
		}
	}
	return size
}

func (b *BackupMetadata) Save(location string) error {
	tbBody, err := json.MarshalIndent(b, "", "\t")
	if err != nil {
		return fmt.Errorf("can't marshall backup metadata: %v", err)
	}
	if err := os.WriteFile(location, tbBody, 0640); err != nil {
		return fmt.Errorf("can't save backup metadata: %v", err)
	}
	return nil
}
