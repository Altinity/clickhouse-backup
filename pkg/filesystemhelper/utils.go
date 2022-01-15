package filesystemhelper

import (
	"strings"

	"github.com/AlexAkulov/clickhouse-backup/pkg/clickhouse"
)

func getDiskByPath(disks []clickhouse.Disk, dataPath string) string {
	resultDisk := clickhouse.Disk{}
	for _, disk := range disks {
		if strings.HasPrefix(dataPath, disk.Path) && len(disk.Path) > len(resultDisk.Path) {
			resultDisk = disk
		}
	}
	if resultDisk.Name == "" {
		return "unknown"
	} else {
		return resultDisk.Name
	}
}

func getDisksByPaths(disks []clickhouse.Disk, dataPaths []string) map[string]string {
	result := map[string]string{}
	for _, dataPath := range dataPaths {
		result[getDiskByPath(disks, dataPath)] = dataPath
	}
	return result
}
