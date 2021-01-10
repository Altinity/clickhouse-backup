package clickhouse

import "strings"

func GetDiskByPath(disks []Disk, dataPath string) string {
	for _, disk := range disks {
		if strings.HasPrefix(dataPath, disk.Path) {
			return disk.Name
		}
	}
	return "unknown"
}

func GetDisksByPaths(disks []Disk, dataPaths []string) map[string]string {
	result := map[string]string{}
	for _, dataPath := range dataPaths {
		result[GetDiskByPath(disks, dataPath)] = dataPath
	}
	return result
}
