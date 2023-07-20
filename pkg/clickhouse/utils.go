package clickhouse

import (
	"strings"
)

func getDisksByPath(disks []Disk, dataPath string) []string {
	resultDisks := make([]Disk, 0)
	for _, disk := range disks {
		if strings.HasPrefix(dataPath, disk.Path) {
			if len(resultDisks) == 0 {
				resultDisks = append(resultDisks, disk)
			} else {
				if len(disk.Path) > len(resultDisks[len(resultDisks)-1].Path) {
					resultDisks[len(resultDisks)-1] = disk
				} else if disk.Name != resultDisks[len(resultDisks)-1].Name && len(disk.Path) == len(resultDisks[len(resultDisks)-1].Path) {
					resultDisks = append(resultDisks, disk)
				}
			}
		}
	}
	if len(resultDisks) == 0 {
		return []string{"default"}
	} else {
		result := make([]string, len(resultDisks))
		for i, disk := range resultDisks {
			result[i] = disk.Name
		}
		return result
	}
}

func GetDisksByPaths(disks []Disk, dataPaths []string) map[string]string {
	result := map[string]string{}
	for _, dataPath := range dataPaths {
		for _, disk := range getDisksByPath(disks, dataPath) {
			result[disk] = dataPath
		}
	}
	return result
}
