package clickhouse

import (
	"strings"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

// getDisksByPath - return the names of the disks whose path is the longest prefix of dataPath,
// matched is false when no disk path matches at all and the `default` fallback was applied
func getDisksByPath(disks []Disk, dataPath string) (names []string, matched bool) {
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
		return []string{"default"}, false
	}
	result := make([]string, len(resultDisks))
	for i, disk := range resultDisks {
		result[i] = disk.Name
	}
	return result, true
}

// GetDisksByPaths - map each disk name to the table data path which lives on it.
// A data path matching no disk path is still attributed to `default` (disk_mapping cross-cluster setups
// rely on it), but such a fallback never overwrites a data path which really matched, and two different
// data paths really matching the same disk is a misconfiguration we refuse instead of silently restoring
// parts into the wrong disk, fix https://github.com/Altinity/clickhouse-backup/issues/1121
func GetDisksByPaths(disks []Disk, dataPaths []string) (map[string]string, error) {
	result := map[string]string{}
	fallback := map[string]bool{}
	for _, dataPath := range dataPaths {
		names, matched := getDisksByPath(disks, dataPath)
		for _, disk := range names {
			prev, exists := result[disk]
			if exists && !matched {
				continue
			}
			if exists && matched && !fallback[disk] && prev != dataPath {
				return nil, errors.Errorf("data paths `%s` and `%s` both resolve to disk `%s`, disks=%v", prev, dataPath, disk, disks)
			}
			if !matched {
				log.Warn().Msgf("data path `%s` is not under any disk path, assume disk `default`", dataPath)
			}
			result[disk], fallback[disk] = dataPath, !matched
		}
	}
	return result, nil
}
