package metadata

import (
	"sort"
	"strconv"
	"strings"
)

type Part struct {
	Name           string `json:"name"`
	Required       bool   `json:"required,omitempty"`
	RebalancedDisk string `json:"rebalanced_disk,omitempty"`
	// Size - sum of part files sizes in bytes, filled during create,
	// used to calculate required free space before download, https://github.com/Altinity/clickhouse-backup/issues/1268
	Size uint64 `json:"size,omitempty"`
}

// SortPartsByMinBlock need to avoid wrong restore for Replacing, Collapsing, https://github.com/ClickHouse/ClickHouse/issues/71009
func SortPartsByMinBlock(parts []Part) {
	sort.Slice(parts, func(i, j int) bool {
		namePartsI := strings.SplitN(parts[i].Name, "_", 3)
		namePartsJ := strings.SplitN(parts[j].Name, "_", 3)
		// partitions different
		if namePartsI[0] != namePartsJ[0] {
			return namePartsI[0] < namePartsJ[0]
		}
		// partition same, min block
		minBlockI, _ := strconv.Atoi(namePartsI[1])
		minBlockJ, _ := strconv.Atoi(namePartsJ[1])
		return minBlockI < minBlockJ
	})
}
