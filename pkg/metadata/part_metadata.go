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
}

// SortPartsByMinBlock need to avoid wrong restore for Replacing, Collapsing, https://github.com/ClickHouse/ClickHouse/issues/71009
func SortPartsByMinBlock(parts []Part) {
	sort.Slice(parts, func(i, j int) bool {
		namePartsI := strings.Split(parts[i].Name, "_")
		namePartsJ := strings.Split(parts[j].Name, "_")
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
