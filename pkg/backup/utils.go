package backup

import (
	"sort"
)

func GetBackupsToDeleteLocal(backups []LocalBackup, keep int) []LocalBackup {
	if len(backups) > keep {
		sort.SliceStable(backups, func(i, j int) bool {
			return backups[i].CreationDate.After(backups[j].CreationDate)
		})
		return backups[keep:]
	}
	return []LocalBackup{}
}
