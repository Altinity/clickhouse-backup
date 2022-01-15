package backup

import (
	"sort"
)

func GetBackupsToDelete(backups []BackupLocal, keep int) []BackupLocal {
	if len(backups) > keep {
		sort.SliceStable(backups, func(i, j int) bool {
			return backups[i].CreationDate.After(backups[j].CreationDate)
		})
		return backups[keep:]
	}
	return []BackupLocal{}
}
