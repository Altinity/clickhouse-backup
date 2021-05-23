package backup

func (b *Backuper) CreateToRemote(backupName, tablePattern, diffFrom string, schemaOnly bool, version string) error {
	if backupName == "" {
		backupName = NewBackupName()
	}
	if err := CreateBackup(b.cfg, backupName, tablePattern, schemaOnly, version); err != nil {
		return err
	}
	return b.Upload(backupName, tablePattern, diffFrom, schemaOnly)
}
