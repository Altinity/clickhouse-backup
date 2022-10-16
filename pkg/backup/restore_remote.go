package backup

func (b *Backuper) RestoreFromRemote(backupName, tablePattern string, databaseMapping, partitions []string, schemaOnly, dataOnly, dropTable, ignoreDependencies, rbacOnly, configsOnly, resume bool, commandId int) error {
	if err := b.Download(backupName, tablePattern, partitions, schemaOnly, resume, commandId); err != nil {
		return err
	}
	return b.Restore(backupName, tablePattern, databaseMapping, partitions, schemaOnly, dataOnly, dropTable, ignoreDependencies, rbacOnly, configsOnly, commandId)
}
