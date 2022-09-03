package backup

func (b *Backuper) RestoreFromRemote(backupName, tablePattern string, databaseMapping, partitions []string, schemaOnly, dataOnly, dropTable, rbacOnly, configsOnly, resume bool) error {
	if err := b.Download(backupName, tablePattern, partitions, schemaOnly, resume); err != nil {
		return err
	}
	return Restore(b.cfg, backupName, tablePattern, databaseMapping, partitions, schemaOnly, dataOnly, dropTable, rbacOnly, configsOnly)
}
