package backup

func (b *Backuper) RestoreFromRemote(backupName, tablePattern, databaseMapping string, partitions []string, schemaOnly, dataOnly, dropTable, rbacOnly, configsOnly bool) error {
	if err := b.Download(backupName, tablePattern, partitions, schemaOnly); err != nil {
		return err
	}
	return Restore(b.cfg, backupName, tablePattern, databaseMapping, partitions, schemaOnly, dataOnly, dropTable, rbacOnly, configsOnly)
}
