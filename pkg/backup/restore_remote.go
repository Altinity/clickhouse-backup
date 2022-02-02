package backup

func (b *Backuper) RestoreFromRemote(backupName string, tablePattern string, partitions []string, schemaOnly, dataOnly, dropTable, rbacOnly, configsOnly bool) error {
	if err := b.Download(backupName, tablePattern, partitions, schemaOnly); err != nil {
		return err
	}
	return Restore(b.cfg, backupName, tablePattern, partitions, schemaOnly, dataOnly, dropTable, rbacOnly, configsOnly)
}
