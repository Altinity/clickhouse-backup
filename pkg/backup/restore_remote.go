package backup

func (b *Backuper) RestoreFromRemote(backupName string, tablePattern string, schemaOnly, dataOnly, dropTable, rbacOnly, configsOnly, skipRBAC, skipConfigs bool) error {
	if err := b.Download(backupName, tablePattern, schemaOnly); err != nil {
		return err
	}
	return Restore(b.cfg, backupName, tablePattern, schemaOnly, dataOnly, dropTable, rbacOnly, configsOnly, skipRBAC, skipConfigs)
}
