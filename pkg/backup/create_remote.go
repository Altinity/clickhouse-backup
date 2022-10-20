package backup

import (
	"context"
	"fmt"
	"github.com/AlexAkulov/clickhouse-backup/pkg/status"
)

func (b *Backuper) CreateToRemote(backupName, diffFrom, diffFromRemote, tablePattern string, partitions []string, schemaOnly, rbac, backupConfig, resume bool, version string, commandId int) error {
	ctx, cancel, err := status.Current.GetContextWithCancel(commandId)
	if err != nil {
		return err
	}
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()
	if backupName == "" {
		backupName = NewBackupName()
	}
	if err := b.CreateBackup(backupName, tablePattern, partitions, schemaOnly, rbac, backupConfig, version, commandId); err != nil {
		return err
	}
	if err := b.Upload(backupName, diffFrom, diffFromRemote, tablePattern, partitions, schemaOnly, resume, commandId); err != nil {
		return err
	}

	if err := b.RemoveOldBackupsLocal(ctx, false, nil); err != nil {
		return fmt.Errorf("can't remove old local backups: %v", err)
	}
	return nil
}
