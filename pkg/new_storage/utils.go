package new_storage

import (
	"fmt"
	"github.com/apex/log"
	"github.com/mholt/archiver/v3"
	"sort"
	"time"
)

func GetBackupsToDelete(backups []Backup, keep int) []Backup {
	if len(backups) > keep {
		sort.SliceStable(backups, func(i, j int) bool {
			return backups[i].UploadDate.After(backups[j].UploadDate)
		})
		// KeepRemoteBackups should respect incremental backups and don't delete required backups
		// fix https://github.com/AlexAkulov/clickhouse-backup/issues/111
		// fix https://github.com/AlexAkulov/clickhouse-backup/issues/385
		deletedBackups := make([]Backup, len(backups)-keep)
		copied := copy(deletedBackups, backups[keep:])
		if copied != len(backups)-keep {
			log.Warnf("copied wrong items from backup list expected=%d, actual=%d", len(backups)-keep, copied)
		}
		for _, b := range backups {
			if b.RequiredBackup != "" {
				for i, deletedBackup := range deletedBackups {
					if b.RequiredBackup == deletedBackup.BackupName {
						deletedBackups = append(deletedBackups[:i], deletedBackups[i+1:]...)
						break
					}
				}
			}
		}
		// remove from old backup list backup with UploadDate `0001-01-01 00:00:00`, to avoid race condition for multiple shards copy
		// fix https://github.com/AlexAkulov/clickhouse-backup/issues/409
		i := 0
		for _, b := range deletedBackups {
			if b.UploadDate != time.Date(1, time.January, 1, 0, 0, 0, 0, time.UTC) {
				deletedBackups[i] = b
				i++
			}
		}
		deletedBackups = deletedBackups[:i]
		return deletedBackups
	}
	return []Backup{}
}

func getArchiveWriter(format string, level int) (archiver.Writer, error) {
	switch format {
	case "tar":
		return &archiver.Tar{}, nil
	case "lz4":
		return &archiver.TarLz4{CompressionLevel: level, Tar: archiver.NewTar()}, nil
	case "bzip2", "bz2":
		return &archiver.TarBz2{CompressionLevel: level, Tar: archiver.NewTar()}, nil
	case "gzip", "gz":
		return &archiver.TarGz{CompressionLevel: level, Tar: archiver.NewTar()}, nil
	case "sz":
		return &archiver.TarSz{Tar: archiver.NewTar()}, nil
	case "xz":
		return &archiver.TarXz{Tar: archiver.NewTar()}, nil
	case "br", "brotli":
		return &archiver.TarBrotli{Quality: level, Tar: archiver.NewTar()}, nil
	case "zstd":
		return &archiver.TarZstd{Tar: archiver.NewTar()}, nil
	}
	return nil, fmt.Errorf("wrong compression_format: %s, supported: 'tar', 'lz4', 'bzip2', 'bz2', 'gzip', 'gz', 'sz', 'xz', 'br', 'brotli', 'zstd'", format)
}

func getArchiveReader(format string) (archiver.Reader, error) {
	switch format {
	case "tar":
		return archiver.NewTar(), nil
	case "lz4":
		return archiver.NewTarLz4(), nil
	case "bzip2", "bz2":
		return archiver.NewTarBz2(), nil
	case "gzip", "gz":
		return archiver.NewTarGz(), nil
	case "sz":
		return archiver.NewTarSz(), nil
	case "xz":
		return archiver.NewTarXz(), nil
	case "br", "brotli":
		return archiver.NewTarBrotli(), nil
	case "zstd":
		return archiver.NewTarZstd(), nil
	}
	return nil, fmt.Errorf("wrong compression_format: %s, supported: 'tar', 'lz4', 'bzip2', 'bz2', 'gzip', 'gz', 'sz', 'xz', 'br', 'brotli', 'zstd'", format)
}
