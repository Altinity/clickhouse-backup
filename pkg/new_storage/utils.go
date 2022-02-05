package new_storage

import (
	"fmt"
	"sort"

	"github.com/mholt/archiver/v3"
)

func GetBackupsToDelete(backups []Backup, keep int) []Backup {
	if len(backups) > keep {
		sort.SliceStable(backups, func(i, j int) bool {
			return backups[i].UploadDate.After(backups[j].UploadDate)
		})
		// KeepRemoteBackups should respect incremental backups, fix https://github.com/AlexAkulov/clickhouse-backup/issues/111
		deletedBackup := backups[keep:]
		for _, b := range backups[:keep] {
			if b.RequiredBackup != "" {
				for i := range deletedBackup {
					if b.RequiredBackup == deletedBackup[i].BackupName {
						deletedBackup = append(deletedBackup[:i], deletedBackup[i+1:]...)
						break
					}
				}
			}
		}
		return deletedBackup
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
