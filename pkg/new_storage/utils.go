package new_storage

import (
	"fmt"
	"github.com/apex/log"
	"github.com/klauspost/compress/zstd"
	"github.com/mholt/archiver/v4"
	"sort"
	"strings"
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

func getArchiveWriter(format string, level int) (*archiver.CompressedArchive, error) {
	switch format {
	case "tar":
		return &archiver.CompressedArchive{Archival: archiver.Tar{}}, nil
	case "lz4":
		return &archiver.CompressedArchive{Compression: archiver.Lz4{CompressionLevel: level}, Archival: archiver.Tar{}}, nil
	case "bzip2", "bz2":
		return &archiver.CompressedArchive{Compression: archiver.Bz2{CompressionLevel: level}, Archival: archiver.Tar{}}, nil
	case "gzip", "gz":
		return &archiver.CompressedArchive{Compression: archiver.Gz{CompressionLevel: level, Multithreaded: true}, Archival: archiver.Tar{}}, nil
	case "sz":
		return &archiver.CompressedArchive{Compression: archiver.Sz{}, Archival: archiver.Tar{}}, nil
	case "xz":
		return &archiver.CompressedArchive{Compression: archiver.Xz{}, Archival: archiver.Tar{}}, nil
	case "br", "brotli":
		return &archiver.CompressedArchive{Compression: archiver.Brotli{Quality: level}, Archival: archiver.Tar{}}, nil
	case "zstd":
		return &archiver.CompressedArchive{Compression: archiver.Zstd{EncoderOptions: []zstd.EOption{zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(level))}}, Archival: archiver.Tar{}}, nil
	}
	return nil, fmt.Errorf("wrong compression_format: %s, supported: 'tar', 'lz4', 'bzip2', 'bz2', 'gzip', 'gz', 'sz', 'xz', 'br', 'brotli', 'zstd'", format)
}

func getArchiveReader(format string) (*archiver.CompressedArchive, error) {
	switch format {
	case "tar":
		return &archiver.CompressedArchive{Archival: archiver.Tar{}}, nil
	case "lz4":
		return &archiver.CompressedArchive{Compression: archiver.Lz4{}, Archival: archiver.Tar{}}, nil
	case "bzip2", "bz2":
		return &archiver.CompressedArchive{Compression: archiver.Bz2{}, Archival: archiver.Tar{}}, nil
	case "gzip", "gz":
		return &archiver.CompressedArchive{Compression: archiver.Gz{Multithreaded: true}, Archival: archiver.Tar{}}, nil
	case "sz":
		return &archiver.CompressedArchive{Compression: archiver.Sz{}, Archival: archiver.Tar{}}, nil
	case "xz":
		return &archiver.CompressedArchive{Compression: archiver.Xz{}, Archival: archiver.Tar{}}, nil
	case "br", "brotli":
		return &archiver.CompressedArchive{Compression: archiver.Brotli{}, Archival: archiver.Tar{}}, nil
	case "zstd":
		return &archiver.CompressedArchive{Compression: archiver.Zstd{}, Archival: archiver.Tar{}}, nil
	}
	return nil, fmt.Errorf("wrong compression_format: %s, supported: 'tar', 'lz4', 'bzip2', 'bz2', 'gzip', 'gz', 'sz', 'xz', 'br', 'brotli', 'zstd'", format)
}

func checkArchiveExtension(ext, format string) bool {
	if (format == "gz" || format == "gzip") && ext != ".gz" && ext != ".gzip" {
		return false
	}
	if (format == "bz2" || format == "bzip2") && ext != ".bz2" && ext != ".bzip2" {
		return false
	}
	if (format == "br" || format == "brotli") && ext != ".br" && ext != ".brotli" {
		return false
	}
	if strings.HasSuffix(ext, format) {
		return true
	}
	if (format == "gz" || format == "gzip") && (ext == ".gz" || ext == ".gzip") {
		return true
	}
	if (format == "bz2" || format == "bzip2") && (ext == ".bz2" || ext == ".bzip2") {
		return true
	}
	if (format == "br" || format == "brotli") && (ext == ".br" || ext == ".brotli") {
		return true
	}
	return false
}
