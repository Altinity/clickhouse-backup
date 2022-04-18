package storage

import (
	"fmt"
	"sort"

	"github.com/mholt/archiver/v4"
)

func GetBackupsToDelete(backups []Backup, keep int) []Backup {
	if len(backups) > keep {
		sort.SliceStable(backups, func(i, j int) bool {
			return backups[i].Date.After(backups[j].Date)
		})
		return backups[keep:]
	}
	return []Backup{}
}

func getArchiveWriter(format string, level int) (*archiver.CompressedArchive, error) {
	switch format {
	case "tar":
		return &archiver.CompressedArchive{Archival: archiver.Tar{}}, nil
	case "lz4":
		return &archiver.CompressedArchive{Compression: archiver.Lz4{CompressionLevel: level}, Archival: archiver.Tar{}}, nil
	case "bzip2":
		return &archiver.CompressedArchive{Compression: archiver.Bz2{CompressionLevel: level}, Archival: archiver.Tar{}}, nil
	case "gzip":
		return &archiver.CompressedArchive{Compression: archiver.Gz{CompressionLevel: level, Multithreaded: true}, Archival: archiver.Tar{}}, nil
	case "sz":
		return &archiver.CompressedArchive{Compression: archiver.Sz{}, Archival: archiver.Tar{}}, nil
	case "xz":
		return &archiver.CompressedArchive{Compression: archiver.Xz{}, Archival: archiver.Tar{}}, nil
	}
	return nil, fmt.Errorf("wrong compression_format: %s, supported: 'tar', 'lz4', 'bzip2', 'gzip', 'sz', 'xz'", format)
}

func getExtension(format string) string {
	switch format {
	case "tar":
		return "tar"
	case "lz4":
		return "tar.lz4"
	case "bzip2":
		return "tar.bz2"
	case "gzip":
		return "tar.gz"
	case "sz":
		return "tar.sz"
	case "xz":
		return "tar.xz"
	}
	return ""
}

func getArchiveReader(format string) (*archiver.CompressedArchive, error) {
	switch format {
	case "tar":
		return &archiver.CompressedArchive{Archival: archiver.Tar{}}, nil
	case "lz4":
		return &archiver.CompressedArchive{Compression: archiver.Lz4{}, Archival: archiver.Tar{}}, nil
	case "bzip2":
		return &archiver.CompressedArchive{Compression: archiver.Bz2{}, Archival: archiver.Tar{}}, nil
	case "gzip":
		return &archiver.CompressedArchive{Compression: archiver.Gz{Multithreaded: true}, Archival: archiver.Tar{}}, nil
	case "sz":
		return &archiver.CompressedArchive{Compression: archiver.Sz{}, Archival: archiver.Tar{}}, nil
	case "xz":
		return &archiver.CompressedArchive{Compression: archiver.Xz{}, Archival: archiver.Tar{}}, nil
	}
	return nil, fmt.Errorf("wrong compression_format: %s, supported: 'tar', 'lz4', 'bzip2', 'gzip', 'sz', 'xz'", format)
}
