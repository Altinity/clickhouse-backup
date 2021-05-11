package new_storage

import (
	"fmt"
	"sort"

	"github.com/mholt/archiver/v3"
)

func GetBackupsToDelete(backups []Backup, keep int) []Backup {
	if len(backups) > keep {
		sort.SliceStable(backups, func(i, j int) bool {
			return backups[i].CreationDate.After(backups[j].CreationDate)
		})
		return backups[keep:]
	}
	return []Backup{}
}

func getArchiveWriter(format string, level int) (archiver.Writer, error) {
	switch format {
	case "tar":
		return &archiver.Tar{}, nil
	case "lz4":
		return &archiver.TarLz4{CompressionLevel: level, Tar: archiver.NewTar()}, nil
	case "bzip2":
		return &archiver.TarBz2{CompressionLevel: level, Tar: archiver.NewTar()}, nil
	case "gzip":
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
	return nil, fmt.Errorf("wrong compression_format, supported: 'bzip2', 'gzip', 'sz', 'xz', 'br', 'zstd'")
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
	case "br", "brotli":
		return "tar.br"
	case "zstd":
		return "tar.zstd"
	}
	return ""
}

func getArchiveReader(format string) (archiver.Reader, error) {
	switch format {
	case "tar":
		return archiver.NewTar(), nil
	case "lz4":
		return archiver.NewTarLz4(), nil
	case "bzip2":
		return archiver.NewTarBz2(), nil
	case "gzip":
		return archiver.NewTarGz(), nil
	case "sz":
		return archiver.NewTarSz(), nil
	case "xz":
		return archiver.NewTarXz(), nil
	case "br", "brotli":
		return &archiver.TarBrotli{}, nil
	case "zstd":
		return &archiver.TarZstd{}, nil
	}
	return nil, fmt.Errorf("wrong compression_format, supported: 'tar', 'bzip2', 'gzip', 'sz', 'xz', 'br', 'zstd'")
}
