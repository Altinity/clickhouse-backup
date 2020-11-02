package config

import (
	"fmt"

	"github.com/mholt/archiver"
)

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
	}
	return nil, fmt.Errorf("wrong compression_format, supported: 'lz4', 'bzip2', 'gzip', 'sz', 'xz'")
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
	}
	return nil, fmt.Errorf("wrong compression_format, supported: 'tar', 'lz4', 'bzip2', 'gzip', 'sz', 'xz'")
}
