package storage

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zstd"
	"github.com/klauspost/pgzip"
	"github.com/mholt/archives"
	"github.com/rs/zerolog/log"
)

func GetBackupsToDeleteRemote(backups []Backup, keep int) []Backup {
	if len(backups) > keep {
		// sort backup ascending
		sort.SliceStable(backups, func(i, j int) bool {
			return backups[i].UploadDate.After(backups[j].UploadDate)
		})
		// KeepRemoteBackups should respect incremental backups sequences and don't deleteKey required backups
		// fix https://github.com/Altinity/clickhouse-backup/issues/111
		// fix https://github.com/Altinity/clickhouse-backup/issues/385
		// fix https://github.com/Altinity/clickhouse-backup/issues/525
		deletedBackups := make([]Backup, len(backups)-keep)
		copied := copy(deletedBackups, backups[keep:])
		if copied != len(backups)-keep {
			log.Warn().Msgf("copied wrong items from backup list expected=%d, actual=%d", len(backups)-keep, copied)
		}
		keepBackups := make([]Backup, keep)
		copied = copy(keepBackups, backups[:keep])
		if copied != keep {
			log.Warn().Msgf("copied wrong items from backup list expected=%d, actual=%d", keep, copied)
		}
		var findRequiredBackup func(b Backup)
		findRequiredBackup = func(b Backup) {
			if b.RequiredBackup != "" {
				for i, deletedBackup := range deletedBackups {
					if b.RequiredBackup == deletedBackup.BackupName {
						deletedBackups = append(deletedBackups[:i], deletedBackups[i+1:]...)
						findRequiredBackup(deletedBackup)
						break
					}
				}
			}
		}
		for _, b := range keepBackups {
			findRequiredBackup(b)
		}
		// remove from old backup list backup with UploadDate `0001-01-01 00:00:00`, to avoid race condition for multiple shards copy
		// fix https://github.com/Altinity/clickhouse-backup/issues/409
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

// pgzipDefaultBlockSize mirrors pgzip's unexported defaultBlockSize (1MB); used as the pgzip block
// size when compression_buffer_size is 0 so the configured thread count still applies.
const pgzipDefaultBlockSize = 1 << 20

func getArchiveWriter(format string, level int, useMultiThread bool, threads, bufferSize int) (*archives.CompressedArchive, error) {
	switch format {
	case "tar":
		return &archives.CompressedArchive{Archival: archives.Tar{}, Extraction: archives.Tar{}}, nil
	case "lz4":
		return &archives.CompressedArchive{Compression: archives.Lz4{CompressionLevel: level}, Archival: archives.Tar{}, Extraction: archives.Tar{}}, nil
	case "bzip2", "bz2":
		return &archives.CompressedArchive{Compression: archives.Bz2{CompressionLevel: level}, Archival: archives.Tar{}, Extraction: archives.Tar{}}, nil
	case "gzip", "gz":
		return &archives.CompressedArchive{Compression: gzipCompression{level: level, multiThread: useMultiThread, threads: threads, bufferSize: bufferSize}, Archival: archives.Tar{}, Extraction: archives.Tar{}}, nil
	case "sz":
		return &archives.CompressedArchive{Compression: archives.Sz{}, Archival: archives.Tar{}, Extraction: archives.Tar{}}, nil
	case "xz":
		return &archives.CompressedArchive{Compression: archives.Xz{}, Archival: archives.Tar{}, Extraction: archives.Tar{}}, nil
	case "br", "brotli":
		return &archives.CompressedArchive{Compression: archives.Brotli{Quality: level}, Archival: archives.Tar{}, Extraction: archives.Tar{}}, nil
	case "zstd":
		encoderOptions := []zstd.EOption{
			zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(level)),
			zstd.WithEncoderConcurrency(threads),
		}
		if bufferSize > 0 {
			encoderOptions = append(encoderOptions, zstd.WithWindowSize(bufferSize))
		}
		return &archives.CompressedArchive{Compression: archives.Zstd{EncoderOptions: encoderOptions}, Archival: archives.Tar{}, Extraction: archives.Tar{}}, nil
	}
	return nil, fmt.Errorf("wrong compression_format: %s, supported: 'tar', 'lz4', 'bzip2', 'bz2', 'gzip', 'gz', 'sz', 'xz', 'br', 'brotli', 'zstd'", format)
}

func getArchiveReader(format string, useMultiThread bool, threads, bufferSize int) (*archives.CompressedArchive, error) {
	switch format {
	case "tar":
		return &archives.CompressedArchive{Extraction: archives.Tar{}}, nil
	case "lz4":
		return &archives.CompressedArchive{Compression: archives.Lz4{}, Extraction: archives.Tar{}}, nil
	case "bzip2", "bz2":
		return &archives.CompressedArchive{Compression: archives.Bz2{}, Extraction: archives.Tar{}}, nil
	case "gzip", "gz":
		return &archives.CompressedArchive{Compression: gzipCompression{multiThread: useMultiThread, threads: threads, bufferSize: bufferSize}, Extraction: archives.Tar{}}, nil
	case "sz":
		return &archives.CompressedArchive{Compression: archives.Sz{}, Extraction: archives.Tar{}}, nil
	case "xz":
		return &archives.CompressedArchive{Compression: archives.Xz{}, Extraction: archives.Tar{}}, nil
	case "br", "brotli":
		return &archives.CompressedArchive{Compression: archives.Brotli{}, Extraction: archives.Tar{}}, nil
	case "zstd":
		return &archives.CompressedArchive{Compression: archives.Zstd{DecoderOptions: []zstd.DOption{zstd.WithDecoderConcurrency(threads)}}, Extraction: archives.Tar{}}, nil
	}
	return nil, fmt.Errorf("wrong compression_format: %s, supported: 'tar', 'lz4', 'bzip2', 'bz2', 'gzip', 'gz', 'sz', 'xz', 'br', 'brotli', 'zstd'", format)
}

// gzipCompression is an archives.Compression for gzip that exposes the gzip/pgzip tuning knobs
// the bundled archives.Gz hides: the DEFLATE window size (single-threaded, via gzip.NewWriterWindow)
// or the pgzip block size and worker count (multi-threaded). archives.Gz only toggles pgzip on/off
// and always uses the library defaults, so we implement the format directly, see
// https://github.com/Altinity/clickhouse-backup/issues/1378
type gzipCompression struct {
	level       int
	multiThread bool
	// threads is the pgzip block worker count in multi-threaded mode (>=1).
	threads int
	// bufferSize is the DEFLATE window in single-threaded mode (32..32768) or the pgzip block size
	// in multi-threaded mode (>16384); 0 keeps the library defaults.
	bufferSize int
}

func (gzipCompression) Extension() string { return ".gz" }
func (gzipCompression) MediaType() string { return "application/gzip" }

func (gzipCompression) Match(ctx context.Context, filename string, stream io.Reader) (archives.MatchResult, error) {
	return archives.Gz{}.Match(ctx, filename, stream)
}

func (gz gzipCompression) OpenWriter(w io.Writer) (io.WriteCloser, error) {
	if !gz.multiThread {
		if gz.bufferSize > 0 {
			return gzip.NewWriterWindow(w, gz.bufferSize)
		}
		level := gz.level
		if level == 0 {
			level = gzip.DefaultCompression
		}
		return gzip.NewWriterLevel(w, level)
	}
	level := gz.level
	if level == 0 {
		level = gzip.DefaultCompression
	}
	zw, err := pgzip.NewWriterLevel(w, level)
	if err != nil {
		return nil, err
	}
	blockSize := gz.bufferSize
	if blockSize <= 0 {
		blockSize = pgzipDefaultBlockSize
	}
	if err = zw.SetConcurrency(blockSize, gz.threads); err != nil {
		return nil, err
	}
	return zw, nil
}

func (gz gzipCompression) OpenReader(r io.Reader) (io.ReadCloser, error) {
	if !gz.multiThread {
		return gzip.NewReader(r)
	}
	blockSize := gz.bufferSize
	if blockSize <= 0 {
		blockSize = pgzipDefaultBlockSize
	}
	return pgzip.NewReaderN(r, blockSize, gz.threads)
}

func checkArchiveExtension(ext, format string) bool {
	if strings.HasSuffix(ext, format) {
		return true
	}
	if format == "gz" || format == "gzip" {
		return ext == ".gz" || ext == ".gzip"
	}
	if format == "bz2" || format == "bzip2" {
		return ext == ".bz2" || ext == ".bzip2"
	}

	if format == "br" || format == "brotli" {
		return ext == ".br" || ext == ".brotli"
	}
	return false
}
