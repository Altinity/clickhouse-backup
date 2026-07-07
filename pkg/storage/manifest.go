package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/common"
	"github.com/Altinity/clickhouse-backup/v2/pkg/storage/bwlimit"
	"github.com/eapache/go-resiliency/retrier"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

const (
	// ManifestFileName is the name of the file manifest stored alongside metadata.json.
	// It lists every file in the backup with its relative path, size, and last-modified time,
	// allowing restore operations to skip the expensive S3 ListObjects Walk.
	ManifestFileName = "manifest.json"
	// ManifestVersion is the current manifest format version.
	ManifestVersion = 1
)

// BackupManifest is a listing of all files within a single backup.
// It is written during upload and read during download to avoid
// the expensive recursive Walk (ListObjects) on object storage.
type BackupManifest struct {
	Version    int             `json:"version"`
	BackupName string          `json:"backup_name"`
	CreatedAt  time.Time       `json:"created_at"`
	TotalSize  int64           `json:"total_size"`
	TotalFiles int             `json:"total_files"`
	Files      []ManifestEntry `json:"files"`
}

// ManifestEntry represents a single file in the backup manifest.
type ManifestEntry struct {
	Path         string    `json:"path"`
	Size         int64     `json:"size"`
	LastModified time.Time `json:"last_modified"`
}

// manifestFile implements RemoteFile interface so manifest entries
// can be used directly in download paths that expect RemoteFile.
type manifestFile struct {
	name         string
	size         int64
	lastModified time.Time
}

func (mf *manifestFile) Name() string            { return mf.name }
func (mf *manifestFile) Size() int64             { return mf.size }
func (mf *manifestFile) LastModified() time.Time { return mf.lastModified }

// ManifestEntryToRemoteFile converts a ManifestEntry to a RemoteFile,
// adjusting the path relative to the given prefix.
func ManifestEntryToRemoteFile(entry ManifestEntry, prefix string) RemoteFile {
	name := entry.Path
	if prefix != "" {
		name = strings.TrimPrefix(name, prefix)
		name = strings.TrimPrefix(name, "/")
	}
	return &manifestFile{
		name:         name,
		size:         entry.Size,
		lastModified: entry.LastModified,
	}
}

// NewBackupManifest creates a new empty manifest for the given backup.
func NewBackupManifest(backupName string) *BackupManifest {
	return &BackupManifest{
		Version:    ManifestVersion,
		BackupName: backupName,
		CreatedAt:  time.Now().UTC(),
		Files:      make([]ManifestEntry, 0, 256),
	}
}

// NewBackupManifestWithCapacity creates a new empty manifest with a pre-allocated
// file slice capacity, reducing GC pressure for large backups by avoiding repeated
// slice growth. Use this when the expected file count is known or can be estimated.
func NewBackupManifestWithCapacity(backupName string, expectedFiles int) *BackupManifest {
	if expectedFiles < 256 {
		expectedFiles = 256
	}
	return &BackupManifest{
		Version:    ManifestVersion,
		BackupName: backupName,
		CreatedAt:  time.Now().UTC(),
		Files:      make([]ManifestEntry, 0, expectedFiles),
	}
}

// AddFile adds a file entry to the manifest.
func (m *BackupManifest) AddFile(relativePath string, size int64, lastModified time.Time) {
	m.Files = append(m.Files, ManifestEntry{
		Path:         relativePath,
		Size:         size,
		LastModified: lastModified,
	})
	m.TotalFiles = len(m.Files)
	m.TotalSize += size
}

// HasFile returns true if the manifest contains an entry with the exact given path.
func (m *BackupManifest) HasFile(relativePath string) bool {
	for _, f := range m.Files {
		if f.Path == relativePath {
			return true
		}
	}
	return false
}

// FilesUnderPrefix returns all manifest entries whose path starts with the given prefix.
// The prefix should NOT include the backup name (e.g., "shadow/default/my_table/default/part1").
func (m *BackupManifest) FilesUnderPrefix(prefix string) []ManifestEntry {
	prefix = strings.TrimSuffix(prefix, "/") + "/"
	var result []ManifestEntry
	for _, f := range m.Files {
		if strings.HasPrefix(f.Path, prefix) {
			result = append(result, f)
		}
	}
	return result
}

// Marshal serializes the manifest to JSON.
func (m *BackupManifest) Marshal() ([]byte, error) {
	return json.MarshalIndent(m, "", "\t")
}

// UnmarshalManifest deserializes a manifest from JSON.
func UnmarshalManifest(data []byte) (*BackupManifest, error) {
	var m BackupManifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

// UploadManifest uploads the manifest to remote storage alongside metadata.json.
func (bd *BackupDestination) UploadManifest(ctx context.Context, backupName string, manifest *BackupManifest) error {
	data, err := manifest.Marshal()
	if err != nil {
		return err
	}
	remotePath := path.Join(backupName, ManifestFileName)
	return bd.PutFile(ctx, remotePath, io.NopCloser(bytes.NewReader(data)), 0)
}

// DownloadManifest attempts to download and parse the manifest for a given backup.
// Returns nil, nil if the manifest does not exist (graceful fallback to Walk).
func (bd *BackupDestination) DownloadManifest(ctx context.Context, backupName string) (*BackupManifest, error) {
	remotePath := path.Join(backupName, ManifestFileName)
	r, err := bd.GetFileReader(ctx, remotePath)
	if err != nil {
		// Manifest doesn't exist — this is expected for older backups
		log.Debug().Str("backup", backupName).Msg("no manifest.json found, will fall back to Walk")
		return nil, nil
	}
	defer r.Close()
	data, err := io.ReadAll(r)
	if err != nil {
		log.Warn().Str("backup", backupName).Err(err).Msg("failed to read manifest.json, will fall back to Walk")
		return nil, nil
	}
	manifest, err := UnmarshalManifest(data)
	if err != nil {
		log.Warn().Str("backup", backupName).Err(err).Msg("failed to parse manifest.json, will fall back to Walk")
		return nil, nil
	}
	log.Info().Str("backup", backupName).Int("total_files", manifest.TotalFiles).
		Int64("total_size", manifest.TotalSize).Msg("loaded backup manifest, skipping Walk")
	return manifest, nil
}

// DownloadPathWithManifest downloads files from remotePath using a pre-loaded manifest
// instead of calling Walk. Only files from manifestFiles are downloaded.
// prefixInManifest should be relative to the backup root (e.g.,
// "shadow/default/my_table/default/part1").
func (bd *BackupDestination) DownloadPathWithManifest(ctx context.Context, remotePath string, localPath string, manifestFiles []ManifestEntry, prefixInManifest string, RetriesOnFailure int, RetriesDuration time.Duration, RetriesJitter int8, RetrierClassifier retrier.Classifier, maxSpeed uint64) (int64, error) {
	downloadedBytes := int64(0)
	limiter := bd.DownloadLimiter(maxSpeed)

	for _, entry := range manifestFiles {
		// Compute the relative name within remotePath
		relativeName := strings.TrimPrefix(entry.Path, prefixInManifest)
		relativeName = strings.TrimPrefix(relativeName, "/")
		if relativeName == "" {
			continue
		}
		f := ManifestEntryToRemoteFile(entry, prefixInManifest)
		retry := retrier.New(retrier.ExponentialBackoff(RetriesOnFailure, common.AddRandomJitter(RetriesDuration, RetriesJitter)), RetrierClassifier)
		err := retry.RunCtx(ctx, func(ctx context.Context) error {
			r, err := bd.GetFileReader(ctx, path.Join(remotePath, f.Name()))
			if err != nil {
				log.Error().Err(err).Send()
				return errors.WithMessage(err, "DownloadPathWithManifest GetFileReader")
			}
			r = bwlimit.ReadCloser(ctx, r, limiter)
			var closeSrcOnce sync.Once
			var srcCloseErr error
			closeSrc := func() { closeSrcOnce.Do(func() { srcCloseErr = r.Close() }) }
			// A stalled read is not interruptible by context alone: copyWithBuffer
			// blocks in Read and never re-checks ctx, so /backup/kill cancels the
			// context but the copy keeps running. Force-close the source reader on
			// cancellation so the blocked read returns and the copy unwinds.
			watchDone := make(chan struct{})
			go func() {
				select {
				case <-ctx.Done():
					closeSrc()
				case <-watchDone:
				}
			}()
			defer close(watchDone)
			dstFilePath := path.Join(localPath, f.Name())
			dstDirPath, _ := path.Split(dstFilePath)
			if err := os.MkdirAll(dstDirPath, 0750); err != nil {
				log.Error().Err(err).Send()
				return errors.WithMessage(err, "DownloadPathWithManifest MkdirAll")
			}
			dst, err := os.Create(dstFilePath)
			if err != nil {
				log.Error().Err(err).Send()
				return errors.WithMessage(err, "DownloadPathWithManifest Create")
			}
			if copyBytes, copyErr := bd.copyWithBuffer(dst, r); copyErr != nil {
				log.Error().Err(copyErr).Send()
				return errors.WithMessage(copyErr, "DownloadPathWithManifest io.Copy")
			} else {
				downloadedBytes += copyBytes
			}
			if dstCloseErr := dst.Close(); dstCloseErr != nil {
				log.Error().Err(dstCloseErr).Send()
				return errors.WithMessage(dstCloseErr, "DownloadPathWithManifest dst.Close")
			}
			if closeSrc(); srcCloseErr != nil {
				log.Error().Err(srcCloseErr).Send()
				return errors.WithMessage(srcCloseErr, "DownloadPathWithManifest r.Close")
			}
			return nil
		})
		if err != nil {
			return downloadedBytes, errors.WithMessage(err, "DownloadPathWithManifest retry")
		}
	}
	return downloadedBytes, nil
}
