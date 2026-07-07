package storage

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
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
	bolt "go.etcd.io/bbolt"
)

const (
	// ManifestFileName is the name of the file manifest stored alongside metadata.json.
	// It is a gzip-compressed bbolt database whose keys are the relative paths of every
	// file in the backup, allowing restore operations to skip the expensive S3
	// ListObjects Walk. bbolt keeps keys sorted, so per-part lookups are prefix seeks
	// instead of full scans, and the read side is mmap-backed instead of loading the
	// whole listing into the heap.
	ManifestFileName = "manifest.bolt.gz"
	// ManifestVersion is the current manifest format version.
	ManifestVersion = 1
	// manifestBatchSize is how many recorded files are buffered in memory
	// before being flushed to the local bolt database in one transaction.
	manifestBatchSize = 10000
)

var (
	manifestFilesBucket = []byte("files")
	manifestMetaBucket  = []byte("meta")
)

// ManifestWriter incrementally records uploaded file paths into a local bbolt
// database, batching writes to bound memory usage. It is safe for concurrent
// use. After a write error it becomes a no-op and the error is returned by
// Finalize, so a broken manifest never fails the backup itself.
type ManifestWriter struct {
	mu         sync.Mutex
	db         *bolt.DB
	localPath  string
	gzPath     string
	backupName string
	createdAt  time.Time
	batch      []string
	err        error
	TotalFiles int
}

// NewManifestWriter creates a manifest writer backed by a temporary local bolt file.
func NewManifestWriter(backupName string) (*ManifestWriter, error) {
	tmpFile, err := os.CreateTemp("", "clickhouse-backup-manifest-*.bolt")
	if err != nil {
		return nil, errors.WithMessage(err, "manifest: can't create temporary file")
	}
	localPath := tmpFile.Name()
	if err = tmpFile.Close(); err != nil {
		_ = os.Remove(localPath)
		return nil, errors.WithMessage(err, "manifest: can't close temporary file")
	}
	db, err := bolt.Open(localPath, 0600, nil)
	if err != nil {
		_ = os.Remove(localPath)
		return nil, errors.WithMessage(err, "manifest: can't open bolt database")
	}
	err = db.Update(func(tx *bolt.Tx) error {
		if _, createErr := tx.CreateBucketIfNotExists(manifestFilesBucket); createErr != nil {
			return createErr
		}
		_, createErr := tx.CreateBucketIfNotExists(manifestMetaBucket)
		return createErr
	})
	if err != nil {
		_ = db.Close()
		_ = os.Remove(localPath)
		return nil, errors.WithMessage(err, "manifest: can't create buckets")
	}
	return &ManifestWriter{
		db:         db,
		localPath:  localPath,
		backupName: backupName,
		createdAt:  time.Now().UTC(),
	}, nil
}

// AddFile records a file path in the manifest. On flush failure the writer is
// marked broken and subsequent calls become no-ops; the error surfaces in Finalize.
func (w *ManifestWriter) AddFile(relativePath string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil || w.db == nil {
		return
	}
	w.batch = append(w.batch, relativePath)
	w.TotalFiles++
	if len(w.batch) >= manifestBatchSize {
		if flushErr := w.flushLocked(); flushErr != nil {
			w.err = flushErr
			log.Warn().Err(flushErr).Msgf("manifest: flush failed, %s will not be uploaded", ManifestFileName)
		}
	}
}

func (w *ManifestWriter) flushLocked() error {
	if len(w.batch) == 0 {
		return nil
	}
	err := w.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(manifestFilesBucket)
		for _, p := range w.batch {
			if putErr := b.Put([]byte(p), []byte{}); putErr != nil {
				return putErr
			}
		}
		return nil
	})
	if err == nil {
		w.batch = w.batch[:0]
	}
	return err
}

// Finalize flushes pending entries, writes the manifest metadata, closes the
// bolt database and gzip-compresses it. It returns the path of the compressed file.
func (w *ManifestWriter) Finalize() (string, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil {
		return "", w.err
	}
	if w.db == nil {
		return "", errors.New("manifest: writer already finalized")
	}
	if flushErr := w.flushLocked(); flushErr != nil {
		w.err = flushErr
		return "", flushErr
	}
	metaErr := w.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(manifestMetaBucket)
		buf := make([]byte, binary.MaxVarintLen64)
		n := binary.PutVarint(buf, int64(ManifestVersion))
		if err := b.Put([]byte("version"), append([]byte(nil), buf[:n]...)); err != nil {
			return err
		}
		if err := b.Put([]byte("backup_name"), []byte(w.backupName)); err != nil {
			return err
		}
		if err := b.Put([]byte("created_at"), []byte(w.createdAt.Format(time.RFC3339))); err != nil {
			return err
		}
		n = binary.PutVarint(buf, int64(w.TotalFiles))
		return b.Put([]byte("total_files"), append([]byte(nil), buf[:n]...))
	})
	if metaErr != nil {
		w.err = metaErr
		return "", metaErr
	}
	if closeErr := w.db.Close(); closeErr != nil {
		w.err = closeErr
		return "", closeErr
	}
	w.db = nil
	gzPath, gzErr := gzipFile(w.localPath)
	if gzErr != nil {
		w.err = gzErr
		return "", gzErr
	}
	w.gzPath = gzPath
	return gzPath, nil
}

func gzipFile(srcPath string) (string, error) {
	src, err := os.Open(srcPath)
	if err != nil {
		return "", errors.WithMessage(err, "manifest: can't open bolt file for compression")
	}
	defer src.Close()
	gzPath := srcPath + ".gz"
	dst, err := os.Create(gzPath)
	if err != nil {
		return "", errors.WithMessage(err, "manifest: can't create gzip file")
	}
	gz := gzip.NewWriter(dst)
	if _, err = io.Copy(gz, src); err != nil {
		_ = dst.Close()
		_ = os.Remove(gzPath)
		return "", errors.WithMessage(err, "manifest: can't compress bolt file")
	}
	if err = gz.Close(); err != nil {
		_ = dst.Close()
		_ = os.Remove(gzPath)
		return "", errors.WithMessage(err, "manifest: can't finish compression")
	}
	if err = dst.Close(); err != nil {
		_ = os.Remove(gzPath)
		return "", errors.WithMessage(err, "manifest: can't close gzip file")
	}
	return gzPath, nil
}

// Close releases the writer's resources and removes its temporary files.
// Safe to call multiple times, after Finalize, and on a nil writer.
func (w *ManifestWriter) Close() {
	if w == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.db != nil {
		if err := w.db.Close(); err != nil {
			log.Warn().Err(err).Msgf("manifest: can't close %s", w.localPath)
		}
		w.db = nil
	}
	if w.localPath != "" {
		_ = os.Remove(w.localPath)
		w.localPath = ""
	}
	if w.gzPath != "" {
		_ = os.Remove(w.gzPath)
		w.gzPath = ""
	}
}

// UploadManifest finalizes the manifest and uploads it to remote storage
// alongside metadata.json.
func (bd *BackupDestination) UploadManifest(ctx context.Context, backupName string, w *ManifestWriter) error {
	gzPath, err := w.Finalize()
	if err != nil {
		return err
	}
	f, err := os.Open(gzPath)
	if err != nil {
		return errors.WithMessage(err, "manifest: can't open compressed manifest")
	}
	fInfo, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return errors.WithMessage(err, "manifest: can't stat compressed manifest")
	}
	remotePath := path.Join(backupName, ManifestFileName)
	return bd.PutFile(ctx, remotePath, f, fInfo.Size())
}

// ManifestReader provides prefix lookups over a downloaded manifest. It is
// backed by a read-only (mmap) bolt database in a local temporary file, so
// lookups don't require loading the whole listing into memory.
type ManifestReader struct {
	db        *bolt.DB
	localPath string
}

// DownloadManifest attempts to download, decompress and open the manifest for
// a given backup. Returns nil if the manifest does not exist or is unreadable
// (graceful fallback to Walk).
func (bd *BackupDestination) DownloadManifest(ctx context.Context, backupName string) *ManifestReader {
	remotePath := path.Join(backupName, ManifestFileName)
	r, err := bd.GetFileReader(ctx, remotePath)
	if err != nil {
		// Manifest doesn't exist — this is expected for older backups
		log.Debug().Str("backup", backupName).Msgf("no %s found, will fall back to Walk", ManifestFileName)
		return nil
	}
	defer r.Close()
	gz, err := gzip.NewReader(r)
	if err != nil {
		log.Warn().Str("backup", backupName).Err(err).Msgf("can't decompress %s, will fall back to Walk", ManifestFileName)
		return nil
	}
	tmpFile, err := os.CreateTemp("", "clickhouse-backup-manifest-*.bolt")
	if err != nil {
		log.Warn().Str("backup", backupName).Err(err).Msgf("can't create temporary file for %s, will fall back to Walk", ManifestFileName)
		return nil
	}
	tmpPath := tmpFile.Name()
	if _, err = io.Copy(tmpFile, gz); err != nil {
		_ = tmpFile.Close()
		_ = os.Remove(tmpPath)
		log.Warn().Str("backup", backupName).Err(err).Msgf("can't read %s, will fall back to Walk", ManifestFileName)
		return nil
	}
	if err = tmpFile.Close(); err != nil {
		_ = os.Remove(tmpPath)
		log.Warn().Str("backup", backupName).Err(err).Msgf("can't write %s, will fall back to Walk", ManifestFileName)
		return nil
	}
	reader, err := openManifestReader(tmpPath)
	if err != nil {
		_ = os.Remove(tmpPath)
		log.Warn().Str("backup", backupName).Err(err).Msgf("can't open %s, will fall back to Walk", ManifestFileName)
		return nil
	}
	return reader
}

func openManifestReader(localPath string) (*ManifestReader, error) {
	db, err := bolt.Open(localPath, 0600, &bolt.Options{ReadOnly: true})
	if err != nil {
		return nil, errors.WithMessage(err, "manifest: can't open bolt database")
	}
	err = db.View(func(tx *bolt.Tx) error {
		if tx.Bucket(manifestFilesBucket) == nil || tx.Bucket(manifestMetaBucket) == nil {
			return errors.New("manifest: missing buckets")
		}
		versionBytes := tx.Bucket(manifestMetaBucket).Get([]byte("version"))
		if versionBytes == nil {
			return errors.New("manifest: missing version")
		}
		version, n := binary.Varint(versionBytes)
		if n <= 0 || version != ManifestVersion {
			return errors.Errorf("manifest: unsupported version %d", version)
		}
		return nil
	})
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return &ManifestReader{db: db, localPath: localPath}, nil
}

// FilesUnderPrefix returns the names (relative to the prefix) of all manifest
// entries whose path starts with the given prefix, using a sorted-key seek
// instead of a full scan. The prefix should NOT include the backup name
// (e.g., "shadow/default/my_table/default/part1").
func (r *ManifestReader) FilesUnderPrefix(prefix string) ([]string, error) {
	prefixBytes := []byte(strings.TrimSuffix(prefix, "/") + "/")
	var result []string
	err := r.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(manifestFilesBucket).Cursor()
		for k, _ := c.Seek(prefixBytes); k != nil && bytes.HasPrefix(k, prefixBytes); k, _ = c.Next() {
			if name := string(k[len(prefixBytes):]); name != "" {
				result = append(result, name)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// Close closes the underlying bolt database and removes the temporary file.
// Safe to call on a nil reader.
func (r *ManifestReader) Close() {
	if r == nil {
		return
	}
	if r.db != nil {
		if err := r.db.Close(); err != nil {
			log.Warn().Err(err).Msgf("manifest: can't close %s", r.localPath)
		}
		r.db = nil
	}
	if r.localPath != "" {
		_ = os.Remove(r.localPath)
		r.localPath = ""
	}
}

// DownloadPathWithManifest downloads the given files from remotePath using a
// pre-loaded manifest listing instead of calling Walk. fileNames are relative
// to remotePath, as returned by ManifestReader.FilesUnderPrefix.
func (bd *BackupDestination) DownloadPathWithManifest(ctx context.Context, remotePath string, localPath string, fileNames []string, RetriesOnFailure int, RetriesDuration time.Duration, RetriesJitter int8, RetrierClassifier retrier.Classifier, maxSpeed uint64) (int64, error) {
	downloadedBytes := int64(0)
	limiter := bd.DownloadLimiter(maxSpeed)

	for _, fileName := range fileNames {
		retry := retrier.New(retrier.ExponentialBackoff(RetriesOnFailure, common.AddRandomJitter(RetriesDuration, RetriesJitter)), RetrierClassifier)
		err := retry.RunCtx(ctx, func(ctx context.Context) error {
			r, err := bd.GetFileReader(ctx, path.Join(remotePath, fileName))
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
			dstFilePath := path.Join(localPath, fileName)
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
