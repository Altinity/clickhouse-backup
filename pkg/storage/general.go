package storage

import (
	"archive/tar"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/common"
	"github.com/pkg/errors"

	"github.com/Altinity/clickhouse-backup/v2/pkg/clickhouse"
	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/Altinity/clickhouse-backup/v2/pkg/metadata"

	"github.com/djherbis/buffer"
	"github.com/djherbis/nio/v3"
	"github.com/eapache/go-resiliency/retrier"
	"github.com/mholt/archiver/v4"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
)

const (
	// defaultPipeBufferSize - fallback size of ring buffer between stream handlers when general.pipe_buffer_size is unset
	defaultPipeBufferSize = 128 * 1024
)

type readerWrapperForContext func(p []byte) (n int, err error)

func (readerWrapper readerWrapperForContext) Read(p []byte) (n int, err error) {
	return readerWrapper(p)
}

type Backup struct {
	metadata.BackupMetadata
	Broken     string
	UploadDate time.Time `json:"upload_date"`
}

type BackupDestination struct {
	RemoteStorage
	compressionFormat      string
	compressionLevel       int
	pipeBufferSize         int64
	downloadCopyBufferSize int64
}

func (bd *BackupDestination) RemoveBackupRemote(ctx context.Context, backup Backup, cfg *config.Config, retrierClassifier retrier.Classifier) error {
	retry := retrier.New(retrier.ExponentialBackoff(cfg.General.RetriesOnFailure, common.AddRandomJitter(cfg.General.RetriesDuration, cfg.General.RetriesJitter)), retrierClassifier)

	// SFTP/FTP: Use DeleteFile which handles directory deletion
	if bd.Kind() == "SFTP" || bd.Kind() == "FTP" {
		return retry.RunCtx(ctx, func(ctx context.Context) error {
			return bd.DeleteFile(ctx, backup.BackupName)
		})
	}

	// Check if storage supports batch deletion
	if batchDeleter, ok := bd.RemoteStorage.(BatchDeleter); ok {
		batchSize := cfg.General.DeleteBatchSize

		// Process deletion in batches to avoid loading all keys in memory
		log.Info().Msgf("RemoveBackupRemote: starting batch deletion for backup %s using %s (batch_size=%d)", backup.BackupName, bd.Kind(), batchSize)

		var keysToDelete []string
		var totalDeleted uint
		walkErr := bd.Walk(ctx, backup.BackupName+"/", true, func(ctx context.Context, f RemoteFile) error {
			// Azure: filter out empty objects (existing logic)
			if bd.Kind() == "azblob" {
				if f.Size() == 0 && f.LastModified().IsZero() {
					return nil
				}
			}
			keysToDelete = append(keysToDelete, path.Join(backup.BackupName, f.Name()))

			// When we've collected enough keys, delete them as a batch
			if len(keysToDelete) >= batchSize {
				deleteErr := retry.RunCtx(ctx, func(ctx context.Context) error {
					return batchDeleter.DeleteKeysBatch(ctx, keysToDelete)
				})
				if deleteErr != nil {
					return errors.WithMessage(deleteErr, "RemoveBackupRemote DeleteKeysBatch batch")
				}
				totalDeleted += uint(len(keysToDelete))
				log.Debug().Msgf("RemoveBackupRemote: deleted batch of %d keys (total: %d)", len(keysToDelete), totalDeleted)
				keysToDelete = keysToDelete[:0] // Reset slice but keep capacity
			}
			return nil
		})
		if walkErr != nil {
			return errors.WithMessage(walkErr, "RemoveBackupRemote Walk")
		}

		// Delete remaining keys
		if len(keysToDelete) > 0 {
			deleteErr := retry.RunCtx(ctx, func(ctx context.Context) error {
				return batchDeleter.DeleteKeysBatch(ctx, keysToDelete)
			})
			if deleteErr != nil {
				return errors.WithMessage(deleteErr, "RemoveBackupRemote DeleteKeysBatch remaining")
			}
			totalDeleted += uint(len(keysToDelete))
		}

		log.Info().Msgf("RemoveBackupRemote: batch deleted %d files from backup %s", totalDeleted, backup.BackupName)
		return nil
	}

	// Fallback: one-by-one deletion (should not happen if all storage types implement BatchDeleter)
	log.Warn().Msgf("RemoveBackupRemote: %s does not implement BatchDeleter, falling back to one-by-one deletion", bd.Kind())
	return bd.Walk(ctx, backup.BackupName+"/", true, func(ctx context.Context, f RemoteFile) error {
		if bd.Kind() == "azblob" {
			if f.Size() > 0 || !f.LastModified().IsZero() {
				return retry.RunCtx(ctx, func(ctx context.Context) error {
					return bd.DeleteFile(ctx, path.Join(backup.BackupName, f.Name()))
				})
			}

			return nil
		}
		return retry.RunCtx(ctx, func(ctx context.Context) error {
			return bd.DeleteFile(ctx, path.Join(backup.BackupName, f.Name()))
		})
	})
}

func (bd *BackupDestination) loadMetadataCache(ctx context.Context) (map[string]Backup, error) {
	listCacheFile := path.Join(os.TempDir(), fmt.Sprintf(".clickhouse-backup-metadata.cache.%s", bd.Kind()))
	listCache := map[string]Backup{}
	if _, err := os.Stat(listCacheFile); os.IsNotExist(err) {
		return listCache, err
	}
	f, err := os.Open(listCacheFile)
	if err != nil {
		log.Warn().Msgf("can't open %s return error %v", listCacheFile, err)
		return listCache, errors.WithMessage(err, "loadMetadataCache Open")
	}
	defer func() {
		if err := f.Close(); err != nil {
			log.Warn().Msgf("can't close %s return error %v", listCacheFile, err)
		}
	}()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		body, err := io.ReadAll(f)
		if err != nil {
			log.Warn().Msgf("can't read %s return error %v", listCacheFile, err)
			return listCache, nil
		}
		if string(body) != "" {
			if err := json.Unmarshal(body, &listCache); err != nil {
				log.Fatal().Stack().Msgf("can't parse %s to map[string]Backup\n\n%s\n\nreturn error %v", listCacheFile, body, err)
			}
		}
		log.Debug().Msgf("%s load %d elements", listCacheFile, len(listCache))
		return listCache, nil
	}
}

// writeMetadataCacheFile atomically writes the listCache map to the on-disk
// metadata cache. Safe to call concurrently — writers can't observe a partial
// file thanks to tempfile + rename. No pruning happens here.
func (bd *BackupDestination) writeMetadataCacheFile(ctx context.Context, listCache map[string]Backup) error {
	listCacheFile := path.Join(os.TempDir(), fmt.Sprintf(".clickhouse-backup-metadata.cache.%s", bd.Kind()))
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	body, err := json.MarshalIndent(&listCache, "", "\t")
	if err != nil {
		log.Warn().Msgf("can't json marshal %s return error %v", listCacheFile, err)
		return nil
	}
	tmp, err := os.CreateTemp(os.TempDir(), filepath.Base(listCacheFile)+".tmp.*")
	if err != nil {
		log.Warn().Msgf("can't create temp for %s return error %v", listCacheFile, err)
		return nil
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(body); err != nil {
		log.Warn().Msgf("can't write to %s return error %v", tmpName, err)
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return nil
	}
	if err := tmp.Close(); err != nil {
		log.Warn().Msgf("can't close %s return error %v", tmpName, err)
		_ = os.Remove(tmpName)
		return nil
	}
	if err := os.Rename(tmpName, listCacheFile); err != nil {
		log.Warn().Msgf("can't rename %s -> %s return error %v", tmpName, listCacheFile, err)
		_ = os.Remove(tmpName)
		return nil
	}
	log.Debug().Msgf("%s save %d elements", listCacheFile, len(listCache))
	return nil
}

func (bd *BackupDestination) saveMetadataCache(ctx context.Context, listCache map[string]Backup, actualList []Backup) error {
	for backupName := range listCache {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			found := false
			for _, actualBackup := range actualList {
				if backupName == actualBackup.BackupName {
					found = true
					break
				}
			}
			if !found {
				delete(listCache, backupName)
			}
		}
	}
	return bd.writeMetadataCacheFile(ctx, listCache)
}

// readBackupMetadataDirect fetches a single backup's metadata.json directly via
// StatFile+GetFileReader, without listing the whole bucket. Returns nil if the
// metadata.json does not exist. Returns a "broken" Backup entry on parse errors,
// mirroring the slow-path semantics of BackupList.
func (bd *BackupDestination) readBackupMetadataDirect(ctx context.Context, backupName string) (*Backup, error) {
	metadataKey := path.Join(backupName, "metadata.json")
	mf, err := bd.StatFile(ctx, metadataKey)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, nil
		}
		return &Backup{
			BackupMetadata: metadata.BackupMetadata{BackupName: backupName},
			Broken:         "broken (can't stat metadata.json)",
		}, nil
	}
	r, err := bd.GetFileReader(ctx, metadataKey)
	if err != nil {
		return &Backup{
			BackupMetadata: metadata.BackupMetadata{BackupName: backupName},
			Broken:         "broken (can't open metadata.json)",
			UploadDate:     mf.LastModified(),
		}, nil
	}
	body, err := io.ReadAll(r)
	closeErr := r.Close()
	if err != nil {
		return &Backup{
			BackupMetadata: metadata.BackupMetadata{BackupName: backupName},
			Broken:         "broken (can't read metadata.json)",
			UploadDate:     mf.LastModified(),
		}, nil
	}
	if closeErr != nil {
		return nil, errors.WithMessage(closeErr, "BackupList close metadata reader")
	}
	var m metadata.BackupMetadata
	if err := json.Unmarshal(body, &m); err != nil {
		return &Backup{
			BackupMetadata: metadata.BackupMetadata{BackupName: backupName},
			Broken:         "broken (bad metadata.json)",
			UploadDate:     mf.LastModified(),
		}, nil
	}
	return &Backup{BackupMetadata: m, UploadDate: mf.LastModified()}, nil
}

// BackupList enumerates backup folders under the bucket root. skipPrefixes
// lists object-key prefixes the walker must ignore — used to exclude the
// CAS subtree (cas/<cluster>/...) which v1 must not interpret as broken
// v1 backups. Pass nil when CAS is disabled.
func (bd *BackupDestination) BackupList(ctx context.Context, parseMetadata bool, parseMetadataOnly string, skipPrefixes []string) ([]Backup, error) {
	backupListStart := time.Now()
	defer func() {
		log.Info().Dur("list_duration", time.Since(backupListStart)).Send()
	}()

	// Fast path: when the caller already knows which backup it wants, look it
	// up in the on-disk metadata cache first; on miss, fetch metadata.json
	// directly via StatFile+GetFileReader instead of listing the whole bucket
	// root. This removes the per-table Walk("/") cost on incremental-chain
	// restores (see https://github.com/Altinity/clickhouse-backup/pull/1361).
	// Staleness on remote delete is healed by the next slow-path list (e.g.
	// `clickhouse-backup list remote`), same as before.
	if parseMetadata && parseMetadataOnly != "" {
		listCache, loadErr := bd.loadMetadataCache(ctx)
		if loadErr != nil && !os.IsNotExist(loadErr) {
			return nil, errors.WithMessage(loadErr, "BackupList loadMetadataCache")
		}
		if cached, ok := listCache[parseMetadataOnly]; ok && cached.Broken == "" {
			log.Debug().Str("backup", parseMetadataOnly).Msg("BackupList: using on-disk metadata cache")
			return []Backup{cached}, nil
		}
		backup, err := bd.readBackupMetadataDirect(ctx, parseMetadataOnly)
		if err != nil {
			return nil, errors.WithMessage(err, "BackupList readBackupMetadataDirect")
		}
		if backup == nil {
			// metadata.json not found — check if the backup prefix has any
			// content at all. Walk with recursive=false returns top-level
			// entries only; if we get at least one the folder exists and the
			// backup is broken (missing metadata.json), otherwise the backup
			// name doesn't exist on remote storage at all.
			found := false
			var lastModified time.Time
			walkErr := bd.Walk(ctx, parseMetadataOnly+"/", false, func(_ context.Context, f RemoteFile) error {
				found = true
				lastModified = f.LastModified()
				return io.EOF // stop after first entry
			})
			_ = walkErr // Walk returns io.EOF, that's fine
			if !found {
				return []Backup{}, nil
			}
			return []Backup{{
				BackupMetadata: metadata.BackupMetadata{BackupName: parseMetadataOnly},
				Broken:         "broken (can't stat metadata.json)",
				UploadDate:     lastModified,
			}}, nil
		}
		if backup.Broken == "" {
			if listCache == nil {
				listCache = map[string]Backup{}
			}
			listCache[parseMetadataOnly] = *backup
			if writeErr := bd.writeMetadataCacheFile(ctx, listCache); writeErr != nil {
				log.Warn().Err(writeErr).Msg("BackupList writeMetadataCacheFile (fast path)")
			}
		}
		return []Backup{*backup}, nil
	}

	result := make([]Backup, 0)
	listCache, err := bd.loadMetadataCache(ctx)
	if err != nil && !os.IsNotExist(err) {
		return nil, errors.WithMessage(err, "BackupList loadMetadataCache")
	}
	if err != nil && os.IsNotExist(err) {
		parseMetadata = true
	}
	cacheMiss := false
	err = bd.Walk(ctx, "/", false, func(ctx context.Context, o RemoteFile) error {
		backupName := strings.Trim(o.Name(), "/")
		// Skip any top-level entry whose name matches a configured skip
		// prefix (e.g. "cas/" when CAS is enabled). The Walk runs at depth
		// 0 with recursive=false, so o.Name() is a single path segment;
		// match by trimmed-equality against a trimmed prefix as well as
		// the literal HasPrefix to be defensive across backends.
		for _, p := range skipPrefixes {
			if p == "" {
				continue
			}
			trimmed := strings.TrimSuffix(p, "/")
			if backupName == trimmed || strings.HasPrefix(o.Name(), p) {
				log.Error().Str("name", o.Name()).Str("matched_prefix", p).Msg("BackupList: skipping entry that matches a CAS skip prefix; rename or move if it was an unrelated v1 backup")
				return nil
			}
		}
		if !parseMetadata || (parseMetadataOnly != "" && parseMetadataOnly != backupName) {
			if cachedMetadata, isCached := listCache[backupName]; isCached {
				result = append(result, cachedMetadata)
			} else {
				result = append(result, Backup{
					BackupMetadata: metadata.BackupMetadata{
						BackupName: backupName,
					},
				})
			}
			return nil
		}
		if cachedMetadata, isCached := listCache[backupName]; isCached {
			result = append(result, cachedMetadata)
			return nil
		}
		mf, err := bd.StatFile(ctx, path.Join(o.Name(), "metadata.json"))
		if err != nil {
			brokenBackup := Backup{
				metadata.BackupMetadata{
					BackupName: backupName,
				},
				"broken (can't stat metadata.json)",
				o.LastModified(), // folder
			}
			result = append(result, brokenBackup)
			return nil
		}
		r, err := bd.GetFileReader(ctx, path.Join(o.Name(), "metadata.json"))
		if err != nil {
			brokenBackup := Backup{
				metadata.BackupMetadata{
					BackupName: backupName,
				},
				"broken (can't open metadata.json)",
				o.LastModified(), // folder
			}
			result = append(result, brokenBackup)
			return nil
		}
		b, err := io.ReadAll(r)
		if err != nil {
			brokenBackup := Backup{
				metadata.BackupMetadata{
					BackupName: backupName,
				},
				"broken (can't read metadata.json)",
				o.LastModified(), // folder
			}
			result = append(result, brokenBackup)
			return nil
		}
		if err := r.Close(); err != nil { // Never use defer in loops
			return errors.WithMessage(err, "BackupList close metadata reader")
		}
		var m metadata.BackupMetadata
		if err := json.Unmarshal(b, &m); err != nil {
			brokenBackup := Backup{
				metadata.BackupMetadata{
					BackupName: backupName,
				},
				"broken (bad metadata.json)",
				o.LastModified(), // folder
			}
			result = append(result, brokenBackup)
			return nil
		}
		goodBackup := Backup{m, "", mf.LastModified()}
		listCache[backupName] = goodBackup
		cacheMiss = true
		result = append(result, goodBackup)
		return nil
	})
	if err != nil {
		log.Warn().Msgf("BackupList bd.Walk return error: %v", err)
	}
	// sort by name for the same not parsed metadata.json
	sort.SliceStable(result, func(i, j int) bool {
		return result[i].BackupName < result[j].BackupName
	})
	sort.SliceStable(result, func(i, j int) bool {
		return result[i].UploadDate.Before(result[j].UploadDate)
	})
	if cacheMiss || len(result) < len(listCache) {
		if err = bd.saveMetadataCache(ctx, listCache, result); err != nil {
			return nil, errors.Wrap(err, "bd.saveMetadataCache return error")
		}
	}
	return result, nil
}

func (bd *BackupDestination) DownloadCompressedStream(ctx context.Context, remotePath string, localPath string, maxSpeed uint64) (int64, error) {
	if err := os.MkdirAll(localPath, 0750); err != nil {
		return 0, errors.WithMessage(err, "DownloadCompressedStream MkdirAll")
	}
	// get this first as GetFileReader blocks the ftp control channel
	remoteFileInfo, statErr := bd.StatFile(ctx, remotePath)
	if statErr != nil {
		return 0, errors.WithMessage(statErr, "DownloadCompressedStream StatFile")
	}
	startTime := time.Now()
	reader, getReaderErr := bd.GetFileReaderWithLocalPath(ctx, remotePath, localPath, remoteFileInfo.Size())
	if getReaderErr != nil {
		return 0, errors.WithMessage(getReaderErr, "DownloadCompressedStream GetFileReaderWithLocalPath")
	}
	defer func() {
		if err := reader.Close(); err != nil {
			log.Warn().Msgf("can't close GetFileReader descriptor %v", reader)
		}
		switch reader.(type) {
		case *os.File:
			fileName := reader.(*os.File).Name()
			if err := os.Remove(fileName); err != nil {
				log.Warn().Msgf("can't remove %s", fileName)
			}
		}
	}()

	buf := buffer.New(bd.pipeBufferSize)
	bufReader := nio.NewReader(reader, buf)
	compressionFormat := bd.compressionFormat
	if !checkArchiveExtension(path.Ext(remotePath), compressionFormat) {
		log.Warn().Msgf("remote file backup extension %s not equal with %s", remotePath, compressionFormat)
		compressionFormat = strings.Replace(path.Ext(remotePath), ".", "", -1)
	}
	downloadedBytes := int64(0)
	z, getArchiveReaderErr := getArchiveReader(compressionFormat)
	if getArchiveReaderErr != nil {
		return 0, errors.WithMessage(getArchiveReaderErr, "DownloadCompressedStream getArchiveReader")
	}
	if extractErr := z.Extract(ctx, bufReader, nil, func(ctx context.Context, file archiver.File) error {
		src, openErr := file.Open()
		if openErr != nil {
			return errors.Errorf("can't open %s", file.NameInArchive)
		}
		header, ok := file.Header.(*tar.Header)
		if !ok {
			return errors.Errorf("expected header to be *tar.Header but was %T", file.Header)
		}
		extractFile := filepath.Join(localPath, header.Name)
		extractDir := filepath.Dir(extractFile)
		if _, err := os.Stat(extractDir); os.IsNotExist(err) {
			_ = os.MkdirAll(extractDir, 0750)
		}
		dst, createErr := os.Create(extractFile)
		if createErr != nil {
			return errors.WithMessage(createErr, "DownloadCompressedStream Create")
		}
		if copyBytes, copyErr := bd.copyWithBuffer(dst, readerWrapperForContext(func(p []byte) (int, error) {
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			default:
				return src.Read(p)
			}
		})); copyErr != nil {
			return errors.WithMessage(copyErr, "DownloadCompressedStream io.Copy")
		} else {
			downloadedBytes += copyBytes
		}
		if dstCloseErr := dst.Close(); dstCloseErr != nil {
			return errors.WithMessage(dstCloseErr, "DownloadCompressedStream dst.Close")
		}
		if srcCloseErr := src.Close(); srcCloseErr != nil {
			return errors.WithMessage(srcCloseErr, "DownloadCompressedStream src.Close")
		}
		//log.Debug().Msgf("extract %s", extractFile)
		return nil
	}); extractErr != nil {
		return 0, errors.WithMessage(extractErr, "DownloadCompressedStream Extract")
	}
	bd.throttleSpeed(startTime, remoteFileInfo.Size(), maxSpeed)
	return downloadedBytes, nil
}

func (bd *BackupDestination) UploadCompressedStream(ctx context.Context, baseLocalPath string, files []string, remotePath string, maxSpeed uint64) error {
	var totalBytes int64
	for _, filename := range files {
		fInfo, err := os.Stat(path.Join(baseLocalPath, filename))
		if err != nil {
			return errors.WithMessage(err, "UploadCompressedStream Stat")
		}
		if fInfo.Mode().IsRegular() {
			totalBytes += fInfo.Size()
		}
	}
	pipeBuffer := buffer.New(bd.pipeBufferSize)
	body, w := nio.Pipe(pipeBuffer)
	g, ctx := errgroup.WithContext(ctx)
	startTime := time.Now()
	var writerErr, readerErr error
	g.Go(func() error {
		defer func() {
			if writerErr != nil {
				if err := w.CloseWithError(writerErr); err != nil {
					log.Error().Msgf("can't close after error %v pipe writer error: %v", writerErr, err)
				}
			} else {
				if err := w.Close(); err != nil {
					log.Error().Msgf("can't close pipe writer: %v", err)
				}
			}
		}()
		z, err := getArchiveWriter(bd.compressionFormat, bd.compressionLevel)
		if err != nil {
			return errors.WithMessage(err, "UploadCompressedStream getArchiveWriter")
		}
		archiveFiles := make([]archiver.File, 0)
		for _, f := range files {
			localPath := path.Join(baseLocalPath, f)
			info, err := os.Stat(localPath)
			if err != nil {
				return errors.WithMessage(err, "UploadCompressedStream Stat file")
			}
			if !info.Mode().IsRegular() {
				continue
			}

			file := archiver.File{
				FileInfo:      info,
				NameInArchive: f,
				Open: func() (io.ReadCloser, error) {
					return os.Open(localPath)
				},
			}
			archiveFiles = append(archiveFiles, file)
			//log.Debug().Msgf("add %s to archive %s", filePath, remotePath)
		}
		if writerErr = z.Archive(ctx, w, archiveFiles); writerErr != nil {
			return writerErr
		}
		return nil
	})
	g.Go(func() error {
		defer func() {
			if readerErr != nil {
				if err := body.CloseWithError(readerErr); err != nil {
					log.Error().Msgf("can't close after error %v pipe reader error: %v", writerErr, err)
				}
			} else {
				if err := body.Close(); err != nil {
					log.Error().Msgf("can't close pipe reader: %v", err)
				}
			}
		}()
		readerErr = bd.PutFile(ctx, remotePath, body, totalBytes)
		return readerErr
	})
	if waitErr := g.Wait(); waitErr != nil {
		return errors.WithMessage(waitErr, "UploadCompressedStream errgroup.Wait")
	}
	bd.throttleSpeed(startTime, totalBytes, maxSpeed)
	return nil
}

func (bd *BackupDestination) DownloadPath(ctx context.Context, remotePath string, localPath string, RetriesOnFailure int, RetriesDuration time.Duration, RetriesJitter int8, RetrierClassifier retrier.Classifier, maxSpeed uint64) (int64, error) {
	downloadedBytes := int64(0)
	walkErr := bd.Walk(ctx, remotePath, true, func(ctx context.Context, f RemoteFile) error {
		if bd.Kind() == "SFTP" && (f.Name() == "." || f.Name() == "..") {
			return nil
		}
		retry := retrier.New(retrier.ExponentialBackoff(RetriesOnFailure, common.AddRandomJitter(RetriesDuration, RetriesJitter)), RetrierClassifier)
		err := retry.RunCtx(ctx, func(ctx context.Context) error {
			startTime := time.Now()
			r, err := bd.GetFileReader(ctx, path.Join(remotePath, f.Name()))
			if err != nil {
				log.Error().Err(err).Send()
				return errors.WithMessage(err, "DownloadPath GetFileReader")
			}
			dstFilePath := path.Join(localPath, f.Name())
			dstDirPath, _ := path.Split(dstFilePath)
			if err := os.MkdirAll(dstDirPath, 0750); err != nil {
				log.Error().Err(err).Send()
				return errors.WithMessage(err, "DownloadPath MkdirAll")
			}
			dst, err := os.Create(dstFilePath)
			if err != nil {
				log.Error().Err(err).Send()
				return errors.WithMessage(err, "DownloadPath Create")
			}
			if copyBytes, copyErr := bd.copyWithBuffer(dst, r); copyErr != nil {
				log.Error().Err(copyErr).Send()
				return errors.WithMessage(copyErr, "DownloadPath io.Copy")
			} else {
				downloadedBytes += copyBytes
			}
			if dstCloseErr := dst.Close(); dstCloseErr != nil {
				log.Error().Err(dstCloseErr).Send()
				return errors.WithMessage(dstCloseErr, "DownloadPath dst.Close")
			}
			if srcCloseErr := r.Close(); srcCloseErr != nil {
				log.Error().Err(srcCloseErr).Send()
				return errors.WithMessage(srcCloseErr, "DownloadPath r.Close")
			}

			if dstFileInfo, statErr := os.Stat(dstFilePath); statErr == nil {
				bd.throttleSpeed(startTime, dstFileInfo.Size(), maxSpeed)
			} else {
				return errors.WithMessage(statErr, "DownloadPath Stat")
			}

			return nil
		})
		if err != nil {
			return errors.WithMessage(err, "DownloadPath retry")
		}
		return nil
	})
	return downloadedBytes, walkErr
}

func (bd *BackupDestination) UploadPath(ctx context.Context, baseLocalPath string, files []string, remotePath string, RetriesOnFailure int, RetriesDuration time.Duration, RetriesJitter int8, RertierClassifier retrier.Classifier, maxSpeed uint64) (int64, error) {
	totalBytes := int64(0)
	for _, filename := range files {
		startTime := time.Now()
		fInfo, err := os.Stat(filepath.Clean(path.Join(baseLocalPath, filename)))
		if err != nil {
			return 0, errors.WithMessage(err, "UploadPath Stat")
		}
		if fInfo.Mode().IsRegular() {
			totalBytes += fInfo.Size()
		}
		f, err := os.Open(filepath.Clean(path.Join(baseLocalPath, filename)))
		if err != nil {
			return 0, errors.WithMessage(err, "UploadPath Open")
		}
		closeFile := func() {
			if err := f.Close(); err != nil {
				log.Warn().Msgf("can't close UploadPath file descriptor %v: %v", f, err)
			}
		}
		retry := retrier.New(retrier.ExponentialBackoff(RetriesOnFailure, common.AddRandomJitter(RetriesDuration, RetriesJitter)), RertierClassifier)
		err = retry.RunCtx(ctx, func(ctx context.Context) error {
			return bd.PutFile(ctx, path.Join(remotePath, filename), f, 0)
		})
		if err != nil {
			closeFile()
			return 0, errors.WithMessage(err, "UploadPath PutFile")
		}
		closeFile()
		bd.throttleSpeed(startTime, fInfo.Size(), maxSpeed)
	}

	return totalBytes, nil
}

// copyWithBuffer copies src to dst using io.CopyBuffer with a configurable buffer size
// (general.download_copy_buffer_size). When the size is 0 it falls back to plain io.Copy
// (Go's default 32KB internal buffer), preserving the previous behavior.
func (bd *BackupDestination) copyWithBuffer(dst io.Writer, src io.Reader) (int64, error) {
	if bd.downloadCopyBufferSize > 0 {
		return io.CopyBuffer(dst, src, make([]byte, bd.downloadCopyBufferSize))
	}
	return io.Copy(dst, src)
}

func (bd *BackupDestination) throttleSpeed(startTime time.Time, size int64, maxSpeed uint64) {
	if maxSpeed > 0 && size > 0 {
		timeSince := time.Since(startTime).Nanoseconds()
		currentSpeed := uint64(size*1000000000) / uint64(timeSince)
		if currentSpeed > maxSpeed {

			// Calculate how long to sleep to reduce the average speed to maxSpeed
			excessSpeed := currentSpeed - maxSpeed
			excessData := uint64(size) - (maxSpeed * uint64(timeSince) / 1000000000)
			sleepTime := time.Duration((excessData*1000000000)/excessSpeed) * time.Nanosecond
			time.Sleep(sleepTime)
		}
	}
}

func NewBackupDestination(ctx context.Context, cfg *config.Config, ch *clickhouse.ClickHouse, backupName string) (*BackupDestination, error) {
	var err error
	// Lock config to safely apply macros and modify config values
	cfg.Lock()
	defer cfg.Unlock()

	pipeBufferSize := cfg.General.PipeBufferSize
	if pipeBufferSize <= 0 {
		pipeBufferSize = defaultPipeBufferSize
	}
	downloadCopyBufferSize := cfg.General.DownloadCopyBufferSize

	switch cfg.General.RemoteStorage {
	case "azblob":
		if cfg.AzureBlob.Path, err = ch.ApplyMacros(ctx, cfg.AzureBlob.Path); err != nil {
			return nil, errors.WithMessage(err, "NewBackupDestination azblob ApplyMacros Path")
		}
		if cfg.AzureBlob.ObjectDiskPath, err = ch.ApplyMacros(ctx, cfg.AzureBlob.ObjectDiskPath); err != nil {
			return nil, errors.WithMessage(err, "NewBackupDestination azblob ApplyMacros ObjectDiskPath")
		}
		azblobStorage := &AzureBlob{
			Config: &cfg.AzureBlob,
		}
		return &BackupDestination{
			RemoteStorage:          azblobStorage,
			compressionFormat:      cfg.AzureBlob.CompressionFormat,
			compressionLevel:       cfg.AzureBlob.CompressionLevel,
			pipeBufferSize:         pipeBufferSize,
			downloadCopyBufferSize: downloadCopyBufferSize,
		}, nil
	case "s3":
		if cfg.S3.Path, err = ch.ApplyMacros(ctx, cfg.S3.Path); err != nil {
			return nil, errors.WithMessage(err, "NewBackupDestination s3 ApplyMacros Path")
		}
		if cfg.S3.ObjectDiskPath, err = ch.ApplyMacros(ctx, cfg.S3.ObjectDiskPath); err != nil {
			return nil, errors.WithMessage(err, "NewBackupDestination s3 ApplyMacros ObjectDiskPath")
		}
		// https://github.com/Altinity/clickhouse-backup/issues/588
		if len(cfg.S3.ObjectLabels) > 0 && backupName != "" {
			cfg.S3.ObjectLabels, err = ch.ApplyMacrosToObjectLabels(ctx, cfg.S3.ObjectLabels, backupName)
			if err != nil {
				return nil, errors.WithMessage(err, "NewBackupDestination s3 ApplyMacrosToObjectLabels")
			}
		}
		s3BufferSize := cfg.S3.BufferSize
		if s3BufferSize <= 0 {
			s3BufferSize = 64 * 1024
		}
		s3Storage := &S3{
			Config:      &cfg.S3,
			Concurrency: cfg.S3.Concurrency,
			BufferSize:  s3BufferSize,
		}
		return &BackupDestination{
			RemoteStorage:          s3Storage,
			compressionFormat:      cfg.S3.CompressionFormat,
			compressionLevel:       cfg.S3.CompressionLevel,
			pipeBufferSize:         pipeBufferSize,
			downloadCopyBufferSize: downloadCopyBufferSize,
		}, nil
	case "gcs":
		if cfg.GCS.Path, err = ch.ApplyMacros(ctx, cfg.GCS.Path); err != nil {
			return nil, errors.WithMessage(err, "NewBackupDestination gcs ApplyMacros Path")
		}
		if cfg.GCS.ObjectDiskPath, err = ch.ApplyMacros(ctx, cfg.GCS.ObjectDiskPath); err != nil {
			return nil, errors.WithMessage(err, "NewBackupDestination gcs ApplyMacros ObjectDiskPath")
		}
		// https://github.com/Altinity/clickhouse-backup/issues/588
		if len(cfg.GCS.ObjectLabels) > 0 && backupName != "" {
			cfg.GCS.ObjectLabels, err = ch.ApplyMacrosToObjectLabels(ctx, cfg.GCS.ObjectLabels, backupName)
			if err != nil {
				return nil, errors.WithMessage(err, "NewBackupDestination gcs ApplyMacrosToObjectLabels")
			}
		}
		googleCloudStorage := &GCS{Config: &cfg.GCS}
		return &BackupDestination{
			RemoteStorage:          googleCloudStorage,
			compressionFormat:      cfg.GCS.CompressionFormat,
			compressionLevel:       cfg.GCS.CompressionLevel,
			pipeBufferSize:         pipeBufferSize,
			downloadCopyBufferSize: downloadCopyBufferSize,
		}, nil
	case "cos":
		if cfg.COS.Path, err = ch.ApplyMacros(ctx, cfg.COS.Path); err != nil {
			return nil, errors.WithMessage(err, "NewBackupDestination cos ApplyMacros Path")
		}
		if cfg.COS.ObjectDiskPath, err = ch.ApplyMacros(ctx, cfg.COS.ObjectDiskPath); err != nil {
			return nil, errors.WithMessage(err, "NewBackupDestination cos ApplyMacros ObjectDiskPath")
		}
		tencentStorage := &COS{
			Config:     &cfg.COS,
			BufferSize: 64 * 1024,
		}
		return &BackupDestination{
			RemoteStorage:          tencentStorage,
			compressionFormat:      cfg.COS.CompressionFormat,
			compressionLevel:       cfg.COS.CompressionLevel,
			pipeBufferSize:         pipeBufferSize,
			downloadCopyBufferSize: downloadCopyBufferSize,
		}, nil
	case "ftp":
		if cfg.FTP.Concurrency < cfg.General.ObjectDiskServerSideCopyConcurrency/4 {
			cfg.FTP.Concurrency = cfg.General.ObjectDiskServerSideCopyConcurrency
		}
		if cfg.FTP.Path, err = ch.ApplyMacros(ctx, cfg.FTP.Path); err != nil {
			return nil, errors.WithMessage(err, "NewBackupDestination ftp ApplyMacros Path")
		}
		if cfg.FTP.ObjectDiskPath, err = ch.ApplyMacros(ctx, cfg.FTP.ObjectDiskPath); err != nil {
			return nil, errors.WithMessage(err, "NewBackupDestination ftp ApplyMacros ObjectDiskPath")
		}
		ftpStorage := &FTP{
			Config:             &cfg.FTP,
			AllowUnsafeMarkers: cfg.CAS.AllowUnsafeMarkers,
		}
		return &BackupDestination{
			RemoteStorage:          ftpStorage,
			compressionFormat:      cfg.FTP.CompressionFormat,
			compressionLevel:       cfg.FTP.CompressionLevel,
			pipeBufferSize:         pipeBufferSize,
			downloadCopyBufferSize: downloadCopyBufferSize,
		}, nil
	case "sftp":
		if cfg.SFTP.Path, err = ch.ApplyMacros(ctx, cfg.SFTP.Path); err != nil {
			return nil, errors.WithMessage(err, "NewBackupDestination sftp ApplyMacros Path")
		}
		if cfg.SFTP.ObjectDiskPath, err = ch.ApplyMacros(ctx, cfg.SFTP.ObjectDiskPath); err != nil {
			return nil, errors.WithMessage(err, "NewBackupDestination sftp ApplyMacros ObjectDiskPath")
		}
		sftpStorage := &SFTP{
			Config: &cfg.SFTP,
		}
		return &BackupDestination{
			RemoteStorage:          sftpStorage,
			compressionFormat:      cfg.SFTP.CompressionFormat,
			compressionLevel:       cfg.SFTP.CompressionLevel,
			pipeBufferSize:         pipeBufferSize,
			downloadCopyBufferSize: downloadCopyBufferSize,
		}, nil
	default:
		return nil, errors.Errorf("NewBackupDestination error: storage type '%s' is not supported", cfg.General.RemoteStorage)
	}
}

func AdjustValueByRange(value, minValue, maxSize int64) int64 {
	if value < minValue {
		value = minValue
	}

	if value > maxSize {
		value = maxSize
	}
	return value
}
