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
	"sync"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/common"

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
	// PipeBufferSize - size of ring buffer between stream handlers
	PipeBufferSize = 128 * 1024
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
	compressionFormat string
	compressionLevel  int
}

var metadataCacheLock sync.RWMutex

func (bd *BackupDestination) RemoveBackupRemote(ctx context.Context, backup Backup, cfg *config.Config, retrierClassifier retrier.Classifier) error {
	retry := retrier.New(retrier.ExponentialBackoff(cfg.General.RetriesOnFailure, common.AddRandomJitter(cfg.General.RetriesDuration, cfg.General.RetriesJitter)), retrierClassifier)
	if bd.Kind() == "SFTP" || bd.Kind() == "FTP" {
		return retry.RunCtx(ctx, func(ctx context.Context) error {
			return bd.DeleteFile(ctx, backup.BackupName)
		})
	}
	return bd.Walk(ctx, backup.BackupName+"/", true, func(ctx context.Context, f RemoteFile) error {
		if bd.Kind() == "azblob" {
			if f.Size() > 0 || !f.LastModified().IsZero() {
				return retry.RunCtx(ctx, func(ctx context.Context) error {
					return bd.DeleteFile(ctx, path.Join(backup.BackupName, f.Name()))
				})
			} else {
				return nil
			}
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
		return listCache, err
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

func (bd *BackupDestination) saveMetadataCache(ctx context.Context, listCache map[string]Backup, actualList []Backup) error {
	listCacheFile := path.Join(os.TempDir(), fmt.Sprintf(".clickhouse-backup-metadata.cache.%s", bd.Kind()))
	f, err := os.OpenFile(listCacheFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		log.Warn().Msgf("can't open %s return error %v", listCacheFile, err)
		return nil
	}
	defer func() {
		if err := f.Close(); err != nil {
			log.Warn().Msgf("can't close %s return error %v", listCacheFile, err)
		}
	}()
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
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		body, err := json.MarshalIndent(&listCache, "", "\t")
		if err != nil {
			log.Warn().Msgf("can't json marshal %s return error %v", listCacheFile, err)
			return nil
		}
		_, err = f.Write(body)
		if err != nil {
			log.Warn().Msgf("can't write to %s return error %v", listCacheFile, err)
			return nil
		}
		log.Debug().Msgf("%s save %d elements", listCacheFile, len(listCache))
		return nil
	}
}

func (bd *BackupDestination) BackupList(ctx context.Context, parseMetadata bool, parseMetadataOnly string) ([]Backup, error) {
	backupListStart := time.Now()
	defer func() {
		log.Info().Dur("list_duration", time.Since(backupListStart)).Send()
	}()
	result := make([]Backup, 0)
	metadataCacheLock.Lock()
	defer metadataCacheLock.Unlock()
	listCache, err := bd.loadMetadataCache(ctx)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	if err != nil && os.IsNotExist(err) {
		parseMetadata = true
	}
	cacheMiss := false
	err = bd.Walk(ctx, "/", false, func(ctx context.Context, o RemoteFile) error {
		backupName := strings.Trim(o.Name(), "/")
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
			return err
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
			return nil, fmt.Errorf("bd.saveMetadataCache return error: %v", err)
		}
	}
	return result, nil
}

func (bd *BackupDestination) DownloadCompressedStream(ctx context.Context, remotePath string, localPath string, maxSpeed uint64) (int64, error) {
	if err := os.MkdirAll(localPath, 0750); err != nil {
		return 0, err
	}
	// get this first as GetFileReader blocks the ftp control channel
	remoteFileInfo, statErr := bd.StatFile(ctx, remotePath)
	if statErr != nil {
		return 0, statErr
	}
	startTime := time.Now()
	reader, getReaderErr := bd.GetFileReaderWithLocalPath(ctx, remotePath, localPath, remoteFileInfo.Size())
	if getReaderErr != nil {
		return 0, getReaderErr
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

	buf := buffer.New(PipeBufferSize)
	bufReader := nio.NewReader(reader, buf)
	compressionFormat := bd.compressionFormat
	if !checkArchiveExtension(path.Ext(remotePath), compressionFormat) {
		log.Warn().Msgf("remote file backup extension %s not equal with %s", remotePath, compressionFormat)
		compressionFormat = strings.Replace(path.Ext(remotePath), ".", "", -1)
	}
	downloadedBytes := int64(0)
	z, getArchieveReaderErr := getArchiveReader(compressionFormat)
	if getArchieveReaderErr != nil {
		return 0, getArchieveReaderErr
	}
	if extractErr := z.Extract(ctx, bufReader, nil, func(ctx context.Context, file archiver.File) error {
		src, openErr := file.Open()
		if openErr != nil {
			return fmt.Errorf("can't open %s", file.NameInArchive)
		}
		header, ok := file.Header.(*tar.Header)
		if !ok {
			return fmt.Errorf("expected header to be *tar.Header but was %T", file.Header)
		}
		extractFile := filepath.Join(localPath, header.Name)
		extractDir := filepath.Dir(extractFile)
		if _, err := os.Stat(extractDir); os.IsNotExist(err) {
			_ = os.MkdirAll(extractDir, 0750)
		}
		dst, createErr := os.Create(extractFile)
		if createErr != nil {
			return createErr
		}
		if copyBytes, copyErr := io.Copy(dst, readerWrapperForContext(func(p []byte) (int, error) {
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			default:
				return src.Read(p)
			}
		})); copyErr != nil {
			return copyErr
		} else {
			downloadedBytes += copyBytes
		}
		if dstCloseErr := dst.Close(); dstCloseErr != nil {
			return dstCloseErr
		}
		if srcCloseErr := src.Close(); srcCloseErr != nil {
			return srcCloseErr
		}
		//log.Debug().Msgf("extract %s", extractFile)
		return nil
	}); extractErr != nil {
		return 0, extractErr
	}
	bd.throttleSpeed(startTime, remoteFileInfo.Size(), maxSpeed)
	return downloadedBytes, nil
}

func (bd *BackupDestination) UploadCompressedStream(ctx context.Context, baseLocalPath string, files []string, remotePath string, maxSpeed uint64) error {
	var totalBytes int64
	for _, filename := range files {
		fInfo, err := os.Stat(path.Join(baseLocalPath, filename))
		if err != nil {
			return err
		}
		if fInfo.Mode().IsRegular() {
			totalBytes += fInfo.Size()
		}
	}
	pipeBuffer := buffer.New(PipeBufferSize)
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
			return err
		}
		archiveFiles := make([]archiver.File, 0)
		for _, f := range files {
			localPath := path.Join(baseLocalPath, f)
			info, err := os.Stat(localPath)
			if err != nil {
				return err
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
		return waitErr
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
				return err
			}
			dstFilePath := path.Join(localPath, f.Name())
			dstDirPath, _ := path.Split(dstFilePath)
			if err := os.MkdirAll(dstDirPath, 0750); err != nil {
				log.Error().Err(err).Send()
				return err
			}
			dst, err := os.Create(dstFilePath)
			if err != nil {
				log.Error().Err(err).Send()
				return err
			}
			if copyBytes, copyErr := io.Copy(dst, r); copyErr != nil {
				log.Error().Err(copyErr).Send()
				return err
			} else {
				downloadedBytes += copyBytes
			}
			if dstCloseErr := dst.Close(); dstCloseErr != nil {
				log.Error().Err(dstCloseErr).Send()
				return dstCloseErr
			}
			if srcCloseErr := r.Close(); srcCloseErr != nil {
				log.Error().Err(srcCloseErr).Send()
				return srcCloseErr
			}

			if dstFileInfo, statErr := os.Stat(dstFilePath); statErr == nil {
				bd.throttleSpeed(startTime, dstFileInfo.Size(), maxSpeed)
			} else {
				return statErr
			}

			return nil
		})
		if err != nil {
			return err
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
			return 0, err
		}
		if fInfo.Mode().IsRegular() {
			totalBytes += fInfo.Size()
		}
		f, err := os.Open(filepath.Clean(path.Join(baseLocalPath, filename)))
		if err != nil {
			return 0, err
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
			return 0, err
		}
		closeFile()
		bd.throttleSpeed(startTime, fInfo.Size(), maxSpeed)
	}

	return totalBytes, nil
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
	switch cfg.General.RemoteStorage {
	case "azblob":
		azblobStorage := &AzureBlob{
			Config: &cfg.AzureBlob,
		}
		if azblobStorage.Config.Path, err = ch.ApplyMacros(ctx, azblobStorage.Config.Path); err != nil {
			return nil, err
		}
		if azblobStorage.Config.ObjectDiskPath, err = ch.ApplyMacros(ctx, azblobStorage.Config.ObjectDiskPath); err != nil {
			return nil, err
		}

		return &BackupDestination{
			azblobStorage,
			cfg.AzureBlob.CompressionFormat,
			cfg.AzureBlob.CompressionLevel,
		}, nil
	case "s3":
		// Use adaptive concurrency if not explicitly set
		concurrency := cfg.S3.Concurrency
		if concurrency <= 0 {
			concurrency = cfg.GetOptimalUploadConcurrency()
		}

		// Use adaptive buffer size
		bufferSize := config.CalculateOptimalBufferSize(0, concurrency) // Use 0 for unknown file size initially

		s3Storage := &S3{
			Config:      &cfg.S3,
			Concurrency: concurrency,
			BufferSize:  bufferSize,
		}
		if s3Storage.Config.Path, err = ch.ApplyMacros(ctx, s3Storage.Config.Path); err != nil {
			return nil, err
		}
		if s3Storage.Config.ObjectDiskPath, err = ch.ApplyMacros(ctx, s3Storage.Config.ObjectDiskPath); err != nil {
			return nil, err
		}
		// https://github.com/Altinity/clickhouse-backup/issues/588
		if len(s3Storage.Config.ObjectLabels) > 0 && backupName != "" {
			objectLabels := s3Storage.Config.ObjectLabels
			objectLabels, err = ch.ApplyMacrosToObjectLabels(ctx, objectLabels, backupName)
			if err != nil {
				return nil, err
			}
			s3Storage.Config.ObjectLabels = objectLabels
		}
		return &BackupDestination{
			s3Storage,
			cfg.S3.CompressionFormat,
			cfg.S3.CompressionLevel,
		}, nil
	case "gcs":
		googleCloudStorage := &GCS{Config: &cfg.GCS, cfg: cfg}
		if googleCloudStorage.Config.Path, err = ch.ApplyMacros(ctx, googleCloudStorage.Config.Path); err != nil {
			return nil, err
		}
		if googleCloudStorage.Config.ObjectDiskPath, err = ch.ApplyMacros(ctx, googleCloudStorage.Config.ObjectDiskPath); err != nil {
			return nil, err
		}
		// https://github.com/Altinity/clickhouse-backup/issues/588
		if len(googleCloudStorage.Config.ObjectLabels) > 0 && backupName != "" {
			objectLabels := googleCloudStorage.Config.ObjectLabels
			objectLabels, err = ch.ApplyMacrosToObjectLabels(ctx, objectLabels, backupName)
			if err != nil {
				return nil, err
			}
			googleCloudStorage.Config.ObjectLabels = objectLabels
		}
		return &BackupDestination{
			googleCloudStorage,
			cfg.GCS.CompressionFormat,
			cfg.GCS.CompressionLevel,
		}, nil
	case "cos":
		tencentStorage := &COS{
			Config:     &cfg.COS,
			BufferSize: 64 * 1024,
		}
		if tencentStorage.Config.Path, err = ch.ApplyMacros(ctx, tencentStorage.Config.Path); err != nil {
			return nil, err
		}
		if tencentStorage.Config.ObjectDiskPath, err = ch.ApplyMacros(ctx, tencentStorage.Config.ObjectDiskPath); err != nil {
			return nil, err
		}
		return &BackupDestination{
			tencentStorage,
			cfg.COS.CompressionFormat,
			cfg.COS.CompressionLevel,
		}, nil
	case "ftp":
		if cfg.FTP.Concurrency < cfg.General.ObjectDiskServerSideCopyConcurrency/4 {
			cfg.FTP.Concurrency = cfg.General.ObjectDiskServerSideCopyConcurrency
		}
		ftpStorage := &FTP{
			Config: &cfg.FTP,
		}
		if ftpStorage.Config.Path, err = ch.ApplyMacros(ctx, ftpStorage.Config.Path); err != nil {
			return nil, err
		}
		if ftpStorage.Config.ObjectDiskPath, err = ch.ApplyMacros(ctx, ftpStorage.Config.ObjectDiskPath); err != nil {
			return nil, err
		}
		return &BackupDestination{
			ftpStorage,
			cfg.FTP.CompressionFormat,
			cfg.FTP.CompressionLevel,
		}, nil
	case "sftp":
		sftpStorage := &SFTP{
			Config: &cfg.SFTP,
		}
		if sftpStorage.Config.Path, err = ch.ApplyMacros(ctx, sftpStorage.Config.Path); err != nil {
			return nil, err
		}
		if sftpStorage.Config.ObjectDiskPath, err = ch.ApplyMacros(ctx, sftpStorage.Config.ObjectDiskPath); err != nil {
			return nil, err
		}

		return &BackupDestination{
			sftpStorage,
			cfg.SFTP.CompressionFormat,
			cfg.SFTP.CompressionLevel,
		}, nil
	default:
		return nil, fmt.Errorf("NewBackupDestination error: storage type '%s' is not supported", cfg.General.RemoteStorage)
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
