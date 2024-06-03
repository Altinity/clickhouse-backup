package storage

import (
	"archive/tar"
	"context"
	"encoding/json"
	"fmt"
	"github.com/Altinity/clickhouse-backup/v2/pkg/clickhouse"
	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/eapache/go-resiliency/retrier"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/metadata"
	"golang.org/x/sync/errgroup"

	apexLog "github.com/apex/log"
	"github.com/djherbis/buffer"
	"github.com/djherbis/nio/v3"
	"github.com/mholt/archiver/v4"
)

const (
	// BufferSize - size of ring buffer between stream handlers
	BufferSize = 128 * 1024
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
	Log               *apexLog.Entry
	compressionFormat string
	compressionLevel  int
}

var metadataCacheLock sync.RWMutex

func (bd *BackupDestination) RemoveBackupRemote(ctx context.Context, backup Backup) error {
	if bd.Kind() == "SFTP" || bd.Kind() == "FTP" {
		return bd.DeleteFile(ctx, backup.BackupName)
	}
	return bd.Walk(ctx, backup.BackupName+"/", true, func(ctx context.Context, f RemoteFile) error {
		if bd.Kind() == "azblob" {
			if f.Size() > 0 || !f.LastModified().IsZero() {
				return bd.DeleteFile(ctx, path.Join(backup.BackupName, f.Name()))
			} else {
				return nil
			}
		}
		return bd.DeleteFile(ctx, path.Join(backup.BackupName, f.Name()))
	})
}

func (bd *BackupDestination) loadMetadataCache(ctx context.Context) (map[string]Backup, error) {
	listCacheFile := path.Join(os.TempDir(), fmt.Sprintf(".clickhouse-backup-metadata.cache.%s", bd.Kind()))
	listCache := map[string]Backup{}
	if info, err := os.Stat(listCacheFile); os.IsNotExist(err) || info.IsDir() {
		bd.Log.Debugf("%s not found, load %d elements", listCacheFile, len(listCache))
		return listCache, nil
	}
	f, err := os.Open(listCacheFile)
	if err != nil {
		bd.Log.Warnf("can't open %s return error %v", listCacheFile, err)
		return listCache, nil
	}
	defer func() {
		if err := f.Close(); err != nil {
			bd.Log.Warnf("can't close %s return error %v", listCacheFile, err)
		}
	}()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		body, err := io.ReadAll(f)
		if err != nil {
			bd.Log.Warnf("can't read %s return error %v", listCacheFile, err)
			return listCache, nil
		}
		if string(body) != "" {
			if err := json.Unmarshal(body, &listCache); err != nil {
				bd.Log.Fatalf("can't parse %s to map[string]Backup\n\n%s\n\nreturn error %v", listCacheFile, body, err)
			}
		}
		bd.Log.Debugf("%s load %d elements", listCacheFile, len(listCache))
		return listCache, nil
	}
}

func (bd *BackupDestination) saveMetadataCache(ctx context.Context, listCache map[string]Backup, actualList []Backup) error {
	listCacheFile := path.Join(os.TempDir(), fmt.Sprintf(".clickhouse-backup-metadata.cache.%s", bd.Kind()))
	f, err := os.OpenFile(listCacheFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		bd.Log.Warnf("can't open %s return error %v", listCacheFile, err)
		return nil
	}
	defer func() {
		if err := f.Close(); err != nil {
			bd.Log.Warnf("can't close %s return error %v", listCacheFile, err)
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
			bd.Log.Warnf("can't json marshal %s return error %v", listCacheFile, err)
			return nil
		}
		_, err = f.Write(body)
		if err != nil {
			bd.Log.Warnf("can't write to %s return error %v", listCacheFile, err)
			return nil
		}
		bd.Log.Debugf("%s save %d elements", listCacheFile, len(listCache))
		return nil
	}
}

func (bd *BackupDestination) BackupList(ctx context.Context, parseMetadata bool, parseMetadataOnly string) ([]Backup, error) {
	result := make([]Backup, 0)
	metadataCacheLock.Lock()
	defer metadataCacheLock.Unlock()
	listCache, err := bd.loadMetadataCache(ctx)
	if err != nil {
		return nil, err
	}
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
		result = append(result, goodBackup)
		return nil
	})
	if err != nil {
		bd.Log.Warnf("BackupList bd.Walk return error: %v", err)
	}
	// sort by name for the same not parsed metadata.json
	sort.SliceStable(result, func(i, j int) bool {
		return result[i].BackupName < result[j].BackupName
	})
	sort.SliceStable(result, func(i, j int) bool {
		return result[i].UploadDate.Before(result[j].UploadDate)
	})
	if err = bd.saveMetadataCache(ctx, listCache, result); err != nil {
		return nil, fmt.Errorf("bd.saveMetadataCache return error: %v", err)
	}
	return result, nil
}

func (bd *BackupDestination) DownloadCompressedStream(ctx context.Context, remotePath string, localPath string, maxSpeed uint64) error {
	if err := os.MkdirAll(localPath, 0750); err != nil {
		return err
	}
	// get this first as GetFileReader blocks the ftp control channel
	remoteFileInfo, err := bd.StatFile(ctx, remotePath)
	if err != nil {
		return err
	}
	startTime := time.Now()
	reader, err := bd.GetFileReaderWithLocalPath(ctx, remotePath, localPath)
	if err != nil {
		return err
	}
	defer func() {
		if err := reader.Close(); err != nil {
			bd.Log.Warnf("can't close GetFileReader descriptor %v", reader)
		}
		switch reader.(type) {
		case *os.File:
			fileName := reader.(*os.File).Name()
			if err := os.Remove(fileName); err != nil {
				bd.Log.Warnf("can't remove %s", fileName)
			}
		}
	}()

	buf := buffer.New(BufferSize)
	bufReader := nio.NewReader(reader, buf)
	compressionFormat := bd.compressionFormat
	if !checkArchiveExtension(path.Ext(remotePath), compressionFormat) {
		bd.Log.Warnf("remote file backup extension %s not equal with %s", remotePath, compressionFormat)
		compressionFormat = strings.Replace(path.Ext(remotePath), ".", "", -1)
	}
	z, err := getArchiveReader(compressionFormat)
	if err != nil {
		return err
	}
	if err := z.Extract(ctx, bufReader, nil, func(ctx context.Context, file archiver.File) error {
		f, err := file.Open()
		if err != nil {
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
		dst, err := os.Create(extractFile)
		if err != nil {
			return err
		}
		if _, err := io.Copy(dst, readerWrapperForContext(func(p []byte) (int, error) {
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			default:
				return f.Read(p)
			}
		})); err != nil {
			return err
		}
		if err := dst.Close(); err != nil {
			return err
		}
		if err := f.Close(); err != nil {
			return err
		}
		//bd.Log.Debugf("extract %s", extractFile)
		return nil
	}); err != nil {
		return err
	}
	bd.throttleSpeed(startTime, remoteFileInfo.Size(), maxSpeed)
	return nil
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
	pipeBuffer := buffer.New(BufferSize)
	body, w := nio.Pipe(pipeBuffer)
	g, ctx := errgroup.WithContext(ctx)
	startTime := time.Now()
	var writerErr, readerErr error
	g.Go(func() error {
		defer func() {
			if writerErr != nil {
				if err := w.CloseWithError(writerErr); err != nil {
					bd.Log.Errorf("can't close after error %v pipe writer error: %v", writerErr, err)
				}
			} else {
				if err := w.Close(); err != nil {
					bd.Log.Errorf("can't close pipe writer: %v", err)
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
			//bd.Log.Debugf("add %s to archive %s", filePath, remotePath)
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
					bd.Log.Errorf("can't close after error %v pipe reader error: %v", writerErr, err)
				}
			} else {
				if err := body.Close(); err != nil {
					bd.Log.Errorf("can't close pipe reader: %v", err)
				}
			}
		}()
		readerErr = bd.PutFile(ctx, remotePath, body)
		return readerErr
	})
	if waitErr := g.Wait(); waitErr != nil {
		return waitErr
	}
	bd.throttleSpeed(startTime, totalBytes, maxSpeed)
	return nil
}

func (bd *BackupDestination) DownloadPath(ctx context.Context, remotePath string, localPath string, RetriesOnFailure int, RetriesDuration time.Duration, maxSpeed uint64) error {
	log := bd.Log.WithFields(apexLog.Fields{
		"path":      remotePath,
		"operation": "download",
	})
	return bd.Walk(ctx, remotePath, true, func(ctx context.Context, f RemoteFile) error {
		if bd.Kind() == "SFTP" && (f.Name() == "." || f.Name() == "..") {
			return nil
		}
		retry := retrier.New(retrier.ConstantBackoff(RetriesOnFailure, RetriesDuration), nil)
		err := retry.RunCtx(ctx, func(ctx context.Context) error {
			startTime := time.Now()
			r, err := bd.GetFileReader(ctx, path.Join(remotePath, f.Name()))
			if err != nil {
				log.Error(err.Error())
				return err
			}
			dstFilePath := path.Join(localPath, f.Name())
			dstDirPath, _ := path.Split(dstFilePath)
			if err := os.MkdirAll(dstDirPath, 0750); err != nil {
				log.Error(err.Error())
				return err
			}
			dst, err := os.Create(dstFilePath)
			if err != nil {
				log.Error(err.Error())
				return err
			}
			if _, err := io.Copy(dst, r); err != nil {
				log.Error(err.Error())
				return err
			}
			if err := dst.Close(); err != nil {
				log.Error(err.Error())
				return err
			}
			if err := r.Close(); err != nil {
				log.Error(err.Error())
				return err
			}

			if dstFileInfo, err := os.Stat(dstFilePath); err == nil {
				bd.throttleSpeed(startTime, dstFileInfo.Size(), maxSpeed)
			} else {
				return err
			}

			return nil
		})
		if err != nil {
			return err
		}
		return nil
	})
}

func (bd *BackupDestination) UploadPath(ctx context.Context, baseLocalPath string, files []string, remotePath string, RetriesOnFailure int, RetriesDuration time.Duration, maxSpeed uint64) (int64, error) {
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
				bd.Log.Warnf("can't close UploadPath file descriptor %v: %v", f, err)
			}
		}
		retry := retrier.New(retrier.ConstantBackoff(RetriesOnFailure, RetriesDuration), nil)
		err = retry.RunCtx(ctx, func(ctx context.Context) error {
			return bd.PutFile(ctx, path.Join(remotePath, filename), f)
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

func NewBackupDestination(ctx context.Context, cfg *config.Config, ch *clickhouse.ClickHouse, calcMaxSize bool, backupName string) (*BackupDestination, error) {
	log := apexLog.WithField("logger", "NewBackupDestination")
	var err error
	// https://github.com/Altinity/clickhouse-backup/issues/404
	if calcMaxSize {
		maxFileSize, err := ch.CalculateMaxFileSize(ctx, cfg)
		if err != nil {
			return nil, err
		}
		if cfg.General.MaxFileSize > 0 && cfg.General.MaxFileSize < maxFileSize {
			log.Warnf("MAX_FILE_SIZE=%d is less than actual %d, please remove general->max_file_size section from your config", cfg.General.MaxFileSize, maxFileSize)
		}
		if cfg.General.MaxFileSize <= 0 || cfg.General.MaxFileSize < maxFileSize {
			cfg.General.MaxFileSize = maxFileSize
		}
	}
	switch cfg.General.RemoteStorage {
	case "azblob":
		azblobStorage := &AzureBlob{
			Config: &cfg.AzureBlob,
			Log:    log.WithField("logger", "AZBLOB"),
		}
		azblobStorage.Config.Path, err = ch.ApplyMacros(ctx, azblobStorage.Config.Path)
		if err != nil {
			return nil, err
		}
		azblobStorage.Config.ObjectDiskPath, err = ch.ApplyMacros(ctx, azblobStorage.Config.ObjectDiskPath)
		if err != nil {
			return nil, err
		}

		bufferSize := azblobStorage.Config.BufferSize
		// https://github.com/Altinity/clickhouse-backup/issues/317
		if bufferSize <= 0 {
			bufferSize = int(cfg.General.MaxFileSize) / cfg.AzureBlob.MaxPartsCount
			if int(cfg.General.MaxFileSize)%cfg.AzureBlob.MaxPartsCount > 0 {
				bufferSize += int(cfg.General.MaxFileSize) % cfg.AzureBlob.MaxPartsCount
			}
			if bufferSize < 2*1024*1024 {
				bufferSize = 2 * 1024 * 1024
			}
			if bufferSize > 10*1024*1024 {
				bufferSize = 10 * 1024 * 1024
			}
		}
		azblobStorage.Config.BufferSize = bufferSize
		return &BackupDestination{
			azblobStorage,
			log.WithField("logger", "azure"),
			cfg.AzureBlob.CompressionFormat,
			cfg.AzureBlob.CompressionLevel,
		}, nil
	case "s3":
		partSize := cfg.S3.PartSize
		if cfg.S3.PartSize <= 0 {
			partSize = cfg.General.MaxFileSize / cfg.S3.MaxPartsCount
			if cfg.General.MaxFileSize%cfg.S3.MaxPartsCount > 0 {
				partSize++
			}
			if partSize < 5*1024*1024 {
				partSize = 5 * 1024 * 1024
			}
			if partSize > 5*1024*1024*1024 {
				partSize = 5 * 1024 * 1024 * 1024
			}
		}
		s3Storage := &S3{
			Config:      &cfg.S3,
			Concurrency: cfg.S3.Concurrency,
			BufferSize:  128 * 1024,
			PartSize:    partSize,
			Log:         log.WithField("logger", "S3"),
		}
		s3Storage.Config.Path, err = ch.ApplyMacros(ctx, s3Storage.Config.Path)
		if err != nil {
			return nil, err
		}
		s3Storage.Config.ObjectDiskPath, err = ch.ApplyMacros(ctx, s3Storage.Config.ObjectDiskPath)
		if err != nil {
			return nil, err
		}
		// https://github.com/Altinity/clickhouse-backup/issues/588
		if len(s3Storage.Config.ObjectLabels) > 0 && backupName != "" {
			objectLabels := s3Storage.Config.ObjectLabels
			objectLabels, err = ApplyMacrosToObjectLabels(ctx, objectLabels, ch, backupName)
			if err != nil {
				return nil, err
			}
			s3Storage.Config.ObjectLabels = objectLabels
		}
		return &BackupDestination{
			s3Storage,
			log.WithField("logger", "s3"),
			cfg.S3.CompressionFormat,
			cfg.S3.CompressionLevel,
		}, nil
	case "gcs":
		googleCloudStorage := &GCS{Config: &cfg.GCS}
		googleCloudStorage.Config.Path, err = ch.ApplyMacros(ctx, googleCloudStorage.Config.Path)
		if err != nil {
			return nil, err
		}
		googleCloudStorage.Config.ObjectDiskPath, err = ch.ApplyMacros(ctx, googleCloudStorage.Config.ObjectDiskPath)
		if err != nil {
			return nil, err
		}
		// https://github.com/Altinity/clickhouse-backup/issues/588
		if len(googleCloudStorage.Config.ObjectLabels) > 0 && backupName != "" {
			objectLabels := googleCloudStorage.Config.ObjectLabels
			objectLabels, err = ApplyMacrosToObjectLabels(ctx, objectLabels, ch, backupName)
			if err != nil {
				return nil, err
			}
			googleCloudStorage.Config.ObjectLabels = objectLabels
		}
		return &BackupDestination{
			googleCloudStorage,
			log.WithField("logger", "gcs"),
			cfg.GCS.CompressionFormat,
			cfg.GCS.CompressionLevel,
		}, nil
	case "cos":
		tencentStorage := &COS{Config: &cfg.COS}
		tencentStorage.Config.Path, err = ch.ApplyMacros(ctx, tencentStorage.Config.Path)
		if err != nil {
			return nil, err
		}
		return &BackupDestination{
			tencentStorage,
			log.WithField("logger", "cos"),
			cfg.COS.CompressionFormat,
			cfg.COS.CompressionLevel,
		}, nil
	case "ftp":
		ftpStorage := &FTP{
			Config: &cfg.FTP,
			Log:    log.WithField("logger", "FTP"),
		}
		ftpStorage.Config.Path, err = ch.ApplyMacros(ctx, ftpStorage.Config.Path)
		if err != nil {
			return nil, err
		}
		return &BackupDestination{
			ftpStorage,
			log.WithField("logger", "FTP"),
			cfg.FTP.CompressionFormat,
			cfg.FTP.CompressionLevel,
		}, nil
	case "sftp":
		sftpStorage := &SFTP{
			Config: &cfg.SFTP,
		}
		sftpStorage.Config.Path, err = ch.ApplyMacros(ctx, sftpStorage.Config.Path)
		if err != nil {
			return nil, err
		}
		return &BackupDestination{
			sftpStorage,
			log.WithField("logger", "SFTP"),
			cfg.SFTP.CompressionFormat,
			cfg.SFTP.CompressionLevel,
		}, nil
	default:
		return nil, fmt.Errorf("NewBackupDestination error: storage type '%s' is not supported", cfg.General.RemoteStorage)
	}
}

// ApplyMacrosToObjectLabels https://github.com/Altinity/clickhouse-backup/issues/588
func ApplyMacrosToObjectLabels(ctx context.Context, objectLabels map[string]string, ch *clickhouse.ClickHouse, backupName string) (map[string]string, error) {
	var err error
	for k, v := range objectLabels {
		v, err = ch.ApplyMacros(ctx, v)
		if err != nil {
			return nil, err
		}
		r := strings.NewReplacer("{backup}", backupName, "{backupName}", backupName, "{backup_name}", backupName, "{BACKUP_NAME}", backupName)
		objectLabels[k] = r.Replace(v)
	}
	return objectLabels, nil
}
