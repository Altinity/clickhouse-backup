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

	"github.com/Altinity/clickhouse-backup/pkg/clickhouse"
	"github.com/Altinity/clickhouse-backup/pkg/config"
	"github.com/Altinity/clickhouse-backup/pkg/metadata"
	"github.com/Altinity/clickhouse-backup/pkg/progressbar"
	"github.com/Altinity/clickhouse-backup/pkg/utils"
	"github.com/djherbis/buffer"
	"github.com/djherbis/nio/v3"
	"github.com/eapache/go-resiliency/retrier"
	"github.com/mholt/archiver/v4"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
)

const (
	// BufferSize - size of ring buffer between stream handlers
	BufferSize = 512 * 1024
)

type readerWrapperForContext func(p []byte) (n int, err error)

func (readerWrapper readerWrapperForContext) Read(p []byte) (n int, err error) {
	return readerWrapper(p)
}

type Backup struct {
	metadata.BackupMetadata
	Legacy        bool
	FileExtension string
	Broken        string
	UploadDate    time.Time
}

type BackupDestination struct {
	RemoteStorage
	compressionFormat  string
	compressionLevel   int
	disableProgressBar bool
}

var metadataCacheLock sync.RWMutex

func (bd *BackupDestination) RemoveOldBackups(ctx context.Context, keep int) error {
	if keep < 1 {
		return nil
	}
	start := time.Now()
	backupList, err := bd.BackupList(ctx, true, "")
	if err != nil {
		return err
	}
	backupsToDelete := GetBackupsToDelete(backupList, keep)
	log.Info().Fields(map[string]interface{}{
		"operation": "RemoveOldBackups",
		"duration":  utils.HumanizeDuration(time.Since(start)),
	}).Msg("calculate backup list for delete")
	for _, backupToDelete := range backupsToDelete {
		startDelete := time.Now()
		if err := bd.RemoveBackup(ctx, backupToDelete); err != nil {
			log.Warn().Msgf("can't delete %s return error : %v", backupToDelete.BackupName, err)
		}
		log.Info().Fields(map[string]interface{}{
			"operation": "RemoveOldBackups",
			"location":  "remote",
			"backup":    backupToDelete.BackupName,
			"duration":  utils.HumanizeDuration(time.Since(startDelete)),
		}).Msg("done")
	}
	log.Info().Fields(map[string]interface{}{
		"operation": "RemoveOldBackups",
		"duration":  utils.HumanizeDuration(time.Since(start)),
	}).Msg("done")
	return nil
}

func (bd *BackupDestination) RemoveBackup(ctx context.Context, backup Backup) error {
	if bd.Kind() == "SFTP" || bd.Kind() == "FTP" {
		return bd.DeleteFile(ctx, backup.BackupName)
	}
	if backup.Legacy {
		archiveName := fmt.Sprintf("%s.%s", backup.BackupName, backup.FileExtension)
		return bd.DeleteFile(ctx, archiveName)
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

func isLegacyBackup(backupName string) (bool, string, string) {
	for _, suffix := range config.ArchiveExtensions {
		if strings.HasSuffix(backupName, "."+suffix) {
			return true, strings.TrimSuffix(backupName, "."+suffix), suffix
		}
	}
	return false, backupName, ""
}

func (bd *BackupDestination) loadMetadataCache(ctx context.Context) (map[string]Backup, error) {
	listCacheFile := path.Join(os.TempDir(), fmt.Sprintf(".clickhouse-backup-metadata.cache.%s", bd.Kind()))
	listCache := map[string]Backup{}
	if info, err := os.Stat(listCacheFile); os.IsNotExist(err) || info.IsDir() {
		log.Debug().Msgf("%s not found, load %d elements", listCacheFile, len(listCache))
		return listCache, nil
	}
	f, err := os.Open(listCacheFile)
	if err != nil {
		log.Warn().Msgf("can't open %s return error %v", listCacheFile, err)
		return listCache, nil
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
	result := make([]Backup, 0)
	metadataCacheLock.Lock()
	defer metadataCacheLock.Unlock()
	listCache, err := bd.loadMetadataCache(ctx)
	if err != nil {
		return nil, err
	}
	err = bd.Walk(ctx, "/", false, func(ctx context.Context, o RemoteFile) error {
		// Legacy backup
		if ok, backupName, fileExtension := isLegacyBackup(strings.TrimPrefix(o.Name(), "/")); ok {
			result = append(result, Backup{
				metadata.BackupMetadata{
					BackupName: backupName,
					DataSize:   uint64(o.Size()),
				},
				true,
				fileExtension,
				"",
				o.LastModified(),
			})
			return nil
		}
		backupName := strings.Trim(o.Name(), "/")
		if !parseMetadata || (parseMetadataOnly != "" && parseMetadataOnly != backupName) {
			if cachedMetadata, isCached := listCache[backupName]; isCached {
				result = append(result, cachedMetadata)
			} else {
				result = append(result, Backup{
					BackupMetadata: metadata.BackupMetadata{
						BackupName: backupName,
					},
					Legacy: false,
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
				false,
				"",
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
				false,
				"",
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
				false,
				"",
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
				false,
				"",
				"broken (bad metadata.json)",
				o.LastModified(), // folder
			}
			result = append(result, brokenBackup)
			return nil
		}
		goodBackup := Backup{
			m, false, "", "", mf.LastModified(),
		}
		listCache[backupName] = goodBackup
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
	if err = bd.saveMetadataCache(ctx, listCache, result); err != nil {
		return nil, err
	}
	return result, err
}

func (bd *BackupDestination) DownloadCompressedStream(ctx context.Context, remotePath string, localPath string) error {
	if err := os.MkdirAll(localPath, 0750); err != nil {
		return err
	}
	// get this first as GetFileReader blocks the ftp control channel
	file, err := bd.StatFile(ctx, remotePath)
	if err != nil {
		return err
	}
	filesize := file.Size()

	reader, err := bd.GetFileReaderWithLocalPath(ctx, remotePath, localPath)
	if err != nil {
		return err
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

	bar := progressbar.StartNewByteBar(!bd.disableProgressBar, filesize)
	buf := buffer.New(BufferSize)
	defer bar.Finish()
	bufReader := nio.NewReader(reader, buf)
	proxyReader := bar.NewProxyReader(bufReader)
	compressionFormat := bd.compressionFormat
	if !checkArchiveExtension(path.Ext(remotePath), compressionFormat) {
		log.Warn().Msgf("remote file backup extension %s not equal with %s", remotePath, compressionFormat)
		compressionFormat = strings.Replace(path.Ext(remotePath), ".", "", -1)
	}
	z, err := getArchiveReader(compressionFormat)
	if err != nil {
		return err
	}
	if err := z.Extract(ctx, proxyReader, nil, func(ctx context.Context, file archiver.File) error {
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
		//log.Debug().Msgf("extract %s", extractFile)
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func (bd *BackupDestination) UploadCompressedStream(ctx context.Context, baseLocalPath string, files []string, remotePath string) error {
	if _, err := bd.StatFile(ctx, remotePath); err != nil {
		if err != ErrNotFound && !os.IsNotExist(err) {
			return err
		}
	}
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
	bar := progressbar.StartNewByteBar(!bd.disableProgressBar, totalBytes)
	defer bar.Finish()
	pipeBuffer := buffer.New(BufferSize)
	body, w := nio.Pipe(pipeBuffer)
	g, ctx := errgroup.WithContext(ctx)

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
			bar.Add64(info.Size())
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
		readerErr = bd.PutFile(ctx, remotePath, body)
		return readerErr
	})
	return g.Wait()
}

func (bd *BackupDestination) DownloadPath(ctx context.Context, size int64, remotePath string, localPath string, RetriesOnFailure int, RetriesDuration time.Duration) error {
	var bar *progressbar.Bar
	if !bd.disableProgressBar {
		totalBytes := size
		if size == 0 {
			if err := bd.Walk(ctx, remotePath, true, func(ctx context.Context, f RemoteFile) error {
				totalBytes += f.Size()
				return nil
			}); err != nil {
				return err
			}
		}
		bar = progressbar.StartNewByteBar(!bd.disableProgressBar, totalBytes)
		defer bar.Finish()
	}
	return bd.Walk(ctx, remotePath, true, func(ctx context.Context, f RemoteFile) error {
		if bd.Kind() == "SFTP" && (f.Name() == "." || f.Name() == "..") {
			return nil
		}
		retry := retrier.New(retrier.ConstantBackoff(RetriesOnFailure, RetriesDuration), nil)
		err := retry.RunCtx(ctx, func(ctx context.Context) error {
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
			if _, err := io.CopyBuffer(dst, r, nil); err != nil {
				log.Error().Err(err).Send()
				return err
			}
			if err := dst.Close(); err != nil {
				log.Error().Err(err).Send()
				return err
			}
			if err := r.Close(); err != nil {
				log.Error().Err(err).Send()
				return err
			}
			return nil
		})
		if err != nil {
			return err
		}
		if !bd.disableProgressBar {
			bar.Add64(f.Size())
		}
		return nil
	})
}

func (bd *BackupDestination) UploadPath(ctx context.Context, size int64, baseLocalPath string, files []string, remotePath string, RetriesOnFailure int, RetriesDuration time.Duration) (int64, error) {
	var bar *progressbar.Bar
	totalBytes := size
	if size == 0 {
		for _, filename := range files {
			fInfo, err := os.Stat(filepath.Clean(path.Join(baseLocalPath, filename)))
			if err != nil {
				return 0, err
			}
			if fInfo.Mode().IsRegular() {
				totalBytes += fInfo.Size()
			}
		}
	}
	if !bd.disableProgressBar {
		bar = progressbar.StartNewByteBar(!bd.disableProgressBar, totalBytes)
		defer bar.Finish()
	}

	for _, filename := range files {
		f, err := os.Open(filepath.Clean(path.Join(baseLocalPath, filename)))
		if err != nil {
			return 0, err
		}
		closeFile := func() {
			if err := f.Close(); err != nil {
				log.Warn().Msgf("can't close UploadPath file descriptor %v: %v", f, err)
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
		fi, err := f.Stat()
		if err != nil {
			return 0, err
		}
		if !bd.disableProgressBar {
			bar.Add64(fi.Size())
		}
		closeFile()
	}

	return totalBytes, nil
}

func NewBackupDestination(ctx context.Context, cfg *config.Config, ch *clickhouse.ClickHouse, calcMaxSize bool, backupName string) (*BackupDestination, error) {
	var err error
	// https://github.com/Altinity/clickhouse-backup/issues/404
	if calcMaxSize {
		maxFileSize, err := ch.CalculateMaxFileSize(ctx, cfg)
		if err != nil {
			return nil, err
		}
		if cfg.General.MaxFileSize > 0 && cfg.General.MaxFileSize < maxFileSize {
			log.Warn().Msgf("MAX_FILE_SIZE=%d is less than actual %d, please remove general->max_file_size section from your config", cfg.General.MaxFileSize, maxFileSize)
		}
		if cfg.General.MaxFileSize <= 0 || cfg.General.MaxFileSize < maxFileSize {
			cfg.General.MaxFileSize = maxFileSize
		}
	}
	switch cfg.General.RemoteStorage {
	case "azblob":
		azblobStorage := &AzureBlob{Config: &cfg.AzureBlob}
		azblobStorage.Config.Path, err = ch.ApplyMacros(ctx, azblobStorage.Config.Path)
		if err != nil {
			return nil, err
		}

		bufferSize := azblobStorage.Config.BufferSize
		// https://github.com/Altinity/clickhouse-backup/issues/317
		if bufferSize <= 0 {
			bufferSize = int(cfg.General.MaxFileSize) / cfg.AzureBlob.MaxPartsCount
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
			cfg.AzureBlob.CompressionFormat,
			cfg.AzureBlob.CompressionLevel,
			cfg.General.DisableProgressBar,
		}, nil
	case "s3":
		partSize := cfg.S3.PartSize
		if cfg.S3.PartSize <= 0 {
			partSize = cfg.General.MaxFileSize / cfg.S3.MaxPartsCount
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
			BufferSize:  512 * 1024,
			PartSize:    partSize,
		}
		s3Storage.Config.Path, err = ch.ApplyMacros(ctx, s3Storage.Config.Path)
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
			cfg.S3.CompressionFormat,
			cfg.S3.CompressionLevel,
			cfg.General.DisableProgressBar,
		}, nil
	case "gcs":
		googleCloudStorage := &GCS{Config: &cfg.GCS}
		googleCloudStorage.Config.Path, err = ch.ApplyMacros(ctx, googleCloudStorage.Config.Path)
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
			cfg.GCS.CompressionFormat,
			cfg.GCS.CompressionLevel,
			cfg.General.DisableProgressBar,
		}, nil
	case "cos":
		tencentStorage := &COS{Config: &cfg.COS}
		tencentStorage.Config.Path, err = ch.ApplyMacros(ctx, tencentStorage.Config.Path)
		if err != nil {
			return nil, err
		}
		return &BackupDestination{
			tencentStorage,
			cfg.COS.CompressionFormat,
			cfg.COS.CompressionLevel,
			cfg.General.DisableProgressBar,
		}, nil
	case "ftp":
		ftpStorage := &FTP{
			Config: &cfg.FTP,
		}
		ftpStorage.Config.Path, err = ch.ApplyMacros(ctx, ftpStorage.Config.Path)
		if err != nil {
			return nil, err
		}
		return &BackupDestination{
			ftpStorage,
			cfg.FTP.CompressionFormat,
			cfg.FTP.CompressionLevel,
			cfg.General.DisableProgressBar,
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
			cfg.SFTP.CompressionFormat,
			cfg.SFTP.CompressionLevel,
			cfg.General.DisableProgressBar,
		}, nil
	default:
		return nil, fmt.Errorf("storage type '%s' is not supported", cfg.General.RemoteStorage)
	}
}

// ApplyMacrosToObjectLabels - https://github.com/Altinity/clickhouse-backup/issues/588
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
