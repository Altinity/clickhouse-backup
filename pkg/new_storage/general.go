package new_storage

import (
	"archive/tar"
	"context"
	"encoding/json"
	"fmt"
	"github.com/AlexAkulov/clickhouse-backup/pkg/config"
	"github.com/AlexAkulov/clickhouse-backup/pkg/progressbar"
	"github.com/AlexAkulov/clickhouse-backup/pkg/utils"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/AlexAkulov/clickhouse-backup/pkg/metadata"
	"golang.org/x/sync/errgroup"

	apexLog "github.com/apex/log"
	"github.com/djherbis/buffer"
	"github.com/djherbis/nio/v3"
	"github.com/mholt/archiver/v3"
)

const (
	// BufferSize - size of ring buffer between stream handlers
	BufferSize = 4 * 1024 * 1024
)

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

func (bd *BackupDestination) RemoveOldBackups(keep int) error {
	if keep < 1 {
		return nil
	}
	start := time.Now()
	backupList, err := bd.BackupList(true, "")
	if err != nil {
		return err
	}
	backupsToDelete := GetBackupsToDelete(backupList, keep)
	apexLog.WithFields(apexLog.Fields{
		"operation": "RemoveOldBackups",
		"duration":  utils.HumanizeDuration(time.Since(start)),
	}).Info("calculate backup list for delete")
	for _, backupToDelete := range backupsToDelete {
		startDelete := time.Now()
		if err := bd.RemoveBackup(backupToDelete); err != nil {
			return err
		}
		apexLog.WithFields(apexLog.Fields{
			"operation": "RemoveOldBackups",
			"location":  "remote",
			"backup":    backupToDelete.BackupName,
			"duration":  utils.HumanizeDuration(time.Since(startDelete)),
		}).Info("done")
	}
	apexLog.WithFields(apexLog.Fields{"operation": "RemoveOldBackups", "duration": utils.HumanizeDuration(time.Since(start))}).Info("done")
	return nil
}

func (bd *BackupDestination) RemoveBackup(backup Backup) error {
	if bd.Kind() == "SFTP" || bd.Kind() == "FTP" {
		return bd.DeleteFile(backup.BackupName)
	}
	if backup.Legacy {
		archiveName := fmt.Sprintf("%s.%s", backup.BackupName, backup.FileExtension)
		return bd.DeleteFile(archiveName)
	}
	return bd.Walk(backup.BackupName+"/", true, func(f RemoteFile) error {
		return bd.DeleteFile(path.Join(backup.BackupName, f.Name()))
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

func (bd *BackupDestination) loadMetadataCache() map[string]Backup {
	listCacheFile := path.Join(os.TempDir(), fmt.Sprintf(".clickhouse-backup-metadata.cache.%s", bd.Kind()))
	listCache := map[string]Backup{}
	if info, err := os.Stat(listCacheFile); os.IsNotExist(err) || info.IsDir() {
		apexLog.Debugf("%s not found, load %d elements", listCacheFile, len(listCache))
		return listCache
	}
	f, err := os.Open(listCacheFile)
	if err != nil {
		apexLog.Warnf("can't open %s return error %v", listCacheFile, err)
		return listCache
	}
	body, err := ioutil.ReadAll(f)
	if err != nil {
		apexLog.Warnf("can't read %s return error %v", listCacheFile, err)
		return listCache
	}
	if err := f.Close(); err != nil { // Never use defer in loops
		apexLog.Warnf("can't close %s return error %v", listCacheFile, err)
		return listCache
	}
	if string(body) != "" {
		if err := json.Unmarshal(body, &listCache); err != nil {
			apexLog.Fatalf("can't parse %s to map[string]Backup\n\n%s\n\nreturn error %v", listCacheFile, body, err)
		}
	}
	apexLog.Debugf("%s load %d elements", listCacheFile, len(listCache))
	return listCache
}

func (bd *BackupDestination) saveMetadataCache(listCache map[string]Backup, actualList []Backup) {
	listCacheFile := path.Join(os.TempDir(), fmt.Sprintf(".clickhouse-backup-metadata.cache.%s", bd.Kind()))
	f, err := os.OpenFile(listCacheFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		apexLog.Warnf("can't open %s return error %v", listCacheFile, err)
		return
	}
	for backupName := range listCache {
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
	body, err := json.MarshalIndent(&listCache, "", "\t")
	if err != nil {
		apexLog.Warnf("can't json marshal %s return error %v", listCacheFile, err)
		return
	}
	_, err = f.Write(body)
	if err != nil {
		apexLog.Warnf("can't write to %s return error %v", listCacheFile, err)
	}
	apexLog.Debugf("%s save %d elements", listCacheFile, len(listCache))
	_ = f.Close()
}

func (bd *BackupDestination) BackupList(parseMetadata bool, parseMetadataOnly string) ([]Backup, error) {
	result := make([]Backup, 0)
	metadataCacheLock.Lock()
	defer metadataCacheLock.Unlock()
	listCache := bd.loadMetadataCache()
	err := bd.Walk("/", false, func(o RemoteFile) error {
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
		mf, err := bd.StatFile(path.Join(o.Name(), "metadata.json"))
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
			listCache[backupName] = brokenBackup
			result = append(result, brokenBackup)
			return nil
		}
		r, err := bd.GetFileReader(path.Join(o.Name(), "metadata.json"))
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
			listCache[backupName] = brokenBackup
			result = append(result, brokenBackup)
			return nil
		}
		b, err := ioutil.ReadAll(r)
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
			listCache[backupName] = brokenBackup
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
			listCache[backupName] = brokenBackup
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
		apexLog.Warnf("BackupList bd.Walk return error: %v", err)
	}
	sort.SliceStable(result, func(i, j int) bool {
		return result[i].UploadDate.Before(result[j].UploadDate)
	})
	bd.saveMetadataCache(listCache, result)
	return result, err
}

func (bd *BackupDestination) CompressedStreamDownload(remotePath string, localPath string) error {
	if err := os.MkdirAll(localPath, 0750); err != nil {
		return err
	}
	// get this first as GetFileReader blocks the ftp control channel
	file, err := bd.StatFile(remotePath)
	if err != nil {
		return err
	}
	filesize := file.Size()

	reader, err := bd.GetFileReader(remotePath)
	if err != nil {
		return err
	}
	defer func() {
		if err := reader.Close(); err != nil {
			apexLog.Warnf("can't close GetFileReader descriptor %v", reader)
		}
	}()

	bar := progressbar.StartNewByteBar(!bd.disableProgressBar, filesize)
	buf := buffer.New(BufferSize)
	defer bar.Finish()
	bufReader := nio.NewReader(reader, buf)
	proxyReader := bar.NewProxyReader(bufReader)
	compressionFormat := bd.compressionFormat
	if !strings.HasSuffix(path.Ext(remotePath), compressionFormat) {
		apexLog.Warnf("remote file backup extension %s not equal with %s", remotePath, compressionFormat)
		compressionFormat = strings.Replace(path.Ext(remotePath), ".", "", -1)
	}
	z, err := getArchiveReader(compressionFormat)
	if err != nil {
		return err
	}
	if err := z.Open(proxyReader, 0); err != nil {
		return err
	}
	defer func() {
		if err := z.Close(); err != nil {
			apexLog.Warnf("can't close getArchiveReader %v: %v", z, err)
		}
	}()
	for {
		file, err := z.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
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
		if _, err := io.Copy(dst, file); err != nil {
			return err
		}
		if err := dst.Close(); err != nil {
			return err
		}
		if err := file.Close(); err != nil {
			return err
		}
		//apexLog.Debugf("extract %s", extractFile)
	}
	return nil
}

func (bd *BackupDestination) CompressedStreamUpload(baseLocalPath string, files []string, remotePath string) error {
	if _, err := bd.StatFile(remotePath); err != nil {
		if err != ErrNotFound && !os.IsNotExist(err) {
			return err
		}
	}
	var totalBytes int64
	for _, filename := range files {
		finfo, err := os.Stat(path.Join(baseLocalPath, filename))
		if err != nil {
			return err
		}
		if finfo.Mode().IsRegular() {
			totalBytes += finfo.Size()
		}
	}
	bar := progressbar.StartNewByteBar(!bd.disableProgressBar, totalBytes)
	defer bar.Finish()
	pipeBuffer := buffer.New(BufferSize)
	body, w := nio.Pipe(pipeBuffer)
	g, _ := errgroup.WithContext(context.Background())

	g.Go(func() error {
		defer func() {
			if err := w.Close(); err != nil {
				apexLog.Warnf("can't close nio.Pipe writer %v", w)
			}
		}()
		localFileBuffer := buffer.New(BufferSize)
		z, err := getArchiveWriter(bd.compressionFormat, bd.compressionLevel)
		if err != nil {
			return err
		}
		if err := z.Create(w); err != nil {
			return err
		}
		defer func() {
			if err := z.Close(); err != nil {
				apexLog.Warnf("can't close getArchiveWriter %v: %v", z, err)
			}
		}()
		for _, f := range files {
			filePath := path.Join(baseLocalPath, f)
			info, err := os.Stat(filePath)
			if err != nil {
				return err
			}
			if !info.Mode().IsRegular() {
				continue
			}
			bar.Add64(info.Size())
			file, err := os.Open(filePath)
			if err != nil {
				return err
			}
			bfile := nio.NewReader(file, localFileBuffer)
			if err := z.Write(archiver.File{
				FileInfo: archiver.FileInfo{
					FileInfo:   info,
					CustomName: f,
				},
				ReadCloser: bfile,
			}); err != nil {
				return err
			}
			if err := bfile.Close(); err != nil { // No use defer for this
				return err
			}
			if err := file.Close(); err != nil { // No use defer for this too
				return err
			}
			//apexLog.Debugf("compress %s to %s", filePath, remotePath)
		}
		return nil
	})
	g.Go(func() error {
		return bd.PutFile(remotePath, body)
	})
	return g.Wait()
}

func (bd *BackupDestination) DownloadPath(size int64, remotePath string, localPath string) error {
	var bar *progressbar.Bar
	if !bd.disableProgressBar {
		totalBytes := size
		if size == 0 {
			if err := bd.Walk(remotePath, true, func(f RemoteFile) error {
				totalBytes += f.Size()
				return nil
			}); err != nil {
				return err
			}
		}
		bar = progressbar.StartNewByteBar(!bd.disableProgressBar, totalBytes)
		defer bar.Finish()
	}
	log := apexLog.WithFields(apexLog.Fields{
		"path":      remotePath,
		"operation": "download",
	})
	return bd.Walk(remotePath, true, func(f RemoteFile) error {
		// TODO: return err break download, think about make Walk error handle and retry
		r, err := bd.GetFileReader(path.Join(remotePath, f.Name()))
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
		if _, err := io.CopyBuffer(dst, r, nil); err != nil {
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
		if !bd.disableProgressBar {
			bar.Add64(f.Size())
		}
		return nil
	})
}

func (bd *BackupDestination) UploadPath(size int64, baseLocalPath string, files []string, remotePath string) error {
	var bar *progressbar.Bar
	if !bd.disableProgressBar {
		totalBytes := size
		if size == 0 {
			for _, filename := range files {
				finfo, err := os.Stat(path.Join(baseLocalPath, filename))
				if err != nil {
					return err
				}
				if finfo.Mode().IsRegular() {
					totalBytes += finfo.Size()
				}
			}
		}
		bar = progressbar.StartNewByteBar(!bd.disableProgressBar, totalBytes)
		defer bar.Finish()
	}

	for _, filename := range files {
		f, err := os.Open(path.Join(baseLocalPath, filename))
		if err != nil {
			return err
		}
		if err := bd.PutFile(path.Join(remotePath, filename), f); err != nil {
			return err
		}
		fi, err := f.Stat()
		if err != nil {
			return err
		}
		if !bd.disableProgressBar {
			bar.Add64(fi.Size())
		}
		if err = f.Close(); err != nil {
			apexLog.Warnf("can't close UploadPath file descriptor %v: %v", f, err)
		}
	}

	return nil
}

func NewBackupDestination(cfg *config.Config) (*BackupDestination, error) {
	switch cfg.General.RemoteStorage {
	case "azblob":
		azblobStorage := &AzureBlob{Config: &cfg.AzureBlob}
		bufferSize := azblobStorage.Config.BufferSize
		// https://github.com/AlexAkulov/clickhouse-backup/issues/317
		if bufferSize <= 0 {
			bufferSize = int(cfg.General.MaxFileSize / 10000)
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
			partSize = cfg.General.MaxFileSize / 10000
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
			BufferSize:  1024 * 1024,
			PartSize:    partSize,
		}
		return &BackupDestination{
			s3Storage,
			cfg.S3.CompressionFormat,
			cfg.S3.CompressionLevel,
			cfg.General.DisableProgressBar,
		}, nil
	case "gcs":
		googleCloudStorage := &GCS{Config: &cfg.GCS}
		return &BackupDestination{
			googleCloudStorage,
			cfg.GCS.CompressionFormat,
			cfg.GCS.CompressionLevel,
			cfg.General.DisableProgressBar,
		}, nil
	case "cos":
		tencentStorage := &COS{Config: &cfg.COS}
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
