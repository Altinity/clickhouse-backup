package new_storage

import (
	"archive/tar"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"github.com/AlexAkulov/clickhouse-backup/config"
	"github.com/AlexAkulov/clickhouse-backup/internal/progressbar"
	"github.com/AlexAkulov/clickhouse-backup/pkg/metadata"

	apexLog "github.com/apex/log"
	"github.com/mholt/archiver"
	"gopkg.in/djherbis/buffer.v1"
	"gopkg.in/djherbis/nio.v2"
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
}

type BackupDestination struct {
	RemoteStorage
	compressionFormat  string
	compressionLevel   int
	disableProgressBar bool
	backupsToKeep      int
}

func (bd *BackupDestination) RemoveOldBackups(keep int) error {
	if keep < 1 {
		return nil
	}
	backupList, err := bd.BackupList()
	if err != nil {
		return err
	}
	backupsToDelete := GetBackupsToDelete(backupList, keep)
	for _, backupToDelete := range backupsToDelete {
		if err := bd.RemoveBackup(backupToDelete.BackupName); err != nil {
			return err
		}
	}
	return nil
}

func (bd *BackupDestination) RemoveBackup(backupName string) error {
  if bd.Kind() == "SSH" {
    return bd.DeleteFile(backupName)
	}
	return bd.Walk(backupName+"/", true, func(f RemoteFile) error {
		return bd.DeleteFile(path.Join(backupName, f.Name()))
	})
}

func (bd *BackupDestination) BackupsToKeep() int {
	return bd.backupsToKeep
}

func isLegacyBackup(backupName string) (bool, string, string) {
	for _, suffix := range config.ArchiveExtensions {
		if strings.HasSuffix(backupName, "."+suffix) {
			return true, strings.TrimSuffix(backupName, "."+suffix), suffix
		}
	}
	return false, backupName, ""
}

func (bd *BackupDestination) BackupList() ([]Backup, error) {
	result := []Backup{}
	err := bd.Walk("/", false, func(o RemoteFile) error {
		// Legacy backup
		if ok, backupName, fileExtension := isLegacyBackup(strings.TrimPrefix(o.Name(), "/")); ok {
			result = append(result, Backup{
				metadata.BackupMetadata{
					BackupName:   backupName,
					CreationDate: o.LastModified(),
					DataSize:     o.Size(),
				},
				true,
				fileExtension,
				"",
			})
			return nil
		}
		r, err := bd.GetFileReader(path.Join(o.Name(), "metadata.json"))
		if err != nil {
			result = append(result, Backup{
				metadata.BackupMetadata{
					BackupName: strings.Trim(o.Name(), "/"),
				}, false, "", "broken (not found metadata.json)",
			})
			// TODO: после того как Walk будет нормально обрабатывать ошибки тут и дальше нужно вернуть err или залогировать их тут
			return nil
		}
		defer r.Close()
		b, err := ioutil.ReadAll(r)
		if err != nil {
			result = append(result, Backup{
				metadata.BackupMetadata{
					BackupName: strings.Trim(o.Name(), "/"),
				}, false, "", "broken (can't get metadata.json)",
			})
			return nil
		}
		var m metadata.BackupMetadata
		if err := json.Unmarshal(b, &m); err != nil {
			result = append(result, Backup{
				metadata.BackupMetadata{
					BackupName: strings.Trim(o.Name(), "/"),
				}, false, "", "broken (bad metadata.json)",
			})
			return nil
		}
		result = append(result, Backup{
			m, false, "", "",
		})
		return nil
	})
	sort.SliceStable(result, func(i, j int) bool {
		return result[i].CreationDate.Before(result[j].CreationDate)
	})
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
	defer reader.Close()

	bar := progressbar.StartNewByteBar(!bd.disableProgressBar, filesize)
	buf := buffer.New(BufferSize)
	defer bar.Finish()
	bufReader := nio.NewReader(reader, buf)
	proxyReader := bar.NewProxyReader(bufReader)
	z, _ := getArchiveReader(bd.compressionFormat)
	if err := z.Open(proxyReader, 0); err != nil {
		return err
	}
	defer z.Close()
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
			os.MkdirAll(extractDir, 0750)
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
	buf := buffer.New(BufferSize)
	body, w := nio.Pipe(buf)
	go func() (err error) {
		defer w.CloseWithError(err)
		iobuf := buffer.New(BufferSize)
		z, _ := getArchiveWriter(bd.compressionFormat, bd.compressionLevel)
		if err = z.Create(w); err != nil {
			return
		}
		defer z.Close()
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
			defer file.Close()
			bfile := nio.NewReader(file, iobuf)
			defer bfile.Close()
			if err := z.Write(archiver.File{
				FileInfo: archiver.FileInfo{
					FileInfo:   info,
					CustomName: f,
				},
				ReadCloser: bfile,
			}); err != nil {
				return err
			}
		}
		return
	}()

	if err := bd.PutFile(remotePath, body); err != nil {
		return err
	}
	return nil
}

func (bd *BackupDestination) DownloadPath(size int64, remotePath string, localPath string) error {
	totalBytes := size
	if size == 0 {
		if err := bd.Walk(remotePath, true, func(f RemoteFile) error {
			totalBytes += f.Size()
			return nil
		}); err != nil {
			return err
		}
	}
	bar := progressbar.StartNewByteBar(!bd.disableProgressBar, totalBytes)
	defer bar.Finish()
	log := apexLog.WithFields(apexLog.Fields{
		"path":      remotePath,
		"operation": "download",
	})
	return bd.Walk(remotePath, true, func(f RemoteFile) error {
		// TODO: return err приостанавливает загрузку, нужно научить Walk обратывать ошибки или добавить какие-то ретраи
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
		bar.Add64(f.Size())
		return nil
	})
}

func (bd *BackupDestination) UploadPath(size int64, baseLocalPath string, files []string, remotePath string) error {
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
	bar := progressbar.StartNewByteBar(!bd.disableProgressBar, totalBytes)
	defer bar.Finish()

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
		bar.Add64(fi.Size())
		f.Close()
	}

	return nil
}

func NewBackupDestination(cfg *config.Config) (*BackupDestination, error) {
	switch cfg.General.RemoteStorage {
	case "azblob":
		azblobStorage := &AzureBlob{Config: &cfg.AzureBlob}
		return &BackupDestination{
			azblobStorage,
			cfg.AzureBlob.CompressionFormat,
			cfg.AzureBlob.CompressionLevel,
			cfg.General.DisableProgressBar,
			cfg.General.BackupsToKeepRemote,
		}, nil
	case "s3":
		s3Storage := &S3{
			Config:     &cfg.S3,
			Concurence: 1,
			BufferSize: 1024 * 1024,
		}
		return &BackupDestination{
			s3Storage,
			cfg.S3.CompressionFormat,
			cfg.S3.CompressionLevel,
			cfg.General.DisableProgressBar,
			cfg.General.BackupsToKeepRemote,
		}, nil
	case "gcs":
		googleCloudStorage := &GCS{Config: &cfg.GCS}
		return &BackupDestination{
			googleCloudStorage,
			cfg.GCS.CompressionFormat,
			cfg.GCS.CompressionLevel,
			cfg.General.DisableProgressBar,
			cfg.General.BackupsToKeepRemote,
		}, nil
	case "cos":
		tencentStorage := &COS{
			Config: &cfg.COS,
			Debug:  cfg.General.LogLevel == "debug",
		}
		return &BackupDestination{
			tencentStorage,
			cfg.COS.CompressionFormat,
			cfg.COS.CompressionLevel,
			cfg.General.DisableProgressBar,
			cfg.General.BackupsToKeepRemote,
		}, nil
	case "ftp":
		ftpStorage := &FTP{
			Config: &cfg.FTP,
			Debug:  cfg.General.LogLevel == "debug",
		}
		return &BackupDestination{
			ftpStorage,
			cfg.FTP.CompressionFormat,
			cfg.FTP.CompressionLevel,
			cfg.General.DisableProgressBar,
			cfg.General.BackupsToKeepRemote,
		}, nil
	case "ssh":
		sshStorage := &SSH{
			Config: &cfg.SSH,
		}
		return &BackupDestination{
			sshStorage,
			cfg.SSH.CompressionFormat,
			cfg.SSH.CompressionLevel,
			cfg.General.DisableProgressBar,
			cfg.General.BackupsToKeepRemote,
		}, nil
	default:
		return nil, fmt.Errorf("storage type '%s' is not supported", cfg.General.RemoteStorage)
	}
}
