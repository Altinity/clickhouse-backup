package chbackup

import (
	"archive/tar"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/mholt/archiver"
	"gopkg.in/djherbis/buffer.v1"
	"gopkg.in/djherbis/nio.v2"
)

const (
	// MetaFileName - meta file name
	MetaFileName = "meta.json"
	// BufferSize - size of ring buffer between stream handlers
	BufferSize = 4 * 1024 * 1024
)

// MetaFile - structure describe meta file that will be added to incremental backups archive.
// Contains info of required files in backup and files
type MetaFile struct {
	RequiredBackup string   `json:"required_backup"`
	Hardlinks      []string `json:"hardlinks"`
}

var (
	// ErrNotFound is returned when file/object cannot be found
	ErrNotFound = errors.New("file not found")
)

// RemoteFile - interface describe file on remote storage
type RemoteFile interface {
	Size() int64
	Name() string
	LastModified() time.Time
}

// RemoteStorage -
type RemoteStorage interface {
	Kind() string
	GetFile(string) (RemoteFile, error)
	DeleteFile(string) error
	Connect() error
	Walk(string, func(RemoteFile)) error
	GetFileReader(key string) (io.ReadCloser, error)
	PutFile(key string, r io.ReadCloser) error
}

type BackupDestination struct {
	RemoteStorage
	path               string
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
		if err := bd.RemoveBackup(backupToDelete.Name); err != nil {
			return err
		}
	}
	return nil
}

func (bd *BackupDestination) RemoveBackup(backupName string) error {
	objects := []string{}
	if err := bd.Walk(bd.path, func(f RemoteFile) {
		if strings.HasPrefix(f.Name(), path.Join(bd.path, backupName)) {
			objects = append(objects, f.Name())
		}
	}); err != nil {
		return err
	}
	for _, key := range objects {
		err := bd.DeleteFile(key)
		if err != nil {
			return err
		}
	}
	return nil
}

func (bd *BackupDestination) BackupsToKeep() int {
	return bd.backupsToKeep
}

func (bd *BackupDestination) BackupList() ([]Backup, error) {
	type ClickhouseBackup struct {
		Metadata bool
		Shadow   bool
		Tar      bool
		Size     int64
		Date     time.Time
	}
	files := map[string]ClickhouseBackup{}
	path := bd.path
	err := bd.Walk(path, func(o RemoteFile) {
		if strings.HasPrefix(o.Name(), path) {
			key := strings.TrimPrefix(o.Name(), path)
			key = strings.TrimPrefix(key, "/")
			parts := strings.Split(key, "/")
			if strings.HasSuffix(parts[0], ".tar") ||
				strings.HasSuffix(parts[0], ".tar.lz4") ||
				strings.HasSuffix(parts[0], ".tar.bz2") ||
				strings.HasSuffix(parts[0], ".tar.gz") ||
				strings.HasSuffix(parts[0], ".tar.sz") ||
				strings.HasSuffix(parts[0], ".tar.xz") {
				files[parts[0]] = ClickhouseBackup{
					Tar:  true,
					Date: o.LastModified(),
					Size: o.Size(),
				}
			}
			if len(parts) > 1 {
				b := files[parts[0]]
				files[parts[0]] = ClickhouseBackup{
					Metadata: b.Metadata || parts[1] == "metadata",
					Shadow:   b.Shadow || parts[1] == "shadow",
					Date:     b.Date,
					Size:     b.Size,
				}
			}
		}
	})
	if err != nil {
		return nil, err
	}
	result := []Backup{}
	for name, e := range files {
		if e.Metadata && e.Shadow || e.Tar {
			result = append(result, Backup{
				Name: name,
				Date: e.Date,
				Size: e.Size,
			})
		}
	}
	sort.SliceStable(result, func(i, j int) bool {
		return result[i].Date.Before(result[j].Date)
	})
	return result, nil
}

func (bd *BackupDestination) CompressedStreamDownload(remotePath string, localPath string) error {
	if err := os.MkdirAll(localPath, os.ModePerm); err != nil {
		return err
	}
	archiveName := path.Join(bd.path, fmt.Sprintf("%s.%s", remotePath, getExtension(bd.compressionFormat)))
	if err := bd.Connect(); err != nil {
		return err
	}

	reader, err := bd.GetFileReader(archiveName)
	if err != nil {
		return err
	}
	defer reader.Close()
	file, err := bd.GetFile(archiveName)
	if err != nil {
		return err
	}
	filesize := file.Size()

	bar := StartNewByteBar(!bd.disableProgressBar, filesize)
	buf := buffer.New(BufferSize)
	bufReader := nio.NewReader(reader, buf)
	proxyReader := bar.NewProxyReader(bufReader)
	z, _ := getArchiveReader(bd.compressionFormat)
	if err := z.Open(proxyReader, 0); err != nil {
		return err
	}
	defer z.Close()
	var metafile MetaFile
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
		if header.Name == MetaFileName {
			b, err := ioutil.ReadAll(file)
			if err != nil {
				return fmt.Errorf("can't read %s", MetaFileName)
			}
			if err := json.Unmarshal(b, &metafile); err != nil {
				return err
			}
			continue
		}
		extractFile := filepath.Join(localPath, header.Name)
		extractDir := filepath.Dir(extractFile)
		if _, err := os.Stat(extractDir); os.IsNotExist(err) {
			os.MkdirAll(extractDir, os.ModePerm)
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
	if metafile.RequiredBackup != "" {
		log.Printf("Backup '%s' required '%s'. Downloading.", remotePath, metafile.RequiredBackup)
		err := bd.CompressedStreamDownload(metafile.RequiredBackup, filepath.Join(filepath.Dir(localPath), metafile.RequiredBackup))
		if err != nil && !os.IsExist(err) {
			return fmt.Errorf("can't download '%s' with %v", metafile.RequiredBackup, err)
		}
	}
	for _, hardlink := range metafile.Hardlinks {
		newname := filepath.Join(localPath, hardlink)
		extractDir := filepath.Dir(newname)
		oldname := filepath.Join(filepath.Dir(localPath), metafile.RequiredBackup, hardlink)
		if _, err := os.Stat(extractDir); os.IsNotExist(err) {
			os.MkdirAll(extractDir, os.ModePerm)
		}
		if err := os.Link(oldname, newname); err != nil {
			return err
		}
	}
	bar.Finish()
	return nil
}

func (bd *BackupDestination) CompressedStreamUpload(localPath, remotePath, diffFromPath string) error {
	archiveName := path.Join(bd.path, fmt.Sprintf("%s.%s", remotePath, getExtension(bd.compressionFormat)))

	if _, err := bd.GetFile(archiveName); err != nil {
		if err != ErrNotFound {
			return err
		}
	}

	var totalBytes int64
	filepath.Walk(localPath, func(filePath string, info os.FileInfo, err error) error {
		if info.Mode().IsRegular() {
			totalBytes += info.Size()
		}
		return nil
	})
	bar := StartNewByteBar(!bd.disableProgressBar, totalBytes)
	if diffFromPath != "" {
		fi, err := os.Stat(diffFromPath)
		if err != nil {
			return err
		}
		if !fi.IsDir() {
			return fmt.Errorf("'%s' is not a directory", diffFromPath)
		}
		if isClickhouseShadow(filepath.Join(diffFromPath, "shadow")) {
			return fmt.Errorf("'%s' is old format backup and doesn't supports diff", filepath.Base(diffFromPath))
		}
	}
	hardlinks := []string{}

	buf := buffer.New(BufferSize)
	body, w := nio.Pipe(buf)
	go func() (ferr error) {
		defer w.CloseWithError(ferr)
		iobuf := buffer.New(BufferSize)
		z, _ := getArchiveWriter(bd.compressionFormat, bd.compressionLevel)
		if ferr = z.Create(w); ferr != nil {
			return
		}
		defer z.Close()
		if ferr = filepath.Walk(localPath, func(filePath string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !info.Mode().IsRegular() {
				return nil
			}
			bar.Add64(info.Size())
			file, err := os.Open(filePath)
			if err != nil {
				return err
			}
			defer file.Close()
			relativePath := strings.TrimPrefix(strings.TrimPrefix(filePath, localPath), "/")
			if diffFromPath != "" {
				diffFromFile, err := os.Stat(filepath.Join(diffFromPath, relativePath))
				if err == nil {
					if os.SameFile(info, diffFromFile) {
						hardlinks = append(hardlinks, relativePath)
						return nil
					}
				}
			}
			bfile := nio.NewReader(file, iobuf)
			defer bfile.Close()
			return z.Write(archiver.File{
				FileInfo: archiver.FileInfo{
					FileInfo:   info,
					CustomName: relativePath,
				},
				ReadCloser: bfile,
			})
		}); ferr != nil {
			return
		}
		if len(hardlinks) > 0 {
			metafile := MetaFile{
				RequiredBackup: filepath.Base(diffFromPath),
				Hardlinks:      hardlinks,
			}
			content, err := json.MarshalIndent(&metafile, "", "\t")
			if err != nil {
				ferr = fmt.Errorf("can't marshal json with %v", err)
				return
			}
			tmpfile, err := ioutil.TempFile("", MetaFileName)
			if err != nil {
				ferr = fmt.Errorf("can't create meta.info with %v", err)
				return
			}
			if _, err := tmpfile.Write(content); err != nil {
				ferr = fmt.Errorf("can't write to meta.info with %v", err)
				return
			}
			tmpfile.Close()
			tmpFileName := tmpfile.Name()
			defer os.Remove(tmpFileName)
			info, err := os.Stat(tmpFileName)
			if err != nil {
				ferr = fmt.Errorf("can't get stat with %v", err)
				return
			}
			mf, err := os.Open(tmpFileName)
			if err != nil {
				ferr = err
				return
			}
			defer mf.Close()
			if err := z.Write(archiver.File{
				FileInfo: archiver.FileInfo{
					FileInfo:   info,
					CustomName: MetaFileName,
				},
				ReadCloser: mf,
			}); err != nil {
				ferr = fmt.Errorf("can't add mata.json to archive with %v", err)
				return
			}
		}
		return
	}()

	if err := bd.PutFile(archiveName, body); err != nil {
		return err
	}
	bar.Finish()
	return nil
}

func NewBackupDestination(config Config) (*BackupDestination, error) {
	switch config.General.RemoteStorage {
	case "s3":
		s3 := &S3{Config: &config.S3}
		return &BackupDestination{
			s3,
			config.S3.Path,
			config.S3.CompressionFormat,
			config.S3.CompressionLevel,
			config.General.DisableProgressBar,
			config.General.BackupsToKeepRemote,
		}, nil
	case "gcs":
		gcs := &GCS{Config: &config.GCS}
		return &BackupDestination{
			gcs,
			config.GCS.Path,
			config.GCS.CompressionFormat,
			config.GCS.CompressionLevel,
			config.General.DisableProgressBar,
			config.General.BackupsToKeepRemote,
		}, nil
	case "cos":
		cos := &COS{Config: &config.COS}
		return &BackupDestination{
			cos,
			config.COS.Path,
			config.COS.CompressionFormat,
			config.COS.CompressionLevel,
			config.General.DisableProgressBar,
			config.General.BackupsToKeepRemote,
		}, nil
	default:
		return nil, fmt.Errorf("storage type '%s' not supported", config.General.RemoteStorage)
	}
}
