package main

import (
	"archive/tar"
	"context"
	"encoding/json"
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

	"cloud.google.com/go/storage"
	"github.com/mholt/archiver"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"gopkg.in/djherbis/buffer.v1"
	"gopkg.in/djherbis/nio.v2"
)

// GCS - presents methods for manipulate data on GCS
type GCS struct {
	client *storage.Client
	Config *GCSConfig
}

// Connect - connect to GCS
func (gcs *GCS) Connect() error {
	var err error

	ctx := context.Background()
	gcs.client, err = storage.NewClient(ctx, option.WithCredentialsFile(gcs.Config.CredentialsFile))
	if err != nil {
		return err
	}

	return nil
}

func (gcs *GCS) CompressedStreamDownload(gcsPath, localPath string) error {
	if err := os.Mkdir(localPath, os.ModePerm); err != nil {
		return err
	}
	archiveName := path.Join(gcs.Config.Path, fmt.Sprintf("%s.%s", gcsPath, getExtension(gcs.Config.CompressionFormat)))

	filesize, err := gcs.GetSize(archiveName)
	if err != nil {
		return err
	}

	ctx := context.Background()
	obj := gcs.client.Bucket(gcs.Config.Bucket).Object(archiveName)
	reader, err := obj.NewReader(ctx)
	if err != nil {
		return err
	}

	bar := StartNewByteBar(gcs.Config.DisableProgressBar, filesize)
	buf := buffer.New(BufferSize)
	bufReader := nio.NewReader(reader, buf)
	proxyReader := bar.NewProxyReader(bufReader)
	z, _ := getArchiveReader(gcs.Config.CompressionFormat)
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
		log.Printf("Backup '%s' required '%s'. Downloading.", gcsPath, metafile.RequiredBackup)
		err := gcs.CompressedStreamDownload(metafile.RequiredBackup, filepath.Join(filepath.Dir(localPath), metafile.RequiredBackup))
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

func (gcs *GCS) CompressedStreamUpload(localPath, gcsPath, diffFromPath string) error {
	archiveName := path.Join(gcs.Config.Path, fmt.Sprintf("%s.%s", gcsPath, getExtension(gcs.Config.CompressionFormat)))
	ctx := context.Background()
	_, err := gcs.client.Bucket(gcs.Config.Bucket).Object(archiveName).Attrs(ctx)
	if err != nil {
		if err != storage.ErrObjectNotExist {
			return fmt.Errorf("'GCS: %s'", err.Error())
		}
	} else {
		return fmt.Errorf("'%s' already uploaded", archiveName)
	}

	var totalBytes int64

	filepath.Walk(localPath, func(filePath string, info os.FileInfo, err error) error {
		if info.Mode().IsRegular() {
			totalBytes = totalBytes + info.Size()
		}
		return nil
	})
	bar := StartNewByteBar(!gcs.Config.DisableProgressBar, totalBytes)
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
		z, _ := getArchiveWriter(gcs.Config.CompressionFormat, gcs.Config.CompressionLevel)
		if ferr = z.Create(w); ferr != nil {
			return
		}
		defer z.Close()
		if ferr = filepath.Walk(localPath, func(filePath string, info os.FileInfo, err error) error {
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
	obj := gcs.client.Bucket(gcs.Config.Bucket).Object(archiveName)
	writer := obj.NewWriter(ctx)
	if _, err := io.Copy(writer, body); err != nil {
		return err
	}

	if err := writer.Close(); err != nil {
		return err
	}

	bar.Finish()
	return nil
}

func (gcs *GCS) BackupList() ([]Backup, error) {
	type gcsBackup struct {
		Metadata bool
		Shadow   bool
		Tar      bool
		Size     int64
		Date     time.Time
	}
	gcsFiles := map[string]gcsBackup{}
	err := gcs.remoteIterator(gcs.Config.Path, false, func(objAttr *storage.ObjectAttrs) {
		if strings.HasPrefix(objAttr.Name, gcs.Config.Path) {
			key := strings.TrimPrefix(objAttr.Name, gcs.Config.Path)
			key = strings.TrimPrefix(key, "/")
			parts := strings.Split(key, "/")
			if strings.HasSuffix(parts[0], ".tar") ||
				strings.HasSuffix(parts[0], ".tar.lz4") ||
				strings.HasSuffix(parts[0], ".tar.bz2") ||
				strings.HasSuffix(parts[0], ".tar.gz") ||
				strings.HasSuffix(parts[0], ".tar.sz") ||
				strings.HasSuffix(parts[0], ".tar.xz") {
				gcsFiles[parts[0]] = gcsBackup{
					Tar:  true,
					Date: objAttr.Created,
					Size: objAttr.Size,
				}
			}
			if len(parts) > 1 {
				b := gcsFiles[parts[0]]
				gcsFiles[parts[0]] = gcsBackup{
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
	for name, e := range gcsFiles {
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

func (gcs *GCS) GetSize(filepath string) (int64, error) {
	ctx := context.Background()
	objAttr, err := gcs.client.Bucket(gcs.Config.Bucket).Object(filepath).Attrs(ctx)
	if err != nil {
		return 0, err
	}
	return objAttr.Size, nil
}

func (gcs *GCS) RemoveBackup(backupName string) error {
	objects := []string{}
	gcs.remoteIterator(gcs.Config.Path, false, func(objAttr *storage.ObjectAttrs) {
		if strings.HasPrefix(objAttr.Name, path.Join(gcs.Config.Path, backupName)) {
			objects = append(objects, objAttr.Name)
		}
	})
	ctx := context.Background()
	for _, name := range objects {
		object := gcs.client.Bucket(gcs.Config.Bucket).Object(name)
		err := object.Delete(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

func (gcs *GCS) RemoveOldBackups(keep int) error {
	if keep < 1 {
		return nil
	}
	backupList, err := gcs.BackupList()
	if err != nil {
		return err
	}
	backupsToDelete := GetBackupsToDelete(backupList, keep)
	for _, backupToDelete := range backupsToDelete {
		if err := gcs.RemoveBackup(backupToDelete.Name); err != nil {
			return err
		}
	}
	return nil
}

func (gcs *GCS) remoteIterator(gcsPath string, delim bool, process func(*storage.ObjectAttrs)) error {
	ctx := context.Background()
	it := gcs.client.Bucket(gcs.Config.Bucket).Objects(ctx, nil)
	for {
		object, err := it.Next()
		switch err {
		case nil:
			process(object)
		case iterator.Done:
			return nil
		default:
			return err
		}
	}
}
