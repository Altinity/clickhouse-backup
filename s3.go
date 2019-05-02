package main

import (
	"archive/tar"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"mime"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/AlexAkulov/s3gof3r"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/mholt/archiver"
	pb "gopkg.in/cheggaaa/pb.v1"
)

const MetaFileName = "meta.json"

// S3 - presents methods for manipulate data on s3
type S3 struct {
	session        *session.Session
	s3Stream       *s3gof3r.Bucket
	Config         *S3Config
	DryRun         bool
	s3StreamConfig *s3gof3r.Config
}

type MetaFile struct {
	RequiredBackup string   `json:"required_backup"`
	Hardlinks      []string `json:"hardlinks"`
}

// Connect - connect to s3
func (s *S3) Connect() error {
	var err error
	if s.session, err = session.NewSession(
		&aws.Config{
			Credentials:      credentials.NewStaticCredentials(s.Config.AccessKey, s.Config.SecretKey, ""),
			Region:           aws.String(s.Config.Region),
			Endpoint:         aws.String(s.Config.Endpoint),
			DisableSSL:       aws.Bool(s.Config.DisableSSL),
			S3ForcePathStyle: aws.Bool(s.Config.ForcePathStyle),
		}); err != nil {
		return err
	}
	endpoint := strings.TrimPrefix(s.Config.Endpoint, "https://")
	endpoint = strings.TrimPrefix(endpoint, "http://")
	httpSchema := "https"
	if s.Config.DisableSSL {
		httpSchema = "http"
	}
	s.s3StreamConfig = &s3gof3r.Config{
		Client:      s3gof3r.ClientWithTimeout(5 * time.Second),
		Concurrency: 10,
		PartSize:    s.Config.PartSize,
		NTry:        3,
		Scheme:      httpSchema,
		PathStyle:   s.Config.ForcePathStyle,
	}
	s3StreamClient := s3gof3r.New(endpoint, s.Config.Region, s3gof3r.Keys{
		AccessKey: s.Config.AccessKey,
		SecretKey: s.Config.SecretKey,
	})
	s.s3Stream = s3StreamClient.Bucket(s.Config.Bucket)

	return nil
}

func (s *S3) CompressedStreamDownload(s3Path, localPath string) error {
	if err := os.Mkdir(localPath, os.ModePerm); err != nil {
		return err
	}
	archiveName := path.Join(s.Config.Path, fmt.Sprintf("%s.%s", s3Path, getExtension(s.Config.CompressionFormat)))
	r, _, err := s.s3Stream.GetReader(archiveName, s.s3StreamConfig)
	if err != nil {
		return err
	}
	defer r.Close()
	z, _ := getArchiveReader(s.Config.CompressionFormat)

	if err := z.Open(r, 0); err != nil {
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
		log.Printf("Backup '%s' required '%s'. Downloading.", s3Path, metafile.RequiredBackup)
		err := s.CompressedStreamDownload(metafile.RequiredBackup, filepath.Join(filepath.Dir(localPath), metafile.RequiredBackup))
		if err != nil && !os.IsExist(err) {
			return fmt.Errorf("can't download '%s' with %v", metafile.RequiredBackup, err)
		}
		log.Printf("  Done.")
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
	return nil
}

func (s *S3) CompressedStreamUpload(localPath, s3Path, diffFromPath string) error {
	var bar *pb.ProgressBar
	if !s.Config.DisableProgressBar {
		var filesCount int
		filepath.Walk(localPath, func(filePath string, info os.FileInfo, err error) error {
			if info.Mode().IsRegular() {
				filesCount++
			}
			return nil
		})
		bar = pb.StartNew(filesCount)
		defer bar.FinishPrint("Done.")
	}
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
	archiveName := path.Join(s.Config.Path, fmt.Sprintf("%s.%s", s3Path, getExtension(s.Config.CompressionFormat)))
	w, err := s.s3Stream.PutWriter(archiveName, http.Header{"X-Amz-Acl": []string{s.Config.ACL}}, s.s3StreamConfig)
	if err != nil {
		return err
	}
	defer w.Close()
	z, _ := getArchiveWriter(s.Config.CompressionFormat, s.Config.CompressionLevel)
	if err := z.Create(w); err != nil {
		return err
	}
	defer z.Close()

	if err := filepath.Walk(localPath, func(filePath string, info os.FileInfo, err error) error {
		if !info.Mode().IsRegular() {
			return nil
		}
		if !s.Config.DisableProgressBar {
			bar.Increment()
		}
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
		return z.Write(archiver.File{
			FileInfo: archiver.FileInfo{
				FileInfo:   info,
				CustomName: relativePath,
			},
			ReadCloser: file,
		})
	}); err != nil {
		return err
	}
	if len(hardlinks) > 0 {
		metafile := MetaFile{
			RequiredBackup: filepath.Base(diffFromPath),
			Hardlinks:      hardlinks,
		}
		content, err := json.MarshalIndent(&metafile, "", "\t")
		if err != nil {
			return fmt.Errorf("can't marshal json with %v", err)
		}
		tmpfile, err := ioutil.TempFile("", MetaFileName)
		if err != nil {
			return fmt.Errorf("can't create meta.info with %v", err)
		}
		if _, err := tmpfile.Write(content); err != nil {
			return fmt.Errorf("can't write to meta.info with %v", err)
		}
		tmpfile.Close()
		tmpFileName := tmpfile.Name()
		defer os.Remove(tmpFileName)
		info, err := os.Stat(tmpFileName)
		if err != nil {
			return fmt.Errorf("can't get stat with %v", err)
		}
		mf, err := os.Open(tmpFileName)
		if err != nil {
			return err
		}
		defer mf.Close()
		if err := z.Write(archiver.File{
			FileInfo: archiver.FileInfo{
				FileInfo:   info,
				CustomName: MetaFileName,
			},
			ReadCloser: mf,
		}); err != nil {
			return fmt.Errorf("can't add mata.json to archive with %v", err)
		}
	}
	return nil
}

// UploadDirectory - synchronize localPath to dstPath on s3
func (s *S3) UploadDirectory(localPath string, dstPath string) error {
	// TODO: it must be refactored like as Download() method
	iter, filesForDelete, err := s.newSyncFolderIterator(localPath, dstPath)
	if err != nil {
		return err
	}
	var bar *pb.ProgressBar
	if !s.Config.DisableProgressBar {
		bar = pb.StartNew(len(iter.fileInfos) + iter.skipFilesCount)
		bar.Set(iter.skipFilesCount)
		defer bar.FinishPrint("Done.")
	}

	uploader := s3manager.NewUploader(s.session)
	uploader.PartSize = s.Config.PartSize
	var errs []s3manager.Error
	for iter.Next() {
		object := iter.UploadObject()
		if !s.DryRun {
			if _, err := uploader.UploadWithContext(aws.BackgroundContext(), object.Object); err != nil {
				s3Err := s3manager.Error{
					OrigErr: err,
					Bucket:  object.Object.Bucket,
					Key:     object.Object.Key,
				}
				errs = append(errs, s3Err)
			}
		}
		if !s.Config.DisableProgressBar {
			bar.Increment()
		}
		if object.After == nil {
			continue
		}
		if err := object.After(); err != nil {
			s3Err := s3manager.Error{
				OrigErr: err,
				Bucket:  object.Object.Bucket,
				Key:     object.Object.Key,
			}
			errs = append(errs, s3Err)
		}
	}
	if len(errs) > 0 {
		return s3manager.NewBatchError("BatchedUploadIncomplete", "some objects have failed to upload.", errs)
	}
	if err := iter.Err(); err != nil {
		return err
	}

	if !s.Config.DeleteExtraFiles {
		return nil
	}
	// Delete
	batcher := s3manager.NewBatchDelete(s.session)
	objects := []s3manager.BatchDeleteObject{}
	for _, file := range filesForDelete {
		objects = append(objects, s3manager.BatchDeleteObject{
			Object: &s3.DeleteObjectInput{
				Bucket: aws.String(s.Config.Bucket),
				Key:    aws.String(file.key),
			},
		})
	}

	if err := batcher.Delete(aws.BackgroundContext(), &s3manager.DeleteObjectsIterator{Objects: objects}); err != nil {
		log.Printf("can't delete objects with: %v", err)
	}
	return nil
}

// UploadFile - synchronize localPath to dstPath on s3
func (s *S3) UploadFile(localPath string, dstPath string) error {
	uploader := s3manager.NewUploader(s.session)
	uploader.PartSize = s.Config.PartSize

	file, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("error opening file %v: %v", localPath, err)
	}
	if !s.DryRun {
		if _, err := uploader.UploadWithContext(aws.BackgroundContext(), &s3manager.UploadInput{
			ACL:    aws.String(s.Config.ACL),
			Bucket: aws.String(s.Config.Bucket),
			Key:    aws.String(path.Join(s.Config.Path, dstPath)),
			Body:   file,
		}); err != nil {
			return err
		}
	}

	return nil
}

// DownloadTree - download files from s3Path to localPath
func (s *S3) DownloadTree(s3Path string, localPath string) error {
	if err := os.MkdirAll(localPath, os.ModePerm); err != nil {
		return fmt.Errorf("can't create '%s' with: %v", localPath, err)
	}
	localFiles, err := s.getLocalFiles(localPath, s3Path)
	if err != nil {
		return fmt.Errorf("can't open '%s' with %v", localPath, err)
	}
	s3Files, err := s.getS3Files(localPath, s3Path)
	if err != nil {
		return err
	}
	var bar *pb.ProgressBar
	if !s.Config.DisableProgressBar {
		bar = pb.StartNew(len(s3Files))
		defer bar.FinishPrint("Done.")
	}
	downloader := s3manager.NewDownloader(s.session)
	for _, s3File := range s3Files {
		if !s.Config.DisableProgressBar {
			bar.Increment()
		}
		if existsFile, ok := localFiles[s3File.key]; ok {
			if existsFile.size == s3File.size {
				switch s.Config.OverwriteStrategy {
				case "skip":
					continue
				case "etag":
					if s3File.etag == GetEtag(existsFile.fullpath, s.Config.PartSize) {
						continue
					}
				}
			}
		}
		params := &s3.GetObjectInput{
			Bucket: aws.String(s.Config.Bucket),
			Key:    aws.String(path.Join(s.Config.Path, s3Path, s3File.key)),
		}
		newFilePath := filepath.Join(localPath, s3File.key)
		newPath := filepath.Dir(newFilePath)
		if s.DryRun {
			log.Printf("Download '%s' to '%s'", s3File.key, newFilePath)
			continue
		}
		if err := os.MkdirAll(newPath, os.ModePerm); err != nil {
			return fmt.Errorf("can't create '%s' with: %v", newPath, err)
		}
		f, err := os.Create(newFilePath)
		if err != nil {
			return fmt.Errorf("can't open '%s' with %v", newFilePath, err)
		}
		if _, err := downloader.DownloadWithContext(aws.BackgroundContext(), f, params); err != nil {
			return fmt.Errorf("can't download file '%s' with %v", s3File.key, err)
		}
	}

	// TODO: Delete extra files
	return nil
}

// DownloadArchive - download files from s3Path to localPath
func (s *S3) DownloadArchive(s3Path string, localPath string) error {
	if err := os.MkdirAll(localPath, os.ModePerm); err != nil {
		return fmt.Errorf("can't create '%s' with: %v", localPath, err)
	}
	downloader := s3manager.NewDownloader(s.session)
	params := &s3.GetObjectInput{
		Bucket: aws.String(s.Config.Bucket),
		Key:    aws.String(path.Join(s.Config.Path, s3Path)),
	}
	newFilePath := filepath.Join(localPath, filepath.Base(s3Path))
	newPath := filepath.Dir(newFilePath)
	if s.DryRun {
		log.Printf("Download '%s' to '%s'", s3Path, newFilePath)
		return nil
	}
	if err := os.MkdirAll(newPath, os.ModePerm); err != nil {
		return fmt.Errorf("can't create '%s' with: %v", newPath, err)
	}
	f, err := os.Create(newFilePath)
	if err != nil {
		return fmt.Errorf("can't open '%s' with %v", newFilePath, err)
	}
	if _, err := downloader.DownloadWithContext(aws.BackgroundContext(), f, params); err != nil {
		return fmt.Errorf("can't download file '%s' with %v", s3Path, err)
	}

	return nil
}

// SyncFolderIterator is used to upload a given folder to Amazon S3.
type SyncFolderIterator struct {
	bucket         string
	fileInfos      []fileInfo
	err            error
	acl            string
	s3path         string
	skipFilesCount int
}

type fileInfo struct {
	key      string
	fullpath string
	size     int64
	etag     string
}

func (s *S3) getLocalFiles(localPath, s3Path string) (localFiles map[string]fileInfo, err error) {
	localFiles = make(map[string]fileInfo)
	err = filepath.Walk(localPath, func(filePath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			filePath := filepath.ToSlash(filePath) // fix fucking Windows slashes
			key := strings.TrimPrefix(filePath, localPath)
			localFiles[key] = fileInfo{
				key:      key,
				fullpath: filePath,
				size:     info.Size(),
			}
		}
		return nil
	})
	return
}

func (s *S3) getS3Files(localPath, s3Path string) (s3Files map[string]fileInfo, err error) {
	s3Files = make(map[string]fileInfo)
	s.remotePager(s.Config.Path, false, func(page *s3.ListObjectsV2Output) {
		for _, c := range page.Contents {
			if strings.HasPrefix(*c.Key, path.Join(s.Config.Path, s3Path)) {
				key := strings.TrimPrefix(*c.Key, path.Join(s.Config.Path, s3Path))
				if !strings.HasSuffix(key, "/") {
					s3Files[key] = fileInfo{
						key:  key,
						size: *c.Size,
						etag: *c.ETag,
					}
				}
			}
		}
	})
	return
}

func (s *S3) BackupList() ([]Backup, error) {
	type s3Backup struct {
		Metadata bool
		Shadow   bool
		Tar      bool
		Date     time.Time
	}
	s3Files := map[string]s3Backup{}
	s.remotePager(s.Config.Path, false, func(page *s3.ListObjectsV2Output) {
		for _, c := range page.Contents {
			if strings.HasPrefix(*c.Key, s.Config.Path) {
				key := strings.TrimPrefix(*c.Key, s.Config.Path)
				key = strings.TrimPrefix(key, "/")
				parts := strings.Split(key, "/")
				if strings.HasSuffix(parts[0], ".tar") ||
					strings.HasSuffix(parts[0], ".tar.lz4") ||
					strings.HasSuffix(parts[0], ".tar.bz2") ||
					strings.HasSuffix(parts[0], ".tar.gz") ||
					strings.HasSuffix(parts[0], ".tar.sz") ||
					strings.HasSuffix(parts[0], ".tar.xz") {
					s3Files[parts[0]] = s3Backup{
						Tar:  true,
						Date: *c.LastModified,
					}
				}
				if len(parts) > 1 {
					b := s3Files[parts[0]]
					s3Files[parts[0]] = s3Backup{
						Metadata: b.Metadata || parts[1] == "metadata",
						Shadow:   b.Shadow || parts[1] == "shadow",
						Date:     b.Date,
					}
				}
			}
		}
	})
	result := []Backup{}
	for name, e := range s3Files {
		if e.Metadata && e.Shadow || e.Tar {
			result = append(result, Backup{
				Name: name,
				Date: e.Date,
			})
		}
	}
	sort.SliceStable(result, func(i, j int) bool {
		return result[i].Date.Before(result[j].Date)
	})
	return result, nil
}

func (s *S3) RemoveOldBackups(keep int) error {
	if keep < 1 {
		return nil
	}
	backupList, err := s.BackupList()
	if err != nil {
		return err
	}
	backupsToDelete := GetBackupsToDelete(backupList, keep)
	objects := []s3manager.BatchDeleteObject{}
	s.remotePager(s.Config.Path, false, func(page *s3.ListObjectsV2Output) {
		for _, c := range page.Contents {
			for _, backupToDelete := range backupsToDelete {
				if strings.HasPrefix(*c.Key, path.Join(s.Config.Path, backupToDelete.Name)) {
					objects = append(objects, s3manager.BatchDeleteObject{
						Object: &s3.DeleteObjectInput{
							Bucket: aws.String(s.Config.Bucket),
							Key:    c.Key,
						},
					})
				}
			}
		}
	})
	if s.DryRun {
		for _, o := range objects {
			log.Println("Delete", *o.Object.Key)
		}
		return nil
	}
	batcher := s3manager.NewBatchDelete(s.session)
	return batcher.Delete(aws.BackgroundContext(), &s3manager.DeleteObjectsIterator{Objects: objects})
}

func (s *S3) newSyncFolderIterator(localPath, dstPath string) (*SyncFolderIterator, map[string]fileInfo, error) {
	existsFiles := make(map[string]fileInfo)
	s.remotePager(s.Config.Path, false, func(page *s3.ListObjectsV2Output) {
		for _, c := range page.Contents {
			if strings.HasPrefix(*c.Key, path.Join(s.Config.Path, dstPath)) {
				key := strings.TrimPrefix(*c.Key, path.Join(s.Config.Path, dstPath))
				existsFiles[key] = fileInfo{
					key:  *c.Key,
					size: *c.Size,
					etag: *c.ETag,
				}
			}
		}
	})

	localFiles := []fileInfo{}
	skipFilesCount := 0
	// TODO: it can be very slow, should add Progres Bar here
	err := filepath.Walk(localPath, func(filePath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			filePath := filepath.ToSlash(filePath) // fix fucking Windows slashes
			key := strings.TrimPrefix(filePath, localPath)
			if existFile, ok := existsFiles[key]; ok {
				delete(existsFiles, key)
				if existFile.size == info.Size() {
					switch s.Config.OverwriteStrategy {
					case "skip":
						skipFilesCount++
						return nil
					case "etag":
						if existFile.etag == GetEtag(filePath, s.Config.PartSize) {
							// log.Printf("File '%s' already uploaded and has the same size and etag. Skip", key)
							skipFilesCount++
							return nil
						}
					}
				}
			}
			localFiles = append(localFiles, fileInfo{
				key:      key,
				fullpath: filePath,
				size:     info.Size(),
			})
		}
		return nil
	})

	return &SyncFolderIterator{
		bucket:         s.Config.Bucket,
		fileInfos:      localFiles,
		acl:            s.Config.ACL,
		s3path:         path.Join(s.Config.Path, dstPath),
		skipFilesCount: skipFilesCount,
	}, existsFiles, err
}

func (s *S3) remotePager(s3Path string, delim bool, pager func(page *s3.ListObjectsV2Output)) error {
	params := &s3.ListObjectsV2Input{
		Bucket:  aws.String(s.Config.Bucket), // Required
		MaxKeys: aws.Int64(1000),
	}
	if s3Path != "" && s3Path != "/" {
		params.Prefix = aws.String(s3Path)
	}
	if delim {
		params.Delimiter = aws.String("/")
	}

	wrapper := func(page *s3.ListObjectsV2Output, lastPage bool) bool {
		pager(page)
		return true
	}

	return s3.New(s.session).ListObjectsV2Pages(params, wrapper)
}

// Next will determine whether or not there is any remaining files to be uploaded.
func (iter *SyncFolderIterator) Next() bool {
	return len(iter.fileInfos) > 0
}

// Err returns any error when os.Open is called.
func (iter *SyncFolderIterator) Err() error {
	return iter.err
}

// UploadObject will prep the new upload object by open that file and constructing a new
// s3manager.UploadInput.
func (iter *SyncFolderIterator) UploadObject() s3manager.BatchUploadObject {
	fi := iter.fileInfos[0]
	iter.fileInfos = iter.fileInfos[1:]
	body, err := os.Open(fi.fullpath)
	if err != nil {
		iter.err = err
	}
	extension := filepath.Ext(fi.key)
	mimeType := mime.TypeByExtension(extension)
	if mimeType == "" {
		mimeType = "binary/octet-stream"
	}
	return s3manager.BatchUploadObject{
		Object: &s3manager.UploadInput{
			ACL:         aws.String(iter.acl),
			Bucket:      aws.String(iter.bucket),
			Key:         aws.String(path.Join(iter.s3path, fi.key)),
			Body:        body,
			ContentType: aws.String(mimeType),
		},
	}
}

// GetEtag - calculate ETag for file
func GetEtag(path string, partSize int64) string {
	content, err := ioutil.ReadFile(path)
	if err != nil {
		return "unknown"
	}
	size := int64(len(content))
	if size <= partSize {
		hash := md5.Sum(content)
		return fmt.Sprintf("\"%x\"", hash)
	}
	parts := 0
	var pos int64
	contentToHash := make([]byte, 0)
	for size > pos {
		endpos := pos + partSize
		if endpos >= size {
			endpos = size
		}
		hash := md5.Sum(content[pos:endpos])
		contentToHash = append(contentToHash, hash[:]...)
		pos += partSize
		parts++
	}
	hash := md5.Sum(contentToHash)
	return fmt.Sprintf("\"%x-%d\"", hash, parts)
}
