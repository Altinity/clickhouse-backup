package main

import (
	"crypto/md5"
	"fmt"
	"io/ioutil"
	"log"
	"mime"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	pb "gopkg.in/cheggaaa/pb.v1"
)

const (
	// PART_SIZE - default s3 part size for calculing ETag
	PART_SIZE = 5 * 1024 * 1024
)

// S3 - presents methods for manipulate data on s3
type S3 struct {
	session *session.Session
	Config  *S3Config
	DryRun  bool
}

// Connect - connect to s3
func (s *S3) Connect() (err error) {
	s.session, err = session.NewSession(
		&aws.Config{
			Credentials:      credentials.NewStaticCredentials(s.Config.AccessKey, s.Config.SecretKey, ""),
			Region:           aws.String(s.Config.Region),
			Endpoint:         aws.String(s.Config.Endpoint),
			DisableSSL:       aws.Bool(s.Config.DisableSSL),
			S3ForcePathStyle: aws.Bool(s.Config.ForcePathStyle),
		},
	)
	return
}

// Upload - synchronize localPath to dstPath on s3
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
	uploader.PartSize = PART_SIZE
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

// Upload - synchronize localPath to dstPath on s3
func (s *S3) UploadFile(localPath string, dstPath string) error {

	uploader := s3manager.NewUploader(s.session)
	uploader.PartSize = PART_SIZE

	file, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("error opening file %v: %v", localPath, err)
	}
	if !s.DryRun {
		_, err := uploader.UploadWithContext(aws.BackgroundContext(), &s3manager.UploadInput{
			ACL:    aws.String(config.S3.ACL),
			Bucket: aws.String(config.S3.Bucket),
			Key:    aws.String(path.Join(s.Config.Path, dstPath)),
			Body:   file,
		})
		if err != nil {
			return err
		}
	}

	return nil
}

// Download - download files from s3Path to localPath
func (s *S3) DownloadTree(s3Path string, localPath string) error {
	if err := os.MkdirAll(localPath, 0755); err != nil {
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
		bar.Increment()
		if existsFile, ok := localFiles[s3File.key]; ok {
			if existsFile.size == s3File.size {
				switch s.Config.OverwriteStrategy {
				case "skip":
					continue
				case "etag":
					if s3File.etag == GetEtag(existsFile.fullpath) {
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
		if err := os.MkdirAll(newPath, 0755); err != nil {
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

// Download - download files from s3Path to localPath
func (s *S3) DownloadArchive(s3Path string, localPath string) error {
	if err := os.MkdirAll(localPath, 0755); err != nil {
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
	if err := os.MkdirAll(newPath, 0644); err != nil {
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
						if existFile.etag == GetEtag(filePath) {
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
func GetEtag(path string) string {
	content, err := ioutil.ReadFile(path)
	if err != nil {
		return "unknown"
	}
	size := len(content)
	if size <= PART_SIZE {
		hash := md5.Sum(content)
		return fmt.Sprintf("\"%x\"", hash)
	}
	parts := 0
	pos := 0
	contentToHash := make([]byte, 0)
	for size > pos {
		endpos := pos + PART_SIZE
		if endpos >= size {
			endpos = size
		}
		hash := md5.Sum(content[pos:endpos])
		contentToHash = append(contentToHash, hash[:]...)
		pos += PART_SIZE
		parts++
	}
	hash := md5.Sum(contentToHash)
	return fmt.Sprintf("\"%x-%d\"", hash, parts)
}

// ListObjects - get list of objects from s3
func (s *S3) ListObjects(s3Path string) ([]*s3.Object, error) {
	params := &s3.ListObjectsInput{
		Bucket: aws.String(s.Config.Bucket),
	}
	resp, err := s3.New(s.session).ListObjects(params)
	if err != nil {
		return nil, err
	}
	return resp.Contents, nil
}

// ListObjects - get list of objects from s3
func (s *S3) DeleteObjects(objects []*s3.Object) error {
	batcher := s3manager.NewBatchDelete(s.session)
	batchObjects := make([]s3manager.BatchDeleteObject, len(objects))
	for i, obj := range objects {
		batchObjects[i] = s3manager.BatchDeleteObject{
			Object: &s3.DeleteObjectInput{
				Key:    obj.Key,
				Bucket: aws.String(s.Config.Bucket),
			},
		}
	}
	if err := batcher.Delete(aws.BackgroundContext(), &s3manager.DeleteObjectsIterator{
		Objects: batchObjects,
	}); err != nil {
		return err
	}
	return nil
}
