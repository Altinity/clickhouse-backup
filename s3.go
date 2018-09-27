package main

import (
	"log"
	"mime"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type S3 struct {
	session *session.Session
	Config  *S3Config
}

func (s3 *S3) Connect() (err error) {
	s3.session, err = session.NewSession(&aws.Config{
		Credentials: credentials.NewStaticCredentials(s3.Config.AccessKey, s3.Config.SecretKey, ""),
		Region:      &s3.Config.Region,
		Endpoint:    &s3.Config.URL,
	})
	return
}

func (s3 *S3) Upload(localPath string, s3Path string) error {
	uploader := s3manager.NewUploader(s3.session)

	iter := NewSyncFolderIterator(localPath, s3Path)
	if err := uploader.UploadWithIterator(aws.BackgroundContext(), iter); err != nil {
		log.Printf("unexpected error has occured: %v", err)
	}

	return iter.Err()
}

func (s3 *S3) Download(s3Path string, localPath string) error {
	return nil
}

// SyncFolderIterator is used to upload a given folder
// to Amazon S3.
type SyncFolderIterator struct {
	bucket    string
	fileInfos []fileInfo
	err       error
}

type fileInfo struct {
	key      string
	fullpath string
}

// NewSyncFolderIterator will walk the path, and store the key and full path
// of the object to be uploaded. This will return a new SyncFolderIterator
// with the data provided from walking the path.
func NewSyncFolderIterator(path, bucket string) *SyncFolderIterator {
	metadata := []fileInfo{}
	filepath.Walk(path, func(p string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			key := strings.TrimPrefix(p, path)
			metadata = append(metadata, fileInfo{key, p})
		}

		return nil
	})

	return &SyncFolderIterator{
		bucket,
		metadata,
		nil,
	}
}

// Next will determine whether or not there is any remaining files to
// be uploaded.
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

	input := s3manager.UploadInput{
		Bucket:      &iter.bucket,
		Key:         &fi.key,
		Body:        body,
		ContentType: &mimeType,
	}

	return s3manager.BatchUploadObject{
		&input,
		nil,
	}
}
