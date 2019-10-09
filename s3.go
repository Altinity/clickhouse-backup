package main

import (
	"archive/tar"
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

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/defaults"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/mholt/archiver"
	"gopkg.in/djherbis/buffer.v1"
	"gopkg.in/djherbis/nio.v2"
)

const (
	MetaFileName = "meta.json"
	BufferSize   = 4 * 1024 * 1024
)

// S3 - presents methods for manipulate data on s3
type S3 struct {
	session *session.Session
	Config  *S3Config
}

type MetaFile struct {
	RequiredBackup string   `json:"required_backup"`
	Hardlinks      []string `json:"hardlinks"`
}

// Connect - connect to s3
func (s *S3) Connect() error {
	var err error
	awsDefaults := defaults.Get()
	defaultCredProviders := defaults.CredProviders(awsDefaults.Config, awsDefaults.Handlers)

	// Define custom static cred provider
	staticCreds := &credentials.StaticProvider{Value: credentials.Value{
		AccessKeyID:     s.Config.AccessKey,
		SecretAccessKey: s.Config.SecretKey,
	}}

	// Append static creds to the defaults
	customCredProviders := append([]credentials.Provider{staticCreds}, defaultCredProviders...)
	creds := credentials.NewChainCredentials(customCredProviders)
	if s.session, err = session.NewSession(
		&aws.Config{
			Credentials:      creds,
			Region:           aws.String(s.Config.Region),
			Endpoint:         aws.String(s.Config.Endpoint),
			DisableSSL:       aws.Bool(s.Config.DisableSSL),
			S3ForcePathStyle: aws.Bool(s.Config.ForcePathStyle),
			MaxRetries:       aws.Int(30),
		}); err != nil {
		return err
	}
	return nil
}

func (s *S3) CompressedStreamDownload(s3Path, localPath string) error {
	if err := os.MkdirAll(localPath, os.ModePerm); err != nil {
		return err
	}
	archiveName := path.Join(s.Config.Path, fmt.Sprintf("%s.%s", s3Path, getExtension(s.Config.CompressionFormat)))
	svc := s3.New(s.session)
	req, resp := svc.GetObjectRequest(&s3.GetObjectInput{
		Bucket: aws.String(s.Config.Bucket),
		Key:    aws.String(archiveName),
	})
	if err := req.Send(); err != nil {
		return err
	}

	filesize, err := s.GetSize(archiveName)
	if err != nil {
		return err
	}
	bar := StartNewByteBar(!s.Config.DisableProgressBar, filesize)
	buf := buffer.New(BufferSize)
	bufReader := nio.NewReader(resp.Body, buf)
	proxyReader := bar.NewProxyReader(bufReader)
	z, _ := getArchiveReader(s.Config.CompressionFormat)
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
		log.Printf("Backup '%s' required '%s'. Downloading.", s3Path, metafile.RequiredBackup)
		err := s.CompressedStreamDownload(metafile.RequiredBackup, filepath.Join(filepath.Dir(localPath), metafile.RequiredBackup))
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

func (s *S3) CompressedStreamUpload(localPath, s3Path, diffFromPath string) error {
	archiveName := path.Join(s.Config.Path, fmt.Sprintf("%s.%s", s3Path, getExtension(s.Config.CompressionFormat)))
	svc := s3.New(s.session)
	if head, err := svc.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(s.Config.Bucket),
		Key:    aws.String(archiveName),
	}); err != nil {
		aerr, ok := err.(awserr.Error)
		if ok && aerr.Code() != "NotFound" {
			return err
		}
	} else if *head.ContentLength > 0 {
		return fmt.Errorf("'%s' already uploaded", archiveName)
	}

	var totalBytes int64
	filepath.Walk(localPath, func(filePath string, info os.FileInfo, err error) error {
		if info.Mode().IsRegular() {
			totalBytes = totalBytes + info.Size()
		}
		return nil
	})
	bar := StartNewByteBar(!s.Config.DisableProgressBar, totalBytes)
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
		z, _ := getArchiveWriter(s.Config.CompressionFormat, s.Config.CompressionLevel)
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
	uploader := s3manager.NewUploader(s.session)
	uploader.Concurrency = 10
	uploader.PartSize = s.Config.PartSize
	var sse *string
	if s.Config.SSE != "" {
		sse = aws.String(s.Config.SSE)
	}
	if _, err := uploader.Upload(&s3manager.UploadInput{
		ACL:                  aws.String(s.Config.ACL),
		Bucket:               aws.String(s.Config.Bucket),
		Key:                  aws.String(archiveName),
		Body:                 body,
		ServerSideEncryption: sse,
	}); err != nil {
		return err
	}
	bar.Finish()
	return nil
}

func (s *S3) GetSize(filepath string) (int64, error) {
	svc := s3.New(s.session)
	result, err := svc.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(s.Config.Bucket),
		Key:    aws.String(filepath),
	})
	if err != nil {
		return 0, err
	}
	return *result.ContentLength, nil
}

func (s *S3) BackupList() ([]Backup, error) {
	type s3Backup struct {
		Metadata bool
		Shadow   bool
		Tar      bool
		Size     int64
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
						Size: *c.Size,
					}
				}
				if len(parts) > 1 {
					b := s3Files[parts[0]]
					s3Files[parts[0]] = s3Backup{
						Metadata: b.Metadata || parts[1] == "metadata",
						Shadow:   b.Shadow || parts[1] == "shadow",
						Date:     b.Date,
						Size:     b.Size,
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
				Size: e.Size,
			})
		}
	}
	sort.SliceStable(result, func(i, j int) bool {
		return result[i].Date.Before(result[j].Date)
	})
	return result, nil
}

func (s *S3) RemoveBackup(backupName string) error {
	objects := []s3manager.BatchDeleteObject{}
	s.remotePager(s.Config.Path, false, func(page *s3.ListObjectsV2Output) {
		for _, c := range page.Contents {
			if strings.HasPrefix(*c.Key, path.Join(s.Config.Path, backupName)) {
				objects = append(objects, s3manager.BatchDeleteObject{
					Object: &s3.DeleteObjectInput{
						Bucket: aws.String(s.Config.Bucket),
						Key:    c.Key,
					},
				})
			}
		}
	})
	batcher := s3manager.NewBatchDelete(s.session)
	return batcher.Delete(aws.BackgroundContext(),
		&s3manager.DeleteObjectsIterator{Objects: objects})
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
	for _, backupToDelete := range backupsToDelete {
		if err := s.RemoveBackup(backupToDelete.Name); err != nil {
			return err
		}
	}
	return nil
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
