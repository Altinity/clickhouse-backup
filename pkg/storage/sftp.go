package storage

import (
	"context"
	"errors"
	"fmt"
	"github.com/AlexAkulov/clickhouse-backup/pkg/config"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/apex/log"
	libSFTP "github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

// SFTP Implement RemoteStorage
type SFTP struct {
	client *libSFTP.Client
	Config *config.SFTPConfig
}

func (sftp *SFTP) Debug(msg string, v ...interface{}) {
	if sftp.Config.Debug {
		log.Infof(msg, v...)
	}
}

func (sftp *SFTP) Kind() string {
	return "SFTP"
}

func (sftp *SFTP) Connect(ctx context.Context) error {
	authMethods := make([]ssh.AuthMethod, 0)

	if sftp.Config.Key == "" && sftp.Config.Password == "" {
		return errors.New("please specify sftp.key or sftp.password for authentication")
	}

	if sftp.Config.Key != "" {
		fSftpKey, err := os.ReadFile(sftp.Config.Key)
		if err != nil {
			return err
		}
		sftpKey, err := ssh.ParsePrivateKey(fSftpKey)
		if err != nil {
			return err
		}

		authMethods = append(authMethods, ssh.PublicKeys(sftpKey))
	}

	if sftp.Config.Password != "" {
		authMethods = append(authMethods, ssh.Password(sftp.Config.Password))
	}

	sftpConfig := &ssh.ClientConfig{
		User:            sftp.Config.Username,
		Auth:            authMethods,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}
	addr := fmt.Sprintf("%s:%d", sftp.Config.Address, sftp.Config.Port)
	sftp.Debug("[SFTP_DEBUG] try connect to tcp://%s", addr)
	sshConnection, err := ssh.Dial("tcp", addr, sftpConfig)
	if err != nil {
		return err
	}
	clientOptions := make([]libSFTP.ClientOption, 0)
	if sftp.Config.Concurrency > 0 {
		clientOptions = append(
			clientOptions,
			libSFTP.UseConcurrentReads(true),
			libSFTP.UseConcurrentWrites(true),
			libSFTP.MaxConcurrentRequestsPerFile(sftp.Config.Concurrency),
		)
	}
	sftpConnection, err := libSFTP.NewClient(sshConnection, clientOptions...)
	if err != nil {
		return err
	}

	sftp.client = sftpConnection
	return nil
}

func (sftp *SFTP) Close(ctx context.Context) error {
	return sftp.client.Close()
}

func (sftp *SFTP) StatFile(ctx context.Context, key string) (RemoteFile, error) {
	filePath := path.Join(sftp.Config.Path, key)

	stat, err := sftp.client.Stat(filePath)
	if err != nil {
		sftp.Debug("[SFTP_DEBUG] StatFile::STAT %s return error %v", filePath, err)
		if strings.Contains(err.Error(), "not exist") {
			return nil, ErrNotFound
		}
		return nil, err
	}

	return &sftpFile{
		size:         stat.Size(),
		lastModified: stat.ModTime(),
		name:         stat.Name(),
	}, nil
}

func (sftp *SFTP) DeleteFile(ctx context.Context, key string) error {
	sftp.Debug("[SFTP_DEBUG] Delete %s", key)
	filePath := path.Join(sftp.Config.Path, key)

	fileStat, err := sftp.client.Stat(filePath)
	if err != nil {
		sftp.Debug("[SFTP_DEBUG] Delete::STAT %s return error %v", filePath, err)
		return err
	}
	if fileStat.IsDir() {
		return sftp.DeleteDirectory(ctx, filePath)
	} else {
		return sftp.client.Remove(filePath)
	}
}

func (sftp *SFTP) DeleteDirectory(ctx context.Context, dirPath string) error {
	sftp.Debug("[SFTP_DEBUG] DeleteDirectory %s", dirPath)
	defer func() {
		if err := sftp.client.RemoveDirectory(dirPath); err != nil {
			log.Warnf("RemoveDirectory err=%v", err)
		}
	}()

	files, err := sftp.client.ReadDir(dirPath)
	if err != nil {
		sftp.Debug("[SFTP_DEBUG] DeleteDirectory::ReadDir %s return error %v", dirPath, err)
		return err
	}
	for _, file := range files {
		filePath := path.Join(dirPath, file.Name())
		if file.IsDir() {
			if err := sftp.DeleteDirectory(ctx, filePath); err != nil {
				log.Warnf("sftp.DeleteDirectory(%s) err=%v", filePath, err)
			}
		} else {
			if err := sftp.client.Remove(filePath); err != nil {
				log.Warnf("sftp.Remove(%s) err=%v", filePath, err)
			}
		}
	}

	return nil
}

func (sftp *SFTP) Walk(ctx context.Context, remotePath string, recursive bool, process func(context.Context, RemoteFile) error) error {
	dir := path.Join(sftp.Config.Path, remotePath)
	sftp.Debug("[SFTP_DEBUG] Walk %s, recursive=%v", dir, recursive)

	if recursive {
		walker := sftp.client.Walk(dir)
		for walker.Step() {
			if err := walker.Err(); err != nil {
				return err
			}
			entry := walker.Stat()
			if entry == nil {
				continue
			}
			relName, _ := filepath.Rel(dir, walker.Path())
			err := process(ctx, &sftpFile{
				size:         entry.Size(),
				lastModified: entry.ModTime(),
				name:         relName,
			})
			if err != nil {
				return err
			}
		}
	} else {
		entries, err := sftp.client.ReadDir(dir)
		if err != nil {
			sftp.Debug("[SFTP_DEBUG] Walk::NonRecursive::ReadDir %s return error %v", dir, err)
			return err
		}
		for _, entry := range entries {
			err := process(ctx, &sftpFile{
				size:         entry.Size(),
				lastModified: entry.ModTime(),
				name:         entry.Name(),
			})
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (sftp *SFTP) GetFileReader(ctx context.Context, key string) (io.ReadCloser, error) {
	filePath := path.Join(sftp.Config.Path, key)
	return sftp.client.OpenFile(filePath, syscall.O_RDWR)
}

func (sftp *SFTP) GetFileReaderWithLocalPath(ctx context.Context, key, _ string) (io.ReadCloser, error) {
	return sftp.GetFileReader(ctx, key)
}

func (sftp *SFTP) PutFile(ctx context.Context, key string, localFile io.ReadCloser) error {
	filePath := path.Join(sftp.Config.Path, key)
	if err := sftp.client.MkdirAll(path.Dir(filePath)); err != nil {
		log.Warnf("sftp.client.MkdirAll(%s) err=%v", path.Dir(filePath), err)
	}
	remoteFile, err := sftp.client.Create(filePath)
	if err != nil {
		return err
	}
	defer func() {
		if err := remoteFile.Close(); err != nil {
			log.Warnf("can't close %s err=%v", filePath, err)
		}
	}()
	if _, err = remoteFile.ReadFrom(localFile); err != nil {
		return err
	}
	return nil
}

// Implement RemoteFile
type sftpFile struct {
	size         int64
	lastModified time.Time
	name         string
}

func (file *sftpFile) Size() int64 {
	return file.size
}

func (file *sftpFile) LastModified() time.Time {
	return file.lastModified
}

func (file *sftpFile) Name() string {
	return file.name
}
