package storage

import (
	"context"
	"errors"
	"fmt"
	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	libSFTP "github.com/pkg/sftp"
	"github.com/rs/zerolog/log"
	"golang.org/x/crypto/ssh"
)

// SFTP Implement RemoteStorage
type SFTP struct {
	sshClient  *ssh.Client
	sftpClient *libSFTP.Client
	Config     *config.SFTPConfig
}

func (sftp *SFTP) Debug(msg string, v ...interface{}) {
	if sftp.Config.Debug {
		log.Info().Msgf(msg, v...)
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

	sftp.sftpClient = sftpConnection
	sftp.sshClient = sshConnection
	return nil
}

func (sftp *SFTP) Close(ctx context.Context) error {
	if err := sftp.sftpClient.Close(); err != nil {
		return fmt.Errorf("sftpClient.Close() error: , %v", err)
	}
	if err := sftp.sshClient.Close(); err != nil {
		return fmt.Errorf("sshClient.Close() error: , %v", err)
	}
	return nil
}

func (sftp *SFTP) StatFile(ctx context.Context, key string) (RemoteFile, error) {
	return sftp.StatFileAbsolute(ctx, path.Join(sftp.Config.Path, key))
}

func (sftp *SFTP) StatFileAbsolute(ctx context.Context, key string) (RemoteFile, error) {
	stat, err := sftp.sftpClient.Stat(key)
	if err != nil {
		sftp.Debug("[SFTP_DEBUG] StatFile::STAT %s return error %v", key, err)
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

	fileStat, err := sftp.sftpClient.Stat(filePath)
	if err != nil {
		sftp.Debug("[SFTP_DEBUG] Delete::STAT %s return error %v", filePath, err)
		return err
	}
	if fileStat.IsDir() {
		return sftp.DeleteDirectory(ctx, filePath)
	} else {
		return sftp.sftpClient.Remove(filePath)
	}
}

func (sftp *SFTP) DeleteDirectory(ctx context.Context, dirPath string) error {
	sftp.Debug("[SFTP_DEBUG] DeleteDirectory %s", dirPath)
	defer func() {
		if err := sftp.sftpClient.RemoveDirectory(dirPath); err != nil {
			log.Warn().Msgf("RemoveDirectory err=%v", err)
		}
	}()

	files, err := sftp.sftpClient.ReadDir(dirPath)
	if err != nil {
		sftp.Debug("[SFTP_DEBUG] DeleteDirectory::ReadDir %s return error %v", dirPath, err)
		return err
	}
	for _, file := range files {
		filePath := path.Join(dirPath, file.Name())
		if file.IsDir() {
			if err := sftp.DeleteDirectory(ctx, filePath); err != nil {
				log.Warn().Msgf("sftp.DeleteDirectory(%s) err=%v", filePath, err)
			}
		} else {
			if err := sftp.sftpClient.Remove(filePath); err != nil {
				log.Warn().Msgf("sftp.Remove(%s) err=%v", filePath, err)
			}
		}
	}

	return nil
}

func (sftp *SFTP) Walk(ctx context.Context, remotePath string, recursive bool, process func(context.Context, RemoteFile) error) error {
	prefix := path.Join(sftp.Config.Path, remotePath)
	return sftp.WalkAbsolute(ctx, prefix, recursive, process)
}

func (sftp *SFTP) WalkAbsolute(ctx context.Context, prefix string, recursive bool, process func(context.Context, RemoteFile) error) error {
	sftp.Debug("[SFTP_DEBUG] Walk %s, recursive=%v", prefix, recursive)

	if recursive {
		walker := sftp.sftpClient.Walk(prefix)
		for walker.Step() {
			if err := walker.Err(); err != nil {
				return err
			}
			entry := walker.Stat()
			if entry == nil {
				continue
			}
			relName, _ := filepath.Rel(prefix, walker.Path())
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
		entries, err := sftp.sftpClient.ReadDir(prefix)
		if err != nil {
			sftp.Debug("[SFTP_DEBUG] Walk::NonRecursive::ReadDir %s return error %v", prefix, err)
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
	return sftp.GetFileReaderAbsolute(ctx, path.Join(sftp.Config.Path, key))
}

func (sftp *SFTP) GetFileReaderAbsolute(ctx context.Context, key string) (io.ReadCloser, error) {
	return sftp.sftpClient.OpenFile(key, syscall.O_RDWR)
}

func (sftp *SFTP) GetFileReaderWithLocalPath(ctx context.Context, key, localPath string, remoteSize int64) (io.ReadCloser, error) {
	return sftp.GetFileReader(ctx, key)
}

func (sftp *SFTP) PutFile(ctx context.Context, key string, r io.ReadCloser, localSize int64) error {
	return sftp.PutFileAbsolute(ctx, path.Join(sftp.Config.Path, key), r, localSize)
}

func (sftp *SFTP) PutFileAbsolute(ctx context.Context, key string, r io.ReadCloser, localSize int64) error {
	if err := sftp.sftpClient.MkdirAll(path.Dir(key)); err != nil {
		log.Warn().Msgf("sftp.sftpClient.MkdirAll(%s) err=%v", path.Dir(key), err)
	}
	remoteFile, err := sftp.sftpClient.Create(key)
	if err != nil {
		return err
	}
	defer func() {
		if err := remoteFile.Close(); err != nil {
			log.Warn().Msgf("can't close %s err=%v", key, err)
		}
	}()
	if _, err = remoteFile.ReadFrom(r); err != nil {
		return err
	}
	return nil
}

func (sftp *SFTP) CopyObject(ctx context.Context, srcSize int64, srcBucket, srcKey, dstKey string) (int64, error) {
	return 0, fmt.Errorf("CopyObject not imlemented for %s", sftp.Kind())
}

func (sftp *SFTP) DeleteFileFromObjectDiskBackup(ctx context.Context, key string) error {
	sftp.Debug("[SFTP_DEBUG] DeleteFileFromObjectDiskBackup %s", key)
	filePath := path.Join(sftp.Config.ObjectDiskPath, key)

	fileStat, err := sftp.sftpClient.Stat(filePath)
	if err != nil {
		sftp.Debug("[SFTP_DEBUG] DeleteFileFromObjectDiskBackup::STAT %s return error %v", filePath, err)
		return err
	}
	if fileStat.IsDir() {
		return sftp.DeleteDirectory(ctx, filePath)
	} else {
		return sftp.sftpClient.Remove(filePath)
	}
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
