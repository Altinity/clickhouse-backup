package new_storage

import (
	"fmt"
	"io"
	"io/ioutil"
	"path"
	"path/filepath"
	"syscall"
	"time"

	"github.com/AlexAkulov/clickhouse-backup/config"
	lib_sftp "github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

// SFTP Implement RemoteStorage
type SFTP struct {
	client   *lib_sftp.Client
	Config   *config.SFTPConfig
	dirCache map[string]struct{}
}

func (sftp *SFTP) Connect() error {
	authMethods := []ssh.AuthMethod{}

	if sftp.Config.Key != "" {
		fSftpKey, err := ioutil.ReadFile(sftp.Config.Key)
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

	sshConnection, _ := ssh.Dial("tcp", sftp.Config.Address+":22", sftpConfig)
	// defer sftpConnection.Close()

	sftpConnection, err := lib_sftp.NewClient(sshConnection)
	if err != nil {
		return err
	}
	// defer sftpConnection.Close()

	sftp.client = sftpConnection

	sftp.dirCache = map[string]struct{}{}
	return nil
}

func (sftp *SFTP) Kind() string {
	return "SFTP"
}

func (sftp *SFTP) StatFile(key string) (RemoteFile, error) {
	filePath := path.Join(sftp.Config.Path, key)

	stat, err := sftp.client.Stat(filePath)
	if err != nil {
		return nil, err
	}

	return &sftpFile{
		size:         stat.Size(),
		lastModified: stat.ModTime(),
		name:         stat.Name(),
	}, nil
}

func (sftp *SFTP) DeleteFile(key string) error {
	filePath := path.Join(sftp.Config.Path, key)

	fileStat, err := sftp.client.Stat(filePath)
	if err != nil {
		return err
	}
	if fileStat.IsDir() {
		return sftp.DeleteDirectory(filePath)
	} else {
		return sftp.client.Remove(filePath)
	}
}

func (sftp *SFTP) DeleteDirectory(dirPath string) error {
	defer sftp.client.RemoveDirectory(dirPath)

	files, err := sftp.client.ReadDir(dirPath)
	if err != nil {
		return err
	}
	for _, file := range files {
		filePath := path.Join(dirPath, file.Name())
		if file.IsDir() {
			sftp.DeleteDirectory(filePath)
		} else {
			defer sftp.client.Remove(filePath)
		}
	}

	return nil
}

func (sftp *SFTP) Walk(remotePath string, recursive bool, process func(RemoteFile) error) error {
	dir := path.Join(sftp.Config.Path, remotePath)

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
			err := process(&sftpFile{
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
			return err
		}
		for _, entry := range entries {
			err := process(&sftpFile{
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

func (sftp *SFTP) GetFileReader(key string) (io.ReadCloser, error) {
	filePath := path.Join(sftp.Config.Path, key)
	sftp.client.MkdirAll(path.Dir(filePath))
	return sftp.client.OpenFile(filePath, syscall.O_RDWR)
}

func (sftp *SFTP) PutFile(key string, localFile io.ReadCloser) error {
	filePath := path.Join(sftp.Config.Path, key)
	sftp.client.MkdirAll(path.Dir(filePath))
	remoteFile, err := sftp.client.Create(filePath)
	if err != nil {
		return err
	}
	defer remoteFile.Close()
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
