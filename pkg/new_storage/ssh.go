package new_storage

import (
	"io"
	"io/ioutil"
	"path"
	"time"
	"syscall"
	"path/filepath"

	"github.com/AlexAkulov/clickhouse-backup/config"

  "github.com/pkg/sftp"
	crypto_ssh "golang.org/x/crypto/ssh"
)

// Implement RemoteStorage
type SSH struct {
	client *sftp.Client
	Config *config.SSHConfig
	dirCache map[string]struct{}
}

func (ssh *SSH) Connect() error {
  f_ssh_key, err := ioutil.ReadFile(ssh.Config.Key)
  if err != nil {
    return err
  }
  ssh_key, err := crypto_ssh.ParsePrivateKey(f_ssh_key)
  if err != nil {
    return err
  }
  ssh_config := &crypto_ssh.ClientConfig{
    User: ssh.Config.Username,
    Auth: []crypto_ssh.AuthMethod{
      crypto_ssh.PublicKeys(ssh_key),
    },
    HostKeyCallback: crypto_ssh.InsecureIgnoreHostKey(),
  }

	ssh_connection, _ := crypto_ssh.Dial("tcp", ssh.Config.Address+":22", ssh_config)
  // defer ssh_connection.Close()

  sftp_connection, err := sftp.NewClient(ssh_connection)
  if err != nil {
    return err
  }
  // defer sftp_connection.Close()

	ssh.client = sftp_connection
	ssh.dirCache = map[string]struct{}{}
	return nil
}

func (ssh *SSH) Kind() string {
	return "SSH"
}

func (ssh *SSH) StatFile(key string) (RemoteFile, error) {
	file_path := path.Join(ssh.Config.Path, key)

  stat, err := ssh.client.Stat(file_path)
  if err != nil {
    return nil, err
  }

	return &sshFile{
    size:         stat.Size(),
    lastModified: stat.ModTime(),
    name:         stat.Name(),
	}, nil
}

func (ssh *SSH) DeleteFile(key string) error {
	file_path := path.Join(ssh.Config.Path, key)

	file_stat, err := ssh.client.Stat(file_path)
	if err != nil { return err }
	if file_stat.IsDir() {
	  return ssh.DeleteDirectory(file_path)
	} else {
	  return ssh.client.Remove(file_path)
	}
}

func (ssh *SSH) DeleteDirectory(dir_path string) error {
  defer ssh.client.RemoveDirectory(dir_path)

	files, err := ssh.client.ReadDir(dir_path)
	if err != nil { return err }
	for _, file := range files {
		file_path := path.Join(dir_path, file.Name())
		if file.IsDir() {
			ssh.DeleteDirectory(file_path)
		} else {
			defer ssh.client.Remove(file_path)
		}
	}

	return nil
}

func (ssh *SSH) Walk(remote_path string, recursive bool, process func(RemoteFile) error) error {
	dir := path.Join(ssh.Config.Path, remote_path)

  if recursive {
    walker := ssh.client.Walk(dir)
    for walker.Step() {
			if err := walker.Err(); err != nil {
			  return err
			}
      entry := walker.Stat()
      if entry == nil {
        continue
      }
			rel_name, _ := filepath.Rel(dir, walker.Path())
			err := process(&sshFile{
	      size:         entry.Size(),
	      lastModified: entry.ModTime(),
	      name:         rel_name,
	    })
			if err != nil {
				return err
			}
    }
	} else {
    entries, err := ssh.client.ReadDir(dir)
    if err != nil {
      return err
    }
    for _, entry := range entries {
			err := process(&sshFile{
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

func (ssh *SSH) GetFileReader(key string) (io.ReadCloser, error) {
	file_path := path.Join(ssh.Config.Path, key)
	ssh.client.MkdirAll(path.Dir(file_path))
	return ssh.client.OpenFile(file_path, syscall.O_RDWR)
}

func (ssh *SSH) PutFile(key string, local_file io.ReadCloser) error {
	file_path := path.Join(ssh.Config.Path, key)

	ssh.client.MkdirAll(path.Dir(file_path))

	remote_file, err := ssh.client.Create(file_path)
	if err != nil {
	  return err
  }
	defer remote_file.Close()

  _, err = remote_file.ReadFrom(local_file)
  if  err!= nil {
    return err
  }

	return nil
}

// Implement RemoteFile
type sshFile struct {
	size         int64
	lastModified time.Time
	name         string
}

func (file *sshFile) Size() int64 {
	return file.size
}

func (file *sshFile) LastModified() time.Time {
	return file.lastModified
}

func (file *sshFile) Name() string {
	return file.name
}
