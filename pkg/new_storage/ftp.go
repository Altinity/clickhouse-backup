package new_storage

import (
	"crypto/tls"
	"io"
	"os"
	"path"
	"strings"
	"time"

	"github.com/AlexAkulov/clickhouse-backup/config"

	"github.com/jlaffaye/ftp"
)

type FTP struct {
	client *ftp.ServerConn
	Config *config.FTPConfig
	Debug  bool
	dirCache map[string]struct{}
}

func (f *FTP) Connect() error {
	timeout, err := time.ParseDuration(f.Config.Timeout)
	if err != nil {
		return err
	}
	options := []ftp.DialOption{
		ftp.DialWithTimeout(timeout),
		ftp.DialWithDisabledEPSV(true),
	}
	if f.Debug {
		options = append(options, ftp.DialWithDebugOutput(os.Stdout))
	}
	if f.Config.TLS {
		tlsConfig := tls.Config{}
		options = append(options, ftp.DialWithTLS(&tlsConfig))
	}
	c, err := ftp.Dial(f.Config.Address, options...)
	if err != nil {
		return err
	}
	if err := c.Login(f.Config.Username, f.Config.Password); err != nil {
		return err
	}
	f.client = c
	f.dirCache = map[string]struct{}{}
	return nil
}

func (f *FTP) Kind() string {
	return "FTP"
}

func (f *FTP) StatFile(key string) (RemoteFile, error) {
	// cant list files, so check the dir
	dir := path.Dir(path.Join(f.Config.Path, key))
	entries, err := f.client.List(dir)
	if err != nil {
		return nil, err
	}
	file := path.Base(path.Join(f.Config.Path, key))
	for i := range entries {
		if file == entries[i].Name {
			// file found, return it
			return &ftpFile{
				size:         int64(entries[i].Size),
				lastModified: entries[i].Time,
				name:         entries[i].Name,
			}, nil
		}
	}

	return nil, ErrNotFound
}

func (f *FTP) DeleteFile(key string) error {
	return f.client.Delete(path.Join(f.Config.Path, key))
}

func (f *FTP) Walk(ftpPath string, recirsive bool, process func(RemoteFile) error) error {
	prefix := path.Join(f.Config.Path, ftpPath)
	if !recirsive {
		entries, err := f.client.List(prefix)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			process(&ftpFile{
				size:         int64(entry.Size),
				lastModified: entry.Time,
				name:         entry.Name,
			})
		}
		return nil
	}
	walker := f.client.Walk(prefix)
	for walker.Next() {
		if err := walker.Err(); err != nil {
			return err
		}
		entry := walker.Stat()
		if entry == nil {
			continue
		}
		process(&ftpFile{
			size:         int64(entry.Size),
			lastModified: entry.Time,
			name:         strings.Trim(walker.Path(), prefix),
		})
	}
	return nil
}

func (f *FTP) GetFileReader(key string) (io.ReadCloser, error) {
	return f.client.Retr(path.Join(f.Config.Path, key))
}

func (f *FTP) PutFile(key string, r io.ReadCloser) error {
	k := path.Join(f.Config.Path, key)
	f.MkdirAll(path.Dir(k))
	return f.client.Stor(k, r)
}

type ftpFile struct {
	size         int64
	lastModified time.Time
	name         string
}

func (f *ftpFile) Size() int64 {
	return f.size
}

func (f *ftpFile) LastModified() time.Time {
	return f.lastModified
}

func (f *ftpFile) Name() string {
	return f.name
}

func (f *FTP) MkdirAll(key string) error {
	dirs := strings.Split(key, "/")
	f.client.ChangeDir("/")
	for i := range dirs {
		d := path.Join(dirs[:i+1]...)
		if _, ok := f.dirCache[d]; ok {
			continue
		}
		f.client.MakeDir(d)
		f.dirCache[d] = struct{}{}
	}
	return nil
}
