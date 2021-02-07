package storage

import (
	"crypto/tls"
	"io"
	"os"
	"path"
	"time"

	"github.com/AlexAkulov/clickhouse-backup/config"

	"github.com/jlaffaye/ftp"
)

type FTP struct {
	client *ftp.ServerConn
	Config *config.FTPConfig
	Debug  bool
}

func (f *FTP) Connect() error {
	timeout, err := time.ParseDuration(f.Config.Timeout)
	if err != nil {
		return err
	}

	options := make([]ftp.DialOption, 0)

	options = append(options, ftp.DialWithTimeout(timeout))
	options = append(options, ftp.DialWithDisabledEPSV(true))

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

	return nil
}

func (f *FTP) Kind() string {
	return "FTP"
}

func (f *FTP) GetFile(key string) (RemoteFile, error) {
	// cant list files, so check the dir
	dir := path.Dir(key)

	entries, err := f.client.List(dir)
	if err != nil {
		return nil, err
	}

	file := path.Base(key)

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
	if err := f.client.Delete(key); err != nil {
		return err
	}
	return nil
}

func (f *FTP) Walk(root string, process func(RemoteFile)) error {
	walker := f.client.Walk(root)

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
			name:         walker.Path(),
		})
	}

	return nil
}

func (f *FTP) GetFileReader(key string) (io.ReadCloser, error) {
	return f.client.Retr(key)
}

func (f *FTP) PutFile(key string, r io.ReadCloser) error {
	return f.client.Stor(key, r)
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
