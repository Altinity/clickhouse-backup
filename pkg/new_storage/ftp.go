package new_storage

import (
	"context"
	"crypto/tls"
	apexLog "github.com/apex/log"
	"io"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/AlexAkulov/clickhouse-backup/config"
	"github.com/jlaffaye/ftp"
	"github.com/jolestar/go-commons-pool/v2"
)

type FTP struct {
	factory       pool.PooledObjectFactory
	clients       *pool.ObjectPool
	ctx           context.Context
	Config        *config.FTPConfig
	dirCache      map[string]struct{}
	dirCacheMutex sync.RWMutex
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
	if f.Config.Debug {
		options = append(options, ftp.DialWithDebugOutput(os.Stdout))
	}
	if f.Config.TLS {
		tlsConfig := tls.Config{}
		options = append(options, ftp.DialWithTLS(&tlsConfig))
	}
	f.factory = pool.NewPooledObjectFactorySimple(
		func(context.Context) (interface{}, error) {
			c, err := ftp.Dial(f.Config.Address, options...)
			if err != nil {
				return nil, err
			}
			if err := c.Login(f.Config.Username, f.Config.Password); err != nil {
				return nil, err
			}
			return c, nil
		},
	)
	f.ctx = context.Background()
	f.clients = pool.NewObjectPoolWithDefaultConfig(f.ctx, f.factory)
	if f.Config.Concurrency > 1 {
		f.clients.Config.MaxTotal = int(f.Config.Concurrency)
	}
	f.clients.Config.MaxIdle = 0
	f.clients.Config.MinIdle = 0

	f.dirCacheMutex.Lock()
	f.dirCache = map[string]struct{}{}
	f.dirCacheMutex.Unlock()
	return nil
}

func (f *FTP) Kind() string {
	return "FTP"
}

// getConnectionFromPool *ftp.ServerConn is not thread-safe, so we need implements connection pool
func (f *FTP) getConnectionFromPool() (*ftp.ServerConn, error) {
	apexLog.Debugf("FTP::getConnectionFromPool()")
	client, err := f.clients.BorrowObject(f.ctx)
	if err != nil {
		apexLog.Errorf("can't BorrowObject from FTP Connection Pool: %v", err)
		return nil, err
	}
	return client.(*ftp.ServerConn), nil
}

func (f *FTP) returnConnectionToPool(client *ftp.ServerConn) {
	apexLog.Debugf("FTP::returnConnectionToPool(%v)", client)
	if client != nil {
		err := f.clients.ReturnObject(f.ctx, client)
		if err != nil {
			apexLog.Errorf("can't ReturnObject to FTP Connection Pool: %v", err)
		}
	}
}

func (f *FTP) StatFile(key string) (RemoteFile, error) {
	// cant list files, so check the dir
	dir := path.Dir(path.Join(f.Config.Path, key))
	client, err := f.getConnectionFromPool()
	defer f.returnConnectionToPool(client)
	if err != nil {
		return nil, err
	}
	entries, err := client.List(dir)
	if err != nil {
		// proftpd return 550 error if `dir` not exists
		if strings.HasPrefix(err.Error(), "550") {
			return nil, ErrNotFound
		}
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
	client, err := f.getConnectionFromPool()
	defer f.returnConnectionToPool(client)
	if err != nil {
		return err
	}
	return client.RemoveDirRecur(path.Join(f.Config.Path, key))
}

func (f *FTP) Walk(ftpPath string, recursive bool, process func(RemoteFile) error) error {
	client, err := f.getConnectionFromPool()
	defer f.returnConnectionToPool(client)
	if err != nil {
		return err
	}
	prefix := path.Join(f.Config.Path, ftpPath)
	if !recursive {
		entries, err := client.List(prefix)
		if err != nil {
			// proftpd return 550 error if prefix not exits
			if strings.HasPrefix(err.Error(), "550") {
				return nil
			}
			return err
		}
		for _, entry := range entries {
			if entry.Name == "." || entry.Name == ".." {
				continue
			}
			if err := process(&ftpFile{
				size:         int64(entry.Size),
				lastModified: entry.Time,
				name:         entry.Name,
			}); err != nil {
				return err
			}
		}
		return nil
	}
	walker := client.Walk(prefix)
	for walker.Next() {
		if err := walker.Err(); err != nil {
			return err
		}
		entry := walker.Stat()
		if entry == nil {
			continue
		}
		if err := process(&ftpFile{
			size:         int64(entry.Size),
			lastModified: entry.Time,
			name:         strings.Trim(walker.Path(), prefix),
		}); err != nil {
			return err
		}
	}
	return nil
}

func (f *FTP) GetFileReader(key string) (io.ReadCloser, error) {
	apexLog.Debugf("FTP::GetFileReader key=%s", key)
	client, err := f.getConnectionFromPool()
	if err != nil {
		return nil, err
	}
	resp, err := client.Retr(path.Join(f.Config.Path, key))
	return &FTPFileReader{
		Response: resp,
		pool:     f,
		client:   client,
	}, err
}

func (f *FTP) PutFile(key string, r io.ReadCloser) error {
	apexLog.Debugf("FTP::PutFile key=%s", key)
	client, err := f.getConnectionFromPool()
	defer f.returnConnectionToPool(client)
	if err != nil {
		return err
	}
	k := path.Join(f.Config.Path, key)
	f.MkdirAll(path.Dir(k))
	return client.Stor(k, r)
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
	client, err := f.getConnectionFromPool()
	defer f.returnConnectionToPool(client)
	if err != nil {
		return err
	}
	dirs := strings.Split(key, "/")
	client.ChangeDir("/")
	for i := range dirs {
		d := path.Join(dirs[:i+1]...)

		f.dirCacheMutex.RLock()
		if _, ok := f.dirCache[d]; ok {
			f.dirCacheMutex.RUnlock()
			continue
		}
		f.dirCacheMutex.RUnlock()

		client.MakeDir(d)

		f.dirCacheMutex.Lock()
		f.dirCache[d] = struct{}{}
		f.dirCacheMutex.Unlock()
	}
	return nil
}

type FTPFileReader struct {
	*ftp.Response
	pool   *FTP
	client *ftp.ServerConn
}

func (fr *FTPFileReader) Close() error {
	defer fr.pool.returnConnectionToPool(fr.client)
	return fr.Response.Close()
}
