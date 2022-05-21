package new_storage

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/AlexAkulov/clickhouse-backup/pkg/config"
	apexLog "github.com/apex/log"
	"io"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/jlaffaye/ftp"
	"github.com/jolestar/go-commons-pool/v2"
)

type FTP struct {
	clients       *pool.ObjectPool
	ctx           context.Context
	Config        *config.FTPConfig
	dirCache      map[string]bool
	dirCacheMutex sync.RWMutex
}

func (f *FTP) Connect() error {
	timeout, err := time.ParseDuration(f.Config.Timeout)
	if err != nil {
		return err
	}
	options := []ftp.DialOption{}
	if timeout > 0 {
		options = append(options, ftp.DialWithTimeout(timeout))
	}
	if f.Config.Debug {
		options = append(options, ftp.DialWithDebugOutput(os.Stdout))
	}
	if f.Config.TLS {
		tlsConfig := tls.Config{}
		options = append(options, ftp.DialWithTLS(&tlsConfig))
	}
	f.ctx = context.Background()
	f.clients = pool.NewObjectPoolWithDefaultConfig(f.ctx, &ftpPoolFactory{options: options, ftp: f})
	if f.Config.Concurrency > 1 {
		f.clients.Config.MaxTotal = int(f.Config.Concurrency)*2 + 1
	}

	f.dirCacheMutex.Lock()
	f.dirCache = map[string]bool{}
	f.dirCacheMutex.Unlock()
	return nil
}

func (f *FTP) Kind() string {
	return "FTP"
}

// getConnectionFromPool *ftp.ServerConn is not thread-safe, so we need implements connection pool
func (f *FTP) getConnectionFromPool(where string) (*ftp.ServerConn, error) {
	apexLog.Debugf("FTP::getConnectionFromPool(%s) active=%d idle=%d", where, f.clients.GetNumActive(), f.clients.GetNumIdle())
	client, err := f.clients.BorrowObject(f.ctx)
	if err != nil {
		apexLog.Errorf("can't BorrowObject from FTP Connection Pool: %v", err)
		return nil, err
	}
	return client.(*ftp.ServerConn), nil
}

func (f *FTP) returnConnectionToPool(where string, client *ftp.ServerConn) {
	apexLog.Debugf("FTP::returnConnectionToPool(%s) active=%d idle=%d", where, f.clients.GetNumActive(), f.clients.GetNumIdle())
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
	client, err := f.getConnectionFromPool(fmt.Sprintf("StatFile, key=%s", key))
	if err != nil {
		return nil, err
	}
	defer f.returnConnectionToPool(fmt.Sprintf("StatFile, key=%s", key), client)
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
	client, err := f.getConnectionFromPool("DeleteFile")
	defer f.returnConnectionToPool("DeleteFile", client)
	if err != nil {
		return err
	}
	return client.RemoveDirRecur(path.Join(f.Config.Path, key))
}

func (f *FTP) Walk(ftpPath string, recursive bool, process func(RemoteFile) error) error {
	client, err := f.getConnectionFromPool("Walk")
	defer f.returnConnectionToPool("Walk", client)
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
	client, err := f.getConnectionFromPool("GetFileReader")
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

func (f *FTP) GetFileReaderWithLocalPath(key, _ string) (io.ReadCloser, error) {
	return f.GetFileReader(key)
}

func (f *FTP) PutFile(key string, r io.ReadCloser) error {
	apexLog.Debugf("FTP::PutFile key=%s", key)
	client, err := f.getConnectionFromPool("PutFile")
	defer f.returnConnectionToPool("PutFile", client)
	if err != nil {
		return err
	}
	k := path.Join(f.Config.Path, key)
	err = f.MkdirAll(path.Dir(k), client)
	if err != nil {
		return err
	}
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

func (f *FTP) MkdirAll(key string, client *ftp.ServerConn) error {
	dirs := strings.Split(key, "/")
	err := client.ChangeDir("/")
	if err != nil {
		return err
	}

	for i := range dirs {
		d := path.Join(dirs[:i+1]...)
		if d != "" {
			f.dirCacheMutex.RLock()
			if _, exists := f.dirCache[d]; exists {
				f.dirCacheMutex.RUnlock()
				apexLog.Debugf("FTP::MkdirAll %s exists in dirCache", d)
				continue
			}
			f.dirCacheMutex.RUnlock()

			f.dirCacheMutex.Lock()
			err = client.MakeDir(d)
			if err != nil {
				apexLog.Warnf("FTP::MkdirAll MakeDir(%s) return error: %v", d, err)
			} else {
				f.dirCache[d] = true
			}
			f.dirCacheMutex.Unlock()
		}
	}
	return nil
}

type FTPFileReader struct {
	*ftp.Response
	pool   *FTP
	client *ftp.ServerConn
}

func (fr *FTPFileReader) Close() error {
	defer fr.pool.returnConnectionToPool("FTPFileReader.Close", fr.client)
	return fr.Response.Close()
}

type ftpPoolFactory struct {
	options []ftp.DialOption
	ftp     *FTP
}

func (f *ftpPoolFactory) MakeObject(ctx context.Context) (*pool.PooledObject, error) {
	c, err := ftp.Dial(f.ftp.Config.Address, f.options...)
	if err != nil {
		return nil, err
	}
	if err := c.Login(f.ftp.Config.Username, f.ftp.Config.Password); err != nil {
		return nil, err
	}
	return pool.NewPooledObject(c), nil
}

func (f *ftpPoolFactory) DestroyObject(ctx context.Context, object *pool.PooledObject) error {
	return object.Object.(*ftp.ServerConn).Quit()
}

func (f *ftpPoolFactory) ValidateObject(ctx context.Context, object *pool.PooledObject) bool {
	return true
}

func (f *ftpPoolFactory) ActivateObject(ctx context.Context, object *pool.PooledObject) error {
	return nil
}

func (f *ftpPoolFactory) PassivateObject(ctx context.Context, object *pool.PooledObject) error {
	return nil
}
