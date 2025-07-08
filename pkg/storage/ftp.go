package storage

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/jlaffaye/ftp"
	"github.com/jolestar/go-commons-pool/v2"
	"github.com/rs/zerolog/log"
)

type FTP struct {
	clients       *pool.ObjectPool
	Config        *config.FTPConfig
	dirCache      map[string]bool
	dirCacheMutex sync.RWMutex
}

func (f *FTP) Kind() string {
	return "FTP"
}

func (f *FTP) Connect(ctx context.Context) error {
	timeout, err := time.ParseDuration(f.Config.Timeout)
	if err != nil {
		return err
	}
	options := make([]ftp.DialOption, 0)
	options = append(options, ftp.DialWithContext(ctx))
	if timeout > 0 {
		options = append(options, ftp.DialWithTimeout(timeout))
	}
	if f.Config.Debug {
		options = append(options, ftp.DialWithDebugOutput(os.Stderr))
	}
	if f.Config.TLS {
		tlsConfig := tls.Config{InsecureSkipVerify: f.Config.SkipTLSVerify}
		options = append(options, ftp.DialWithTLS(&tlsConfig))
	}
	f.clients = pool.NewObjectPoolWithDefaultConfig(ctx, &ftpPoolFactory{options: options, ftp: f})
	if f.Config.Concurrency > 1 {
		f.clients.Config.MaxTotal = int(f.Config.Concurrency) * 4
	}

	f.dirCacheMutex.Lock()
	f.dirCache = map[string]bool{}
	f.dirCacheMutex.Unlock()
	return nil
}

func (f *FTP) Close(ctx context.Context) error {
	f.clients.Close(ctx)
	return nil
}

// getConnectionFromPool *ftp.ServerConn is not thread-safe, so we need implements connection pool
func (f *FTP) getConnectionFromPool(ctx context.Context, where string) (*ftp.ServerConn, error) {
	log.Debug().Msgf("getConnectionFromPool(%s) active=%d idle=%d", where, f.clients.GetNumActive(), f.clients.GetNumIdle())
	client, err := f.clients.BorrowObject(ctx)
	if err != nil {
		log.Error().Msgf("can't BorrowObject(%s) from FTP Connection Pool: %v", where, err)
		return nil, err
	}
	return client.(*ftp.ServerConn), nil
}

func (f *FTP) returnConnectionToPool(ctx context.Context, where string, client *ftp.ServerConn) {
	log.Debug().Msgf("returnConnectionToPool(%s) active=%d idle=%d", where, f.clients.GetNumActive(), f.clients.GetNumIdle())
	if client != nil {
		err := f.clients.ReturnObject(ctx, client)
		if err != nil {
			log.Error().Msgf("can't ReturnObject(%s) to FTP Connection Pool: %v", where, err)
		}
	}
}

func (f *FTP) StatFile(ctx context.Context, key string) (RemoteFile, error) {
	return f.StatFileAbsolute(ctx, path.Join(f.Config.Path, key))
}

func (f *FTP) StatFileAbsolute(ctx context.Context, key string) (RemoteFile, error) {
	// can't list files, so check the dir
	dir := path.Dir(key)
	client, err := f.getConnectionFromPool(ctx, fmt.Sprintf("StatFile, key=%s", key))
	if err != nil {
		return nil, err
	}
	defer f.returnConnectionToPool(ctx, fmt.Sprintf("StatFile, key=%s", key), client)
	entries, err := client.List(dir)
	if err != nil {
		// proftpd return 550 error if `dir` not exists
		if strings.HasPrefix(err.Error(), "550") {
			return nil, ErrNotFound
		}
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

func (f *FTP) DeleteFile(ctx context.Context, key string) error {
	where := fmt.Sprintf("DeleteFile->%s", key)
	client, err := f.getConnectionFromPool(ctx, where)
	defer f.returnConnectionToPool(ctx, where, client)
	if err != nil {
		return err
	}
	return client.RemoveDirRecur(path.Join(f.Config.Path, key))
}

func (f *FTP) Walk(ctx context.Context, ftpPath string, recursive bool, process func(context.Context, RemoteFile) error) error {
	prefix := path.Join(f.Config.Path, ftpPath)
	return f.WalkAbsolute(ctx, prefix, recursive, process)
}

func (f *FTP) WalkAbsolute(ctx context.Context, prefix string, recursive bool, process func(context.Context, RemoteFile) error) error {
	client, err := f.getConnectionFromPool(ctx, "Walk")
	if err != nil {
		return err
	}
	if !recursive {
		entries, err := client.List(prefix)
		f.returnConnectionToPool(ctx, "Walk", client)
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
			if err := process(ctx, &ftpFile{
				size:         int64(entry.Size),
				lastModified: entry.Time,
				name:         entry.Name,
			}); err != nil {
				return err
			}
		}
		return nil
	}
	defer f.returnConnectionToPool(ctx, "Walk", client)
	walker := client.Walk(prefix)
	for walker.Next() {
		if err := walker.Err(); err != nil {
			return err
		}
		entry := walker.Stat()
		if entry == nil {
			continue
		}
		if err := process(ctx, &ftpFile{
			size:         int64(entry.Size),
			lastModified: entry.Time,
			name:         strings.TrimPrefix(walker.Path(), prefix),
		}); err != nil {
			return err
		}
	}
	return nil
}

func (f *FTP) GetFileReader(ctx context.Context, key string) (io.ReadCloser, error) {
	return f.GetFileReaderAbsolute(ctx, path.Join(f.Config.Path, key))
}
func (f *FTP) GetFileReaderAbsolute(ctx context.Context, key string) (io.ReadCloser, error) {
	where := fmt.Sprintf("GetFileReaderAbsolute->%s", key)
	client, err := f.getConnectionFromPool(ctx, where)
	if err != nil {
		return nil, err
	}
	resp, err := client.Retr(key)
	return &FTPFileReader{
		Response: resp,
		pool:     f,
		ctx:      ctx,
		client:   client,
	}, err
}

func (f *FTP) GetFileReaderWithLocalPath(ctx context.Context, key, localPath string, remoteSize int64) (io.ReadCloser, error) {
	return f.GetFileReader(ctx, key)
}

func (f *FTP) PutFile(ctx context.Context, key string, r io.ReadCloser, localSize int64) error {
	return f.PutFileAbsolute(ctx, path.Join(f.Config.Path, key), r, localSize)
}

func (f *FTP) PutFileAbsolute(ctx context.Context, key string, r io.ReadCloser, localSize int64) error {
	where := fmt.Sprintf("PutFileReaderAbsolute->%s", key)
	client, err := f.getConnectionFromPool(ctx, where)
	defer f.returnConnectionToPool(ctx, where, client)
	if err != nil {
		return err
	}
	err = f.MkdirAll(path.Dir(key), client)
	if err != nil {
		return err
	}
	return client.Stor(key, r)
}

func (f *FTP) CopyObject(ctx context.Context, srcSize int64, srcBucket, srcKey, dstKey string) (int64, error) {
	return 0, fmt.Errorf("CopyObject not imlemented for %s", f.Kind())
}

func (f *FTP) DeleteFileFromObjectDiskBackup(ctx context.Context, key string) error {
	where := fmt.Sprintf("DeleteFileFromObjectDiskBackup->%s", key)
	client, err := f.getConnectionFromPool(ctx, where)
	defer f.returnConnectionToPool(ctx, where, client)
	if err != nil {
		return err
	}
	return client.RemoveDirRecur(path.Join(f.Config.ObjectDiskPath, key))
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
				log.Debug().Msgf("MkdirAll %s exists in dirCache", d)
				continue
			}
			f.dirCacheMutex.RUnlock()

			f.dirCacheMutex.Lock()
			err = client.MakeDir(d)
			if err != nil {
				log.Warn().Msgf("MkdirAll MakeDir(%s) return error: %v", d, err)
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
	ctx    context.Context
	client *ftp.ServerConn
}

func (fr *FTPFileReader) Close() error {
	defer fr.pool.returnConnectionToPool(fr.ctx, "FTPFileReader.Close", fr.client)
	return fr.Response.Close()
}

type ftpPoolFactory struct {
	options []ftp.DialOption
	ftp     *FTP
}

func (f *ftpPoolFactory) MakeObject(ctx context.Context) (*pool.PooledObject, error) {
	c, err := ftp.Dial(f.ftp.Config.Address, f.options...)
	if err != nil {
		return nil, fmt.Errorf("ftpPoolFactory->MakeObject Dial error: %v", err)
	}
	if err = c.Login(f.ftp.Config.Username, f.ftp.Config.Password); err != nil {
		return nil, fmt.Errorf("ftpPoolFactory->MakeObject Login error: %v", err)
	}
	return pool.NewPooledObject(c), nil
}

func (f *ftpPoolFactory) DestroyObject(ctx context.Context, object *pool.PooledObject) error {
	if err := object.Object.(*ftp.ServerConn).Quit(); err != nil {
		log.Warn().Msgf("ftpPoolFactory->Destroy Quit error: %v", err)
		return err
	}
	return nil
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
