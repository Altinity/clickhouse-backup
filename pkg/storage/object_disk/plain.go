package object_disk

import (
	"context"
	"crypto/rand"
	"io"
	"path"
	"strings"
	"sync"

	"github.com/Altinity/clickhouse-backup/v2/pkg/storage"
	"github.com/pkg/errors"
)

// ClickHouse plain_rewritable bucket layout, see PlainRewritableLayout.h in ClickHouse sources:
// <disk_root>/__meta/<token>/prefix.path - one object per logical directory, body = logical dir path with trailing slash
// <disk_root>/<token>/<file_name>        - data files, flat, file names verbatim
// <disk_root>/__root/<file_name>         - files in the disk root (ClickHouse 25.11+)
const (
	plainRewritableMetaDir        = "__meta"
	plainRewritableRootDir        = "__root"
	plainRewritablePrefixPathName = "prefix.path"
)

// PlainFile describes one data object of a plain/plain_rewritable disk
type PlainFile struct {
	// RemoteKey - object key relative to the disk root
	RemoteKey string
	Size      int64
}

// PlainDiskLayout resolves logical directory paths to remote object key prefixes for
// object storage disks with metadata_type=plain (key == logical path) and
// plain_rewritable (flat random tokens + __meta/<token>/prefix.path mapping)
type PlainDiskLayout struct {
	diskName   string
	rewritable bool
	mu         sync.Mutex
	// dirs: logical dir path (relative to disk root, no trailing slash) -> remote dir prefix (relative to disk root),
	// filled only for plain_rewritable
	dirs map[string]string
}

// NewPlainDiskLayout builds the layout for diskName, for plain_rewritable disks it lists
// <disk_root>/__meta/ and reads every prefix.path object (the same way ClickHouse builds its
// in-memory path map at startup)
func NewPlainDiskLayout(ctx context.Context, diskName string) (*PlainDiskLayout, error) {
	connection, exists := DisksConnections.Load(diskName)
	if !exists {
		return nil, errors.Errorf("NewPlainDiskLayout: %s is not present in object_disk.DisksConnections", diskName)
	}
	if connection.MetadataType != "plain" && connection.MetadataType != "plain_rewritable" {
		return nil, errors.Errorf("NewPlainDiskLayout: disk %s have unexpected metadata_type %q", diskName, connection.MetadataType)
	}
	l := &PlainDiskLayout{
		diskName:   diskName,
		rewritable: connection.MetadataType == "plain_rewritable",
		dirs:       map[string]string{},
	}
	if !l.rewritable {
		return l, nil
	}
	remoteStorage := connection.GetRemoteStorage()
	tokens := make([]string, 0)
	if walkErr := remoteStorage.Walk(ctx, plainRewritableMetaDir, true, func(_ context.Context, f storage.RemoteFile) error {
		name := strings.Trim(f.Name(), "/")
		if !strings.HasSuffix(name, "/"+plainRewritablePrefixPathName) {
			return nil
		}
		tokens = append(tokens, strings.TrimSuffix(name, "/"+plainRewritablePrefixPathName))
		return nil
	}); walkErr != nil {
		return nil, errors.Wrapf(walkErr, "NewPlainDiskLayout: can't list %s for disk %s", plainRewritableMetaDir, diskName)
	}
	for _, token := range tokens {
		prefixPathKey := path.Join(plainRewritableMetaDir, token, plainRewritablePrefixPathName)
		r, readErr := remoteStorage.GetFileReader(ctx, prefixPathKey)
		if readErr != nil {
			return nil, errors.Wrapf(readErr, "NewPlainDiskLayout: can't read %s for disk %s", prefixPathKey, diskName)
		}
		content, readAllErr := io.ReadAll(r)
		closeErr := r.Close()
		if readAllErr != nil {
			return nil, errors.Wrapf(readAllErr, "NewPlainDiskLayout: can't read %s for disk %s", prefixPathKey, diskName)
		}
		if closeErr != nil {
			return nil, errors.Wrapf(closeErr, "NewPlainDiskLayout: can't close %s for disk %s", prefixPathKey, diskName)
		}
		logicalDir := strings.TrimSuffix(strings.TrimSpace(string(content)), "/")
		l.dirs[logicalDir] = token
	}
	if len(l.dirs) == 0 {
		if legacyErr := l.checkLegacyLayout(ctx, remoteStorage); legacyErr != nil {
			return nil, legacyErr
		}
	}
	return l, nil
}

// checkLegacyLayout distinguishes an empty plain_rewritable disk from the pre-24.8 legacy layout
// (nested random prefixes with prefix.path stored in-place among the data objects, no __meta subtree)
func (l *PlainDiskLayout) checkLegacyLayout(ctx context.Context, remoteStorage storage.RemoteStorage) error {
	legacyEntry := ""
	if walkErr := remoteStorage.Walk(ctx, "", false, func(_ context.Context, f storage.RemoteFile) error {
		name := strings.Trim(f.Name(), "/")
		if name != "" && name != plainRewritableMetaDir && name != plainRewritableRootDir && legacyEntry == "" {
			legacyEntry = name
		}
		return nil
	}); walkErr != nil {
		return errors.Wrapf(walkErr, "checkLegacyLayout: can't list disk %s root", l.diskName)
	}
	if legacyEntry != "" {
		return errors.Errorf("disk %s contains data (%s) but no %s subtree: legacy plain_rewritable layout (ClickHouse 24.6/24.7) is not supported", l.diskName, legacyEntry, plainRewritableMetaDir)
	}
	return nil
}

// ListTree returns all data files under logicalBase (recursively):
// logical path relative to logicalBase -> remote object key (relative to disk root) and size
func (l *PlainDiskLayout) ListTree(ctx context.Context, logicalBase string) (map[string]PlainFile, error) {
	connection, exists := DisksConnections.Load(l.diskName)
	if !exists {
		return nil, errors.Errorf("ListTree: %s is not present in object_disk.DisksConnections", l.diskName)
	}
	remoteStorage := connection.GetRemoteStorage()
	logicalBase = strings.Trim(logicalBase, "/")
	tree := map[string]PlainFile{}
	if !l.rewritable {
		if walkErr := remoteStorage.Walk(ctx, logicalBase, true, func(_ context.Context, f storage.RemoteFile) error {
			name := strings.Trim(f.Name(), "/")
			if name == "" || f.Size() == 0 && strings.HasSuffix(name, "/") {
				return nil
			}
			tree[name] = PlainFile{RemoteKey: path.Join(logicalBase, name), Size: f.Size()}
			return nil
		}); walkErr != nil {
			return nil, errors.Wrapf(walkErr, "ListTree: can't list %s on disk %s", logicalBase, l.diskName)
		}
		return tree, nil
	}
	l.mu.Lock()
	dirsUnderBase := map[string]string{}
	for logicalDir, token := range l.dirs {
		if logicalDir == logicalBase || strings.HasPrefix(logicalDir, logicalBase+"/") {
			dirsUnderBase[logicalDir] = token
		}
	}
	l.mu.Unlock()
	for logicalDir, token := range dirsUnderBase {
		relDir := strings.Trim(strings.TrimPrefix(logicalDir, logicalBase), "/")
		if walkErr := remoteStorage.Walk(ctx, token, true, func(_ context.Context, f storage.RemoteFile) error {
			name := strings.Trim(f.Name(), "/")
			if name == "" {
				return nil
			}
			tree[path.Join(relDir, name)] = PlainFile{RemoteKey: path.Join(token, name), Size: f.Size()}
			return nil
		}); walkErr != nil {
			return nil, errors.Wrapf(walkErr, "ListTree: can't list %s (logical %s) on disk %s", token, logicalDir, l.diskName)
		}
	}
	return tree, nil
}

// EnsureDir returns the remote key prefix (relative to disk root) for logicalDir,
// for plain_rewritable it creates the __meta/<token>/prefix.path objects for the directory and
// every missing ancestor (ClickHouse builds its in-memory path map from prefix.path objects only,
// each directory level must have its own entry)
func (l *PlainDiskLayout) EnsureDir(ctx context.Context, logicalDir string) (string, error) {
	logicalDir = strings.Trim(logicalDir, "/")
	if !l.rewritable {
		return logicalDir, nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if token, exists := l.dirs[logicalDir]; exists {
		return token, nil
	}
	dirParts := strings.Split(logicalDir, "/")
	token := ""
	for i := range dirParts {
		currentDir := strings.Join(dirParts[:i+1], "/")
		if existingToken, exists := l.dirs[currentDir]; exists {
			token = existingToken
			continue
		}
		newToken, err := randomASCIIToken(32)
		if err != nil {
			return "", errors.Wrap(err, "EnsureDir: randomASCIIToken")
		}
		// ClickHouse expects the full logical directory path with a trailing slash as prefix.path body
		if putErr := PutFile(ctx, l.diskName, path.Join(plainRewritableMetaDir, newToken, plainRewritablePrefixPathName), []byte(currentDir+"/")); putErr != nil {
			return "", errors.Wrapf(putErr, "EnsureDir: can't write prefix.path for %s on disk %s", currentDir, l.diskName)
		}
		l.dirs[currentDir] = newToken
		token = newToken
	}
	return token, nil
}

const asciiTokenAlphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

// randomASCIIToken mirrors ClickHouse getRandomASCIIString used for plain_rewritable directory tokens
func randomASCIIToken(size int) (string, error) {
	buf := make([]byte, size)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	for i := range buf {
		buf[i] = asciiTokenAlphabet[int(buf[i])%len(asciiTokenAlphabet)]
	}
	return string(buf), nil
}
