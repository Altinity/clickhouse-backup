package storage

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestIsFTPDirAlreadyExists MkdirAll must treat an existing directory as success and cache it,
// otherwise every concurrent uploaded object re-issues MKD for the same directories
func TestIsFTPDirAlreadyExists(t *testing.T) {
	assert.True(t, isFTPDirAlreadyExists(errors.New(`550 "object_disk/22_3: File exists"`)))
	assert.True(t, isFTPDirAlreadyExists(errors.New("521 Directory already exists")))
	assert.False(t, isFTPDirAlreadyExists(errors.New(`550 "object_disk/22_3: Permission denied"`)))
	assert.False(t, isFTPDirAlreadyExists(errors.New(`550 "object_disk/22_3: No such file or directory"`)))
	assert.False(t, isFTPDirAlreadyExists(errors.New("550 parent directory does not exists")))
}

// TestForgetDirCache a failed store proves the cached directory tree is stale, all its levels
// have to be dropped so the retry re-creates them
func TestForgetDirCache(t *testing.T) {
	f := &FTP{dirCache: map[string]bool{
		"object_disk":                  true,
		"object_disk/22_3":             true,
		"object_disk/22_3/backup":      true,
		"object_disk/22_3/backup/disk": true,
		"other":                        true,
	}}
	f.forgetDirCache("/object_disk/22_3/backup/disk")
	assert.Equal(t, map[string]bool{"other": true}, f.dirCache)
}
