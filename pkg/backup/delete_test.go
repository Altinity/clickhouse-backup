package backup

import (
	"os"
	"path"
	"testing"
)

func TestCleanDir(t *testing.T) {
	t.Run("Test deletion of nonexistent directory",
		func(t *testing.T) {
			b := &Backuper{}

			dir := path.Join(t.TempDir(), t.Name(), "does-not-exist")
			if err := b.cleanDir(dir); err != nil {
				t.Fatalf("unexpected error when deleting nonexistent dir: %v", err)
			}
		},
	)

	t.Run("Test deletion of existing directory",
		func(t *testing.T) {
			b := &Backuper{}

			dir := t.TempDir()
			if err := os.MkdirAll(dir, 0644); err != nil {
				t.Fatalf("unexpected error while creating temporary directory: %v", err)
			}
			if err := b.cleanDir(dir); err != nil {
				t.Fatalf("unexpected error while deleting existing dir: %v", err)
			}
			if err := b.cleanDir(dir); err != nil {
				t.Fatalf("unexpected error during back to back invocation of delete: %v", err)
			}
		},
	)
}
