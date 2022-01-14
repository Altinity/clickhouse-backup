package backup

import (
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"github.com/AlexAkulov/clickhouse-backup/pkg/metadata"
	apexLog "github.com/apex/log"
)

func moveShadow(shadowPath, backupPartsPath string) ([]metadata.Part, int64, error) {
	size := int64(0)
	parts := []metadata.Part{}
	err := filepath.Walk(shadowPath, func(filePath string, info os.FileInfo, err error) error {
		relativePath := strings.Trim(strings.TrimPrefix(filePath, shadowPath), "/")
		pathParts := strings.SplitN(relativePath, "/", 4)
		// [store 1f9 1f9dc899-0de9-41f8-b95c-26c1f0d67d93 20181023_2_2_0/partition.dat]
		if len(pathParts) != 4 {
			return nil
		}
		dstFilePath := filepath.Join(backupPartsPath, pathParts[3])
		if info.IsDir() {
			parts = append(parts, metadata.Part{
				Name: pathParts[3],
			})
			return os.MkdirAll(dstFilePath, 0750)
		}
		if !info.Mode().IsRegular() {
			apexLog.Debugf("'%s' is not a regular file, skipping", filePath)
			return nil
		}
		size += info.Size()
		return os.Rename(filePath, dstFilePath)
	})
	return parts, size, err
}

func copyFile(srcFile string, dstFile string) error {
	if err := os.MkdirAll(path.Dir(dstFile), os.ModePerm); err != nil {
		return err
	}
	src, err := os.Open(srcFile)
	if err != nil {
		return err
	}
	defer src.Close()
	dst, err := os.Create(dstFile)
	if err != nil {
		return err
	}
	defer dst.Close()
	_, err = io.Copy(dst, src)
	return err
}

func GetBackupsToDelete(backups []BackupLocal, keep int) []BackupLocal {
	if len(backups) > keep {
		sort.SliceStable(backups, func(i, j int) bool {
			return backups[i].CreationDate.After(backups[j].CreationDate)
		})
		return backups[keep:]
	}
	return []BackupLocal{}
}
