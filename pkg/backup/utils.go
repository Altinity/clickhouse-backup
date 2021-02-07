package backup

import (
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/apex/log"
)

func moveShadow(shadowPath, backupPath string) error {
	return filepath.Walk(shadowPath, func(filePath string, info os.FileInfo, err error) error {
		relativePath := strings.Trim(strings.TrimPrefix(filePath, shadowPath), "/")
		pathParts := strings.SplitN(relativePath, "/", 3)
		if len(pathParts) != 3 {
			return nil
		}
		dstFilePath := filepath.Join(backupPath, pathParts[2])
		if info.IsDir() {
			return os.MkdirAll(dstFilePath, os.ModePerm)
		}
		if !info.Mode().IsRegular() {
			log.Debugf("'%s' is not a regular file, skipping", filePath)
			return nil
		}
		return os.Rename(filePath, dstFilePath)
	})
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

func getPathByDiskName(diskMapConfig map[string]string, chDiskMap map[string]string, diskName string) (string, error) {
	if p, ok := diskMapConfig[diskName]; ok {
		return p, nil
	}
	if p, ok := chDiskMap[diskName]; ok {
		return p, nil
	}
	return "", fmt.Errorf("disk '%s' not found in clickhouse, you can add nonexistent disks to disk_mapping config", diskName)
}
