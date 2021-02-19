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

func moveShadow(shadowPath, backupPath string) (int64, error) {
	size := int64(0)
	err := filepath.Walk(shadowPath, func(filePath string, info os.FileInfo, err error) error {
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
		size += info.Size()
		return os.Rename(filePath, dstFilePath)
	})
	return size, err
}

func moveShadowNew(shadowPath, backupPath string) error {
	shadow, err := os.Open(shadowPath)
	if err != nil {
		return err
	}
	dataDirs, err := shadow.Readdirnames(-1)
	if err != nil {
		return err
	}
	shadow.Close()
	for _, dataDir := range dataDirs {
		dbDirF, err := os.Open(path.Join(shadowPath, dataDir))
		if err != nil {
			return err
		}
		dbDirs, err := dbDirF.Readdirnames(-1)
		if err != nil {
			return err
		}
		dbDirF.Close()
		for _, dbDir := range dbDirs {
			if err := os.Mkdir(path.Join(backupPath, dbDir), 750); err != nil && !os.IsExist(err) {
				return err
			}
			tableDirF, err := os.Open(path.Join(shadowPath, dataDir, dbDir))
			if err != nil {
				return err
			}
			tableDirs, err := tableDirF.Readdirnames(-1)
			if err != nil {
				return err
			}
			tableDirF.Close()
			for _, tableDir := range tableDirs {
				oldPath := path.Join(shadowPath, dataDir, dbDir, tableDir)
				newPath := path.Join(backupPath, dbDir, tableDir)
				if err := os.Rename(oldPath, newPath); err != nil {
					return err
				}
			}
		}
	}
	return nil
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
