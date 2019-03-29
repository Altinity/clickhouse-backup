package main

import (
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

func cleanDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	names, err := d.Readdirnames(-1)
	if err != nil {
		return err
	}
	for _, name := range names {
		err = os.RemoveAll(filepath.Join(dir, name))
		if err != nil {
			return err
		}
	}
	return nil
}

func copyPath(src, dst string, dryRun bool) error {
	return filepath.Walk(src, func(filePath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		filePath = filepath.ToSlash(filePath) // fix Windows slashes
		filename := strings.Trim(strings.TrimPrefix(filePath, src), "/")
		dstFilePath := filepath.Join(dst, filename)
		if dryRun {
			if info.IsDir() {
				log.Printf("make path %s", dstFilePath)
				return nil
			}
			if !info.Mode().IsRegular() {
				log.Printf("'%s' is not a regular file, skipping", filePath)
				return nil
			}
			log.Printf("copy %s -> %s", filePath, dstFilePath)
			return nil
		}
		if info.IsDir() {
			return os.MkdirAll(dstFilePath, os.ModePerm)
		}
		if !info.Mode().IsRegular() {
			log.Printf("'%s' is not a regular file, skipping", filePath)
			return nil
		}
		return copyFile(filePath, dstFilePath)
	})
}

func copyFile(srcFile string, dstFile string) error {
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

func GetBackupsToDelete(backups []string, keep int) []string {
	type parsedBackup struct {
		name string
		time time.Time
	}
	backupList := []parsedBackup{}
	for _, backupName := range backups {
		t, err := time.Parse(BackupTimeFormat, strings.TrimSuffix(backupName, ".tar"))
		if err == nil {
			backupList = append(backupList, parsedBackup{
				name: backupName,
				time: t,
			})
		}
	}
	sort.SliceStable(backupList, func(i, j int) bool {
		return backupList[i].time.Before(backupList[j].time)
	})
	result := []string{}
	if len(backupList) > keep {
		for i := 0; i < len(backupList)-keep; i++ {
			result = append(result, backupList[i].name)
		}
	}
	return result
}
