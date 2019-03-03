package main

import (
	"io"
	"os"
)

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
	if _, err = io.Copy(dst, src); err != nil {
		return err
	}
	return nil
}

func moveFile(srcFile string, dstFile string) error {
	err := os.Rename(srcFile, dstFile)
	if err != nil {
		return err
	}
	return nil
}
