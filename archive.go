package main

import (
	tarArchive "archive/tar"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

func TarDirs(w io.Writer, dirs ...string) error {
	tw := tarArchive.NewWriter(w)
	defer tw.Close()
	for _, dir := range dirs {
		if err := TarDir(tw, dir); err != nil {
			return err
		}
	}
	return nil
}

func TarDir(tw *tarArchive.Writer, dir string) error {
	return tarDir(tw, dir)
}

type devino struct {
	Dev uint64
	Ino uint64
}

func tarDir(tw *tarArchive.Writer, dir string) (err error) {
	t0 := time.Now()
	nFiles := 0
	hLinks := 0
	defer func() {
		td := time.Since(t0)
		if err == nil {
			log.Printf("added to tarball with: %d files, %d hard links (%v)", nFiles, hLinks, td)
		} else {
			log.Printf("error adding to tarball after %d files, %d hard links, %v: %v", nFiles, hLinks, td, err)
		}
	}()

	fi, err := os.Stat(dir)
	if err != nil {
		return fmt.Errorf("unable to tar files - %v", err.Error())
	}
	if !fi.IsDir() {
		return fmt.Errorf("data path is not a directory - %v", err.Error())
	}

	seen := make(map[devino]string)

	return filepath.Walk(dir, func(file string, fi os.FileInfo, err error) error {

		if err != nil {
			return err
		}

		if fi.IsDir() {
			return nil
		}

		header, err := tarArchive.FileInfoHeader(fi, "")
		if err != nil {
			return err
		}

		filename := strings.TrimPrefix(strings.Replace(file, dir, filepath.Base(dir), -1), string(filepath.Separator))
		header.Name = filename

		st := fi.Sys().(*syscall.Stat_t)
		di := devino{
			Dev: st.Dev,
			Ino: st.Ino,
		}
		orig, ok := seen[di]
		if ok {
			header.Typeflag = tarArchive.TypeLink
			header.Linkname = orig
			header.Size = 0

			err = tw.WriteHeader(header)
			if err != nil {
				return err
			}

			hLinks++
			return nil
		}

		if err := tw.WriteHeader(header); err != nil {
			return err
		}

		f, err := os.Open(file)
		if err != nil {
			return err
		}

		io.Copy(tw, f)

		f.Close()

		seen[di] = filename
		nFiles++

		return nil
	})
	return nil
}

func Untar(r io.Reader, dir string) (err error) {
	t0 := time.Now()
	nFiles := 0
	madeDir := map[string]bool{}
	defer func() {
		td := time.Since(t0)
		if err == nil {
			log.Printf("extracted tarball into %s: %d files, %d dirs (%v)", dir, nFiles, len(madeDir), td)
		} else {
			log.Printf("error extracting tarball into %s after %d files, %d dirs, %v: %v", dir, nFiles, len(madeDir), td, err)
		}
	}()
	tr := tarArchive.NewReader(r)
	loggedChtimesError := false
	for {
		f, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("tar reading error: %v", err)
			return fmt.Errorf("tar error: %v", err)
		}
		if !validRelPath(f.Name) {
			return fmt.Errorf("tar contained invalid name error %q", f.Name)
		}
		rel := filepath.FromSlash(f.Name)
		abs := filepath.Join(dir, rel)

		fi := f.FileInfo()
		mode := fi.Mode()
		switch {
		case mode.IsRegular():
			// Make the directory. This is redundant because it should
			// already be made by a directory entry in the tar
			// beforehand. Thus, don't check for errors; the next
			// write will fail with the same error.
			dir := filepath.Dir(abs)
			if !madeDir[dir] {
				if err := os.MkdirAll(filepath.Dir(abs), 0755); err != nil {
					return err
				}
				madeDir[dir] = true
			}
			wf, err := os.OpenFile(abs, os.O_RDWR|os.O_CREATE|os.O_TRUNC, mode.Perm())
			if err != nil {
				return err
			}
			n, err := io.Copy(wf, tr)
			if closeErr := wf.Close(); closeErr != nil && err == nil {
				err = closeErr
			}
			if err != nil {
				return fmt.Errorf("error writing to %s: %v", abs, err)
			}
			if n != f.Size {
				return fmt.Errorf("only wrote %d bytes to %s; expected %d", n, abs, f.Size)
			}
			modTime := f.ModTime
			if modTime.After(t0) {
				// Clamp modtimes at system time. See
				// golang.org/issue/19062 when clock on
				// buildlet was behind the gitmirror server
				// doing the git-archive.
				modTime = t0
			}
			if !modTime.IsZero() {
				if err := os.Chtimes(abs, modTime, modTime); err != nil && !loggedChtimesError {
					// benign error. Gerrit doesn't even set the
					// modtime in these, and we don't end up relying
					// on it anywhere (the gomote push command relies
					// on digests only), so this is a little pointless
					// for now.
					log.Printf("error changing modtime: %v (further Chtimes errors suppressed)", err)
					loggedChtimesError = true // once is enough
				}
			}
			nFiles++
		case mode.IsDir():
			if err := os.MkdirAll(abs, 0755); err != nil {
				return err
			}
			madeDir[abs] = true
		default:
			return fmt.Errorf("tar file entry %s contained unsupported file type %v", f.Name, mode)
		}
	}
	return nil
}

func validRelPath(p string) bool {
	if p == "" || strings.Contains(p, `\`) || strings.HasPrefix(p, "/") || strings.Contains(p, "../") {
		return false
	}
	return true
}
