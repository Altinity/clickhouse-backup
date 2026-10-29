package cas

import (
	"archive/tar"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/klauspost/compress/zstd"
)

// ArchiveEntry describes one file to write into a tar.zstd archive.
// NameInArchive must be a forward-slash-separated relative path with no
// leading "/", no embedded "..", and no NUL bytes; WriteArchive validates
// this and returns *UnsafePathError on violation. LocalPath is the
// filesystem source.
type ArchiveEntry struct {
	NameInArchive string
	LocalPath     string
}

// UnsafePathError signals a tar entry name (or LocalPath stat result) that
// would escape the destination root, contain ".." or NUL, or otherwise be
// unsafe to extract.
type UnsafePathError struct{ Path string }

func (e *UnsafePathError) Error() string { return "cas: unsafe path in archive: " + e.Path }

// WriteArchive writes entries into w as zstd-compressed tar. Validates each
// entry's NameInArchive before write; partial archives are NOT cleaned up
// (caller decides). Closes the tar and zstd writers on return.
func WriteArchive(w io.Writer, entries []ArchiveEntry) error {
	if err := validateNoDuplicateNames(entries); err != nil {
		return err
	}

	zw, err := zstd.NewWriter(w)
	if err != nil {
		return fmt.Errorf("cas: zstd new writer: %w", err)
	}
	defer zw.Close()
	tw := tar.NewWriter(zw)
	defer tw.Close()

	for _, e := range entries {
		if err := validateArchiveName(e.NameInArchive); err != nil {
			return err
		}
		st, err := os.Stat(e.LocalPath)
		if err != nil {
			return fmt.Errorf("cas: stat %s: %w", e.LocalPath, err)
		}
		if st.IsDir() {
			return fmt.Errorf("cas: archive entry must be a regular file: %s", e.LocalPath)
		}
		hdr := &tar.Header{
			Name:     e.NameInArchive,
			Mode:     int64(st.Mode().Perm()),
			Size:     st.Size(),
			Typeflag: tar.TypeReg,
			ModTime:  st.ModTime(),
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		f, err := os.Open(e.LocalPath)
		if err != nil {
			return err
		}
		n, copyErr := io.Copy(tw, f)
		_ = f.Close()
		if copyErr != nil {
			return copyErr
		}
		if n != st.Size() {
			// File changed under us between Stat and copy. Treat as failure
			// — silently truncated archives corrupt restore.
			return fmt.Errorf("cas: %s changed size mid-write (stat=%d copied=%d)", e.LocalPath, st.Size(), n)
		}
	}
	if err := tw.Close(); err != nil {
		return err
	}
	return zw.Close()
}

// ExtractArchive reads a zstd-compressed tar from r and writes each entry
// under dstRoot. Validates every header name; rejects entries whose
// destination would escape dstRoot.
func ExtractArchive(r io.Reader, dstRoot string) error {
	absRoot, err := filepath.Abs(dstRoot)
	if err != nil {
		return err
	}
	rootPrefix := absRoot + string(filepath.Separator)

	zr, err := zstd.NewReader(r)
	if err != nil {
		return fmt.Errorf("cas: zstd new reader: %w", err)
	}
	defer zr.Close()
	tr := tar.NewReader(zr)
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := validateArchiveName(hdr.Name); err != nil {
			return err
		}
		// Containment: filepath.Join(absRoot, FromSlash(name)) followed by
		// Clean must remain under absRoot.
		dst := filepath.Join(absRoot, filepath.FromSlash(hdr.Name))
		cleanDst := filepath.Clean(dst)
		if cleanDst != absRoot && !strings.HasPrefix(cleanDst+string(filepath.Separator), rootPrefix) {
			return &UnsafePathError{Path: hdr.Name}
		}
		switch hdr.Typeflag {
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(cleanDst), 0o755); err != nil {
				return err
			}
			f, err := os.OpenFile(cleanDst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(hdr.Mode)&0o777)
			if err != nil {
				return err
			}
			if _, err := io.Copy(f, tr); err != nil {
				_ = f.Close()
				return err
			}
			if err := f.Close(); err != nil {
				return err
			}
		default:
			// CAS archives only contain regular files. Reject anything else.
			return fmt.Errorf("cas: unexpected tar entry type %d for %q", hdr.Typeflag, hdr.Name)
		}
	}
}

// validateArchiveName rejects names that would be unsafe to extract.
// Rules: non-empty; no NUL; no leading "/"; no path component equal to "..".
func validateArchiveName(name string) error {
	if name == "" {
		return &UnsafePathError{Path: name}
	}
	if strings.ContainsRune(name, 0) {
		return &UnsafePathError{Path: name}
	}
	if strings.HasPrefix(name, "/") {
		return &UnsafePathError{Path: name}
	}
	if strings.HasPrefix(name, `\`) {
		return &UnsafePathError{Path: name}
	}
	for _, seg := range strings.Split(name, "/") {
		if seg == ".." {
			return &UnsafePathError{Path: name}
		}
		if strings.Contains(seg, `\`) {
			return &UnsafePathError{Path: name}
		}
	}
	return nil
}

func validateNoDuplicateNames(entries []ArchiveEntry) error {
	seen := make(map[string]struct{}, len(entries))
	for _, e := range entries {
		if _, ok := seen[e.NameInArchive]; ok {
			return fmt.Errorf("cas: duplicate archive entry name %q", e.NameInArchive)
		}
		seen[e.NameInArchive] = struct{}{}
	}
	return nil
}
