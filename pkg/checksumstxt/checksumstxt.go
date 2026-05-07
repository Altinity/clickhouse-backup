// Package checksumstxt parses the checksums.txt metadata file written next to
// every MergeTree data part. It supports versions 2 (legacy text), 3 (legacy
// binary), 4 (binary wrapped in a ClickHouse compressed-block stream — the
// default written today), and the standalone version-5 "minimalistic" blob
// used as the ZooKeeper payload (not on disk).
//
// Reference C++ implementation: src/Storages/MergeTree/MergeTreeDataPartChecksum.{h,cpp}
//
// The on-disk file is parsed by Parse; the minimalistic blob by ParseMinimalistic.
package checksumstxt

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	chproto "github.com/ClickHouse/ch-go/proto"
)

const headerPrefix = "checksums format version: "

type Hash128 struct {
	Low, High uint64
}

type Checksum struct {
	FileSize         uint64
	FileHash         Hash128
	IsCompressed     bool
	UncompressedSize uint64
	UncompressedHash Hash128
}

type File struct {
	Version int
	Files   map[string]Checksum
}

type Minimalistic struct {
	NumCompressedFiles                uint64
	NumUncompressedFiles              uint64
	HashOfAllFiles                    Hash128
	HashOfUncompressedFiles           Hash128
	UncompressedHashOfCompressedFiles Hash128
}

func Parse(r io.Reader) (*File, error) {
	br := bufio.NewReader(r)
	version, err := readVersion(br)
	if err != nil {
		return nil, err
	}
	f := &File{Version: version}
	switch version {
	case 2:
		f.Files, err = parseV2(br)
	case 3:
		f.Files, err = parseBinary(br, false)
	case 4:
		f.Files, err = parseBinary(br, true)
	case 5:
		return nil, errors.New("checksumstxt: version 5 is the minimalistic blob; use ParseMinimalistic")
	case 1:
		return nil, errors.New("checksumstxt: format version 1 is too old to read")
	default:
		return nil, fmt.Errorf("checksumstxt: unsupported version %d", version)
	}
	if err != nil {
		return nil, err
	}
	return f, nil
}

func ParseMinimalistic(r io.Reader) (*Minimalistic, error) {
	br := bufio.NewReader(r)
	version, err := readVersion(br)
	if err != nil {
		return nil, err
	}
	if version != 5 {
		return nil, fmt.Errorf("checksumstxt: minimalistic blob has version %d, want 5", version)
	}
	pr := chproto.NewReader(br)
	var m Minimalistic
	if m.NumCompressedFiles, err = pr.UVarInt(); err != nil {
		return nil, fmt.Errorf("checksumstxt: num_compressed_files: %w", err)
	}
	if m.NumUncompressedFiles, err = pr.UVarInt(); err != nil {
		return nil, fmt.Errorf("checksumstxt: num_uncompressed_files: %w", err)
	}
	if m.HashOfAllFiles, err = readHash128(pr); err != nil {
		return nil, fmt.Errorf("checksumstxt: hash_of_all_files: %w", err)
	}
	if m.HashOfUncompressedFiles, err = readHash128(pr); err != nil {
		return nil, fmt.Errorf("checksumstxt: hash_of_uncompressed_files: %w", err)
	}
	if m.UncompressedHashOfCompressedFiles, err = readHash128(pr); err != nil {
		return nil, fmt.Errorf("checksumstxt: uncompressed_hash_of_compressed_files: %w", err)
	}
	if err := assertEOF(pr); err != nil {
		return nil, err
	}
	return &m, nil
}

func readVersion(br *bufio.Reader) (int, error) {
	line, err := br.ReadString('\n')
	if err != nil {
		return 0, fmt.Errorf("checksumstxt: read header: %w", err)
	}
	if !strings.HasPrefix(line, headerPrefix) || !strings.HasSuffix(line, "\n") {
		return 0, fmt.Errorf("checksumstxt: bad header %q", line)
	}
	v := strings.TrimSuffix(line[len(headerPrefix):], "\n")
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("checksumstxt: parse version %q: %w", v, err)
	}
	return n, nil
}

func parseBinary(br *bufio.Reader, compressed bool) (map[string]Checksum, error) {
	pr := chproto.NewReader(br)
	if compressed {
		pr.EnableCompression()
	}
	count, err := pr.UVarInt()
	if err != nil {
		return nil, fmt.Errorf("checksumstxt: count: %w", err)
	}
	out := make(map[string]Checksum, count)
	for i := uint64(0); i < count; i++ {
		name, err := pr.Str()
		if err != nil {
			return nil, fmt.Errorf("checksumstxt: record %d name: %w", i, err)
		}
		if _, dup := out[name]; dup {
			return nil, fmt.Errorf("checksumstxt: duplicate name %q", name)
		}
		var c Checksum
		if c.FileSize, err = pr.UVarInt(); err != nil {
			return nil, fmt.Errorf("checksumstxt: %q size: %w", name, err)
		}
		if c.FileHash, err = readHash128(pr); err != nil {
			return nil, fmt.Errorf("checksumstxt: %q hash: %w", name, err)
		}
		if c.IsCompressed, err = pr.Bool(); err != nil {
			return nil, fmt.Errorf("checksumstxt: %q is_compressed: %w", name, err)
		}
		if c.IsCompressed {
			if c.UncompressedSize, err = pr.UVarInt(); err != nil {
				return nil, fmt.Errorf("checksumstxt: %q uncompressed_size: %w", name, err)
			}
			if c.UncompressedHash, err = readHash128(pr); err != nil {
				return nil, fmt.Errorf("checksumstxt: %q uncompressed_hash: %w", name, err)
			}
		}
		out[name] = c
	}
	if err := assertEOF(pr); err != nil {
		return nil, err
	}
	return out, nil
}

func readHash128(pr *chproto.Reader) (Hash128, error) {
	u, err := pr.UInt128()
	if err != nil {
		return Hash128{}, err
	}
	return Hash128{Low: u.Low, High: u.High}, nil
}

// assertEOF returns nil iff the reader is at EOF. For a v4 stream this also
// catches a partially-formed trailing compressed block, which the underlying
// compress.Reader will surface as a non-EOF error wrapping io.EOF.
func assertEOF(pr *chproto.Reader) error {
	var b [1]byte
	n, err := pr.Read(b[:])
	if n > 0 {
		return errors.New("checksumstxt: trailing bytes after body")
	}
	if err == nil || errors.Is(err, io.EOF) {
		return nil
	}
	return fmt.Errorf("checksumstxt: trailing data check: %w", err)
}

func parseV2(br *bufio.Reader) (map[string]Checksum, error) {
	const countSuffix = " files:\n"
	line, err := br.ReadString('\n')
	if err != nil {
		return nil, fmt.Errorf("checksumstxt: count line: %w", err)
	}
	if !strings.HasSuffix(line, countSuffix) {
		return nil, fmt.Errorf("checksumstxt: bad count line %q", line)
	}
	count, err := strconv.Atoi(line[:len(line)-len(countSuffix)])
	if err != nil {
		return nil, fmt.Errorf("checksumstxt: parse count: %w", err)
	}
	out := make(map[string]Checksum, count)
	for i := 0; i < count; i++ {
		name, err := readLine(br)
		if err != nil {
			return nil, fmt.Errorf("checksumstxt: record %d name: %w", i, err)
		}
		var c Checksum
		size, err := readKV(br, "\tsize: ")
		if err != nil {
			return nil, fmt.Errorf("%q size: %w", name, err)
		}
		if c.FileSize, err = strconv.ParseUint(size, 10, 64); err != nil {
			return nil, fmt.Errorf("%q size value: %w", name, err)
		}
		hash, err := readKV(br, "\thash: ")
		if err != nil {
			return nil, fmt.Errorf("%q hash: %w", name, err)
		}
		if c.FileHash, err = parseHash128Decimal(hash); err != nil {
			return nil, fmt.Errorf("%q hash value: %w", name, err)
		}
		comp, err := readKV(br, "\tcompressed: ")
		if err != nil {
			return nil, fmt.Errorf("%q compressed: %w", name, err)
		}
		switch comp {
		case "0":
		case "1":
			c.IsCompressed = true
		default:
			return nil, fmt.Errorf("%q compressed value %q", name, comp)
		}
		if c.IsCompressed {
			us, err := readKV(br, "\tuncompressed size: ")
			if err != nil {
				return nil, fmt.Errorf("%q uncompressed_size: %w", name, err)
			}
			if c.UncompressedSize, err = strconv.ParseUint(us, 10, 64); err != nil {
				return nil, fmt.Errorf("%q uncompressed_size value: %w", name, err)
			}
			uh, err := readKV(br, "\tuncompressed hash: ")
			if err != nil {
				return nil, fmt.Errorf("%q uncompressed_hash: %w", name, err)
			}
			if c.UncompressedHash, err = parseHash128Decimal(uh); err != nil {
				return nil, fmt.Errorf("%q uncompressed_hash value: %w", name, err)
			}
		}
		if _, dup := out[name]; dup {
			return nil, fmt.Errorf("checksumstxt: duplicate name %q", name)
		}
		out[name] = c
	}
	if _, err := br.ReadByte(); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, errors.New("checksumstxt: trailing bytes after body")
		}
		return nil, fmt.Errorf("checksumstxt: trailing data: %w", err)
	}
	return out, nil
}

func readLine(br *bufio.Reader) (string, error) {
	s, err := br.ReadString('\n')
	if err != nil {
		return "", err
	}
	return s[:len(s)-1], nil
}

func readKV(br *bufio.Reader, prefix string) (string, error) {
	s, err := readLine(br)
	if err != nil {
		return "", err
	}
	if !strings.HasPrefix(s, prefix) {
		return "", fmt.Errorf("expected prefix %q, got %q", prefix, s)
	}
	return s[len(prefix):], nil
}

func parseHash128Decimal(s string) (Hash128, error) {
	sp := strings.IndexByte(s, ' ')
	if sp < 0 {
		return Hash128{}, fmt.Errorf("expected two decimals, got %q", s)
	}
	low, err := strconv.ParseUint(s[:sp], 10, 64)
	if err != nil {
		return Hash128{}, fmt.Errorf("low64: %w", err)
	}
	high, err := strconv.ParseUint(s[sp+1:], 10, 64)
	if err != nil {
		return Hash128{}, fmt.Errorf("high64: %w", err)
	}
	return Hash128{Low: low, High: high}, nil
}
