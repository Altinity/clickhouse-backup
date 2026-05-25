package part_checksum

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/pierrec/lz4/v4"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

const (
	headerPrefix = "checksums format version: "
	lz4Method    = 0x82
)

// Hash128 is the 128-bit hash value as stored in checksums.txt (little-endian uint128).
type Hash128 struct {
	Low64  uint64
	High64 uint64
}

// FileChecksum is a single file entry from checksums.txt.
type FileChecksum struct {
	Name             string
	FileSize         uint64
	FileHash         Hash128
	IsCompressed     bool
	UncompressedSize uint64
	UncompressedHash Hash128
}

// HashOfAllFiles reads <partDir>/checksums.txt and returns the value that
// system.parts.hash_of_all_files would report (lowercase hex of SipHash128, high64 then low64).
func HashOfAllFiles(partDir string) (string, error) {
	f, err := os.Open(filepath.Join(partDir, "checksums.txt"))
	if err != nil {
		return "", errors.Wrapf(err, "open checksums.txt in %s", partDir)
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			log.Error().Err(closeErr).Stack().Msg("can't close checksums.txt")
		}
	}()

	br := bufio.NewReader(f)
	header, err := readHeaderLine(br)
	if err != nil {
		return "", errors.Wrap(err, "read header")
	}
	switch header {
	case "2":
		files, err := parseV2(br)
		if err != nil {
			return "", errors.Wrap(err, "parse v2")
		}
		return computeHashOfAllFiles(files), nil
	case "3":
		files, err := parseV3(br)
		if err != nil {
			return "", errors.Wrap(err, "parse v3")
		}
		return computeHashOfAllFiles(files), nil
	case "4":
		files, err := parseV4(br)
		if err != nil {
			return "", errors.Wrap(err, "parse v4")
		}
		return computeHashOfAllFiles(files), nil
	case "5":
		h, err := parseV5(br)
		if err != nil {
			return "", errors.Wrap(err, "parse v5")
		}
		return formatHash(h), nil
	default:
		return "", errors.Errorf("unsupported checksums.txt format version: %s", header)
	}
}

func readHeaderLine(br *bufio.Reader) (string, error) {
	line, err := br.ReadString('\n')
	if err != nil {
		return "", err
	}
	line = strings.TrimRight(line, "\n")
	if !strings.HasPrefix(line, headerPrefix) {
		return "", errors.Errorf("missing %q in %q", headerPrefix, line)
	}
	return strings.TrimPrefix(line, headerPrefix), nil
}

func formatHash(h Hash128) string {
	return fmt.Sprintf("%016x%016x", h.High64, h.Low64)
}

func computeHashOfAllFiles(files []FileChecksum) string {
	sort.Slice(files, func(i, j int) bool { return files[i].Name < files[j].Name })

	state := NewSipHash128()
	var buf [8]byte

	for _, fc := range files {
		binary.LittleEndian.PutUint64(buf[:], uint64(len(fc.Name)))
		state.Write(buf[:])
		state.Write([]byte(fc.Name))
		binary.LittleEndian.PutUint64(buf[:], fc.FileHash.Low64)
		state.Write(buf[:])
		binary.LittleEndian.PutUint64(buf[:], fc.FileHash.High64)
		state.Write(buf[:])
	}

	lo, hi := state.Sum128()
	return formatHash(Hash128{Low64: lo, High64: hi})
}

// --- v2 (text) ---

func parseV2(br *bufio.Reader) ([]FileChecksum, error) {
	countLine, err := br.ReadString('\n')
	if err != nil {
		return nil, err
	}
	countLine = strings.TrimRight(countLine, "\n")
	countStr := strings.TrimSuffix(countLine, " files:")
	count, err := strconv.Atoi(strings.TrimSpace(countStr))
	if err != nil {
		return nil, errors.Wrapf(err, "parse files count from %q", countLine)
	}

	files := make([]FileChecksum, 0, count)
	for i := 0; i < count; i++ {
		var fc FileChecksum
		name, err := br.ReadString('\n')
		if err != nil {
			return nil, err
		}
		fc.Name = strings.TrimRight(name, "\n")

		if err := readKV(br, "\tsize: ", func(v string) error {
			fc.FileSize, err = strconv.ParseUint(v, 10, 64)
			return err
		}); err != nil {
			return nil, err
		}
		if err := readKV(br, "\thash: ", func(v string) error {
			low, high, perr := parseTwoUint64(v)
			fc.FileHash = Hash128{Low64: low, High64: high}
			return perr
		}); err != nil {
			return nil, err
		}
		if err := readKV(br, "\tcompressed: ", func(v string) error {
			fc.IsCompressed = v == "1"
			return nil
		}); err != nil {
			return nil, err
		}
		if fc.IsCompressed {
			if err := readKV(br, "\tuncompressed size: ", func(v string) error {
				fc.UncompressedSize, err = strconv.ParseUint(v, 10, 64)
				return err
			}); err != nil {
				return nil, err
			}
			if err := readKV(br, "\tuncompressed hash: ", func(v string) error {
				low, high, perr := parseTwoUint64(v)
				fc.UncompressedHash = Hash128{Low64: low, High64: high}
				return perr
			}); err != nil {
				return nil, err
			}
		}
		files = append(files, fc)
	}
	return files, nil
}

func readKV(br *bufio.Reader, prefix string, set func(string) error) error {
	line, err := br.ReadString('\n')
	if err != nil {
		return err
	}
	line = strings.TrimRight(line, "\n")
	if !strings.HasPrefix(line, prefix) {
		return errors.Errorf("expected %q got %q", prefix, line)
	}
	return set(strings.TrimPrefix(line, prefix))
}

func parseTwoUint64(s string) (uint64, uint64, error) {
	fields := strings.Fields(s)
	if len(fields) != 2 {
		return 0, 0, errors.Errorf("expected 2 numbers, got %q", s)
	}
	low, err := strconv.ParseUint(fields[0], 10, 64)
	if err != nil {
		return 0, 0, err
	}
	high, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return 0, 0, err
	}
	return low, high, nil
}

// --- v3 (binary) ---

func parseV3(r io.Reader) ([]FileChecksum, error) {
	br, ok := r.(*bufio.Reader)
	if !ok {
		br = bufio.NewReader(r)
	}
	count, err := binary.ReadUvarint(br)
	if err != nil {
		return nil, errors.Wrap(err, "read files count")
	}
	files := make([]FileChecksum, 0, count)
	for i := uint64(0); i < count; i++ {
		fc, err := readV3File(br)
		if err != nil {
			return nil, errors.Wrapf(err, "read file #%d", i)
		}
		files = append(files, fc)
	}
	return files, nil
}

func readV3File(br *bufio.Reader) (FileChecksum, error) {
	r := br
	var fc FileChecksum
	nameLen, err := binary.ReadUvarint(br)
	if err != nil {
		return fc, errors.Wrap(err, "read name length")
	}
	nameBuf := make([]byte, nameLen)
	if _, err := io.ReadFull(r, nameBuf); err != nil {
		return fc, errors.Wrap(err, "read name")
	}
	fc.Name = string(nameBuf)

	if fc.FileSize, err = binary.ReadUvarint(br); err != nil {
		return fc, errors.Wrap(err, "read file_size")
	}
	if fc.FileHash, err = readHash128(r); err != nil {
		return fc, errors.Wrap(err, "read file_hash")
	}

	var b [1]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return fc, errors.Wrap(err, "read is_compressed")
	}
	fc.IsCompressed = b[0] != 0
	if fc.IsCompressed {
		if fc.UncompressedSize, err = binary.ReadUvarint(br); err != nil {
			return fc, errors.Wrap(err, "read uncompressed_size")
		}
		if fc.UncompressedHash, err = readHash128(r); err != nil {
			return fc, errors.Wrap(err, "read uncompressed_hash")
		}
	}
	return fc, nil
}

func readHash128(r io.Reader) (Hash128, error) {
	var buf [16]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return Hash128{}, err
	}
	return Hash128{
		Low64:  binary.LittleEndian.Uint64(buf[0:8]),
		High64: binary.LittleEndian.Uint64(buf[8:16]),
	}, nil
}

// --- v4 (LZ4-compressed v3) ---
//
// After the text header line, ClickHouse writes a sequence of compressed blocks
// in its native framing:
//
//	checksum(16) + method(1)=0x82 + compressed_size_uint32_LE + uncompressed_size_uint32_LE + lz4_payload
//
// where compressed_size includes the 9-byte (method+sizes) header. The payload
// itself is a raw LZ4 block (not the LZ4 frame format).
func parseV4(br *bufio.Reader) ([]FileChecksum, error) {
	var (
		full []byte
		buf  [9]byte
		ck   [16]byte
	)
	for {
		// CityHash128 checksum of the compressed block — skip it.
		_, err := io.ReadFull(br, ck[:])
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, errors.Wrap(err, "read block checksum")
		}
		if _, err := io.ReadFull(br, buf[:]); err != nil {
			return nil, errors.Wrap(err, "read block header")
		}
		if buf[0] != lz4Method {
			return nil, errors.Errorf("unexpected compression method 0x%x, want 0x%x", buf[0], lz4Method)
		}
		compressed := binary.LittleEndian.Uint32(buf[1:5])
		uncompressed := binary.LittleEndian.Uint32(buf[5:9])
		if compressed < 9 {
			return nil, errors.Errorf("compressed size %d too small", compressed)
		}
		payload := make([]byte, compressed-9)
		if _, err := io.ReadFull(br, payload); err != nil {
			return nil, errors.Wrap(err, "read lz4 payload")
		}
		dst := make([]byte, uncompressed)
		n, err := lz4.UncompressBlock(payload, dst)
		if err != nil {
			return nil, errors.Wrap(err, "lz4.UncompressBlock")
		}
		full = append(full, dst[:n]...)
	}
	return parseV3(strings.NewReader(string(full)))
}

// --- v5 (minimalistic) ---
//
// Format (from MergeTreeDataPartChecksum.cpp): after the header line we read
//
//	varint(num_compressed_files)
//	varint(num_uncompressed_files)
//	16 bytes hash_of_all_files
//	16 bytes hash_of_uncompressed_files
//	16 bytes uncompressed_hash_of_compressed_files
func parseV5(br *bufio.Reader) (Hash128, error) {
	if _, err := binary.ReadUvarint(br); err != nil {
		return Hash128{}, errors.Wrap(err, "read num_compressed_files")
	}
	if _, err := binary.ReadUvarint(br); err != nil {
		return Hash128{}, errors.Wrap(err, "read num_uncompressed_files")
	}
	return readHash128(br)
}
