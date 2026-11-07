package backup

import (
	"context"
	"encoding/binary"
	"encoding/xml"
	"fmt"
	"io"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/Altinity/clickhouse-backup/v2/pkg/storage"

	"github.com/go-faster/city"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
)

// embeddedManifestFileName is the manifest of BACKUP, it lists name, size and checksum of every file of the backup
const embeddedManifestFileName = ".backup"

// embeddedManifestSQLRE matches table and database metadata files which fixEmbeddedMetadataSQLQuery can rewrite,
// in the flat and in the ON CLUSTER layout
var embeddedManifestSQLRE = regexp.MustCompile(`^(shards/\d+/replicas/\d+/)?metadata/.+\.sql$`)

// embeddedChecksum calculates the checksum ClickHouse keeps in the .backup manifest, HashingReadBuffer chains
// CityHash128 v1.0.2 over 2048 bytes blocks, getHexUIntLowercase writes UInt128 high 64 bits first
func embeddedChecksum(data []byte) string {
	const blockSize = 2048
	state := city.U128{}
	for len(data) >= blockSize {
		state = city.CH128Seed(data[:blockSize], state)
		data = data[blockSize:]
	}
	if len(data) > 0 {
		state = city.CH128Seed(data, state)
	}
	var b [16]byte
	binary.BigEndian.PutUint64(b[:8], state.High)
	binary.BigEndian.PutUint64(b[8:], state.Low)
	return fmt.Sprintf("%x", b)
}

type embeddedManifestFile struct {
	size     string
	checksum string
}

// updateEmbeddedRemoteManifest updates size and checksum of .sql files in the .backup manifest after
// fixEmbeddedMetadataRemote rewrote them in place, ClickHouse 26.9+ refuses to read a backup file which size
// doesn't match the manifest, https://github.com/Altinity/clickhouse-backup/issues/1668
// It recalculates every .sql of the manifest instead of tracking own changes, because ON CLUSTER workers rewrite
// metadata of their own replicas concurrently and only the node which executes RESTORE may write the shared manifest
func (b *Backuper) updateEmbeddedRemoteManifest(ctx context.Context, backupName string) error {
	objectDiskPath, err := b.getObjectDiskPath()
	if err != nil {
		return errors.Wrap(err, "getObjectDiskPath")
	}
	backupPath := path.Join(objectDiskPath, backupName)
	manifestPath := path.Join(backupPath, embeddedManifestFileName)

	// data file name -> size and checksum from the manifest
	sqlFiles := map[string]embeddedManifestFile{}
	if err = b.rewriteEmbeddedManifest(ctx, manifestPath, nil, func(dataFile string, f embeddedManifestFile) {
		if embeddedManifestSQLRE.MatchString(dataFile) {
			sqlFiles[dataFile] = f
		}
	}); err != nil {
		return err
	}
	// fixEmbeddedMetadataRemote rewrites only files which it finds under the backup path, ClickHouse 24.3 BACKUP to
	// AzureBlobStorage puts files to the container root instead
	metadataDirs := map[string]struct{}{}
	for dataFile := range sqlFiles {
		metadataDirs[dataFile[:strings.Index(dataFile, "metadata/")+len("metadata")]] = struct{}{}
	}
	existingFiles := map[string]struct{}{}
	for metadataDir := range metadataDirs {
		if err = b.dst.WalkAbsolute(ctx, path.Join(backupPath, metadataDir), true, func(ctx context.Context, fInfo storage.RemoteFile) error {
			existingFiles[path.Join(metadataDir, fInfo.Name())] = struct{}{}
			return nil
		}); err != nil {
			return errors.Wrapf(err, "WalkAbsolute %s", path.Join(backupPath, metadataDir))
		}
	}
	for dataFile := range sqlFiles {
		if _, exists := existingFiles[dataFile]; !exists {
			log.Debug().Msgf("%s not found in %s, skip", dataFile, backupPath)
			delete(sqlFiles, dataFile)
		}
	}

	changed := map[string]embeddedManifestFile{}
	changedMtx := sync.Mutex{}
	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(max(int(b.cfg.General.DownloadConcurrency), 1))
	for dataFile, expected := range sqlFiles {
		g.Go(func() error {
			reader, readErr := b.dst.GetFileReaderAbsolute(gCtx, path.Join(backupPath, dataFile))
			if readErr != nil {
				return errors.Wrapf(readErr, "GetFileReaderAbsolute %s", dataFile)
			}
			data, readErr := io.ReadAll(reader)
			if closeErr := reader.Close(); readErr == nil {
				readErr = closeErr
			}
			if readErr != nil {
				return errors.Wrapf(readErr, "read %s", dataFile)
			}
			actual := embeddedManifestFile{size: strconv.Itoa(len(data)), checksum: embeddedChecksum(data)}
			if actual != expected {
				changedMtx.Lock()
				changed[dataFile] = actual
				changedMtx.Unlock()
			}
			return nil
		})
	}
	if err = g.Wait(); err != nil {
		return err
	}
	if len(changed) == 0 {
		return nil
	}
	log.Info().Msgf("update size and checksum of %d metadata files in %s", len(changed), manifestPath)
	return b.rewriteEmbeddedManifest(ctx, manifestPath, changed, nil)
}

// rewriteEmbeddedManifest streams the manifest, calls onFile for every file with non-zero size which data isn't
// in the base backup, when changed isn't empty uploads the manifest with replaced <size> and <checksum>
func (b *Backuper) rewriteEmbeddedManifest(ctx context.Context, manifestPath string, changed map[string]embeddedManifestFile, onFile func(dataFile string, f embeddedManifestFile)) error {
	reader, err := b.dst.GetFileReaderAbsolute(ctx, manifestPath)
	if err != nil {
		return errors.Wrapf(err, "GetFileReaderAbsolute %s", manifestPath)
	}
	defer func() {
		if closeErr := reader.Close(); closeErr != nil {
			log.Warn().Msgf("can't close %s: %v", manifestPath, closeErr)
		}
	}()
	if len(changed) == 0 {
		return errors.Wrapf(processEmbeddedManifest(reader, io.Discard, nil, onFile), "parse %s", manifestPath)
	}
	pipeReader, pipeWriter := io.Pipe()
	go func() {
		pipeWriter.CloseWithError(processEmbeddedManifest(reader, pipeWriter, changed, onFile))
	}()
	if err = b.dst.PutFileAbsolute(ctx, manifestPath, pipeReader, 0); err != nil {
		_ = pipeReader.CloseWithError(err)
		return errors.Wrapf(err, "PutFileAbsolute %s", manifestPath)
	}
	return nil
}

// processEmbeddedManifest copies XML tokens from in to out, every <file> element is buffered until </file> because
// <data_file> goes after <size> and <checksum>
func processEmbeddedManifest(in io.Reader, out io.Writer, changed map[string]embeddedManifestFile, onFile func(dataFile string, f embeddedManifestFile)) error {
	decoder := xml.NewDecoder(in)
	encoder := xml.NewEncoder(out)
	var fileTokens []xml.Token
	inFile := false
	for {
		token, err := decoder.RawToken()
		if err == io.EOF {
			break
		}
		if err != nil {
			return errors.WithStack(err)
		}
		token = xml.CopyToken(token)
		if start, isStart := token.(xml.StartElement); isStart && start.Name.Local == "file" {
			inFile = true
			fileTokens = fileTokens[:0]
		}
		if !inFile {
			if err = encoder.EncodeToken(token); err != nil {
				return errors.WithStack(err)
			}
			continue
		}
		fileTokens = append(fileTokens, token)
		if end, isEnd := token.(xml.EndElement); isEnd && end.Name.Local == "file" {
			inFile = false
			if err = processEmbeddedManifestFile(fileTokens, changed, onFile); err != nil {
				return err
			}
			for _, t := range fileTokens {
				if err = encoder.EncodeToken(t); err != nil {
					return errors.WithStack(err)
				}
			}
		}
	}
	return errors.WithStack(encoder.Flush())
}

// processEmbeddedManifestFile reads the fields of one <file> and replaces <size> and <checksum> in place
func processEmbeddedManifestFile(tokens []xml.Token, changed map[string]embeddedManifestFile, onFile func(dataFile string, f embeddedManifestFile)) error {
	fields := map[string]string{}
	fieldIdx := map[string]int{}
	element := ""
	for i, t := range tokens {
		switch v := t.(type) {
		case xml.StartElement:
			element = v.Name.Local
		case xml.EndElement:
			element = ""
		case xml.CharData:
			if element != "" {
				fields[element] += string(v)
				fieldIdx[element] = i
			}
		}
	}
	name, size, checksum := fields["name"], strings.TrimSpace(fields["size"]), strings.TrimSpace(fields["checksum"])
	if name == "" || size == "" || size == "0" || fields["use_base"] == "true" || fields["base_size"] != "" {
		return nil
	}
	dataFile := name
	if fields["data_file"] != "" {
		dataFile = fields["data_file"]
	}
	if onFile != nil {
		onFile(dataFile, embeddedManifestFile{size: size, checksum: checksum})
	}
	newFile, isChanged := changed[dataFile]
	if !isChanged {
		return nil
	}
	sizeIdx, sizeOk := fieldIdx["size"]
	checksumIdx, checksumOk := fieldIdx["checksum"]
	if !sizeOk || !checksumOk {
		return errors.Errorf("<file> %s in .backup doesn't contain <size> or <checksum>", name)
	}
	tokens[sizeIdx] = xml.CharData(newFile.size)
	tokens[checksumIdx] = xml.CharData(newFile.checksum)
	return nil
}
