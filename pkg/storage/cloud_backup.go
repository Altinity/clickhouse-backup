package storage

import (
	"context"
	"encoding/xml"
	"io"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/metadata"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

// CloudBackupDataFormat - DataFormat of a remote backup without metadata.json but with the `.backup` manifest,
// created by native `BACKUP ... TO S3 / AzureBlobStorage` (ClickHouse Cloud), restore_remote switches to restore_cloud for it,
// https://github.com/Altinity/clickhouse-backup/issues/1574
const CloudBackupDataFormat = "cloud"

// CloudBackupHeader - fields of the `.backup` manifest written before <contents>
type CloudBackupHeader struct {
	Timestamp time.Time
	UUID      string
	// BaseBackup - `<base_backup>` of an incremental backup, BackupInfo::toString() of ClickHouse keeps the credentials,
	// never log it as is, use ParseCloudBackupLocation
	BaseBackup     string
	BaseBackupUUID string
}

// cloudBackupSummary - aggregates of the `.backup` manifest, the manifest has no totals,
// ClickHouse computes them in BackupImpl::writeBackupMetadata and keeps them in system.backups only
type cloudBackupSummary struct {
	CloudBackupHeader
	// DataSize - bytes stored under this backup prefix, same formula as size_of_entries in BackupImpl::writeBackupMetadata
	DataSize uint64
}

// ReadCloudBackupHeader streams the `.backup` XML until <contents>, so the file list of a big manifest is not downloaded
func ReadCloudBackupHeader(r io.Reader) (*CloudBackupHeader, error) {
	summary, err := parseCloudBackup(r, true)
	if err != nil {
		return nil, err
	}
	return &summary.CloudBackupHeader, nil
}

func parseCloudBackupSummary(r io.Reader) (*cloudBackupSummary, error) {
	return parseCloudBackup(r, false)
}

// parseCloudBackup streams the `.backup` XML token by token, the manifest has one <file> per logical file
// of the backup and can be hundreds of MB, so the file list is never kept in memory
func parseCloudBackup(r io.Reader, headerOnly bool) (*cloudBackupSummary, error) {
	summary := &cloudBackupSummary{}
	decoder := xml.NewDecoder(r)
	deduplicateFiles, checksumGenerator := true, false
	var text strings.Builder
	var fileName, dataFile string
	var size, baseSize uint64
	var useBase bool
	parseUint := func(s string) (uint64, error) {
		return strconv.ParseUint(strings.TrimSpace(s), 10, 64)
	}
	for {
		token, err := decoder.Token()
		if err == io.EOF {
			return summary, nil
		}
		if err != nil {
			return nil, errors.Wrap(err, "can't parse .backup manifest")
		}
		switch t := token.(type) {
		case xml.StartElement:
			text.Reset()
			if headerOnly && t.Name.Local == "contents" {
				return summary, nil
			}
			if t.Name.Local == "file" {
				fileName, dataFile, size, baseSize, useBase = "", "", 0, 0, false
			}
		case xml.CharData:
			text.Write(t)
		case xml.EndElement:
			value := strings.TrimSpace(text.String())
			text.Reset()
			switch t.Name.Local {
			case "timestamp":
				if ts, parseErr := time.Parse(time.DateTime, value); parseErr == nil {
					summary.Timestamp = ts
				}
			case "uuid":
				summary.UUID = value
			case "base_backup":
				summary.BaseBackup = value
			case "base_backup_uuid":
				summary.BaseBackupUUID = value
			case "deduplicate_files":
				deduplicateFiles = value != "0" && !strings.EqualFold(value, "false")
			case "data_file_name_generator":
				checksumGenerator = strings.EqualFold(value, "checksum")
			case "name":
				fileName = value
			case "data_file":
				dataFile = value
			case "use_base":
				useBase = strings.EqualFold(value, "true")
			case "size":
				if size, err = parseUint(value); err != nil {
					return nil, errors.Wrapf(err, "bad <size> %q in .backup manifest", value)
				}
			case "base_size":
				if baseSize, err = parseUint(value); err != nil {
					return nil, errors.Wrapf(err, "bad <base_size> %q in .backup manifest", value)
				}
			case "file":
				// <base_size> is written only when it differs from <size>
				if useBase && baseSize == 0 {
					baseSize = size
				}
				// a deduplicated file of the FirstFileName generator points <data_file> to another file which holds the bytes,
				// the checksum generator always writes <data_file> = the checksum-named blob
				hasEntry := !deduplicateFiles || (size > 0 && size != baseSize && (dataFile == "" || dataFile == fileName || checksumGenerator))
				if hasEntry && size > baseSize {
					summary.DataSize += size - baseSize
				}
			}
		}
	}
}

// readCloudBackupMetadata detects the native BACKUP layout for a backup without metadata.json,
// returns nil when `<backupName>/.backup` does not exist, the second result is false when
// the manifest could not be read and the entry shall not be cached
func (bd *BackupDestination) readCloudBackupMetadata(ctx context.Context, backupName string) (*Backup, bool) {
	manifestKey := path.Join(backupName, ".backup")
	mf, err := bd.StatFile(ctx, manifestKey)
	if err != nil {
		return nil, false
	}
	cloudBackup := &Backup{
		BackupMetadata: metadata.BackupMetadata{
			BackupName:   backupName,
			DataFormat:   CloudBackupDataFormat,
			CreationDate: mf.LastModified(),
			MetadataSize: uint64(mf.Size()),
		},
		UploadDate: mf.LastModified(),
	}
	// the backup is not marked as broken on a read error, clean_remote_broken would delete a backup which clickhouse-backup did not create
	r, err := bd.GetFileReader(ctx, manifestKey)
	if err != nil {
		log.Warn().Err(err).Str("backup", backupName).Msg("can't open .backup, data size is unknown")
		return cloudBackup, false
	}
	summary, err := parseCloudBackupSummary(r)
	_ = r.Close()
	if err != nil {
		log.Warn().Err(err).Str("backup", backupName).Msg("can't read .backup, data size is unknown")
		return cloudBackup, false
	}
	if !summary.Timestamp.IsZero() {
		cloudBackup.CreationDate = summary.Timestamp
	}
	cloudBackup.DataSize = summary.DataSize
	if summary.BaseBackup != "" {
		bucketOrContainer, remotePath := bd.cloudBucketAndPath()
		cloudBackup.RequiredBackup, cloudBackup.Tags = cloudBaseBackupName(summary.BaseBackup, bucketOrContainer, remotePath)
	}
	return cloudBackup, true
}

// cloudBucketAndPath - bucket/container and path of the remote storage, to map `<base_backup>` to a backup name
func (bd *BackupDestination) cloudBucketAndPath() (string, string) {
	switch rs := bd.RemoteStorage.(type) {
	case *S3:
		return rs.Config.Bucket, rs.Config.Path
	case *GCS:
		return rs.Config.Bucket, rs.Config.Path
	case *AzureBlob:
		return rs.Config.Container, rs.Config.Path
	}
	return "", ""
}

// cloudBaseBackupName maps `<base_backup>` to the name of a backup under the same remote path (RequiredBackup),
// a base stored elsewhere is described in tags without credentials
func cloudBaseBackupName(baseBackup, bucketOrContainer, remotePath string) (string, string) {
	loc, err := ParseCloudBackupLocation(baseBackup)
	if err != nil {
		log.Warn().Err(err).Msg("can't parse <base_backup> in .backup")
		return "", "base=unknown"
	}
	if key, ok := loc.KeyIn(bucketOrContainer); ok {
		remotePath = strings.Trim(remotePath, "/")
		name, found := strings.CutPrefix(key, remotePath+"/")
		if remotePath == "" {
			name, found = key, true
		}
		if found && !strings.Contains(name, "/") {
			return name, ""
		}
	}
	return "", "base=" + loc.String()
}

// isStaleCloudCacheEntry - embedded `create_remote` writes `.backup` via BACKUP ... TO S3 before metadata.json is uploaded,
// a `list` in between caches the backup as cloud, so a cached cloud entry is dropped once metadata.json appears
func (bd *BackupDestination) isStaleCloudCacheEntry(ctx context.Context, cached Backup) bool {
	if cached.DataFormat != CloudBackupDataFormat {
		return false
	}
	_, err := bd.StatFile(ctx, path.Join(cached.BackupName, "metadata.json"))
	return err == nil
}
