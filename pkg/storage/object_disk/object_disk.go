package object_disk

import (
	"bufio"
	"context"
	"fmt"
	"github.com/AlexAkulov/clickhouse-backup/pkg/clickhouse"
	apexLog "github.com/apex/log"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
)

var log *apexLog.Entry

type MetadataVersion uint32

// origin https://github.com/ClickHouse/ClickHouse/blame/master/src/Disks/ObjectStorages/DiskObjectStorageMetadata.h#L15
const (
	VersionAbsolutePaths MetadataVersion = 1
	VersionRelativePath  MetadataVersion = 2
	VersionReadOnlyFlag  MetadataVersion = 3
	VersionInlineData    MetadataVersion = 4
)

type StorageObject struct {
	ObjectSize         int
	ObjectRelativePath string
}

type Metadata struct {
	Version            MetadataVersion `json:"key"`
	StorageObjectCount int             `json:"created"`
	TotalSize          int             `json:"deleted"`
	StorageObjects     []StorageObject
	RefCount           int
	ReadOnly           bool
	InlineData         string
	Path               string
}

type Object struct {
	Key      string    `json:"key"`
	Created  time.Time `json:"created"`
	Deleted  time.Time `json:"deleted"`
	Versions []string  `json:"versions"`
	Deletes  []string  `json:"deletes"`
	Size     int64
}

func ReadIntText(scanner *bufio.Scanner) (int, error) {

	scanner.Scan()
	value := scanner.Text()
	intValue, err := strconv.Atoi(value)
	if err != nil {
		return -1, err
	}
	return intValue, nil
}

func ReadBoolText(scanner *bufio.Scanner) (bool, error) {

	scanner.Scan()
	value := scanner.Text()
	intValue, err := strconv.Atoi(value)
	if err != nil {
		return false, err
	}
	return intValue > 0, nil
}

func (m *Metadata) deserialize(file io.Reader) error {

	objectStorageRootPath := ""
	scanner := bufio.NewScanner(file)
	// todo think about, resize scanner's capacity for lines over 64K
	scanner.Split(bufio.ScanWords)

	version, err := ReadIntText(scanner)

	if err != nil {
		return err
	}

	if version < int(VersionAbsolutePaths) || version > int(VersionInlineData) {
		return fmt.Errorf("invalid metadata.Version=%v", m.Version)
	}

	m.Version = MetadataVersion(version)

	m.StorageObjectCount, err = ReadIntText(scanner)

	m.TotalSize, err = ReadIntText(scanner)

	for i := 0; i < m.StorageObjectCount; i++ {

		objectSize, _ := ReadIntText(scanner)
		scanner.Scan()
		objectRelativePath := scanner.Text()

		if version == int(VersionAbsolutePaths) {
			if !strings.HasPrefix(objectRelativePath, objectStorageRootPath) {

			}
			objectRelativePath = objectRelativePath[len(objectStorageRootPath):]
		}

		m.StorageObjects = append(m.StorageObjects, StorageObject{ObjectSize: objectSize, ObjectRelativePath: objectRelativePath})

	}

	m.RefCount, err = ReadIntText(scanner)

	if version >= int(VersionReadOnlyFlag) {
		m.ReadOnly, err = ReadBoolText(scanner)
	}

	if version >= int(VersionInlineData) {
		scanner.Scan()
		m.InlineData = scanner.Text()
	}
	return nil
}

func (m *Metadata) serialize(file *os.File) error {
	var b2i = map[bool]string{false: "0", true: "1"}
	var err error

	if _, err = file.WriteString(strconv.FormatInt(int64(m.Version), 10) + "\n"); err != nil {
		return err
	}

	if _, err = file.WriteString(strconv.FormatInt(int64(m.StorageObjectCount), 10) + "\t" + strconv.FormatInt(int64(m.TotalSize), 10) + "\n"); err != nil {
		return err
	}

	for i := 0; i < m.StorageObjectCount; i++ {
		if _, err = file.WriteString(strconv.FormatInt(int64(m.StorageObjects[i].ObjectSize), 10) + "\t" + m.StorageObjects[i].ObjectRelativePath + "\n"); err != nil {
			return err
		}
	}

	if _, err = file.WriteString(strconv.FormatInt(int64(m.RefCount), 10) + "\n"); err != nil {
		return err
	}

	if m.Version >= VersionReadOnlyFlag {
		if _, err = file.WriteString(b2i[m.ReadOnly] + "\n"); err != nil {
			return err
		}
	}

	if m.Version >= VersionInlineData {
		if _, err = file.WriteString(m.InlineData + "\n"); err != nil {
			return err
		}
	}
	return nil
}

func ReadMetadataFromFile(path string) (error, *Metadata) {
	metadataFile, err := os.Open(path)
	if err != nil {
		return err, nil
	}
	defer func() {
		if err = metadataFile.Close(); err != nil {
			log.Warnf("can't close %s: %v", path, err)
		}
	}()

	var metadata Metadata
	metadata.Path = path
	if err = metadata.deserialize(metadataFile); err != nil {
		return err, nil
	}
	return nil, &metadata
}

func WriteMetadataToFile(metadata *Metadata, path string) error {
	metadataFile, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() {
		if err = metadataFile.Close(); err != nil {
			log.Warnf("can't close %s: %v", path, err)
		}
	}()
	return metadata.serialize(metadataFile)
}

type TableCredentials struct {
	Type               string // aws, gcs, azblob
	EndPoint           string
	S3AccessKey        string
	S3SecretKey        string
	AzureAccountName   string
	AzureAccountKey    string
	AzureContainerName string
}

func GetObjectDisksCredentials(ctx context.Context, ch *clickhouse.ClickHouse) (error, *map[string]TableCredentials) {
	credentials := make(map[string]TableCredentials, 0)
	if version, err := ch.GetVersion(ctx); err != nil {
		return err, nil
	} else if version <= 20006000 {
		return nil, &credentials
	}
	disks, err := ch.GetDisks(ctx)
	if err != nil {
		return err, nil
	}
	preprocessedConfigPath, err := ch.GetPreprocessedConfigPath(ctx)
	if err != nil {
		return err, nil
	}
	return nil, &credentials
}
