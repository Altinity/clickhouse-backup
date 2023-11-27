package object_disk

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/Altinity/clickhouse-backup/pkg/clickhouse"
	"github.com/Altinity/clickhouse-backup/pkg/config"
	"github.com/Altinity/clickhouse-backup/pkg/storage"
	"github.com/antchfx/xmlquery"
	apexLog "github.com/apex/log"
)

type MetadataVersion uint32

// origin https://github.com/ClickHouse/ClickHouse/blame/master/src/Disks/ObjectStorages/DiskObjectStorageMetadata.h#L15
const (
	VersionAbsolutePaths MetadataVersion = 1
	VersionRelativePath  MetadataVersion = 2
	VersionReadOnlyFlag  MetadataVersion = 3
	VersionInlineData    MetadataVersion = 4
)

type StorageObject struct {
	ObjectSize         int64
	ObjectRelativePath string
}

type Metadata struct {
	Version            MetadataVersion
	StorageObjectCount int
	TotalSize          int64
	StorageObjects     []StorageObject
	RefCount           int
	ReadOnly           bool
	InlineData         string
	Path               string
}

type Object struct {
	Key      string
	Created  time.Time
	Deleted  time.Time
	Versions []string
	Deletes  []string
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

func ReadInt64Text(scanner *bufio.Scanner) (int64, error) {
	scanner.Scan()
	value := scanner.Text()
	intValue, err := strconv.ParseInt(value, 10, 64)
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

func (m *Metadata) readFromFile(file io.Reader) error {

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

	m.TotalSize, err = ReadInt64Text(scanner)

	for i := 0; i < m.StorageObjectCount; i++ {

		objectSize, _ := ReadInt64Text(scanner)
		scanner.Scan()
		objectRelativePath := scanner.Text()

		if version == int(VersionAbsolutePaths) {
			if !strings.HasPrefix(objectRelativePath, objectStorageRootPath) {
				return fmt.Errorf("%s doesn't contains %s", objectRelativePath, objectStorageRootPath)
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

func (m *Metadata) writeToFile(file *os.File) error {
	var b2i = map[bool]string{false: "0", true: "1"}
	var err error

	if _, err = file.WriteString(strconv.FormatInt(int64(m.Version), 10) + "\n"); err != nil {
		return err
	}

	if _, err = file.WriteString(strconv.FormatInt(int64(m.StorageObjectCount), 10) + "\t" + strconv.FormatInt(m.TotalSize, 10) + "\n"); err != nil {
		return err
	}

	for i := 0; i < m.StorageObjectCount; i++ {
		if _, err = file.WriteString(strconv.FormatInt(m.StorageObjects[i].ObjectSize, 10) + "\t" + m.StorageObjects[i].ObjectRelativePath + "\n"); err != nil {
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

type ObjectStorageCredentials struct {
	Type               string // aws, gcs, azblob
	EndPoint           string
	S3AccessKey        string
	S3SecretKey        string
	S3AssumeRole       string
	S3Region           string
	S3StorageClass     string
	AzureAccountName   string
	AzureAccountKey    string
	AzureContainerName string
}

var DisksCredentials map[string]ObjectStorageCredentials

type ObjectStorageConnection struct {
	Type         string
	S3           *storage.S3
	AzureBlob    *storage.AzureBlob
	MetadataPath string
}

func (c *ObjectStorageConnection) GetRemoteStorage() storage.RemoteStorage {
	switch c.Type {
	case "s3":
		return c.S3
	case "azure_blob_storage":
		return c.AzureBlob
	}
	apexLog.Fatalf("invalid ObjectStorageConnection.type %s", c.Type)
	return nil
}

func (c *ObjectStorageConnection) GetRemoteBucket() string {
	switch c.Type {
	case "s3":
		return c.S3.Config.Bucket
	case "azure_blob_storage":
		return c.AzureBlob.Config.Container
	}
	apexLog.Fatalf("invalid ObjectStorageConnection.type %s", c.Type)
	return ""
}

func (c *ObjectStorageConnection) GetRemotePath() string {
	switch c.Type {
	case "s3":
		return c.S3.Config.Path
	case "azure_blob_storage":
		return c.AzureBlob.Config.Path
	}
	apexLog.Fatalf("invalid ObjectStorageConnection.type %s", c.Type)
	return ""
}

var DisksConnections map[string]ObjectStorageConnection
var SystemDisks map[string]clickhouse.Disk

func InitCredentialsAndConnections(ctx context.Context, ch *clickhouse.ClickHouse, cfg *config.Config, diskName string) error {
	var err error
	if _, exists := DisksCredentials[diskName]; !exists {
		DisksCredentials, err = getObjectDisksCredentials(ctx, ch)
		if err != nil {
			return err
		}
	}
	if _, exists := DisksConnections[diskName]; !exists {
		if DisksConnections == nil {
			DisksConnections = make(map[string]ObjectStorageConnection)
		}
		connection, err := makeObjectDiskConnection(ctx, ch, cfg, diskName)
		if err != nil {
			return err
		}
		DisksConnections[diskName] = *connection
	}
	return nil
}

func ReadMetadataFromFile(path string) (*Metadata, error) {
	metadataFile, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return ReadMetadataFromReader(metadataFile, path)
}

func ReadMetadataFromReader(metadataFile io.ReadCloser, path string) (*Metadata, error) {
	defer func() {
		if err := metadataFile.Close(); err != nil {
			apexLog.Warnf("can't close reader %s: %v", path, err)
		}
	}()

	var metadata Metadata
	metadata.Path = path
	if err := metadata.readFromFile(metadataFile); err != nil {
		return nil, err
	}
	return &metadata, nil
}

func WriteMetadataToFile(metadata *Metadata, path string) error {
	metadataFile, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() {
		if err = metadataFile.Close(); err != nil {
			apexLog.Warnf("can't close %s: %v", path, err)
		}
	}()
	return metadata.writeToFile(metadataFile)
}

func getObjectDisksCredentials(ctx context.Context, ch *clickhouse.ClickHouse) (map[string]ObjectStorageCredentials, error) {
	credentials := make(map[string]ObjectStorageCredentials)
	var version int
	var err error
	if version, err = ch.GetVersion(ctx); err != nil {
		return nil, err
	} else if version <= 20006000 {
		return credentials, nil
	}
	configFile, doc, err := ch.ParseXML(ctx, "config.xml")
	if err != nil {
		return nil, err
	}
	root := xmlquery.FindOne(doc, "/")
	disks := xmlquery.Find(doc, fmt.Sprintf("/%s/storage_configuration/disks/*", root.Data))
	for _, d := range disks {
		diskName := d.Data
		if diskTypeNode := d.SelectElement("type"); diskTypeNode != nil {
			diskType := diskTypeNode.InnerText()
			switch diskType {
			case "s3", "s3_plain":
				creds := ObjectStorageCredentials{
					Type: "s3",
				}
				if batchDeleteNode := d.SelectElement("support_batch_delete"); batchDeleteNode != nil {
					if strings.Trim(batchDeleteNode.InnerText(), "\r\n \t") == "false" {
						creds.Type = "gcs"
					}
				}
				if endPointNode := d.SelectElement("endpoint"); endPointNode != nil {
					creds.EndPoint = strings.Trim(endPointNode.InnerText(), "\r\n \t")
					// macros works only after 23.3+ https://github.com/Altinity/clickhouse-backup/issues/750
					if version > 23003000 {
						if creds.EndPoint, err = ch.ApplyMacros(ctx, creds.EndPoint); err != nil {
							return nil, fmt.Errorf("%s -> /%s/storage_configuration/disks/%s apply macros to <endpoint> error: %v", configFile, root.Data, diskName, err)
						}
					}
				} else {
					return nil, fmt.Errorf("%s -> /%s/storage_configuration/disks/%s doesn't contains <endpoint>", configFile, root.Data, diskName)
				}
				if regionNode := d.SelectElement("region"); regionNode != nil {
					creds.S3Region = strings.Trim(regionNode.InnerText(), "\r\n \t")
				}
				creds.S3StorageClass = "STANDARD"
				if storageClassNode := d.SelectElement("s3_storage_class"); storageClassNode != nil {
					creds.S3StorageClass = strings.Trim(storageClassNode.InnerText(), "\r\n \t")
				}
				accessKeyNode := d.SelectElement("access_key_id")
				secretKeyNode := d.SelectElement("secret_access_key")
				useEnvironmentCredentials := d.SelectElement("use_environment_credentials")
				if accessKeyNode != nil && secretKeyNode != nil {
					creds.S3AccessKey = strings.Trim(accessKeyNode.InnerText(), "\r\n \t")
					creds.S3SecretKey = strings.Trim(secretKeyNode.InnerText(), "\r\n \t")
				} else {
					apexLog.Warnf("%s -> /%s/storage_configuration/disks/%s doesn't contains <access_key_id> and <secret_access_key> environment variables will use", configFile, root.Data, diskName)
					creds.S3AssumeRole = os.Getenv("AWS_ROLE_ARN")
					if useEnvironmentCredentials != nil {
						creds.S3AccessKey = os.Getenv("AWS_ACCESS_KEY_ID")
						creds.S3SecretKey = os.Getenv("AWS_SECRET_ACCESS_KEY")
					}
				}
				credentials[diskName] = creds
				break
			case "azure_blob_storage":
				creds := ObjectStorageCredentials{
					Type: "azblob",
				}
				accountUrlNode := d.SelectElement("storage_account_url")
				if accountUrlNode == nil {
					return nil, fmt.Errorf("%s -> /%s/storage_configuration/disks/%s doesn't contains <storage_account_url>", configFile, root.Data, diskName)
				}
				creds.EndPoint = strings.Trim(accountUrlNode.InnerText(), "\r\n \t")
				containerNameNode := d.SelectElement("container_name")
				if containerNameNode == nil {
					return nil, fmt.Errorf("%s -> /%s/storage_configuration/disks/%s doesn't contains <container_name>", configFile, root.Data, diskName)
				}
				creds.AzureContainerName = strings.Trim(containerNameNode.InnerText(), "\r\n \t")
				accountNameNode := d.SelectElement("account_name")
				if containerNameNode == nil {
					return nil, fmt.Errorf("%s -> /%s/storage_configuration/disks/%s doesn't contains <account_name>", configFile, root.Data, diskName)
				}
				creds.AzureAccountName = strings.Trim(accountNameNode.InnerText(), "\r\n \t")
				accountKeyNode := d.SelectElement("account_key")
				if containerNameNode == nil {
					return nil, fmt.Errorf("%s -> /%s/storage_configuration/disks/%s doesn't contains <account_key>", configFile, root.Data, diskName)
				}
				creds.AzureAccountKey = strings.Trim(accountKeyNode.InnerText(), "\r\n \t")
				credentials[diskName] = creds
				break
			}
		}
	}
	for _, d := range disks {
		diskName := d.Data
		if diskTypeNode := d.SelectElement("type"); diskTypeNode != nil {
			diskType := diskTypeNode.InnerText()
			switch diskType {
			case "encrypted", "cache":
				_, exists := credentials[diskName]
				if !exists {
					if diskNode := d.SelectElement("disk"); diskNode != nil {
						childDiskName := diskNode.InnerText()
						credentials[diskName] = credentials[childDiskName]
					}
				}
			}
		}
	}
	return credentials, nil
}

func makeObjectDiskConnection(ctx context.Context, ch *clickhouse.ClickHouse, cfg *config.Config, diskName string) (*ObjectStorageConnection, error) {
	creds, exists := DisksCredentials[diskName]
	if !exists {
		return nil, fmt.Errorf("%s is not present in object_disk.DisksCredentials", diskName)
	}
	connection := ObjectStorageConnection{}
	if SystemDisks == nil || len(SystemDisks) == 0 {
		disks, err := ch.GetDisks(ctx, false)
		if err != nil {
			return nil, err
		}
		SystemDisks = make(map[string]clickhouse.Disk, len(disks))
		for _, d := range disks {
			SystemDisks[d.Name] = d
		}
	}
	disk, exists := SystemDisks[diskName]
	if !exists {
		return nil, fmt.Errorf("%s is not presnet in object_disk.SystemDisks", diskName)
	}
	if disk.Type != "s3" && disk.Type != "s3_plain" && disk.Type != "azure_blob_storage" {
		return nil, fmt.Errorf("%s have unsupported type %s", diskName, disk.Type)
	}
	connection.MetadataPath = disk.Path

	switch creds.Type {
	case "s3", "gcs":
		connection.Type = "s3"
		s3cfg := config.S3Config{Debug: cfg.S3.Debug, MaxPartsCount: cfg.S3.MaxPartsCount, Concurrency: 1}
		s3URL, err := url.Parse(creds.EndPoint)
		if err != nil {
			return nil, err
		}
		s3cfg.Endpoint = s3URL.Scheme + "://" + s3URL.Host
		if cfg.S3.Concurrency > 0 {
			s3cfg.Concurrency = cfg.S3.Concurrency
		}
		s3cfg.Region = "us-east-1"
		if creds.S3Region != "" {
			s3cfg.Region = creds.S3Region
		} else if cfg.S3.Region != "" {
			s3cfg.Region = cfg.S3.Region
		}
		if creds.S3StorageClass != "" {
			s3cfg.StorageClass = creds.S3StorageClass
		} else {
			s3cfg.StorageClass = cfg.S3.StorageClass
		}
		if creds.S3AssumeRole != "" {
			s3cfg.AssumeRoleARN = creds.S3AssumeRole
		}
		if creds.S3AccessKey != "" {
			s3cfg.AccessKey = creds.S3AccessKey
		}
		if creds.S3SecretKey != "" {
			s3cfg.SecretKey = creds.S3SecretKey
		}
		// https://docs.aws.amazon.com/AmazonS3/latest/userguide/VirtualHosting.html
		// https://kb.altinity.com/altinity-kb-integrations/altinity-kb-google-s3-gcs/
		// https://aws.amazon.com/compliance/fips/
		if strings.HasSuffix(s3URL.Host, ".amazonaws.com") && (strings.Contains(s3URL.Host, ".s3.") || strings.Contains(s3URL.Host, ".s3-fisp.")) {
			hostParts := strings.Split(s3URL.Host, ".")
			s3cfg.Bucket = hostParts[0]
			if len(hostParts) >= 3 {
				s3cfg.Region = hostParts[2]
			}
			s3cfg.Path = strings.Trim(s3URL.Path, "/")
			s3cfg.ForcePathStyle = false
		} else {
			pathItems := strings.Split(strings.Trim(s3URL.Path, "/"), "/")
			s3cfg.Bucket = pathItems[0]
			s3cfg.Path = path.Join(pathItems[1:]...)
			s3cfg.ForcePathStyle = true
		}
		// need for CopyObject
		s3cfg.ObjectDiskPath = s3cfg.Path
		connection.S3 = &storage.S3{Config: &s3cfg, Log: apexLog.WithField("logger", "S3")}
		if err = connection.S3.Connect(ctx); err != nil {
			return nil, err
		}
	case "azblob":
		connection.Type = "azure_blob_storage"
		azureCfg := config.AzureBlobConfig{
			Timeout:       cfg.AzureBlob.Timeout,
			BufferSize:    cfg.AzureBlob.BufferSize,
			MaxBuffers:    cfg.AzureBlob.MaxBuffers,
			MaxPartsCount: cfg.AzureBlob.MaxPartsCount,
		}
		azureURL, err := url.Parse(creds.EndPoint)
		if err != nil {
			return nil, err
		}
		azureCfg.EndpointSchema = "http"
		if azureURL.Scheme != "" {
			azureCfg.EndpointSchema = azureURL.Scheme
		}
		azureCfg.EndpointSuffix = azureURL.Host
		if creds.AzureAccountName != "" {
			azureCfg.AccountName = creds.AzureAccountName
		}
		if azureURL.Path != "" {
			azureCfg.Path = azureURL.Path
			if azureCfg.AccountName != "" && strings.HasPrefix(azureCfg.Path, "/"+creds.AzureAccountName) {
				azureCfg.Path = strings.TrimPrefix(azureURL.Path, "/"+creds.AzureAccountName)
			}
			// need for CopyObject
			azureCfg.ObjectDiskPath = azureCfg.Path
		}
		if creds.AzureAccountKey != "" {
			azureCfg.AccountKey = creds.AzureAccountKey
		}
		if creds.AzureContainerName != "" {
			azureCfg.Container = creds.AzureContainerName
		}
		connection.AzureBlob = &storage.AzureBlob{Config: &azureCfg}
		if err = connection.AzureBlob.Connect(ctx); err != nil {
			return nil, err
		}
	}
	return &connection, nil
}

func ConvertLocalPathToRemote(diskName, localPath string) (string, error) {
	connection, exists := DisksConnections[diskName]
	if !exists {
		return "", fmt.Errorf("%s is not present in object_disk.DisksConnections", diskName)
	}
	if !strings.HasPrefix(localPath, "/") && !strings.HasPrefix(localPath, connection.MetadataPath) {
		localPath = path.Join(connection.MetadataPath, localPath)
	}
	meta, err := ReadMetadataFromFile(localPath)
	if err != nil {
		return "", err
	}
	return meta.StorageObjects[0].ObjectRelativePath, nil
}

func GetFileReader(ctx context.Context, diskName, remotePath string) (io.ReadCloser, error) {
	connection, exists := DisksConnections[diskName]
	if !exists {
		return nil, fmt.Errorf("%s not exits in object_disk.DisksConnections", diskName)
	}
	var f io.ReadCloser
	var err error
	remoteStorage := connection.GetRemoteStorage()
	f, err = remoteStorage.GetFileReader(ctx, remotePath)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func ReadFileContent(ctx context.Context, ch *clickhouse.ClickHouse, cfg *config.Config, diskName, localPath string) ([]byte, error) {
	if err := InitCredentialsAndConnections(ctx, ch, cfg, diskName); err != nil {
		return nil, err
	}
	remotePath, err := ConvertLocalPathToRemote(diskName, localPath)
	if err != nil {
		return nil, err
	}

	f, err := GetFileReader(ctx, diskName, remotePath)
	if err != nil {
		return nil, err
	}
	return io.ReadAll(f)
}

func PutFile(ctx context.Context, diskName, remotePath string, content []byte) error {
	connection, exists := DisksConnections[diskName]
	if !exists {
		return fmt.Errorf("%s not exits in object_disk.DisksConnections", diskName)
	}
	f := bytes.NewReader(content)
	fCloser := io.NopCloser(f)
	remoteStorage := connection.GetRemoteStorage()
	return remoteStorage.PutFile(ctx, remotePath, fCloser)
}

func WriteFileContent(ctx context.Context, ch *clickhouse.ClickHouse, cfg *config.Config, diskName, localPath string, content []byte) error {
	if err := InitCredentialsAndConnections(ctx, ch, cfg, diskName); err != nil {
		return err
	}

	remotePath, err := ConvertLocalPathToRemote(diskName, localPath)
	if err != nil {
		return err
	}
	return PutFile(ctx, diskName, remotePath, content)
}

func DeleteFile(ctx context.Context, diskName, remotePath string) error {
	connection, exists := DisksConnections[diskName]
	if !exists {
		return fmt.Errorf("%s not exits in object_disk.DisksConnections", diskName)
	}
	remoteStorage := connection.GetRemoteStorage()
	return remoteStorage.DeleteFile(ctx, remotePath)
}

func DeleteFileWithContent(ctx context.Context, ch *clickhouse.ClickHouse, cfg *config.Config, diskName, localPath string) error {
	if err := InitCredentialsAndConnections(ctx, ch, cfg, diskName); err != nil {
		return err
	}

	remotePath, err := ConvertLocalPathToRemote(diskName, localPath)
	if err != nil {
		return err
	}
	return DeleteFile(ctx, diskName, remotePath)
}

func GetFileSize(ctx context.Context, ch *clickhouse.ClickHouse, cfg *config.Config, diskName, remotePath string) (int64, error) {
	if err := InitCredentialsAndConnections(ctx, ch, cfg, diskName); err != nil {
		return 0, err
	}
	connection, exists := DisksConnections[diskName]
	if !exists {
		return 0, fmt.Errorf("%s not exits in object_disk.DisksConnections", diskName)
	}
	remoteStorage := connection.GetRemoteStorage()
	fileInfo, err := remoteStorage.StatFile(ctx, remotePath)
	if err != nil {
		return 0, err
	}
	return fileInfo.Size(), nil
}

func CopyObject(ctx context.Context, ch *clickhouse.ClickHouse, cfg *config.Config, diskName, srcBucket, srcKey, dstPath string) error {
	if err := InitCredentialsAndConnections(ctx, ch, cfg, diskName); err != nil {
		return err
	}
	connection := DisksConnections[diskName]
	remoteStorage := connection.GetRemoteStorage()
	_, err := remoteStorage.CopyObject(ctx, srcBucket, srcKey, dstPath)
	return err
}
