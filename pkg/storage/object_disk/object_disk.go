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
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/clickhouse"
	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/Altinity/clickhouse-backup/v2/pkg/storage"
	"github.com/antchfx/xmlquery"
	"github.com/puzpuzpuz/xsync"
	"github.com/rs/zerolog/log"
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
	Type                      string // aws, gcs, azblob
	EndPoint                  string
	S3AccessKey               string
	S3SecretKey               string
	S3AssumeRole              string
	S3Region                  string
	S3StorageClass            string
	S3SSECustomerKey          string // <server_side_encryption_customer_key_base64>
	S3SSEKMSKeyId             string // <server_side_encryption_kms_key_id>
	S3SSEKMSEncryptionContext string // <server_side_encryption_kms_encryption_context>
	AzureAccountName          string
	AzureAccountKey           string
	AzureContainerName        string
}

var DisksCredentials = xsync.NewMapOf[ObjectStorageCredentials]()

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
	case "azure", "azure_blob_storage":
		return c.AzureBlob
	}
	log.Fatal().Stack().Msgf("invalid ObjectStorageConnection.type %s", c.Type)
	return nil
}

func (c *ObjectStorageConnection) GetRemoteBucket() string {
	switch c.Type {
	case "s3":
		return c.S3.Config.Bucket
	case "azure", "azure_blob_storage":
		return c.AzureBlob.Config.Container
	}
	log.Fatal().Stack().Msgf("invalid ObjectStorageConnection.type %s", c.Type)
	return ""
}

func (c *ObjectStorageConnection) GetRemotePath() string {
	switch c.Type {
	case "s3":
		return c.S3.Config.Path
	case "azure", "azure_blob_storage":
		return c.AzureBlob.Config.Path
	}
	log.Fatal().Stack().Msgf("invalid ObjectStorageConnection.type %s", c.Type)
	return ""
}

func (c *ObjectStorageConnection) GetRemoteObjectDiskPath() string {
	switch c.Type {
	case "s3":
		return c.S3.Config.ObjectDiskPath
	case "azure", "azure_blob_storage":
		return c.AzureBlob.Config.ObjectDiskPath
	}
	log.Fatal().Stack().Msgf("invalid ObjectStorageConnection.type %s", c.Type)
	return ""
}

var DisksConnections = xsync.NewMapOf[*ObjectStorageConnection]()

var SystemDisks = xsync.NewMapOf[clickhouse.Disk]()

var InitCredentialsAndConnectionsMutex sync.Mutex

func InitCredentialsAndConnections(ctx context.Context, ch *clickhouse.ClickHouse, cfg *config.Config, diskName string) error {
	var err error
	InitCredentialsAndConnectionsMutex.Lock()
	defer InitCredentialsAndConnectionsMutex.Unlock()
	if _, exists := DisksCredentials.Load(diskName); !exists {
		if err = getObjectDisksCredentials(ctx, ch); err != nil {
			return err
		}
	}
	if _, exists := DisksConnections.Load(diskName); !exists {
		connection, err := makeObjectDiskConnection(ctx, ch, cfg, diskName)
		if err != nil {
			return err
		}
		DisksConnections.Store(diskName, connection)
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
			log.Warn().Msgf("can't close reader %s: %v", path, err)
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
			log.Warn().Msgf("can't close %s: %v", path, err)
		}
	}()
	return metadata.writeToFile(metadataFile)
}

func getObjectDisksCredentials(ctx context.Context, ch *clickhouse.ClickHouse) error {
	var version int
	var err error
	if version, err = ch.GetVersion(ctx); err != nil {
		return err
	} else if version <= 20006000 {
		return nil
	}
	configFile, doc, err := ch.ParseXML(ctx, "config.xml")
	if err != nil {
		return err
	}
	root := xmlquery.FindOne(doc, "/")
	disks := xmlquery.Find(doc, fmt.Sprintf("/%s/storage_configuration/disks/*", root.Data))
	for _, d := range disks {
		diskName := d.Data
		if diskTypeNode := d.SelectElement("type"); diskTypeNode != nil {
			diskType := strings.Trim(diskTypeNode.InnerText(), "\r\n \t")
			// https://github.com/Altinity/clickhouse-backup/issues/1112
			if diskType == "object_storage" {
				diskTypeNode = d.SelectElement("object_storage_type")
				if diskTypeNode == nil {
					return fmt.Errorf("/%s/storage_configuration/disks/%s, contains <type>object_storage</type>, but doesn't contains <object_storage_type> tag", root.Data, diskName)
				}
				diskType = strings.Trim(diskTypeNode.InnerText(), "\r\n \t")
				if metadataTypeNode := d.SelectElement("metadata_type"); metadataTypeNode != nil {
					metadataType := strings.Trim(metadataTypeNode.InnerText(), "\r\n \t")
					if metadataType != "local" {
						return fmt.Errorf("/%s/storage_configuration/disks/%s, unsupported <metadata_type>%s</metadata_type>", root.Data, diskName, metadataType)
					}
				}
			}
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
							return fmt.Errorf("%s -> /%s/storage_configuration/disks/%s apply macros to <endpoint> error: %v", configFile, root.Data, diskName, err)
						}
					}
				} else {
					return fmt.Errorf("%s -> /%s/storage_configuration/disks/%s doesn't contains <endpoint>", configFile, root.Data, diskName)
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
					log.Warn().Msgf("%s -> /%s/storage_configuration/disks/%s doesn't contains <access_key_id> and <secret_access_key> environment variables will use", configFile, root.Data, diskName)
					creds.S3AssumeRole = os.Getenv("AWS_ROLE_ARN")
					if useEnvironmentCredentials != nil {
						creds.S3AccessKey = os.Getenv("AWS_ACCESS_KEY_ID")
						creds.S3SecretKey = os.Getenv("AWS_SECRET_ACCESS_KEY")
					}
				}
				if S3SSECustomerKey := d.SelectElement("server_side_encryption_customer_key_base64"); S3SSECustomerKey != nil {
					creds.S3SSECustomerKey = strings.Trim(S3SSECustomerKey.InnerText(), "\r\n \t")
				}
				if S3SSEKMSKeyId := d.SelectElement("server_side_encryption_kms_key_id"); S3SSEKMSKeyId != nil {
					creds.S3SSEKMSKeyId = strings.Trim(S3SSEKMSKeyId.InnerText(), "\r\n \t")
				}
				if S3SSEKMSEncryptionContext := d.SelectElement("server_side_encryption_kms_encryption_context"); S3SSEKMSEncryptionContext != nil {
					creds.S3SSEKMSEncryptionContext = strings.Trim(S3SSEKMSEncryptionContext.InnerText(), "\r\n \t")
				}
				DisksCredentials.Store(diskName, creds)
				break
			case "azure", "azure_blob_storage":
				creds := ObjectStorageCredentials{
					Type: "azblob",
				}
				accountUrlNode := d.SelectElement("storage_account_url")
				if accountUrlNode == nil {
					return fmt.Errorf("%s -> /%s/storage_configuration/disks/%s doesn't contains <storage_account_url>", configFile, root.Data, diskName)
				}
				creds.EndPoint = strings.Trim(accountUrlNode.InnerText(), "\r\n \t")
				containerNameNode := d.SelectElement("container_name")
				if containerNameNode == nil {
					return fmt.Errorf("%s -> /%s/storage_configuration/disks/%s doesn't contains <container_name>", configFile, root.Data, diskName)
				}
				creds.AzureContainerName = strings.Trim(containerNameNode.InnerText(), "\r\n \t")
				accountNameNode := d.SelectElement("account_name")
				if accountNameNode == nil {
					return fmt.Errorf("%s -> /%s/storage_configuration/disks/%s doesn't contains <account_name>", configFile, root.Data, diskName)
				}
				creds.AzureAccountName = strings.Trim(accountNameNode.InnerText(), "\r\n \t")
				accountKeyNode := d.SelectElement("account_key")
				if accountKeyNode == nil {
					return fmt.Errorf("%s -> /%s/storage_configuration/disks/%s doesn't contains <account_key>", configFile, root.Data, diskName)
				}
				creds.AzureAccountKey = strings.Trim(accountKeyNode.InnerText(), "\r\n \t")
				DisksCredentials.Store(diskName, creds)
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
				_, exists := DisksCredentials.Load(diskName)
				if !exists {
					if diskNode := d.SelectElement("disk"); diskNode != nil {
						childDiskName := diskNode.InnerText()
						if childCreds, childExists := DisksCredentials.Load(childDiskName); childExists {
							DisksCredentials.Store(diskName, childCreds)
						} else {
							log.Warn().Msgf("disk %s with type %s, reference to  childDisk %s which not contains DiskCredentials", diskName, diskType, childDiskName)
						}
					}
				}
			}
		}
	}
	return nil
}

// S3VirtualHostBucketRE https://docs.aws.amazon.com/AmazonS3/latest/dev/VirtualHosting.html#virtual-hosted-style-access
var S3VirtualHostBucketRE = regexp.MustCompile(`((.+)\.(s3express[\-a-z0-9]+|s3|cos|obs|oss-data-acc|oss|eos)([.\-][a-z0-9\-.:]+))`)

func makeObjectDiskConnection(ctx context.Context, ch *clickhouse.ClickHouse, cfg *config.Config, diskName string) (*ObjectStorageConnection, error) {
	creds, exists := DisksCredentials.Load(diskName)
	if !exists {
		return nil, fmt.Errorf("%s is not present in object_disk.DisksCredentials", diskName)
	}
	connection := ObjectStorageConnection{}
	if SystemDisks.Size() == 0 {
		disks, err := ch.GetDisks(ctx, false)
		if err != nil {
			return nil, err
		}
		for _, d := range disks {
			SystemDisks.Store(d.Name, d)
		}
	}
	disk, exists := SystemDisks.Load(diskName)
	if !exists {
		return nil, fmt.Errorf("%s is not presnet in object_disk.SystemDisks", diskName)
	}
	if disk.Type != "s3" && disk.Type != "s3_plain" && disk.Type != "azure_blob_storage" && disk.Type != "azure" && disk.Type != "encrypted" {
		return nil, fmt.Errorf("%s have unsupported type %s", diskName, disk.Type)
	}
	connection.MetadataPath = disk.Path

	switch creds.Type {
	case "s3", "gcs":
		connection.Type = "s3"
		s3cfg := config.S3Config{
			Debug: cfg.S3.Debug, MaxPartsCount: cfg.S3.MaxPartsCount, Concurrency: 1,
		}
		s3URL, err := url.Parse(creds.EndPoint)
		if err != nil {
			return nil, err
		}
		if s3URL.Scheme == "http" {
			s3cfg.DisableSSL = true
		}
		if cfg.S3.Concurrency > 0 {
			s3cfg.Concurrency = cfg.S3.Concurrency
		}
		if cfg.S3.DisableCertVerification && !strings.HasSuffix(s3URL.Host, ".amazonaws.com") && !strings.HasSuffix(s3URL.Host, ".googleapis.com") {
			s3cfg.DisableCertVerification = cfg.S3.DisableCertVerification
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
		if creds.S3SSECustomerKey != "" {
			s3cfg.SSECustomerKey = creds.S3SSECustomerKey
		}
		if creds.S3SSEKMSKeyId != "" {
			s3cfg.SSEKMSKeyId = creds.S3SSEKMSKeyId
		}
		if creds.S3SSEKMSEncryptionContext != "" {
			s3cfg.SSEKMSEncryptionContext = creds.S3SSEKMSEncryptionContext
		}
		// https://docs.aws.amazon.com/AmazonS3/latest/userguide/VirtualHosting.html
		// https://kb.altinity.com/altinity-kb-integrations/altinity-kb-google-s3-gcs/
		// https://aws.amazon.com/compliance/fips/
		if strings.HasSuffix(s3URL.Host, ".amazonaws.com") && (strings.Contains(s3URL.Host, ".s3.") || strings.Contains(s3URL.Host, ".s3-fisp.")) {
			hostParts := strings.Split(s3URL.Host, ".")
			//https://region.s3.amazonaws.com/bucket/
			if len(hostParts) == 4 && creds.S3Region == "" {
				pathParts := strings.Split(strings.Trim(s3URL.Path, "/"), "/")
				s3cfg.Bucket = pathParts[0]
				s3cfg.Region = hostParts[0]
				s3cfg.Path = path.Join(pathParts[1:]...)
			}
			//https://bucket-name.s3.amazonaws.com/
			if len(hostParts) == 4 && creds.S3Region != "" {
				s3cfg.Bucket = hostParts[0]
				s3cfg.Path = strings.Trim(s3URL.Path, "/")
			}
			//https://bucket-name.s3.region-code.amazonaws.com
			if len(hostParts) == 5 {
				s3cfg.Bucket = hostParts[0]
				s3cfg.Region = hostParts[2]
				s3cfg.Path = strings.Trim(s3URL.Path, "/")
			}
			s3cfg.ForcePathStyle = false
		} else if s3URL.Scheme == "s3" {
			// https://github.com/Altinity/clickhouse-backup/issues/1035
			s3cfg.Bucket = s3URL.Host
			s3cfg.Path = s3URL.Path
			s3cfg.ForcePathStyle = false
		} else if S3VirtualHostBucketRE.MatchString(s3URL.Host) {
			hostParts := strings.Split(s3URL.Host, ".")
			s3cfg.Bucket = hostParts[0]
			s3cfg.Endpoint = s3URL.Scheme + "://" + strings.Join(hostParts[1:], ".")
			s3cfg.Path = strings.Trim(s3URL.Path, "/")
			s3cfg.ForcePathStyle = false
		} else {
			s3cfg.Endpoint = s3URL.Scheme + "://" + s3URL.Host
			pathItems := strings.Split(strings.Trim(s3URL.Path, "/"), "/")
			s3cfg.Bucket = pathItems[0]
			s3cfg.Path = path.Join(pathItems[1:]...)
			s3cfg.ForcePathStyle = true
		}
		// need for CopyObject
		s3cfg.ObjectDiskPath = s3cfg.Path
		connection.S3 = &storage.S3{Config: &s3cfg}
		if err = connection.S3.Connect(ctx); err != nil {
			return nil, err
		}
	case "azblob":
		connection.Type = "azure_blob_storage"
		azureCfg := config.AzureBlobConfig{
			Timeout:       cfg.AzureBlob.Timeout,
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
			azureCfg.EndpointSuffix = strings.TrimPrefix(azureCfg.EndpointSuffix, azureCfg.AccountName+".blob.")
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
		azureCfg.Debug = cfg.AzureBlob.Debug
		connection.AzureBlob = &storage.AzureBlob{Config: &azureCfg}
		if err = connection.AzureBlob.Connect(ctx); err != nil {
			return nil, err
		}
	}
	return &connection, nil
}

func ConvertLocalPathToRemote(diskName, localPath string) (string, error) {
	connection, exists := DisksConnections.Load(diskName)
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
	connection, exists := DisksConnections.Load(diskName)
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
	connection, exists := DisksConnections.Load(diskName)
	if !exists {
		return fmt.Errorf("%s not exits in object_disk.DisksConnections", diskName)
	}
	f := bytes.NewReader(content)
	fCloser := io.NopCloser(f)
	remoteStorage := connection.GetRemoteStorage()
	return remoteStorage.PutFile(ctx, remotePath, fCloser, 0)
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
	connection, exists := DisksConnections.Load(diskName)
	if !exists {
		return fmt.Errorf("%s not exits in object_disk.DisksConnections", diskName)
	}
	remoteStorage := connection.GetRemoteStorage()
	return remoteStorage.DeleteFile(ctx, remotePath)
}

/*
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
*/

func CopyObject(ctx context.Context, diskName string, srcSize int64, srcBucket, srcKey, dstPath string) (int64, error) {
	connection, _ := DisksConnections.Load(diskName)
	remoteStorage := connection.GetRemoteStorage()
	return remoteStorage.CopyObject(ctx, srcSize, srcBucket, srcKey, dstPath)
}

func CopyObjectStreaming(ctx context.Context, srcStorage storage.RemoteStorage, dstStorage storage.RemoteStorage, srcKey, dstKey string) error {
	srcInfo, statErr := srcStorage.StatFileAbsolute(ctx, srcKey)
	if statErr != nil {
		return fmt.Errorf("srcStorage.StatFileReaderAbsolute(%s) error: %v", srcKey, statErr)
	}

	srcReader, srcErr := srcStorage.GetFileReaderAbsolute(ctx, srcKey)
	if srcErr != nil {
		return fmt.Errorf("srcStorage.GetFileReaderAbsolute(%s) error: %v", srcKey, srcErr)
	}
	defer func() {
		if closeErr := srcReader.Close(); closeErr != nil {
			log.Error().Msgf("srcReader.Close(%s) error: %v", srcKey, closeErr)
		}
	}()
	if putErr := dstStorage.PutFileAbsolute(ctx, dstKey, srcReader, srcInfo.Size()); putErr != nil {
		return fmt.Errorf("dstStorage.PutFileAbsolute(%s) error: %v", dstKey, putErr)
	}
	return nil
}
