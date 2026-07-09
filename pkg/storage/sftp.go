package storage

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/Altinity/clickhouse-backup/v2/pkg/config"
	"github.com/pkg/errors"
	libSFTP "github.com/pkg/sftp"
	"github.com/rs/zerolog/log"
	"golang.org/x/crypto/ssh"
)

// SFTP Implement RemoteStorage
type SFTP struct {
	sshClient  *ssh.Client
	sftpClient *libSFTP.Client
	Config     *config.SFTPConfig
}

func (sftp *SFTP) Debug(msg string, v ...interface{}) {
	if sftp.Config.Debug {
		log.Info().Msgf(msg, v...)
	}
}

func (sftp *SFTP) Kind() string {
	return "SFTP"
}

func (sftp *SFTP) Connect(ctx context.Context) error {
	authMethods := make([]ssh.AuthMethod, 0)

	if sftp.Config.Key == "" && sftp.Config.Password == "" {
		return errors.New("please specify sftp.key or sftp.password for authentication")
	}

	if sftp.Config.Key != "" {
		fSftpKey, err := os.ReadFile(sftp.Config.Key)
		if err != nil {
			return errors.Wrap(err, "SFTP Connect ReadFile key")
		}
		sftpKey, err := ssh.ParsePrivateKey(fSftpKey)
		if err != nil {
			return errors.Wrap(err, "SFTP Connect ParsePrivateKey")
		}

		authMethods = append(authMethods, ssh.PublicKeys(sftpKey))
	}

	if sftp.Config.Password != "" {
		authMethods = append(authMethods, ssh.Password(sftp.Config.Password))
	}

	sftpConfig := &ssh.ClientConfig{
		User:            sftp.Config.Username,
		Auth:            authMethods,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}
	addr := fmt.Sprintf("%s:%d", sftp.Config.Address, sftp.Config.Port)
	sftp.Debug("[SFTP_DEBUG] try connect to tcp://%s", addr)
	sshConnection, err := ssh.Dial("tcp", addr, sftpConfig)
	if err != nil {
		return errors.Wrap(err, "SFTP Connect ssh.Dial")
	}
	clientOptions := make([]libSFTP.ClientOption, 0)
	if sftp.Config.Concurrency > 0 {
		clientOptions = append(
			clientOptions,
			libSFTP.UseConcurrentReads(true),
			libSFTP.UseConcurrentWrites(true),
			libSFTP.MaxConcurrentRequestsPerFile(sftp.Config.Concurrency),
		)
	}
	// MaxPacketUnchecked allows payloads above the 32KB default for servers that support them, see https://github.com/Altinity/clickhouse-backup/issues/1376
	if sftp.Config.MaxPacketSize > 0 {
		clientOptions = append(clientOptions, libSFTP.MaxPacketUnchecked(sftp.Config.MaxPacketSize))
	}
	sftpConnection, err := libSFTP.NewClient(sshConnection, clientOptions...)
	if err != nil {
		return errors.Wrap(err, "SFTP Connect NewClient")
	}

	sftp.sftpClient = sftpConnection
	sftp.sshClient = sshConnection
	return nil
}

func (sftp *SFTP) Close(ctx context.Context) error {
	if err := sftp.sftpClient.Close(); err != nil {
		return errors.Wrap(err, "sftpClient.Close()")
	}
	if err := sftp.sshClient.Close(); err != nil {
		return errors.Wrap(err, "sshClient.Close()")
	}
	return nil
}

func (sftp *SFTP) StatFile(ctx context.Context, key string) (RemoteFile, error) {
	return sftp.StatFileAbsolute(ctx, path.Join(sftp.Config.Path, key))
}

func (sftp *SFTP) StatFileAbsolute(ctx context.Context, key string) (RemoteFile, error) {
	stat, err := sftp.sftpClient.Stat(key)
	if err != nil {
		sftp.Debug("[SFTP_DEBUG] StatFile::STAT %s return error %v", key, err)
		if strings.Contains(err.Error(), "not exist") {
			return nil, NewErrNotFound(key)
		}
		return nil, errors.Wrap(err, "SFTP StatFileAbsolute Stat")
	}

	return &sftpFile{
		size:         stat.Size(),
		lastModified: stat.ModTime(),
		name:         stat.Name(),
	}, nil
}

func (sftp *SFTP) DeleteFile(ctx context.Context, key string) error {
	sftp.Debug("[SFTP_DEBUG] Delete %s", key)
	filePath := path.Join(sftp.Config.Path, key)

	fileStat, err := sftp.sftpClient.Stat(filePath)
	if err != nil {
		sftp.Debug("[SFTP_DEBUG] Delete::STAT %s return error %v", filePath, err)
		return errors.Wrap(err, "SFTP DeleteFile Stat")
	}
	if fileStat.IsDir() {
		return sftp.DeleteDirectory(ctx, filePath)
	} else {
		return sftp.sftpClient.Remove(filePath)
	}
}

func (sftp *SFTP) DeleteDirectory(ctx context.Context, dirPath string) error {
	sftp.Debug("[SFTP_DEBUG] DeleteDirectory %s", dirPath)
	defer func() {
		if err := sftp.sftpClient.RemoveDirectory(dirPath); err != nil {
			log.Warn().Msgf("RemoveDirectory err=%v", err)
		}
	}()

	files, err := sftp.sftpClient.ReadDir(dirPath)
	if err != nil {
		sftp.Debug("[SFTP_DEBUG] DeleteDirectory::ReadDir %s return error %v", dirPath, err)
		return errors.Wrap(err, "SFTP DeleteDirectory ReadDir")
	}
	for _, file := range files {
		filePath := path.Join(dirPath, file.Name())
		if file.IsDir() {
			if err := sftp.DeleteDirectory(ctx, filePath); err != nil {
				log.Warn().Msgf("sftp.DeleteDirectory(%s) err=%v", filePath, err)
			}
		} else {
			if err := sftp.sftpClient.Remove(filePath); err != nil {
				log.Warn().Msgf("sftp.Remove(%s) err=%v", filePath, err)
			}
		}
	}

	return nil
}

func (sftp *SFTP) Walk(ctx context.Context, remotePath string, recursive bool, process func(context.Context, RemoteFile) error) error {
	prefix := path.Join(sftp.Config.Path, remotePath)
	return sftp.WalkAbsolute(ctx, prefix, recursive, process)
}

func (sftp *SFTP) WalkAbsolute(ctx context.Context, prefix string, recursive bool, process func(context.Context, RemoteFile) error) error {
	sftp.Debug("[SFTP_DEBUG] Walk %s, recursive=%v", prefix, recursive)

	if recursive {
		walker := sftp.sftpClient.Walk(prefix)
		for walker.Step() {
			if err := walker.Err(); err != nil {
				return errors.Wrap(err, "SFTP WalkAbsolute walker.Err")
			}
			entry := walker.Stat()
			if entry == nil {
				continue
			}
			// walker emits the root and directory entries while descending, process only files like object storages do
			if entry.IsDir() {
				continue
			}
			relName, _ := filepath.Rel(prefix, walker.Path())
			err := process(ctx, &sftpFile{
				size:         entry.Size(),
				lastModified: entry.ModTime(),
				name:         relName,
			})
			if err != nil {
				return errors.Wrap(err, "SFTP WalkAbsolute process")
			}
		}
	} else {
		entries, err := sftp.sftpClient.ReadDir(prefix)
		if err != nil {
			sftp.Debug("[SFTP_DEBUG] Walk::NonRecursive::ReadDir %s return error %v", prefix, err)
			return errors.Wrap(err, "SFTP WalkAbsolute ReadDir")
		}
		for _, entry := range entries {
			err := process(ctx, &sftpFile{
				size:         entry.Size(),
				lastModified: entry.ModTime(),
				name:         entry.Name(),
			})
			if err != nil {
				return errors.Wrap(err, "SFTP WalkAbsolute process entry")
			}
		}
	}
	return nil
}

func (sftp *SFTP) GetFileReader(ctx context.Context, key string) (io.ReadCloser, error) {
	return sftp.GetFileReaderAbsolute(ctx, path.Join(sftp.Config.Path, key))
}

func (sftp *SFTP) GetFileReaderAbsolute(ctx context.Context, key string) (io.ReadCloser, error) {
	return sftp.sftpClient.OpenFile(key, syscall.O_RDWR)
}

func (sftp *SFTP) GetFileReaderWithLocalPath(ctx context.Context, key, localPath string, remoteSize int64) (io.ReadCloser, error) {
	return sftp.GetFileReader(ctx, key)
}

func (sftp *SFTP) PutFile(ctx context.Context, key string, r io.ReadCloser, localSize int64) error {
	return sftp.PutFileAbsolute(ctx, path.Join(sftp.Config.Path, key), r, localSize)
}

func (sftp *SFTP) PutFileAbsolute(ctx context.Context, key string, r io.ReadCloser, localSize int64) error {
	if err := sftp.sftpClient.MkdirAll(path.Dir(key)); err != nil {
		log.Warn().Msgf("sftp.sftpClient.MkdirAll(%s) err=%v", path.Dir(key), err)
	}
	remoteFile, err := sftp.sftpClient.Create(key)
	if err != nil {
		return errors.Wrap(err, "SFTP PutFileAbsolute Create")
	}
	defer func() {
		if err := remoteFile.Close(); err != nil {
			log.Warn().Msgf("can't close %s err=%v", key, err)
		}
	}()
	if _, err = remoteFile.ReadFrom(r); err != nil {
		return errors.Wrap(err, "SFTP PutFileAbsolute ReadFrom")
	}
	return nil
}

// CopyObject copy file inside the same SFTP server (like `echo "cp src dst" | sftp -b -`), srcBucket is ignored,
// uses the `copy-data` SFTP protocol extension (OpenSSH 9.0+) for server-side copy,
// falls back to `hardlink@openssh.com` (OpenSSH 5.7+, backup files are immutable so a hardlink is equivalent to a copy),
// streams through the client as the last resort
func (sftp *SFTP) CopyObject(ctx context.Context, srcSize int64, srcBucket, srcKey, dstKey string) (int64, error) {
	// non-empty srcBucket means the source lives in a bucket-based storage (object disk),
	// copy inside the SFTP server is impossible, fail fast so callers fall back to streaming
	if srcBucket != "" {
		return 0, errors.Errorf("CopyObject from bucket %s not supported for %s", srcBucket, sftp.Kind())
	}
	sftp.Debug("[SFTP_DEBUG] CopyObject %s -> %s", srcKey, dstKey)
	if err := sftp.sftpClient.MkdirAll(path.Dir(dstKey)); err != nil {
		log.Warn().Msgf("sftp.sftpClient.MkdirAll(%s) err=%v", path.Dir(dstKey), err)
	}
	if _, supported := sftp.sftpClient.HasExtension("copy-data"); supported {
		copyErr := sftp.copyDataServerSide(srcKey, dstKey)
		if copyErr == nil {
			return srcSize, nil
		}
		log.Warn().Msgf("SFTP CopyObject `copy-data` %s -> %s error: %v, will try hardlink", srcKey, dstKey, copyErr)
	}
	// remove possible leftover from a failed previous attempt, Link fails when dstKey exists
	if removeErr := sftp.sftpClient.Remove(dstKey); removeErr != nil && !strings.Contains(removeErr.Error(), "not exist") {
		sftp.Debug("[SFTP_DEBUG] CopyObject Remove(%s) err=%v", dstKey, removeErr)
	}
	linkErr := sftp.sftpClient.Link(srcKey, dstKey)
	if linkErr == nil {
		return srcSize, nil
	}
	log.Warn().Msgf("SFTP CopyObject hardlink %s -> %s error: %v, will stream through the client", srcKey, dstKey, linkErr)
	reader, err := sftp.GetFileReaderAbsolute(ctx, srcKey)
	if err != nil {
		return 0, errors.Wrapf(err, "SFTP CopyObject GetFileReaderAbsolute(%s)", srcKey)
	}
	defer func() {
		if closeErr := reader.Close(); closeErr != nil {
			log.Warn().Msgf("SFTP CopyObject can't close reader for %s error: %v", srcKey, closeErr)
		}
	}()
	if err = sftp.PutFileAbsolute(ctx, dstKey, reader, srcSize); err != nil {
		return 0, errors.Wrapf(err, "SFTP CopyObject PutFileAbsolute(%s)", dstKey)
	}
	return srcSize, nil
}

// SFTP protocol constants for copyDataServerSide, see https://datatracker.ietf.org/doc/html/draft-ietf-secsh-filexfer-02
// and the `copy-data` extension https://datatracker.ietf.org/doc/html/draft-ietf-secsh-filexfer-extensions-00#section-7
const (
	sshFxpInit     = 1
	sshFxpVersion  = 2
	sshFxpOpen     = 3
	sshFxpClose    = 4
	sshFxpStatus   = 101
	sshFxpHandle   = 102
	sshFxpExtended = 200

	sshFxOK = 0

	sshFxfRead  = 0x00000001
	sshFxfWrite = 0x00000002
	sshFxfCreat = 0x00000008
	sshFxfTrunc = 0x00000010
)

// copyDataServerSide sends the `copy-data` extended request over a dedicated `sftp` subsystem channel,
// pkg/sftp v1.13 doesn't expose file handles or arbitrary extended requests, so speak the wire protocol directly
func (sftp *SFTP) copyDataServerSide(srcKey, dstKey string) error {
	session, err := sftp.sshClient.NewSession()
	if err != nil {
		return errors.Wrap(err, "NewSession")
	}
	defer func() {
		if closeErr := session.Close(); closeErr != nil && closeErr != io.EOF {
			log.Warn().Msgf("SFTP copyDataServerSide session.Close error: %v", closeErr)
		}
	}()
	stdin, err := session.StdinPipe()
	if err != nil {
		return errors.Wrap(err, "StdinPipe")
	}
	stdout, err := session.StdoutPipe()
	if err != nil {
		return errors.Wrap(err, "StdoutPipe")
	}
	if err = session.RequestSubsystem("sftp"); err != nil {
		return errors.Wrap(err, "RequestSubsystem(sftp)")
	}

	if err = sftpWritePacket(stdin, []byte{sshFxpInit, 0, 0, 0, 3}); err != nil {
		return errors.Wrap(err, "send SSH_FXP_INIT")
	}
	packetType, _, err := sftpReadPacket(stdout)
	if err != nil {
		return errors.Wrap(err, "read SSH_FXP_VERSION")
	}
	if packetType != sshFxpVersion {
		return errors.Errorf("expected SSH_FXP_VERSION got packet type %d", packetType)
	}

	srcHandle, err := sftpOpen(stdin, stdout, 1, srcKey, sshFxfRead)
	if err != nil {
		return errors.Wrapf(err, "SSH_FXP_OPEN %s", srcKey)
	}
	dstHandle, err := sftpOpen(stdin, stdout, 2, dstKey, sshFxfWrite|sshFxfCreat|sshFxfTrunc)
	if err != nil {
		return errors.Wrapf(err, "SSH_FXP_OPEN %s", dstKey)
	}

	copyData := []byte{sshFxpExtended}
	copyData = sftpAppendUint32(copyData, 3) // request-id
	copyData = sftpAppendString(copyData, "copy-data")
	copyData = sftpAppendString(copyData, srcHandle)
	copyData = sftpAppendUint64(copyData, 0) // read-from-offset
	copyData = sftpAppendUint64(copyData, 0) // read-data-length, 0 means until EOF
	copyData = sftpAppendString(copyData, dstHandle)
	copyData = sftpAppendUint64(copyData, 0) // write-to-offset
	if err = sftpWritePacket(stdin, copyData); err != nil {
		return errors.Wrap(err, "send `copy-data` extended request")
	}
	copyErr := sftpExpectStatusOK(stdout, 3, "copy-data")

	for requestId, handle := range map[uint32]string{4: srcHandle, 5: dstHandle} {
		closePacket := []byte{sshFxpClose}
		closePacket = sftpAppendUint32(closePacket, requestId)
		closePacket = sftpAppendString(closePacket, handle)
		if err = sftpWritePacket(stdin, closePacket); err != nil {
			return errors.Wrap(err, "send SSH_FXP_CLOSE")
		}
		if closeErr := sftpExpectStatusOK(stdout, requestId, "SSH_FXP_CLOSE"); closeErr != nil && copyErr == nil {
			log.Warn().Msgf("SFTP copyDataServerSide close handle error: %v", closeErr)
		}
	}
	return copyErr
}

// sftpOpen sends SSH_FXP_OPEN and returns the file handle
func sftpOpen(stdin io.Writer, stdout io.Reader, requestId uint32, filePath string, pflags uint32) (string, error) {
	packet := []byte{sshFxpOpen}
	packet = sftpAppendUint32(packet, requestId)
	packet = sftpAppendString(packet, filePath)
	packet = sftpAppendUint32(packet, pflags)
	packet = sftpAppendUint32(packet, 0) // empty ATTRS flags
	if err := sftpWritePacket(stdin, packet); err != nil {
		return "", err
	}
	packetType, packetBody, err := sftpReadPacket(stdout)
	if err != nil {
		return "", err
	}
	if len(packetBody) < 4 || binary.BigEndian.Uint32(packetBody[:4]) != requestId {
		return "", errors.Errorf("unexpected request-id in response to SSH_FXP_OPEN")
	}
	if packetType == sshFxpStatus {
		return "", sftpStatusToError(packetBody[4:])
	}
	if packetType != sshFxpHandle {
		return "", errors.Errorf("expected SSH_FXP_HANDLE got packet type %d", packetType)
	}
	if len(packetBody) < 8 {
		return "", errors.Errorf("truncated SSH_FXP_HANDLE response")
	}
	handleLen := binary.BigEndian.Uint32(packetBody[4:8])
	if len(packetBody) < int(8+handleLen) {
		return "", errors.Errorf("truncated SSH_FXP_HANDLE response")
	}
	return string(packetBody[8 : 8+handleLen]), nil
}

// sftpExpectStatusOK reads one packet and expects SSH_FXP_STATUS with SSH_FX_OK
func sftpExpectStatusOK(stdout io.Reader, requestId uint32, operation string) error {
	packetType, packetBody, err := sftpReadPacket(stdout)
	if err != nil {
		return errors.Wrapf(err, "read %s response", operation)
	}
	if packetType != sshFxpStatus || len(packetBody) < 4 {
		return errors.Errorf("expected SSH_FXP_STATUS for %s got packet type %d", operation, packetType)
	}
	if binary.BigEndian.Uint32(packetBody[:4]) != requestId {
		return errors.Errorf("unexpected request-id in response to %s", operation)
	}
	if statusErr := sftpStatusToError(packetBody[4:]); statusErr != nil {
		return errors.Wrap(statusErr, operation)
	}
	return nil
}

func sftpStatusToError(statusBody []byte) error {
	if len(statusBody) < 4 {
		return errors.Errorf("truncated SSH_FXP_STATUS packet")
	}
	statusCode := binary.BigEndian.Uint32(statusBody[:4])
	if statusCode == sshFxOK {
		return nil
	}
	message := ""
	if len(statusBody) >= 8 {
		messageLen := binary.BigEndian.Uint32(statusBody[4:8])
		if len(statusBody) >= int(8+messageLen) {
			message = string(statusBody[8 : 8+messageLen])
		}
	}
	return errors.Errorf("SSH_FXP_STATUS code=%d %s", statusCode, message)
}

func sftpWritePacket(stdin io.Writer, packet []byte) error {
	packetLen := make([]byte, 4)
	binary.BigEndian.PutUint32(packetLen, uint32(len(packet)))
	if _, err := stdin.Write(packetLen); err != nil {
		return err
	}
	_, err := stdin.Write(packet)
	return err
}

func sftpReadPacket(stdout io.Reader) (byte, []byte, error) {
	packetLen := make([]byte, 4)
	if _, err := io.ReadFull(stdout, packetLen); err != nil {
		return 0, nil, err
	}
	packet := make([]byte, binary.BigEndian.Uint32(packetLen))
	if _, err := io.ReadFull(stdout, packet); err != nil {
		return 0, nil, err
	}
	if len(packet) < 1 {
		return 0, nil, errors.Errorf("empty SFTP packet")
	}
	return packet[0], packet[1:], nil
}

func sftpAppendUint32(packet []byte, v uint32) []byte {
	return binary.BigEndian.AppendUint32(packet, v)
}

func sftpAppendUint64(packet []byte, v uint64) []byte {
	return binary.BigEndian.AppendUint64(packet, v)
}

func sftpAppendString(packet []byte, s string) []byte {
	packet = binary.BigEndian.AppendUint32(packet, uint32(len(s)))
	return append(packet, s...)
}

func (sftp *SFTP) DeleteFileFromObjectDiskBackup(ctx context.Context, key string) error {
	sftp.Debug("[SFTP_DEBUG] DeleteFileFromObjectDiskBackup %s", key)
	filePath := path.Join(sftp.Config.ObjectDiskPath, key)

	fileStat, err := sftp.sftpClient.Stat(filePath)
	if err != nil {
		sftp.Debug("[SFTP_DEBUG] DeleteFileFromObjectDiskBackup::STAT %s return error %v", filePath, err)
		return errors.Wrap(err, "SFTP DeleteFileFromObjectDiskBackup Stat")
	}
	if fileStat.IsDir() {
		return sftp.DeleteDirectory(ctx, filePath)
	} else {
		return sftp.sftpClient.Remove(filePath)
	}
}

// DeleteKeys implements BatchDeleter interface for SFTP
// SFTP uses sequential deletion due to protocol limitations (single connection)
func (sftp *SFTP) DeleteKeys(ctx context.Context, keys []string) error {
	if len(keys) == 0 {
		return nil
	}

	sftp.Debug("[SFTP_DEBUG] DeleteKeys: deleting %d keys sequentially", len(keys))

	var failures []KeyError
	deletedCount := 0

	for _, key := range keys {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		filePath := path.Join(sftp.Config.Path, key)
		err := sftp.deleteKeyInternal(ctx, filePath)
		if err != nil {
			// Check if it's a "not found" error - that's OK
			if strings.Contains(err.Error(), "not exist") {
				deletedCount++
				continue
			}
			failures = append(failures, KeyError{Key: key, Err: err})
			continue
		}
		deletedCount++
	}

	if len(failures) > 0 {
		return &BatchDeleteError{
			Message:  fmt.Sprintf("SFTP batch delete: %d keys deleted, %d failed", deletedCount, len(failures)),
			Failures: failures,
		}
	}

	log.Debug().Msgf("SFTP batch delete: successfully deleted %d keys", deletedCount)
	return nil
}

// DeleteKeysFromObjectDiskBackup implements BatchDeleter interface for SFTP
func (sftp *SFTP) DeleteKeysFromObjectDiskBackup(ctx context.Context, keys []string) error {
	if len(keys) == 0 {
		return nil
	}

	sftp.Debug("[SFTP_DEBUG] DeleteKeysFromObjectDiskBackup: deleting %d keys sequentially", len(keys))

	var failures []KeyError
	deletedCount := 0

	for _, key := range keys {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		filePath := path.Join(sftp.Config.ObjectDiskPath, key)
		err := sftp.deleteKeyInternal(ctx, filePath)
		if err != nil {
			// Check if it's a "not found" error - that's OK
			if strings.Contains(err.Error(), "not exist") {
				deletedCount++
				continue
			}
			failures = append(failures, KeyError{Key: key, Err: err})
			continue
		}
		deletedCount++
	}

	if len(failures) > 0 {
		return &BatchDeleteError{
			Message:  fmt.Sprintf("SFTP batch delete: %d keys deleted, %d failed", deletedCount, len(failures)),
			Failures: failures,
		}
	}

	log.Debug().Msgf("SFTP batch delete: successfully deleted %d keys", deletedCount)
	return nil
}

// deleteKeyInternal deletes a single key (file or directory)
func (sftp *SFTP) deleteKeyInternal(ctx context.Context, filePath string) error {
	fileStat, err := sftp.sftpClient.Stat(filePath)
	if err != nil {
		return errors.Wrap(err, "SFTP deleteKeyInternal Stat")
	}
	if fileStat.IsDir() {
		return sftp.DeleteDirectory(ctx, filePath)
	}
	return sftp.sftpClient.Remove(filePath)
}

// Implement RemoteFile
type sftpFile struct {
	size         int64
	lastModified time.Time
	name         string
}

func (file *sftpFile) Size() int64 {
	return file.size
}

func (file *sftpFile) LastModified() time.Time {
	return file.lastModified
}

func (file *sftpFile) Name() string {
	return file.name
}
