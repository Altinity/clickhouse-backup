package keeper

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/antchfx/xmlquery"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/Altinity/clickhouse-backup/v2/pkg/clickhouse"
	"github.com/Altinity/clickhouse-backup/v2/pkg/utils"
	"github.com/go-zookeeper/zk"
)

type LogKeeperToApexLogAdapter struct {
	log zerolog.Logger
}

func newKeeperLogger() LogKeeperToApexLogAdapter {
	return LogKeeperToApexLogAdapter{
		log: log.Logger,
	}
}

func (KeeperLogToApexLogAdapter LogKeeperToApexLogAdapter) Printf(msg string, args ...interface{}) {
	msg = fmt.Sprintf("[keeper] %s", msg)
	if len(args) > 0 {
		KeeperLogToApexLogAdapter.log.Debug().Msgf(msg, args...)
	} else {
		KeeperLogToApexLogAdapter.log.Debug().Msg(msg)
	}
}

type DumpNode struct {
	Path  string `json:"path"`
	Value []byte `json:"value"` // json encodes/decodes as base64 automatically
}

// old format used during restore
type DumpNodeString struct {
	Path  string `json:"path"`
	Value string `json:"value"`
}

type Keeper struct {
	conn          *zk.Conn
	root          string
	doc           *xmlquery.Node
	xmlConfigFile string
}

// https://clickhouse.com/docs/operations/ssl-zookeeper - parse TLS configuration from ClickHouse's config.xml */openSSL/client/* section
func parseClientTLSConfig(doc *xmlquery.Node, configFile string) (caPath, certPath, keyPath string, skipVerify, loadDefaultCAFile bool) {
	clientNode := xmlquery.FindOne(doc, "//openSSL/client")
	if clientNode == nil {
		log.Warn().Msgf("no /openSSL/client in %s, using empty TLS config", configFile)
		return "", "", "", true, true
	}

	getText := func(selector string) string {
		if n := clientNode.SelectElement(selector); n != nil {
			return n.InnerText()
		}
		return ""
	}

	certPath = getText("certificateFile")
	keyPath = getText("privateKeyFile")
	caPath = getText("caConfig")
	loadDefaultCAFile = true
	if val := getText("loadDefaultCAFile"); val != "" {
		if useDefault, err := strconv.ParseBool(val); err == nil {
			loadDefaultCAFile = useDefault
		}
	}
	if !loadDefaultCAFile && caPath == "" {
		log.Warn().Msgf("//openSSL/client/loadDefaultCAFile=false provided in %s but caConfig is empty; system CAs will NOT be loaded", configFile)
	}

	if mode := getText("verificationMode"); mode == "none" {
		skipVerify = true
	}
	// invalidCertificateHandler is alternative way to disable verification
	if handler := getText("invalidCertificateHandler/name"); handler == "AcceptCertificateHandler" {
		skipVerify = true
	}

	log.Debug().Msgf("parsed TLS config from %s: certPath=%s, keyPath=%s, caPath=%s, skipVerify=%v, loadDefaultCAFile=%v", configFile, certPath, keyPath, caPath, skipVerify, loadDefaultCAFile)
	return caPath, certPath, keyPath, skipVerify, loadDefaultCAFile
}

// Connect - connect to any zookeeper server from /var/lib/clickhouse/preprocessed_configs/config.xml
func (k *Keeper) Connect(ctx context.Context, ch *clickhouse.ClickHouse) error {
	configFile, doc, err := ch.ParseXML(ctx, "config.xml")
	if err != nil {
		return errors.Wrapf(err, "can't parse config.xml from %s, error", configFile)
	}
	k.xmlConfigFile = configFile
	k.doc = doc
	zookeeperNode := xmlquery.FindOne(doc, "//zookeeper")
	if zookeeperNode == nil {
		return errors.WithStack(fmt.Errorf("no /zookeeper in %s", configFile))
	}
	sessionTimeout := 15 * time.Second
	if sessionTimeoutMsNode := zookeeperNode.SelectElement("session_timeout_ms"); sessionTimeoutMsNode != nil {
		if sessionTimeoutMs, err := strconv.ParseInt(sessionTimeoutMsNode.InnerText(), 10, 64); err == nil {
			sessionTimeout = time.Duration(sessionTimeoutMs) * time.Millisecond
		} else {
			log.Warn().Msgf("can't parse /zookeeper/session_timeout_ms in %s, value: %v, error: %v ", configFile, sessionTimeoutMsNode.InnerText(), err)
		}
	}
	nodeList := zookeeperNode.SelectElements("node")
	if len(nodeList) == 0 {
		return errors.WithStack(fmt.Errorf("/zookeeper/node not exists in %s", configFile))
	}
	keeperHosts := make([]string, len(nodeList))
	isSecure := false
	for i, node := range nodeList {
		hostNode := node.SelectElement("host")
		if hostNode == nil {
			return errors.WithStack(fmt.Errorf("/zookeeper/node[%d]/host not exists in %s", i, configFile))
		}
		port := "2181"
		portNode := node.SelectElement("port")
		if portNode != nil {
			port = portNode.InnerText()
		}
		secureNode := node.SelectElement("secure")
		if secureNode != nil && (secureNode.InnerText() == "1" || secureNode.InnerText() == "true") {
			isSecure = true
		}
		keeperHosts[i] = fmt.Sprintf("%s:%s", hostNode.InnerText(), port)
	}
	var conn *zk.Conn
	if isSecure {
		// Parse TLS config from ClickHouse's config.xml /openSSL/client/* section
		// according to https://clickhouse.com/docs/operations/ssl-zookeeper
		caPath, certPath, keyPath, skipVerify, loadDefaultCAFile := parseClientTLSConfig(doc, configFile)
		log.Info().Msgf("isSecure=%v, keeperHosts=%v, caPath=%v, certPath=%v, keyPath=%v, skipVerify=%v, loadDefaultCAFile=%v, use TLS for keeper connection (from config.xml /openSSL/client/*)", isSecure, keeperHosts, caPath, certPath, keyPath, skipVerify, loadDefaultCAFile)
		tlsConfig, err := utils.NewTLSConfig(caPath, certPath, keyPath, skipVerify, loadDefaultCAFile)
		if err != nil {
			return errors.Wrap(err, "can't create TLS config from config.xml /openSSL/client/* settings")
		}
		conn, _, err = zk.Connect(keeperHosts, sessionTimeout, zk.WithLogger(newKeeperLogger()), zk.WithDialer(func(network, address string, timeout time.Duration) (net.Conn, error) {
			tlsConn, dialErr := tls.DialWithDialer(&net.Dialer{Timeout: timeout}, network, address, tlsConfig)
			if dialErr != nil {
				log.Error().Msgf("TLS dial to %s failed: %v", address, dialErr)
				return nil, dialErr
			}
			return tlsConn, nil
		}))
		if err != nil {
			log.Error().Msgf("zk.Connect with TLS failed: %v", err)
			return err
		}
	} else {
		log.Info().Msgf("isSecure=%v, keeperHosts=%v", isSecure, keeperHosts)
		conn, _, err = zk.Connect(keeperHosts, sessionTimeout, zk.WithLogger(newKeeperLogger()))
		if err != nil {
			return err
		}
	}
	if digestNode := zookeeperNode.SelectElement("digest"); digestNode != nil {
		if err = conn.AddAuth("digest", []byte(digestNode.InnerText())); err != nil {
			return errors.Wrap(err, "keeper digest authorization error")
		}
	}
	k.conn = conn
	if keeperRootPathNode := zookeeperNode.SelectElement("root"); keeperRootPathNode != nil {
		k.root = keeperRootPathNode.InnerText()
	}
	return nil
}

func (k *Keeper) GetReplicatedAccessPath(userDirectory string) (string, error) {
	xPathQuery := fmt.Sprintf("//user_directories/%s/zookeeper_path", userDirectory)
	zookeeperPathNode := xmlquery.FindOne(k.doc, xPathQuery)
	if zookeeperPathNode == nil {
		return "", errors.WithStack(fmt.Errorf("can't find %s in %s", xPathQuery, k.xmlConfigFile))
	}
	zookeeperPath := zookeeperPathNode.InnerText()
	if zookeeperPath != "/" {
		zookeeperPath = strings.TrimSuffix(zookeeperPathNode.InnerText(), "/")
	}
	log.Debug().Str("userDirectory", userDirectory).Str("zookeeper_path", zookeeperPath).Msg("k->GetReplicatedAccessPath")
	return zookeeperPath, nil
}

func (k *Keeper) Dump(prefix, dumpFile string) (int, error) {
	f, err := os.Create(dumpFile)
	if err != nil {
		return 0, errors.Wrapf(err, "can't create %s", dumpFile)
	}
	defer func() {
		if err = f.Close(); err != nil {
			log.Warn().Msgf("can't close %s: %v", dumpFile, err)
		}
	}()
	if k.root != "" && !strings.HasPrefix(prefix, k.root) {
		prefix = path.Join(k.root, prefix)
	}
	bytes, err := k.dumpNodeRecursive(prefix, "", f)
	if err != nil {
		return 0, errors.Wrapf(err, "dumpNodeRecursive(%s) return error", prefix)
	}
	return bytes, nil
}

func (k *Keeper) ChildCount(prefix, nodePath string) (int, error) {
	if k.root != "" && !strings.HasPrefix(prefix, k.root) {
		prefix = path.Join(k.root, prefix)
	}
	log.Debug().Str("prefix", prefix).Str("nodePath", nodePath).Msg("k->ChildCount")
	childrenNodes, _, err := k.conn.Children(path.Join(prefix, nodePath))
	return len(childrenNodes), err
}

func (k *Keeper) dumpNodeRecursive(prefix, nodePath string, f *os.File) (int, error) {
	value, _, err := k.conn.Get(path.Join(prefix, nodePath))
	if err != nil {
		return 0, err
	}
	bytes, err := k.writeJsonString(f, DumpNode{Path: strings.TrimPrefix(nodePath, k.root), Value: value})
	if err != nil {
		return 0, err
	}
	children, _, err := k.conn.Children(path.Join(prefix, nodePath))
	if err != nil {
		return 0, err
	}
	for _, childPath := range children {
		if childBytes, err := k.dumpNodeRecursive(prefix, path.Join(nodePath, childPath), f); err != nil {
			return 0, err
		} else {
			bytes += childBytes
		}
	}
	return bytes, nil
}

func (k *Keeper) writeJsonString(f *os.File, node DumpNode) (int, error) {
	jsonLine, err := json.Marshal(node)
	if err != nil {
		return 0, err
	}
	bytes, err := f.Write(jsonLine)
	if err != nil {
		return bytes, err
	}
	lnBytes, err := f.Write([]byte("\n"))
	return bytes + lnBytes, err
}

func (k *Keeper) Restore(dumpFile, prefix string) error {
	f, err := os.Open(dumpFile)
	if err != nil {
		return errors.Wrapf(err, "can't open %s", dumpFile)
	}
	defer func() {
		if err = f.Close(); err != nil {
			log.Warn().Msgf("can't close %s: %v", dumpFile, err)
		}
	}()
	if k.root != "" && !strings.HasPrefix(prefix, k.root) {
		prefix = path.Join(k.root, prefix)
	}
	reader := bufio.NewReader(f)
	for {
		line, readErr := reader.ReadString('\n')
		if readErr != nil && readErr != io.EOF {
			return errors.Wrapf(readErr, "can't read %s", dumpFile)
		}
		line = strings.TrimSuffix(line, "\n")
		if line == "" {
			if readErr == io.EOF {
				break
			}
			continue
		}
		node := DumpNode{}
		binaryData := []byte(line)
		if binaryUnmarshalErr := json.Unmarshal(binaryData, &node); binaryUnmarshalErr != nil {
			//convert from old format
			nodeString := DumpNodeString{}
			if stringUnmarshalErr := json.Unmarshal(binaryData, &nodeString); stringUnmarshalErr != nil {
				return errors.WithStack(fmt.Errorf("k.Restore can't read data binaryErr=%v, stringErr=%v", binaryUnmarshalErr, stringUnmarshalErr))
			}
		}
		node.Path = path.Join(prefix, node.Path)
		version := int32(0)
		_, stat, keeperErr := k.conn.Get(node.Path)
		if keeperErr != nil {
			_, keeperErr = k.conn.Create(node.Path, node.Value, 0, zk.WorldACL(zk.PermAll))
			if keeperErr != nil {
				return errors.Wrapf(keeperErr, "can't create znode %s, error", node.Path)
			}
		} else {
			version = stat.Version
			_, keeperErr = k.conn.Set(node.Path, node.Value, version)
			if keeperErr != nil {
				return errors.Wrapf(keeperErr, "can't set znode %s, error", node.Path)
			}
		}
		if readErr == io.EOF {
			break
		}
	}
	return nil
}

type WalkCallBack = func(node DumpNode) (bool, error)

func (k *Keeper) Walk(prefix, relativePath string, recursive bool, callback WalkCallBack) error {
	nodePath := path.Join(prefix, relativePath)
	value, stat, err := k.conn.Get(nodePath)
	log.Debug().Msgf("k.Walk->get(%s) = %v, err = %v", nodePath, string(value), err)
	if err != nil {
		return errors.WithStack(fmt.Errorf("k.Walk->get(%s) = %v, err = %v", nodePath, string(value), err))
	}
	var isDone bool
	callbackNode := DumpNode{Path: nodePath, Value: value}
	if isDone, err = callback(callbackNode); err != nil {
		return errors.Wrapf(err, "k.Walk->callback(%v) error", callbackNode)
	}
	if isDone {
		return nil
	}
	if recursive && stat.NumChildren > 0 {
		children, _, err := k.conn.Children(path.Join(prefix, relativePath))
		log.Debug().Msgf("k.Walk->Children(%s) = %v, err = %v", path.Join(prefix, relativePath), children, err)
		if err != nil {
			return errors.WithStack(fmt.Errorf("k.Walk->Children(%s) = %v, err = %v", path.Join(prefix, relativePath), children, err))
		}
		for _, childPath := range children {
			if childErr := k.Walk(prefix, path.Join(relativePath, childPath), recursive, callback); childErr != nil {
				return errors.WithStack(childErr)
			}
		}
	}
	return nil
}

func (k *Keeper) Delete(nodePath string) error {
	return errors.WithStack(k.conn.Delete(nodePath, -1))
}
func (k *Keeper) Close() {
	k.conn.Close()
}
