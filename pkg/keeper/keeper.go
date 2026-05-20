package keeper

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/antchfx/xmlquery"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/Altinity/clickhouse-backup/v2/pkg/clickhouse"
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

type clientTLSConfig struct {
	caPath             string
	certPath           string
	keyPath            string
	skipChainVerify    bool
	skipHostnameVerify bool
	loadDefaultCAFile  bool
}

type keeperNode struct {
	address string
	secure  bool
}

type staticHostProvider struct {
	mu      sync.Mutex
	servers []string
	curr    int
	last    int
}

func (hp *staticHostProvider) Init(servers []string) error {
	hp.mu.Lock()
	defer hp.mu.Unlock()

	if len(servers) == 0 {
		return errors.New("no keeper hosts found")
	}
	hp.servers = append([]string{}, servers...)
	hp.curr = -1
	hp.last = -1
	return nil
}

func (hp *staticHostProvider) Len() int {
	hp.mu.Lock()
	defer hp.mu.Unlock()
	return len(hp.servers)
}

func (hp *staticHostProvider) Next() (server string, retryStart bool) {
	hp.mu.Lock()
	defer hp.mu.Unlock()

	hp.curr = (hp.curr + 1) % len(hp.servers)
	retryStart = hp.curr == hp.last
	if hp.last == -1 {
		hp.last = 0
	}
	return hp.servers[hp.curr], retryStart
}

func (hp *staticHostProvider) Connected() {
	hp.mu.Lock()
	defer hp.mu.Unlock()
	hp.last = hp.curr
}

// https://clickhouse.com/docs/operations/ssl-zookeeper - parse TLS configuration from ClickHouse's config.xml */openSSL/client/* section
func parseClientTLSConfig(doc *xmlquery.Node, configFile string) (clientTLSConfig, error) {
	tlsConfig := clientTLSConfig{
		loadDefaultCAFile:  true,
		skipHostnameVerify: true,
	}
	clientNode := xmlquery.FindOne(doc, "//openSSL/client")
	if clientNode == nil {
		log.Warn().Msgf("no /openSSL/client in %s, using default ClickHouse TLS client settings", configFile)
		return tlsConfig, nil
	}

	getText := func(selector string) string {
		if n := clientNode.SelectElement(selector); n != nil {
			return strings.TrimSpace(n.InnerText())
		}
		return ""
	}

	tlsConfig.certPath = getText("certificateFile")
	tlsConfig.keyPath = getText("privateKeyFile")
	if tlsConfig.certPath == "" && tlsConfig.keyPath != "" {
		tlsConfig.certPath = tlsConfig.keyPath
	}
	tlsConfig.caPath = getText("caConfig")
	if val := getText("loadDefaultCAFile"); val != "" {
		useDefault, err := strconv.ParseBool(val)
		if err != nil {
			return tlsConfig, errors.Wrapf(err, "invalid //openSSL/client/loadDefaultCAFile=%s in %s", val, configFile)
		}
		tlsConfig.loadDefaultCAFile = useDefault
	}
	if !tlsConfig.loadDefaultCAFile && tlsConfig.caPath == "" {
		log.Warn().Msgf("//openSSL/client/loadDefaultCAFile=false provided in %s but caConfig is empty; system CAs will NOT be loaded", configFile)
	}

	extendedVerification := false
	if val := getText("extendedVerification"); val != "" {
		useExtendedVerification, err := strconv.ParseBool(val)
		if err != nil {
			return tlsConfig, errors.Wrapf(err, "invalid //openSSL/client/extendedVerification=%s in %s", val, configFile)
		}
		extendedVerification = useExtendedVerification
	}

	verificationMode := strings.ToLower(getText("verificationMode"))
	if verificationMode == "" {
		verificationMode = "relaxed"
	}
	// Poco treats strict and once the same as relaxed for clients. Host/IP
	// checks are controlled by extendedVerification, not verificationMode.
	switch verificationMode {
	case "none":
		tlsConfig.skipChainVerify = true
		tlsConfig.skipHostnameVerify = true
	case "relaxed", "strict", "once":
		tlsConfig.skipHostnameVerify = !extendedVerification
	default:
		return tlsConfig, errors.Errorf("unknown //openSSL/client/verificationMode=%s in %s", verificationMode, configFile)
	}

	switch handler := getText("invalidCertificateHandler/name"); handler {
	case "", "RejectCertificateHandler":
	case "AcceptCertificateHandler":
		tlsConfig.skipChainVerify = true
		tlsConfig.skipHostnameVerify = true
	default:
		return tlsConfig, errors.Errorf("unknown //openSSL/client/invalidCertificateHandler/name=%s in %s", handler, configFile)
	}

	log.Debug().Msgf("parsed TLS config from %s: certPath=%s, keyPath=%s, caPath=%s, skipChainVerify=%v, skipHostnameVerify=%v, loadDefaultCAFile=%v", configFile, tlsConfig.certPath, tlsConfig.keyPath, tlsConfig.caPath, tlsConfig.skipChainVerify, tlsConfig.skipHostnameVerify, tlsConfig.loadDefaultCAFile)
	return tlsConfig, nil
}

func newKeeperTLSConfig(parsed clientTLSConfig) (*tls.Config, error) {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: parsed.skipChainVerify || parsed.skipHostnameVerify,
	}
	if parsed.certPath != "" || parsed.keyPath != "" {
		cert, err := tls.LoadX509KeyPair(parsed.certPath, parsed.keyPath)
		if err != nil {
			return nil, errors.Wrap(err, "tls.LoadX509KeyPair")
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	var caCertPool *x509.CertPool
	if parsed.loadDefaultCAFile && parsed.caPath != "" {
		systemCertPool, err := x509.SystemCertPool()
		if err != nil {
			return nil, errors.Wrap(err, "x509.SystemCertPool")
		}
		caCertPool = systemCertPool
	}
	if parsed.caPath != "" {
		caCert, err := os.ReadFile(parsed.caPath)
		if err != nil {
			return nil, errors.Wrapf(err, "read ca file %s", parsed.caPath)
		}
		if caCertPool == nil {
			caCertPool = x509.NewCertPool()
		}
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, errors.Errorf("AppendCertsFromPEM %s return false", parsed.caPath)
		}
		tlsConfig.RootCAs = caCertPool
	} else if !parsed.loadDefaultCAFile {
		tlsConfig.RootCAs = x509.NewCertPool()
	}

	if parsed.skipHostnameVerify && !parsed.skipChainVerify {
		tlsConfig.VerifyConnection = func(state tls.ConnectionState) error {
			if len(state.PeerCertificates) == 0 {
				return errors.New("no peer certificates")
			}
			if err := verifyKeeperCertificateChain(state.PeerCertificates, tlsConfig.RootCAs); err != nil {
				return err
			}
			return nil
		}
	}
	return tlsConfig, nil
}

func verifyKeeperCertificateChain(certificates []*x509.Certificate, roots *x509.CertPool) error {
	verifyOptions := x509.VerifyOptions{
		Roots:       roots,
		CurrentTime: time.Now(),
		KeyUsages:   []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	if len(certificates) > 1 {
		verifyOptions.Intermediates = x509.NewCertPool()
		for _, cert := range certificates[1:] {
			verifyOptions.Intermediates.AddCert(cert)
		}
	}
	if _, err := certificates[0].Verify(verifyOptions); err != nil {
		return errors.Wrap(err, "verify peer certificate")
	}
	return nil
}

func parseKeeperNodes(zookeeperNode *xmlquery.Node, configFile string) ([]keeperNode, error) {
	nodeList := zookeeperNode.SelectElements("node")
	if len(nodeList) == 0 {
		return nil, errors.WithStack(fmt.Errorf("/zookeeper/node not exists in %s", configFile))
	}

	nodes := make([]keeperNode, 0, len(nodeList))
	for i, node := range nodeList {
		hostNode := node.SelectElement("host")
		if hostNode == nil {
			return nil, errors.WithStack(fmt.Errorf("/zookeeper/node[%d]/host not exists in %s", i, configFile))
		}
		host := strings.TrimSpace(hostNode.InnerText())
		if host == "" {
			return nil, errors.WithStack(fmt.Errorf("/zookeeper/node[%d]/host is empty in %s", i, configFile))
		}

		port := "2181"
		if portNode := node.SelectElement("port"); portNode != nil {
			port = strings.TrimSpace(portNode.InnerText())
		}
		if port == "" {
			return nil, errors.WithStack(fmt.Errorf("/zookeeper/node[%d]/port is empty in %s", i, configFile))
		}

		secure := false
		if secureNode := node.SelectElement("secure"); secureNode != nil {
			secureText := strings.TrimSpace(secureNode.InnerText())
			if secureText != "" {
				secureValue, err := strconv.ParseBool(secureText)
				if err != nil {
					return nil, errors.Wrapf(err, "invalid /zookeeper/node[%d]/secure=%s in %s", i, secureText, configFile)
				}
				secure = secureValue
			}
		}

		nodes = append(nodes, keeperNode{
			address: net.JoinHostPort(trimIPv6Brackets(host), port),
			secure:  secure,
		})
	}
	return nodes, nil
}

func trimIPv6Brackets(host string) string {
	if strings.HasPrefix(host, "[") && strings.HasSuffix(host, "]") {
		return strings.TrimSuffix(strings.TrimPrefix(host, "["), "]")
	}
	return host
}

func keeperTLSConfigForNode(tlsConfig *tls.Config, node keeperNode) *tls.Config {
	nodeTLSConfig := tlsConfig.Clone()
	host, _, err := net.SplitHostPort(node.address)
	if err == nil {
		nodeTLSConfig.ServerName = trimIPv6Brackets(host)
	}
	return nodeTLSConfig
}

func newKeeperDialer(tlsConfig *tls.Config, nodesByAddress map[string]keeperNode) zk.Dialer {
	return func(network, address string, timeout time.Duration) (net.Conn, error) {
		node, ok := nodesByAddress[address]
		if !ok {
			return nil, errors.Errorf("keeper node %s not found in parsed config", address)
		}

		dialer := &net.Dialer{Timeout: timeout}
		if !node.secure {
			return dialer.Dial(network, address)
		}

		tlsConn, dialErr := tls.DialWithDialer(dialer, network, address, keeperTLSConfigForNode(tlsConfig, node))
		if dialErr != nil {
			log.Error().Msgf("TLS dial to %s failed: %v", address, dialErr)
			return nil, dialErr
		}
		return tlsConn, nil
	}
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
	nodes, err := parseKeeperNodes(zookeeperNode, configFile)
	if err != nil {
		return err
	}
	keeperHosts := make([]string, 0, len(nodes))
	nodesByAddress := make(map[string]keeperNode, len(nodes))
	isSecure := false
	for _, node := range nodes {
		keeperHosts = append(keeperHosts, node.address)
		nodesByAddress[node.address] = node
		isSecure = isSecure || node.secure
	}
	var conn *zk.Conn
	if isSecure {
		// Parse TLS config from ClickHouse's config.xml /openSSL/client/* section
		// according to https://clickhouse.com/docs/operations/ssl-zookeeper
		parsedTLSConfig, err := parseClientTLSConfig(doc, configFile)
		if err != nil {
			return errors.Wrap(err, "can't parse TLS config from config.xml /openSSL/client/* settings")
		}
		log.Info().Msgf("isSecure=%v, keeperHosts=%v, caPath=%v, certPath=%v, keyPath=%v, skipChainVerify=%v, skipHostnameVerify=%v, loadDefaultCAFile=%v, use TLS for keeper connection (from config.xml /openSSL/client/*)", isSecure, keeperHosts, parsedTLSConfig.caPath, parsedTLSConfig.certPath, parsedTLSConfig.keyPath, parsedTLSConfig.skipChainVerify, parsedTLSConfig.skipHostnameVerify, parsedTLSConfig.loadDefaultCAFile)
		tlsConfig, err := newKeeperTLSConfig(parsedTLSConfig)
		if err != nil {
			return errors.Wrap(err, "can't create TLS config from config.xml /openSSL/client/* settings")
		}
		conn, _, err = zk.Connect(
			keeperHosts,
			sessionTimeout,
			zk.WithLogger(newKeeperLogger()),
			zk.WithHostProvider(&staticHostProvider{}),
			zk.WithDialer(newKeeperDialer(tlsConfig, nodesByAddress)),
		)
		if err != nil {
			log.Error().Msgf("zk.Connect with TLS failed: %v", err)
			return errors.Wrap(err, "zk.Connect with TLS")
		}
	} else {
		log.Info().Msgf("isSecure=%v, keeperHosts=%v", isSecure, keeperHosts)
		conn, _, err = zk.Connect(keeperHosts, sessionTimeout, zk.WithLogger(newKeeperLogger()))
		if err != nil {
			return errors.Wrap(err, "zk.Connect")
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
	if err != nil {
		return 0, errors.WithMessage(err, "ChildCount conn.Children")
	}
	return len(childrenNodes), nil
}

func (k *Keeper) dumpNodeRecursive(prefix, nodePath string, f *os.File) (int, error) {
	value, _, err := k.conn.Get(path.Join(prefix, nodePath))
	if err != nil {
		return 0, errors.WithMessage(err, "dumpNodeRecursive conn.Get")
	}
	bytes, err := k.writeJsonString(f, DumpNode{Path: strings.TrimPrefix(nodePath, k.root), Value: value})
	if err != nil {
		return 0, errors.WithMessage(err, "dumpNodeRecursive writeJsonString")
	}
	children, _, err := k.conn.Children(path.Join(prefix, nodePath))
	if err != nil {
		return 0, errors.WithMessage(err, "dumpNodeRecursive conn.Children")
	}
	for _, childPath := range children {
		if childBytes, err := k.dumpNodeRecursive(prefix, path.Join(nodePath, childPath), f); err != nil {
			return 0, errors.WithMessage(err, "dumpNodeRecursive child")
		} else {
			bytes += childBytes
		}
	}
	return bytes, nil
}

func (k *Keeper) writeJsonString(f *os.File, node DumpNode) (int, error) {
	jsonLine, err := json.Marshal(node)
	if err != nil {
		return 0, errors.WithMessage(err, "writeJsonString json.Marshal")
	}
	bytes, err := f.Write(jsonLine)
	if err != nil {
		return bytes, errors.WithMessage(err, "writeJsonString file.Write jsonLine")
	}
	lnBytes, err := f.Write([]byte("\n"))
	if err != nil {
		return bytes + lnBytes, errors.WithMessage(err, "writeJsonString file.Write newline")
	}
	return bytes + lnBytes, nil
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
