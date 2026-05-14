package keeper

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/antchfx/xmlquery"
)

func TestParseClientTLSConfigVerificationModes(t *testing.T) {
	tests := []struct {
		name                      string
		xml                       string
		wantSkipChainVerify       bool
		wantSkipHostnameVerify    bool
		wantLoadDefaultCAFile     bool
		wantCertPath, wantKeyPath string
		wantCAPath                string
	}{
		{
			name:                   "without openSSL client uses ClickHouse defaults",
			xml:                    "<clickhouse/>",
			wantSkipHostnameVerify: true,
			wantLoadDefaultCAFile:  true,
		},
		{
			name: "official zookeeper TLS example verifies certificate chain without hostname",
			xml: `<clickhouse><openSSL><client>` +
				`<certificateFile>/client.crt</certificateFile>` +
				`<privateKeyFile>/client.key</privateKeyFile>` +
				`<loadDefaultCAFile>true</loadDefaultCAFile>` +
				`<cacheSessions>true</cacheSessions>` +
				`<disableProtocols>sslv2,sslv3</disableProtocols>` +
				`<preferServerCiphers>true</preferServerCiphers>` +
				`<invalidCertificateHandler><name>RejectCertificateHandler</name></invalidCertificateHandler>` +
				`</client></openSSL></clickhouse>`,
			wantSkipHostnameVerify: true,
			wantLoadDefaultCAFile:  true,
			wantCertPath:           "/client.crt",
			wantKeyPath:            "/client.key",
		},
		{
			name:                   "relaxed skips hostname verification but keeps certificate verification",
			xml:                    clientTLSXML("<verificationMode>relaxed</verificationMode>"),
			wantSkipHostnameVerify: true,
			wantLoadDefaultCAFile:  true,
			wantCertPath:           "/cert.pem",
			wantKeyPath:            "/key.pem",
			wantCAPath:             "/ca.pem",
		},
		{
			name: "relaxed with extended verification checks peer name",
			xml: clientTLSXML(
				"<verificationMode>relaxed</verificationMode>",
				"<extendedVerification>true</extendedVerification>",
			),
			wantLoadDefaultCAFile: true,
			wantCertPath:          "/cert.pem",
			wantKeyPath:           "/key.pem",
			wantCAPath:            "/ca.pem",
		},
		{
			name:                   "strict follows extendedVerification for peer name checks",
			xml:                    clientTLSXML("<verificationMode>strict</verificationMode>"),
			wantSkipHostnameVerify: true,
			wantLoadDefaultCAFile:  true,
			wantCertPath:           "/cert.pem",
			wantKeyPath:            "/key.pem",
			wantCAPath:             "/ca.pem",
		},
		{
			name: "strict with extended verification checks peer name",
			xml: clientTLSXML(
				"<verificationMode>strict</verificationMode>",
				"<extendedVerification>true</extendedVerification>",
			),
			wantLoadDefaultCAFile: true,
			wantCertPath:          "/cert.pem",
			wantKeyPath:           "/key.pem",
			wantCAPath:            "/ca.pem",
		},
		{
			name: "verificationMode none skips all verification",
			xml: clientTLSXML(
				"<verificationMode>none</verificationMode>",
				"<extendedVerification>true</extendedVerification>",
			),
			wantSkipChainVerify:    true,
			wantSkipHostnameVerify: true,
			wantLoadDefaultCAFile:  true,
			wantCertPath:           "/cert.pem",
			wantKeyPath:            "/key.pem",
			wantCAPath:             "/ca.pem",
		},
		{
			name: "accept certificate handler skips all verification",
			xml: clientTLSXML(
				"<verificationMode>strict</verificationMode>",
				"<extendedVerification>true</extendedVerification>",
				"<invalidCertificateHandler><name>AcceptCertificateHandler</name></invalidCertificateHandler>",
			),
			wantSkipChainVerify:    true,
			wantSkipHostnameVerify: true,
			wantLoadDefaultCAFile:  true,
			wantCertPath:           "/cert.pem",
			wantKeyPath:            "/key.pem",
			wantCAPath:             "/ca.pem",
		},
		{
			name:                   "loadDefaultCAFile false is preserved",
			xml:                    clientTLSXML("<loadDefaultCAFile>false</loadDefaultCAFile>"),
			wantSkipHostnameVerify: true,
			wantLoadDefaultCAFile:  false,
			wantCertPath:           "/cert.pem",
			wantKeyPath:            "/key.pem",
			wantCAPath:             "/ca.pem",
		},
		{
			name:                   "certificate file defaults to private key file",
			xml:                    `<clickhouse><openSSL><client><privateKeyFile>/combined.pem</privateKeyFile></client></openSSL></clickhouse>`,
			wantSkipHostnameVerify: true,
			wantLoadDefaultCAFile:  true,
			wantCertPath:           "/combined.pem",
			wantKeyPath:            "/combined.pem",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseClientTLSConfig(parseXML(t, tc.xml), "config.xml")
			if err != nil {
				t.Fatalf("parseClientTLSConfig() error = %v", err)
			}

			if got.skipChainVerify != tc.wantSkipChainVerify {
				t.Fatalf("skipChainVerify = %v, want %v", got.skipChainVerify, tc.wantSkipChainVerify)
			}
			if got.skipHostnameVerify != tc.wantSkipHostnameVerify {
				t.Fatalf("skipHostnameVerify = %v, want %v", got.skipHostnameVerify, tc.wantSkipHostnameVerify)
			}
			if got.loadDefaultCAFile != tc.wantLoadDefaultCAFile {
				t.Fatalf("loadDefaultCAFile = %v, want %v", got.loadDefaultCAFile, tc.wantLoadDefaultCAFile)
			}
			if got.certPath != tc.wantCertPath {
				t.Fatalf("certPath = %q, want %q", got.certPath, tc.wantCertPath)
			}
			if got.keyPath != tc.wantKeyPath {
				t.Fatalf("keyPath = %q, want %q", got.keyPath, tc.wantKeyPath)
			}
			if got.caPath != tc.wantCAPath {
				t.Fatalf("caPath = %q, want %q", got.caPath, tc.wantCAPath)
			}
		})
	}
}

func TestParseClientTLSConfigRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name string
		xml  string
	}{
		{
			name: "invalid verification mode",
			xml:  clientTLSXML("<verificationMode>bad</verificationMode>"),
		},
		{
			name: "invalid loadDefaultCAFile bool",
			xml:  clientTLSXML("<loadDefaultCAFile>bad</loadDefaultCAFile>"),
		},
		{
			name: "invalid extendedVerification bool",
			xml:  clientTLSXML("<extendedVerification>bad</extendedVerification>"),
		},
		{
			name: "unknown invalid certificate handler",
			xml:  clientTLSXML("<invalidCertificateHandler><name>UnknownHandler</name></invalidCertificateHandler>"),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := parseClientTLSConfig(parseXML(t, tc.xml), "config.xml"); err == nil {
				t.Fatal("parseClientTLSConfig() error = nil, want error")
			}
		})
	}
}

func TestParseKeeperNodes(t *testing.T) {
	doc := parseXML(t, `<clickhouse><zookeeper>`+
		`<node><host> zookeeper </host><port> 2281 </port><secure>1</secure></node>`+
		`<node><host>plain</host></node>`+
		`</zookeeper></clickhouse>`)

	nodes, err := parseKeeperNodes(xmlquery.FindOne(doc, "//zookeeper"), "config.xml")
	if err != nil {
		t.Fatalf("parseKeeperNodes() error = %v", err)
	}
	if len(nodes) != 2 {
		t.Fatalf("len(nodes) = %d, want 2", len(nodes))
	}
	if nodes[0].address != "zookeeper:2281" || !nodes[0].secure {
		t.Fatalf("nodes[0] = %+v, want secure zookeeper:2281", nodes[0])
	}
	if nodes[1].address != "plain:2181" || nodes[1].secure {
		t.Fatalf("nodes[1] = %+v, want plain plain:2181", nodes[1])
	}
}

func TestStaticHostProviderPreservesOriginalHost(t *testing.T) {
	hostProvider := &staticHostProvider{}
	if err := hostProvider.Init([]string{"zookeeper:2281"}); err != nil {
		t.Fatalf("Init() error = %v", err)
	}
	got, _ := hostProvider.Next()
	if got != "zookeeper:2281" {
		t.Fatalf("Next() = %q, want original host", got)
	}
}

func TestKeeperTLSConfigForNodeUsesConfiguredHostAsServerName(t *testing.T) {
	got := keeperTLSConfigForNode(&tls.Config{}, keeperNode{
		address: "zookeeper:2281",
		secure:  true,
	})
	if got.ServerName != "zookeeper" {
		t.Fatalf("ServerName = %q, want zookeeper", got.ServerName)
	}
}

func TestNewKeeperTLSConfigHandshakeVerification(t *testing.T) {
	tests := []struct {
		name               string
		skipHostnameVerify bool
		useUnknownCA       bool
		wantErr            bool
	}{
		{
			name:               "skip hostname verify still verifies certificate chain",
			skipHostnameVerify: true,
		},
		{
			name:    "rejects hostname mismatch",
			wantErr: true,
		},
		{
			name:               "skip hostname verify rejects unknown CA",
			skipHostnameVerify: true,
			useUnknownCA:       true,
			wantErr:            true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			caCertPEM, _, serverCertPEM, serverKeyPEM := generateTestCertificates(t, "keeper.example")
			if tc.useUnknownCA {
				caCertPEM, _, _, _ = generateTestCertificates(t, "other-ca.example")
			}
			dir := t.TempDir()
			caPath := filepath.Join(dir, "ca.crt")
			if err := os.WriteFile(caPath, caCertPEM, 0o644); err != nil {
				t.Fatalf("os.WriteFile(ca.crt) error = %v", err)
			}

			serverTLSConfig := serverTLSConfigFromPEM(t, serverCertPEM, serverKeyPEM)
			clientTLSConfig, err := newKeeperTLSConfig(clientTLSConfig{
				caPath:             caPath,
				skipHostnameVerify: tc.skipHostnameVerify,
			})
			if err != nil {
				t.Fatalf("newKeeperTLSConfig() error = %v", err)
			}
			clientTLSConfig.ServerName = "wrong.example"

			err = runTLSHandshake(serverTLSConfig, clientTLSConfig)
			if tc.wantErr && err == nil {
				t.Fatal("TLS handshake error = nil, want error")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("TLS handshake error = %v", err)
			}
		})
	}
}

func TestNewKeeperTLSConfigMergesCAPathWithSystemCAs(t *testing.T) {
	systemCertPool, err := x509.SystemCertPool()
	if err != nil {
		t.Skipf("x509.SystemCertPool() error = %v", err)
	}
	if systemCertPool == nil || len(systemCertPool.Subjects()) == 0 {
		t.Skip("system cert pool is empty")
	}

	caCertPEM, _, serverCertPEM, serverKeyPEM := generateTestCertificates(t, "keeper.example")
	dir := t.TempDir()
	caPath := filepath.Join(dir, "ca.crt")
	if err := os.WriteFile(caPath, caCertPEM, 0o644); err != nil {
		t.Fatalf("os.WriteFile(ca.crt) error = %v", err)
	}

	serverTLSConfig := serverTLSConfigFromPEM(t, serverCertPEM, serverKeyPEM)
	clientTLSConfig, err := newKeeperTLSConfig(clientTLSConfig{
		caPath:             caPath,
		skipHostnameVerify: true,
		loadDefaultCAFile:  true,
	})
	if err != nil {
		t.Fatalf("newKeeperTLSConfig() error = %v", err)
	}
	if clientTLSConfig.RootCAs == nil {
		t.Fatal("RootCAs = nil, want merged system and configured CAs")
	}
	if len(clientTLSConfig.RootCAs.Subjects()) <= len(systemCertPool.Subjects()) {
		t.Fatalf("RootCAs subjects = %d, want more than system subjects %d", len(clientTLSConfig.RootCAs.Subjects()), len(systemCertPool.Subjects()))
	}
	clientTLSConfig.ServerName = "wrong.example"

	if err := runTLSHandshake(serverTLSConfig, clientTLSConfig); err != nil {
		t.Fatalf("TLS handshake with merged CA pool error = %v", err)
	}
}

func clientTLSXML(extra ...string) string {
	return `<clickhouse><openSSL><client>` +
		`<caConfig>/ca.pem</caConfig>` +
		`<certificateFile>/cert.pem</certificateFile>` +
		`<privateKeyFile>/key.pem</privateKeyFile>` +
		strings.Join(extra, "") +
		`</client></openSSL></clickhouse>`
}

func parseXML(t *testing.T, xml string) *xmlquery.Node {
	t.Helper()
	doc, err := xmlquery.Parse(strings.NewReader(xml))
	if err != nil {
		t.Fatalf("xmlquery.Parse() error = %v", err)
	}
	return doc
}

func runTLSHandshake(serverConfig, clientConfig *tls.Config) error {
	listener, err := tls.Listen("tcp", "127.0.0.1:0", serverConfig)
	if err != nil {
		return err
	}
	defer listener.Close()

	serverErr := make(chan error, 1)
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			serverErr <- acceptErr
			return
		}
		defer conn.Close()
		serverErr <- conn.(*tls.Conn).Handshake()
	}()

	conn, err := tls.DialWithDialer(&net.Dialer{Timeout: 5 * time.Second}, "tcp", listener.Addr().String(), clientConfig)
	if err == nil {
		_ = conn.Close()
	}
	if serverHandshakeErr := <-serverErr; err == nil && serverHandshakeErr != nil {
		return serverHandshakeErr
	}
	return err
}

func serverTLSConfigFromPEM(t *testing.T, certPEM, keyPEM []byte) *tls.Config {
	t.Helper()
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatalf("tls.X509KeyPair() error = %v", err)
	}
	return &tls.Config{Certificates: []tls.Certificate{cert}}
}

func generateTestCertificates(t *testing.T, serverName string) (caCertPEM, caKeyPEM, serverCertPEM, serverKeyPEM []byte) {
	t.Helper()
	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa.GenerateKey(ca) error = %v", err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caCertDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("x509.CreateCertificate(ca) error = %v", err)
	}

	serverKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa.GenerateKey(server) error = %v", err)
	}
	serverTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: serverName},
		DNSNames:     []string{serverName},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	serverCertDER, err := x509.CreateCertificate(rand.Reader, serverTemplate, caTemplate, &serverKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("x509.CreateCertificate(server) error = %v", err)
	}

	caCertPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caCertDER})
	caKeyPEM = pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(caKey)})
	serverCertPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverCertDER})
	serverKeyPEM = pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(serverKey)})
	return caCertPEM, caKeyPEM, serverCertPEM, serverKeyPEM
}
