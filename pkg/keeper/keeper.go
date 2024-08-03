package keeper

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"github.com/antchfx/xmlquery"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

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
	Value string `json:"value"`
}

type Keeper struct {
	conn          *zk.Conn
	root          string
	doc           *xmlquery.Node
	xmlConfigFile string
}

// Connect - connect to any zookeeper server from /var/lib/clickhouse/preprocessed_configs/config.xml
func (k *Keeper) Connect(ctx context.Context, ch *clickhouse.ClickHouse) error {
	configFile, doc, err := ch.ParseXML(ctx, "config.xml")
	if err != nil {
		return fmt.Errorf("can't parse config.xml from %s, error: %v", configFile, err)
	}
	k.xmlConfigFile = configFile
	k.doc = doc
	zookeeperNode := xmlquery.FindOne(doc, "//zookeeper")
	if zookeeperNode == nil {
		return fmt.Errorf("no /zookeeper in %s", configFile)
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
		return fmt.Errorf("/zookeeper/node not exists in %s", configFile)
	}
	keeperHosts := make([]string, len(nodeList))
	for i, node := range nodeList {
		hostNode := node.SelectElement("host")
		if hostNode == nil {
			return fmt.Errorf("/zookeeper/node[%d]/host not exists in %s", i, configFile)
		}
		port := "2181"
		portNode := node.SelectElement("port")
		if portNode != nil {
			port = portNode.InnerText()
		}
		keeperHosts[i] = fmt.Sprintf("%s:%s", hostNode.InnerText(), port)
	}
	conn, _, err := zk.Connect(keeperHosts, sessionTimeout, zk.WithLogger(newKeeperLogger()))
	if err != nil {
		return err
	}
	if digestNode := zookeeperNode.SelectElement("digest"); digestNode != nil {
		if err = conn.AddAuth("digest", []byte(digestNode.InnerText())); err != nil {
			return fmt.Errorf("keeper digest authorization error: %v", err)
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
		return "", fmt.Errorf("can't find %s in %s", xPathQuery, k.xmlConfigFile)
	}
	return zookeeperPathNode.InnerText(), nil
}

func (k *Keeper) Dump(prefix, dumpFile string) (int, error) {
	f, err := os.Create(dumpFile)
	if err != nil {
		return 0, fmt.Errorf("can't create %s: %v", dumpFile, err)
	}
	defer func() {
		if err = f.Close(); err != nil {
			log.Warn().Msgf("can't close %s: %v", dumpFile, err)
		}
	}()
	if !strings.HasPrefix(prefix, "/") && k.root != "" {
		prefix = path.Join(k.root, prefix)
	}
	bytes, err := k.dumpNodeRecursive(prefix, "", f)
	if err != nil {
		return 0, fmt.Errorf("dumpNodeRecursive(%s) return error: %v", prefix, err)
	}
	return bytes, nil
}

func (k *Keeper) ChildCount(prefix, nodePath string) (int, error) {
	childrenNodes, _, err := k.conn.Children(path.Join(prefix, nodePath))
	return len(childrenNodes), err
}

func (k *Keeper) dumpNodeRecursive(prefix, nodePath string, f *os.File) (int, error) {
	value, _, err := k.conn.Get(path.Join(prefix, nodePath))
	if err != nil {
		return 0, err
	}
	bytes, err := k.writeJsonString(f, DumpNode{Path: nodePath, Value: string(value)})
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
		return fmt.Errorf("can't open %s: %v", dumpFile, err)
	}
	defer func() {
		if err = f.Close(); err != nil {
			log.Warn().Msgf("can't close %s: %v", dumpFile, err)
		}
	}()
	if !strings.HasPrefix(prefix, "/") && k.root != "" {
		prefix = path.Join(k.root, prefix)
	}
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		node := DumpNode{}
		if err = json.Unmarshal(scanner.Bytes(), &node); err != nil {
			return err
		}
		node.Path = path.Join(prefix, node.Path)
		version := int32(0)
		_, stat, err := k.conn.Get(node.Path)
		if err != nil {
			_, err = k.conn.Create(node.Path, []byte(node.Value), 0, zk.WorldACL(zk.PermAll))
			if err != nil {
				return fmt.Errorf("can't create znode %s, error: %v", node.Path, err)
			}
		} else {
			version = stat.Version
			_, err = k.conn.Set(node.Path, []byte(node.Value), version)
		}
	}

	if err = scanner.Err(); err != nil {
		return fmt.Errorf("can't scan %s, error: %s", dumpFile, err)
	}
	return nil
}

type WalkCallBack = func(node DumpNode) (bool, error)

func (k *Keeper) Walk(prefix, relativePath string, recursive bool, callback WalkCallBack) error {
	nodePath := path.Join(prefix, relativePath)
	value, stat, err := k.conn.Get(nodePath)
	log.Debug().Msgf("Walk->get(%s) = %v, err = %v", nodePath, string(value), err)
	if err != nil {
		return err
	}
	var isDone bool
	if isDone, err = callback(DumpNode{Path: nodePath, Value: string(value)}); err != nil {
		return err
	}
	if isDone {
		return nil
	}
	if recursive && stat.NumChildren > 0 {
		children, _, err := k.conn.Children(path.Join(prefix, relativePath))
		if err != nil {
			return err
		}
		for _, childPath := range children {
			if childErr := k.Walk(prefix, path.Join(relativePath, childPath), recursive, callback); childErr != nil {
				return childErr
			}
		}
	}
	return nil
}

func (k *Keeper) Delete(nodePath string) error {
	return k.conn.Delete(nodePath, -1)
}
func (k *Keeper) Close() {
	k.conn.Close()
}
