package solr

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/samuel/go-zookeeper/zk"
	"strings"
	"time"
)

type zookeeper struct {
	connectionString string
	zkConnection     *zk.Conn
	collection       string
	zkRoot           string
	pollSleep        time.Duration
}

type stateChanged func([]byte, error)

type Zookeeper interface {
	Connect() error
	GetConnectionString() string
	Get(path string) ([]byte, error)
	Poll(path string, cb stateChanged)
	GetClusterState() (map[string]Collection, error)
	GetClusterStateW() (map[string]Collection, <-chan zk.Event, error)
	GetLiveNodes() ([]string, error)
	GetLiveNodesW() ([]string, <-chan zk.Event, error)
}

func NewZookeeper(connectionString string, zkRoot string, collection string) Zookeeper {
	return &zookeeper{connectionString: connectionString, zkRoot: zkRoot, collection: collection, pollSleep: time.Duration(1) * time.Second}
}

func (z *zookeeper) Connect() error {
	servers := strings.Split(z.connectionString, ",")
	zkConnection, _, err := zk.Connect(servers, time.Second) //*10)
	if err != nil {
		return err
	}
	z.zkConnection = zkConnection
	return nil
}

func (z *zookeeper) Get(node string) ([]byte, error) {
	bytes, _, err := z.zkConnection.Get(node)
	if err != nil {
		return nil, err
	}
	val := bytes[:len(bytes)]
	return val, nil
}

func (z *zookeeper) GetConnectionString() string {
	return z.connectionString
}

func (z *zookeeper) Poll(path string, cb stateChanged) {
	for {
		bytes, err := z.Get(path)
		cb(bytes, err)
		time.Sleep(z.pollSleep)
	}
}

func (z *zookeeper) GetClusterStateW() (map[string]Collection, <-chan zk.Event, error) {
	node, _, events, err := z.zkConnection.GetW(z.getClusterStatePath(z.zkRoot, z.collection))
	if err != nil {
		return nil, events, err
	}
	cs, err := deserializeClusterState(node)
	if err != nil {
		return cs, events, err
	}
	return cs, events, nil
}

func (z *zookeeper) GetClusterState() (map[string]Collection, error) {
	node, _, err := z.zkConnection.Get(z.getClusterStatePath(z.zkRoot, z.collection))
	if err != nil {
		return nil, err
	}
	cs, err := deserializeClusterState(node)
	if err != nil {
		return cs, err
	}
	return cs, nil
}

func (z *zookeeper) GetLiveNodesW() ([]string, <-chan zk.Event, error) {
	children, _, events, err := z.zkConnection.ChildrenW(z.getLiveNodesPath(z.zkRoot))
	if err != nil {
		return children, events, err
	}
	for i, node := range children {
		children[i] = strings.Replace(node, "_solr", "", -1)
	}
	return children, events, nil
}

func (z *zookeeper) GetLiveNodes() ([]string, error) {
	children, _, err := z.zkConnection.Children(z.getLiveNodesPath(z.zkRoot))
	if err != nil {
		return children, err
	}
	for i, node := range children {
		children[i] = strings.Replace(node, "_solr", "", -1)
	}
	return children, nil
}

func (z *zookeeper) getLiveNodesPath(root string) string {
	return fmt.Sprintf("/%s/live_nodes", root)
}
func deserializeClusterState(node []byte) (map[string]Collection, error) {
	var collections map[string]Collection
	decoder := json.NewDecoder(bytes.NewBuffer(node))
	if err := decoder.Decode(&collections); err != nil {
		return nil, err
	}
	return collections, nil
}

func (z *zookeeper) getClusterStatePath(root string, collection string) string {
	return fmt.Sprintf("/%s/collections/%s/state.json", root, collection)
}
