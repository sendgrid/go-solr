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
	pollSleep        time.Duration
}

type stateChanged func([]byte, error)

type Zookeeper interface {
	Connect() error
	GetConnectionString() string
	Get(path string) ([]byte, error)
	Poll(path string, cb stateChanged)
	GetClusterState(root string) (*ClusterState, error)
	PollForClusterState(root string, cb func(*ClusterState, error))
}

func NewZookeeper(connectionString string) Zookeeper {
	return &zookeeper{connectionString: connectionString, pollSleep: time.Duration(1) * time.Second}
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

func (z *zookeeper) GetClusterState(root string) (*ClusterState, error) {
	node, err := z.Get(z.getClusterStatePath(root))
	if err != nil {
		return &ClusterState{}, err
	}
	cs, err := deserializeClusterState(node)
	if err != nil {
		return cs, err
	}
	children, _, err := z.zkConnection.Children(fmt.Sprintf("/%s/live_nodes", root))
	if err != nil {
		return cs, err
	}
	for i, node := range children {
		children[i] = strings.Replace(node, "_solr", "", -1)
	}
	cs.LiveNodes = children
	return cs, nil
}

func (z *zookeeper) PollForClusterState(root string, cb func(*ClusterState, error)) {
	for {
		clusterState, err := z.GetClusterState(root)
		if err != nil {
			cb(clusterState, nil)

		}
		cb(clusterState, nil)
		time.Sleep(z.pollSleep)
	}
}
func deserializeClusterState(node []byte) (*ClusterState, error) {
	var collections map[string]Collection
	decoder := json.NewDecoder(bytes.NewBuffer(node))
	if err := decoder.Decode(&collections); err != nil {
		return &ClusterState{}, err
	}
	return &ClusterState{Collections: collections}, nil
}

func (z *zookeeper) getClusterStatePath(root string) string {
	return fmt.Sprintf("/%s/collections/goseg/state.json", root)
}
