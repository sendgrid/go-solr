package solr

import (
	"fmt"
	"math/rand"
	"sync"
)

func init() {

}

type SolrZK interface {
	GetZookeepers() string
	GetNextReadHost() string
	GetClusterState() (ClusterState, error)
	GetClusterProps() (ClusterProps, error)
	GetLeader(id string) (string, error)
	Listen() error
	Listening() bool
	FindLiveReplicaUrls(key string) ([]string, error)
	FindReplicaForRoute(key string) (string, error)
}

type solrZkInstance struct {
	zookeeper         Zookeeper
	collection        string
	host              string
	baseUrl           string
	currentNode       int
	clusterState      ClusterState
	clusterStateMutex *sync.Mutex
	currentNodeMutex  *sync.Mutex
	listening         bool
}

func NewSolrZK(zookeepers string, zkRoot string, collectionName string) SolrZK {
	instance := solrZkInstance{zookeeper: NewZookeeper(zookeepers, zkRoot, collectionName), collection: collectionName}

	instance.clusterStateMutex = &sync.Mutex{}
	instance.currentNodeMutex = &sync.Mutex{}
	instance.listening = false
	return &instance
}

func (s *solrZkInstance) FindReplicaForRoute(shardKey string) (string, error) {
	replicas, err := s.FindLiveReplicaUrls(shardKey)
	if err != nil {
		return "", err
	}
	//pick a random node
	node := replicas[rand.Intn(len(replicas))]
	return node, nil
}

func (s *solrZkInstance) FindLiveReplicaUrls(key string) ([]string, error) {
	collection, ok := s.clusterState.Collections[s.collection]
	if !ok {
		return nil, fmt.Errorf("Collection %s does not exist ", s.collection)
	}
	return findLiveReplicaUrls(key, &collection)
}

func (s *solrZkInstance) GetZookeepers() string {
	return s.zookeeper.GetConnectionString()
}

func (s *solrZkInstance) GetLeader(id string) (string, error) {
	cs, err := s.GetClusterState()
	if err != nil {
		return "", err
	}
	collectionMap := cs.Collections[s.collection]
	leader, err := findLeader(id, &collectionMap)
	return leader, err
}

// GetClusterProps Intentionally return a copy vs a pointer want to be thread safe
func (s *solrZkInstance) GetClusterProps() (ClusterProps, error) {
	return s.zookeeper.GetClusterProps()
}

func (s *solrZkInstance) Listening() bool {
	return s.listening
}
