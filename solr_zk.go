package solr

import (
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"sync"
)

func init() {

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
	logger            Logger
}

func NewSolrZK(zookeepers string, zkRoot string, collectionName string, opts ...func(*solrZkInstance)) SolrZK {
	instance := solrZkInstance{zookeeper: NewZookeeper(zookeepers, zkRoot, collectionName), collection: collectionName}

	instance.clusterStateMutex = &sync.Mutex{}
	instance.currentNodeMutex = &sync.Mutex{}
	instance.listening = false
	instance.logger = log.New(ioutil.Discard, "[SolrClient] ", log.LstdFlags)

	for _, opt := range opts {
		opt(&instance)
	}

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

func (s *solrZkInstance) Logger(logger Logger) func(s *solrZkInstance) {
	return func(solrZk *solrZkInstance) {
		s.logger = logger
	}
}
