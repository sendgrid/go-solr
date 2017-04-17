package solr

import (
	"fmt"
	"math/rand"
	"sync"
)

func init() {

}

type Solr interface {
	GetZookeepers() string
	GetNextReadHost() string
	GetClusterState() (ClusterState, error)
	GetLeader(id string) (string, error)
	SolrHttp() SolrHttp
	Listen() error
	FindLiveReplicaUrls(key string) ([]string, error)
	FindReplicaForRoute(key string) (string, error)
}

type solrInstance struct {
	zookeeper         Zookeeper
	collection        string
	host              string
	baseUrl           string
	currentNode       int
	clusterState      ClusterState
	solrHttp          SolrHttp
	clusterStateMutex *sync.Mutex
	currentNodeMutex  *sync.Mutex
}

func NewSolr(zookeepers string, zkRoot string, collectionName string, options ...func(*solrHttp)) (Solr, error) {
	instance := solrInstance{zookeeper: NewZookeeper(zookeepers, zkRoot, collectionName), collection: collectionName}

	instance.clusterStateMutex = &sync.Mutex{}
	instance.currentNodeMutex = &sync.Mutex{}
	var err error
	instance.solrHttp, err = NewSolrHttp(&instance, collectionName, options...)
	return &instance, err
}

func (s *solrInstance) SolrHttp() SolrHttp {
	return s.solrHttp
}

func (s *solrInstance) FindReplicaForRoute(shardKey string) (string, error) {
	replicas, err := s.FindLiveReplicaUrls(shardKey)
	if err != nil {
		return "", err
	}
	//pick a random node
	node := replicas[rand.Intn(len(replicas))]
	return node, nil
}

func (s *solrInstance) FindLiveReplicaUrls(key string) ([]string, error) {
	collection, ok := s.clusterState.Collections[s.collection]
	if !ok {
		return nil, fmt.Errorf("Collection %s does not exist ", s.collection)
	}
	return findLiveReplicaUrls(key, &collection)
}

func (s *solrInstance) GetZookeepers() string {
	return s.zookeeper.GetConnectionString()
}

func (s *solrInstance) GetLeader(id string) (string, error) {
	cs, err := s.GetClusterState()
	if err != nil {
		return "", err
	}
	collectionMap := cs.Collections[s.collection]
	leader, err := findLeader(id, &collectionMap)
	return leader, err
}
