package solr

import (
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"strings"
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
	listening         bool
	logger            Logger
}

func NewSolrZK(zookeepers string, zkRoot string, collectionName string, opts ...func(*solrZkInstance)) SolrZK {
	instance := solrZkInstance{zookeeper: NewZookeeper(zookeepers, zkRoot, collectionName), collection: collectionName}

	instance.clusterStateMutex = &sync.Mutex{}
	instance.listening = false
	instance.logger = log.New(ioutil.Discard, "[SolrClient] ", log.LstdFlags)

	for _, opt := range opts {
		opt(&instance)
	}

	return &instance
}

func (s *solrZkInstance) GetZookeepers() string {
	return s.zookeeper.GetConnectionString()
}

func (s *solrZkInstance) GetLeaders(docID string) ([]string, error) {
	cs, err := s.GetClusterState()
	if err != nil {
		return []string{}, err
	}
	collectionMap := cs.Collections[s.collection]
	leader, err := findLeader(docID, &collectionMap)
	return []string{leader}, err
}

func (s *solrZkInstance) GetLeadersAndReplicas(docID string) ([]string, error) {
	keys := strings.Split(docID, "!")
	var uris []string
	leaders, err := s.GetLeaders(docID)
	if err != nil {
		return uris, err
	}
	replicas, err := s.GetReplicasFromRoute(keys[0])
	if err != nil {
		return uris, err
	}

	set := make(map[string]bool, len(leaders)+len(replicas))
	all := []string{}
	for _, v := range leaders {
		set[v] = true
		all = append(all, v)
	}
	for _, v := range replicas {
		if there := set[v]; !there {
			all = append(all, v)
		}
		set[v] = true
	}
	return all, err
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

func (s *solrZkInstance) GetReplicaUris(baseURL string) ([]string, error) {
	props, err := s.GetClusterProps()
	if err != nil {
		return []string{}, err
	}

	useHTTPS := props.UrlScheme == "https"

	if baseURL == "" {
		baseURL = "solr"
	}
	protocol := "http"
	if useHTTPS {
		protocol = "https"
	}
	cs, err := s.GetClusterState()
	if err != nil {
		return []string{}, nil
	}
	nodes := cs.LiveNodes
	var uris []string
	for _, v := range nodes {
		host := fmt.Sprintf("%s://%s/%s", protocol, v, baseURL)
		uris = append(uris, host)
	}
	return shuffleNodes(uris), nil

}

func (s *solrZkInstance) GetReplicasFromRoute(route string) ([]string, error) {
	if strings.LastIndex(route, "!") != len(route)-1 {
		route += "!"
	}
	collection, ok := s.clusterState.Collections[s.collection]
	if !ok {
		return nil, fmt.Errorf("Collection %s does not exist ", s.collection)
	}
	//if contains route don't round robin
	hosts, err := findLiveReplicaUrls(route, &collection)
	if err != nil {
		return hosts, err
	}

	return shuffleNodes(hosts), nil

}
func shuffleNodes(nodes []string) []string {
	if len(nodes) == 1 {
		return nodes
	}
	dest := make([]string, len(nodes))
	perm := rand.Perm(len(nodes))
	for i, v := range perm {
		dest[v] = nodes[i]
	}
	return dest
}
