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
	cert              string
	queryClient       HTTPer
	writeClient       HTTPer
	defaultRows       uint32
	user              string
	password          string
	batchSize         int
	minRf             int
	useHttps          bool
	baseUrl           string
	currentNode       int
	clusterState      ClusterState
	solrHttp          SolrHttp
	clusterStateMutex *sync.Mutex
	currentNodeMutex  *sync.Mutex
}

func NewSolr(zookeepers string, zkRoot string, collectionName string, options ...func(*solrInstance)) (Solr, error) {
	instance := solrInstance{zookeeper: NewZookeeper(zookeepers, zkRoot, collectionName), minRf: 1, collection: collectionName, baseUrl: "solr", useHttps: false}

	for _, opt := range options {
		opt(&instance)
	}
	instance.clusterStateMutex = &sync.Mutex{}
	instance.currentNodeMutex = &sync.Mutex{}
	var err error
	instance.solrHttp, err = NewSolrHttp(&instance, collectionName, instance.user, instance.password, instance.minRf, instance.baseUrl, instance.queryClient,
		instance.writeClient, instance.cert, instance.useHttps)
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

//HTTPClient sets the HTTPer
func HTTPClient(cli HTTPer) func(*solrInstance) {
	return func(c *solrInstance) {
		c.queryClient = cli
		c.writeClient = cli
	}
}

//DefaultRows sets number of rows for pagination
//in calls that don't pass a number of rows in
func DefaultRows(rows uint32) func(*solrInstance) {
	return func(c *solrInstance) {
		c.defaultRows = rows
	}
}

//The path to tls certificate (optional)
func Cert(cert string) func(*solrInstance) {
	return func(c *solrInstance) {
		c.cert = cert
	}
}

func User(user string) func(*solrInstance) {
	return func(c *solrInstance) {
		c.user = user
	}
}

func Password(password string) func(*solrInstance) {
	return func(c *solrInstance) {
		c.password = password
	}
}

func BatchSize(size int) func(*solrInstance) {
	return func(c *solrInstance) {
		c.batchSize = size
	}
}

func UseHttps(useHttps bool) func(*solrInstance) {
	return func(c *solrInstance) {
		c.useHttps = useHttps
	}
}

func BaseUrl(baseUrl string) func(*solrInstance) {
	return func(c *solrInstance) {
		c.baseUrl = baseUrl
	}
}

func MinRF(minRf int) func(*solrInstance) {
	return func(c *solrInstance) {
		c.minRf = minRf
	}
}
