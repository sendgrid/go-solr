package solr

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"sync"
	"time"
)

func init() {

}

type Solr interface {
	GetZookeepers() string
	GetClusterState() (ClusterState, error)
	GetLeader(id string) (string, error)
	Read(opts ...func(url.Values)) (SolrResponse, error)
	Update(docID string, updateOnly bool, doc interface{}, opts ...func(url.Values)) error
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
	clusterStateMutex *sync.Mutex
	currentNodeMutex  *sync.Mutex
}

func NewSolr(zookeepers string, zkRoot string, collectionName string, options ...func(*solrInstance)) (Solr, error) {
	instance := &solrInstance{zookeeper: NewZookeeper(zookeepers, zkRoot, collectionName), minRf: 1, collection: collectionName, baseUrl: "solr", useHttps: false}

	for _, opt := range options {
		opt(instance)
	}
	var err error
	if instance.writeClient == nil {
		instance.writeClient, err = DefaultWriteClient(instance.cert)
		if err != nil {
			return nil, err
		}
	}

	if instance.queryClient == nil {
		instance.queryClient, err = DefaultReadClient(instance.cert)
		if err != nil {
			return nil, err
		}
	}

	instance.clusterStateMutex = &sync.Mutex{}
	instance.currentNodeMutex = &sync.Mutex{}
	return instance, err
}

func (s *solrInstance) Read(opts ...func(url.Values)) (SolrResponse, error) {
	var node string
	urlVals := url.Values{
		"wt": {"json"},
	}
	for _, opt := range opts {
		opt(urlVals)
	}
	//if contains route don't round robin
	if route, ok := urlVals["_route_"]; ok {
		var err error
		node, err = s.FindReplicaForRoute(route[0])
		if err != nil {
			return SolrResponse{}, err
		}

	} else {
		protocol := "http"
		if s.useHttps {
			protocol = "https"
		}
		node = fmt.Sprintf("%s://%s/%s", protocol, s.getNextNode(), s.baseUrl)
	}

	return s.read(node, s.collection, urlVals)
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

func (s *solrInstance) Update(docID string, updateOnly bool, doc interface{}, opts ...func(url.Values)) error {
	collectionInstance := s.clusterState.Collections[s.collection]
	leader, err := findLeader(docID, &collectionInstance)
	if err != nil {
		return err
	}

	urlVals := url.Values{
		"min_rf": {fmt.Sprintf("%d", s.minRf)},
	}
	for _, opt := range opts {
		opt(urlVals)
	}
	return s.update(leader, s.collection, updateOnly, doc, urlVals)
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

func DefaultWriteClient(cert string) (HTTPer, error) {
	cli := &http.Client{
		Timeout: time.Duration(30) * time.Second,
	}
	if cert != "" {
		tlsConfig, err := getTLSConfig(cert)
		if err != nil {
			return nil, err
		}
		cli.Transport = &http.Transport{TLSClientConfig: tlsConfig, MaxIdleConnsPerHost: 10}
	}
	return cli, nil
}

func DefaultReadClient(cert string) (HTTPer, error) {
	cli := &http.Client{
		Timeout: time.Duration(20) * time.Second,
	}
	if cert != "" {
		tlsConfig, err := getTLSConfig(cert)
		if err != nil {
			return nil, err
		}
		cli.Transport = &http.Transport{TLSClientConfig: tlsConfig, MaxIdleConnsPerHost: 10}
	}
	return cli, nil
}

func getTLSConfig(certPath string) (*tls.Config, error) {
	tlsConf := &tls.Config{InsecureSkipVerify: true}
	if certPath != "" {
		zkRootPEM, err := ioutil.ReadFile(certPath)
		if err != nil {
			return nil, err
		}

		zkRoots := x509.NewCertPool()
		ok := zkRoots.AppendCertsFromPEM([]byte(zkRootPEM))
		if !ok {
			log.Fatal("failed to parse zkRoot certificate")
		}
		tlsConf.RootCAs = zkRoots
	}
	return tlsConf, nil
}
