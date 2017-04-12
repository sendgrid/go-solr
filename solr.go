package solr

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"sync"
	"time"
)

func init() {

}

type Solr interface {
	GetZookeepers() string
	GetClusterState() (*ClusterState, error)
	GetLeader(collection string, id string) (string, error)
	Read(collection string, opts ...func(url.Values)) (SolrResponse, error)
	Update(docID string, collection string, updateOnly bool, doc interface{}) error
}

type solrInstance struct {
	zookeeper         Zookeeper
	zkRoot            string
	host              string
	cert              string
	queryClient       HTTPer
	writeClient       HTTPer
	defaultRows       uint32
	user              string
	password          string
	batchSize         int
	minRf             int
	currentNode       int
	clusterState      *ClusterState
	clusterStateMutex *sync.Mutex
	currentNodeMutex  *sync.Mutex
	connected         bool
}

func NewSolr(zookeepers string, zkRoot string, options ...func(*solrInstance)) (Solr, error) {
	instance := &solrInstance{zookeeper: NewZookeeper(zookeepers), zkRoot: zkRoot, connected: false}

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
	err = instance.connect()
	return instance, err
}

func (s *solrInstance) connect() error {
	if s.connected {
		return nil
	}
	err := s.zookeeper.Connect()
	s.currentNode = 0
	if err != nil {
		return err
	}
	s.clusterState, err = s.zookeeper.GetClusterState(s.zkRoot)
	if err != nil {
		return err
	}
	go s.pollForState()
	s.connected = true
	return nil
}

func (s *solrInstance) Read(collection string, opts ...func(url.Values)) (SolrResponse, error) {
	node := s.getNextNode()
	return s.read(node, collection, opts)
}
func (s *solrInstance) Update(docID string, collection string, updateOnly bool, doc interface{}) error {
	collectionInstance := s.clusterState.Collections[collection]
	leader, err := findLeader(docID, &collectionInstance)
	if err != nil {
		return err
	}
	return s.update(leader, collection, updateOnly, doc)
}

func (s *solrInstance) GetZookeepers() string {
	return s.zookeeper.GetConnectionString()
}

func (s *solrInstance) GetLeader(collection string, id string) (string, error) {
	cs, err := s.GetClusterState()
	if err != nil {
		return "", err
	}
	collectionMap := cs.Collections[collection]
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

func ZkzkRoot(zkzkRoot string) func(*solrInstance) {
	return func(c *solrInstance) {
		c.zkRoot = zkzkRoot
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
