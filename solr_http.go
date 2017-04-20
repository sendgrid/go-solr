package solr

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	b64 "encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

type SolrHTTP interface {
	Read(opts ...func(url.Values)) (SolrResponse, error)
	Update(docID string, updateOnly bool, doc interface{}, opts ...func(url.Values)) error
}
type solrHttp struct {
	minRF       int
	user        string
	password    string
	baseURL     string
	queryClient HTTPer
	writeClient HTTPer
	solrZk      SolrZK
	useHTTPS    bool
	collection  string
	cert        string
	defaultRows uint32
	batchSize   int
	minRf       int
}

func NewSolrHTTP(solrZk SolrZK, collection string, options ...func(*solrHttp)) (SolrHTTP, error) {
	solrCli := solrHttp{solrZk: solrZk, collection: collection, minRF: 1, baseURL: "solr", useHTTPS: false}
	if !solrZk.Listening() {
		return nil, fmt.Errorf("must call solr.Listen")
	}
	for _, opt := range options {
		opt(&solrCli)
	}
	var err error
	var props ClusterProps
	props, err = solrZk.GetClusterProps()
	if err != nil {
		return nil, err
	}
	log.Printf("Fetched cluster props %v", props)
	solrCli.useHTTPS = props.UrlScheme == "https"

	if solrCli.writeClient == nil {
		solrCli.writeClient, err = defaultWriteClient(solrCli.cert)
		if err != nil {
			return nil, err
		}
	}

	if solrCli.queryClient == nil {
		solrCli.queryClient, err = defaultReadClient(solrCli.cert)
		if err != nil {
			return nil, err
		}
	}

	return &solrCli, nil
}

func (s *solrHttp) Update(docID string, updateOnly bool, doc interface{}, opts ...func(url.Values)) error {
	leader, err := s.solrZk.GetLeader(docID)
	if err != nil {
		return err
	}

	urlVals := url.Values{
		"min_rf": {fmt.Sprintf("%d", s.minRF)},
	}
	for _, opt := range opts {
		opt(urlVals)
	}

	uri := fmt.Sprintf("%s/%s/update", leader, s.collection)
	if updateOnly {
		uri += "/json/docs"
	}

	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	if err := enc.Encode(doc); err != nil {
		return err
	}

	req, err := http.NewRequest("POST", uri, &buf)
	if err != nil {
		return err
	}

	req.URL.RawQuery = urlVals.Encode()

	req.Header.Add("Content-Type", "application/json")
	basicCred := s.getBasicCredential(s.user, s.password)
	if basicCred != "" {
		req.Header.Add("Authorization", fmt.Sprintf("Basic %s", basicCred))
	}

	resp, err := s.writeClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		htmlData, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("error reading response body for StatusCode %d, err: %s", resp.StatusCode, err)
		}
		if resp.StatusCode < 500 {
			return NewSolrError(resp.StatusCode, string(htmlData))
		} else {
			return NewSolrInternalError(resp.StatusCode, string(htmlData))
		}
	}
	var r UpdateResponse
	dec := json.NewDecoder(resp.Body)
	if err := dec.Decode(&r); err != nil {
		return NewSolrParseError(resp.StatusCode, err.Error())
	}

	if r.Response.Status != 0 {
		msg := r.Error.Msg
		return NewSolrError(r.Response.Status, msg)
	}

	if r.Response.RF < r.Response.MinRF {
		return NewSolrRFError(r.Response.RF, r.Response.MinRF)
	}
	return nil
}

func (s *solrHttp) Read(opts ...func(url.Values)) (SolrResponse, error) {
	var host string
	urlValues := url.Values{
		"wt": {"json"},
	}
	for _, opt := range opts {
		opt(urlValues)
	}
	//if contains route don't round robin
	if route, ok := urlValues["_route_"]; ok {
		var err error
		host, err = s.solrZk.FindReplicaForRoute(route[0])
		if err != nil {
			return SolrResponse{}, err
		}

	} else {
		protocol := "http"
		if s.useHTTPS {
			protocol = "https"
		}
		host = fmt.Sprintf("%s://%s/%s", protocol, s.solrZk.GetNextReadHost(), s.baseURL)
	}

	var sr SolrResponse
	u := fmt.Sprintf("%s/%s/select", host, s.collection)
	body := bytes.NewBufferString(urlValues.Encode())
	req, err := http.NewRequest("POST", u, body)
	log.Printf("Reading from %s %v", u, body)
	if err != nil {
		return sr, err
	}
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	basicCred := s.getBasicCredential(s.user, s.password)
	if basicCred != "" {
		req.Header.Add("Authorization", fmt.Sprintf("Basic %s", basicCred))
	}
	resp, err := s.queryClient.Do(req)
	if err != nil {
		return sr, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		sr.Status = 404
		return sr, ErrNotFound
	}
	if resp.StatusCode >= 400 {
		htmlData, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return sr, err
		}
		sr.Status = resp.StatusCode
		return sr, NewSolrError(resp.StatusCode, string(htmlData))
	}

	dec := json.NewDecoder(resp.Body)

	return sr, dec.Decode(&sr)
}

func getMapChunks(in []map[string]interface{}, chunkSize int) [][]map[string]interface{} {
	var out [][]map[string]interface{}
	for i := 0; i < len(in); i += chunkSize {
		end := i + chunkSize
		if end > len(in) {
			end = len(in)
		}
		out = append(out, in[i:end])
	}
	return out
}
func getidChunks(in []string, chunkSize int) [][]string {
	var out [][]string
	for i := 0; i < len(in); i += chunkSize {
		end := i + chunkSize
		if end > len(in) {
			end = len(in)
		}
		out = append(out, in[i:end])
	}
	return out
}

func Query(q string) func(url.Values) {
	return func(p url.Values) {
		p["q"] = []string{q}
	}
}

//Helper funcs for setting the solr query params
func FilterQuery(fq string) func(url.Values) {
	return func(p url.Values) {
		p["fq"] = []string{fq}
	}
}

func Rows(rows uint32) func(url.Values) {
	return func(p url.Values) {
		p["rows"] = []string{strconv.FormatUint(uint64(rows), 10)}
	}
}

func Route(r string) func(url.Values) {
	return func(p url.Values) {
		if r != "" {
			p["_route_"] = []string{r}
		}
	}
}

func Start(start uint32) func(url.Values) {
	return func(p url.Values) {
		p["start"] = []string{strconv.FormatUint(uint64(start), 10)}
	}
}

func Sort(s string) func(url.Values) {
	return func(p url.Values) {
		p["sort"] = []string{s}
	}
}

func Commit(commit bool) func(url.Values) {
	return func(p url.Values) {
		commitString := "false"
		if commit {
			commitString = "true"
		}
		p["commit"] = []string{commitString}
	}
}

func Cursor(c string) func(url.Values) {
	return func(p url.Values) {
		p["cursorMark"] = []string{c}
	}
}

func UrlVals(urlVals url.Values) func(url.Values) {
	return func(p url.Values) {
		for key, _ := range urlVals {
			p[key] = urlVals[key]
		}
	}
}

func defaultWriteClient(cert string) (HTTPer, error) {
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

func defaultReadClient(cert string) (HTTPer, error) {
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

func (s *solrHttp) getBasicCredential(user string, password string) string {
	if user != "" {
		userPass := fmt.Sprintf("%s:%s", user, password)
		return b64.StdEncoding.EncodeToString([]byte(userPass))
	}
	return ""
}

//HTTPClient sets the HTTPer
func HTTPClient(cli HTTPer) func(*solrHttp) {
	return func(c *solrHttp) {
		c.queryClient = cli
		c.writeClient = cli
	}
}

//DefaultRows sets number of rows for pagination
//in calls that don't pass a number of rows in
func DefaultRows(rows uint32) func(*solrHttp) {
	return func(c *solrHttp) {
		c.defaultRows = rows
	}
}

//The path to tls certificate (optional)
func Cert(cert string) func(*solrHttp) {
	return func(c *solrHttp) {
		c.cert = cert
	}
}

func User(user string) func(*solrHttp) {
	return func(c *solrHttp) {
		c.user = user
	}
}

func Password(password string) func(*solrHttp) {
	return func(c *solrHttp) {
		c.password = password
	}
}

func BatchSize(size int) func(*solrHttp) {
	return func(c *solrHttp) {
		c.batchSize = size
	}
}

func BaseURL(baseURL string) func(*solrHttp) {
	return func(c *solrHttp) {
		c.baseURL = baseURL
	}
}

func MinRF(minRf int) func(*solrHttp) {
	return func(c *solrHttp) {
		c.minRf = minRf
	}
}
