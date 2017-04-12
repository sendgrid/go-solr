package solr

import (
	"bytes"
	b64 "encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
)

var bucketExp = regexp.MustCompile(`_yz_rb:([_a-z0-9]*)`)

func (c *solrInstance) update(host string, collection string, updateOnly bool, doc interface{}) error {
	uri := fmt.Sprintf("%s/solr/%s/update", host, collection)
	if updateOnly {
		uri += "/json/docs"
	}
	var err error
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	if err := enc.Encode(doc); err != nil {
		return err
	}

	req, err := http.NewRequest("POST", uri, &buf)
	if err != nil {
		return err
	}

	p := url.Values{
		"min_rf": {fmt.Sprintf("%d", c.minRf)},
	}
	req.URL.RawQuery = p.Encode()

	req.Header.Add("Content-Type", "application/json")
	basicCred := c.getBasicCredential(c.user, c.password)
	if basicCred != "" {
		req.Header.Add("Authorization", fmt.Sprintf("Basic %s", basicCred))
	}

	resp, err := c.writeClient.Do(req)
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
	var r updateResponse
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

func (c *solrInstance) read(host string, collection string, opts []func(url.Values)) (SolrResponse, error) {
	var sr SolrResponse
	u := fmt.Sprintf("%s/solr/%s/select", host, collection)

	p := url.Values{
		"wt": {"json"},
	}
	for _, opt := range opts {
		opt(p)
	}

	c.addRoute(p)

	req, err := http.NewRequest("POST", u, bytes.NewBufferString(p.Encode()))
	if err != nil {
		return sr, err
	}
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	basicCred := c.getBasicCredential(c.user, c.password)
	if basicCred != "" {
		req.Header.Add("Authorization", fmt.Sprintf("Basic %s", basicCred))
	}
	resp, err := c.queryClient.Do(req)
	if err != nil {
		return sr, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return sr, ErrNotFound
	}
	if resp.StatusCode >= 400 {
		htmlData, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return sr, err
		}
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

func query(q string) func(url.Values) {
	return func(p url.Values) {
		p["q"] = []string{q}
	}
}

//Helper funcs for setting the solr query params
func filterQuery(fq string) func(url.Values) {
	return func(p url.Values) {
		p["fq"] = []string{fq}
	}
}

func rows(rows uint32) func(url.Values) {
	return func(p url.Values) {
		p["rows"] = []string{strconv.FormatUint(uint64(rows), 10)}
	}
}

func route(r string) func(url.Values) {
	return func(p url.Values) {
		p["_route_"] = []string{r}
	}
}

func start(start uint32) func(url.Values) {
	return func(p url.Values) {
		p["start"] = []string{strconv.FormatUint(uint64(start), 10)}
	}
}

func sort(s string) func(url.Values) {
	return func(p url.Values) {
		p["sort"] = []string{s}
	}
}

func cursor(c string) func(url.Values) {
	return func(p url.Values) {
		p["cursorMark"] = []string{c}
	}
}

func (c *solrInstance) getBasicCredential(user string, password string) string {
	if user != "" {
		userPass := fmt.Sprintf("%s:%s", user, password)
		return b64.StdEncoding.EncodeToString([]byte(userPass))
	}
	return ""
}

func (c *solrInstance) addRoute(params url.Values) {
	var s string
	for _, values := range params {
		for _, v := range values {
			if strings.Contains(v, "_yz_rb") {
				s = v
			}
		}
	}

	if len(s) == 0 {
		return
	}

	r := c.getRoute(s)
	if len(r) > 0 {
		params.Set("_route_", r)
	}
}

func (c *solrInstance) getRoute(s string) string {
	matches := bucketExp.FindStringSubmatch(s)
	var r string
	if len(matches) > 1 {
		r = matches[1] + "!"
	}
	return r
}
