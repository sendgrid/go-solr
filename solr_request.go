package solr

import (
	"bytes"
	b64 "encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strconv"
)

func (c *solrInstance) update(host string, collection string, updateOnly bool, doc interface{}, urlValues url.Values) error {
	uri := fmt.Sprintf("%s/%s/update", host, collection)
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

	req.URL.RawQuery = urlValues.Encode()

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

func (c *solrInstance) read(host string, collection string, urlValues url.Values) (SolrResponse, error) {
	var sr SolrResponse
	u := fmt.Sprintf("%s/%s/select", host, collection)
	body := bytes.NewBufferString(urlValues.Encode())
	req, err := http.NewRequest("POST", u, body)
	log.Printf("Reading from %s %v", u, body)
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
		p["_route_"] = []string{r}
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

func (c *solrInstance) getBasicCredential(user string, password string) string {
	if user != "" {
		userPass := fmt.Sprintf("%s:%s", user, password)
		return b64.StdEncoding.EncodeToString([]byte(userPass))
	}
	return ""
}
