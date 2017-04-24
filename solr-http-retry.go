package solr

import (
	"log"
	"net/url"
	"time"
)

type SolrHttpRetrier struct {
	solrCli            SolrHTTP
	retries            int
	exponentialBackoff time.Duration
	readTimeout        time.Duration
	updateTimeout      time.Duration
}

func NewSolrHttpRetrier(solrHttp SolrHTTP, retries int, exponentialBackoff time.Duration, readTimeout time.Duration, updateTimeout time.Duration) SolrHTTP {

	solrRetrier := SolrHttpRetrier{solrCli: solrHttp, retries: retries, exponentialBackoff: exponentialBackoff, readTimeout: readTimeout, updateTimeout: updateTimeout}
	return &solrRetrier
}

func (s *SolrHttpRetrier) Read(opts ...func(url.Values)) (SolrResponse, error) {
	now := time.Now()
	var resp SolrResponse
	var err error
	for attempt := 0; attempt < s.retries; attempt++ {
		resp, err = s.solrCli.Read(opts...)
		if err != nil {
			s.Logger().Printf("[Solr Http Retrier] Error Retrying %v ", err)
			if !s.backoff(now, s.updateTimeout, attempt) {
				s.Logger().Println("[Solr Http Retrier] Timeout exceeded", err)
				break
			}
			continue
		}
		break
	}
	return resp, err
}

func (s *SolrHttpRetrier) Update(docID string, updateOnly bool, doc interface{}, opts ...func(url.Values)) error {
	now := time.Now()
	var err error
	for attempt := 0; attempt < s.retries; attempt++ {
		err := s.solrCli.Update(docID, updateOnly, doc, opts...)
		if err != nil {
			log.Printf("[Solr Http Retrier] Error Retrying %v ", err)
			if !s.backoff(now, s.readTimeout, attempt) {
				log.Println("[Solr Http Retrier] Timeout exceeded", err)
				break
			}
			continue
		}
		break
	}
	return err
}

func (s *SolrHttpRetrier) Logger() Logger {
	return s.solrCli.Logger()
}

//returns whether cap has been passed
func (s *SolrHttpRetrier) backoff(now time.Time, timeout time.Duration, attempt int) bool {
	//cap the time, whichever is less
	cap := min(timeout.Seconds(), s.exponentialBackoff.Seconds()*float64(attempt))
	totalRunTime := time.Since(now)*time.Second + time.Duration(cap)*time.Second
	// exit if its about to timeout
	if totalRunTime < timeout {
		s.Logger().Printf("Retrying request attempt: %d, sleeping for %v seconds", attempt, cap)
		time.Sleep(time.Duration(cap) * time.Second)
		return true
	}

	s.Logger().Printf("Exiting request attempt: %d, will run past timeout", attempt, cap)
	return false
}

func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}
