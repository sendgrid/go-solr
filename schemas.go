package solr

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

type SolrZK interface {
	GetZookeepers() string
	GetClusterState() (ClusterState, error)
	GetClusterProps() (ClusterProps, error)
	Listen() error
	Listening() bool
	GetSolrLocator() SolrLocator
	UseHTTPS() (bool, error)
}

type SolrLocator interface {
	GetLeaders(docID string) ([]string, error)
	GetReplicaUris() ([]string, error)
	GetReplicasFromRoute(route string) ([]string, error)
	GetShardFromRoute(route string) (string, error)
	GetLeadersAndReplicas(docID string) ([]string, error)
}

type SolrHTTP interface {
	Read(nodeUris []string, opts ...func(url.Values)) (SolrResponse, error)
	Update(nodeUris []string, jsonDocs bool, doc interface{}, opts ...func(url.Values)) error
	Logger() Logger
}

type Logger interface {
	Print(v ...interface{})
	Printf(format string, v ...interface{})
	Println(v ...interface{})
	Fatal(v ...interface{})
	Fatalf(format string, v ...interface{})
	Panic(v ...interface{})
	Panicf(format string, v ...interface{})
}

type HTTPer interface {
	Do(*http.Request) (*http.Response, error)
}

type CompositeKey struct {
	ShardKey string
	DocID    string
	Bits     uint
}

type HashRange struct {
	Low  int32
	High int32
}

func NewCompositeKey(id string) (CompositeKey, error) {
	keys := strings.Split(id, "!")

	if len(keys) == 1 {
		if strings.Index(id, "!") < 0 {
			return CompositeKey{DocID: keys[0]}, nil
		} else {
			return CompositeKey{ShardKey: id}, nil
		}
	}

	if len(keys) == 2 {
		shard := keys[0]
		bitShift := 0
		var err error
		i := strings.Index(shard, "/")
		if i > 0 {
			bits := strings.Split(shard, "/")
			shard = bits[0]
			bitShift, err = strconv.Atoi(bits[1])
			if err != nil {
				return CompositeKey{}, err
			}
		}

		return CompositeKey{ShardKey: keys[0], DocID: shard, Bits: uint(bitShift)}, nil
	}

	if len(keys) > 2 {
		return CompositeKey{}, fmt.Errorf("Cant deal with composite keys %s", id)
	}

	panic("failed all cases")
}

type ClusterProps struct {
	UrlScheme string `json:"urlScheme"`
}
