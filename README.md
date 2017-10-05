# go-solr
solr go client from Sendgrid

![travis](https://travis-ci.org/sendgrid/go-solr.svg?branch=master)

[![CLA assistant](https://cla.sendgrid.com/readme/badge/sendgrid/go-solr)](https://cla.sendgrid.com/sendgrid/go-solr)

## Usage
To start the client
```
solrzk := solr.NewSolrZK(...)
solrzk.Listen()
solrhttp := solr.NewSolrHttp(solrzk)
solrClient := solr.NewSolrHttpRetrier(solrhttp)
```
The Read and Update methods take a node list use the SolrLocator interface to return a node list

```
locator := solr.GetSolrLocator(solr.NewSolrZK(...))
type SolrLocator interface {
	GetLeaders(docID string) ([]string, error)
	GetReplicaUris() ([]string, error)
	GetReplicasFromRoute(route string) ([]string, error)
	GetLeadersAndReplicas(docID string) ([]string, error)
}
```


To make requests
```
solrClient.Select(locator.GetReplicasFromRoute("shard!"),solr.FilterQuery("myfield:test"),solr.Route("shardkey!"))
```
To make updates
```
solrClient.Update(locator.GetLeadersAndReplicas("{anydocidtoroute}"),collectionName,callsSolrJsonDocs, docsMap)
```

## Tests
1. `docker-compose up`
2. ``` docker-compose run gotests bash ```
3. ```go test```
