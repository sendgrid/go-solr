# go-solr
solr go client from Sendgrid

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
	GetReplicaUris(baseURL string) ([]string, error)
	GetReplicasFromRoute(route string) ([]string, error)
	GetLeadersAndReplicas(docID string) ([]string, error)
}
```


To make requests
```
solrClient.Read(locator.GetReplicasFromRoute("shard!"),solr.FilterQuery("myfield:test"),solr.Route("shardkey!"))
```
To make updates
```
solrClient.Update(locator.GetLeadersAndReplicas("{anydocidtoroute}"),collectionName,callsSolrJsonDocs, docsMap)
```

## Tests
1. `docker-compose up`
2. ``` docker-compose run gotests bash ```
3. ```go test```
