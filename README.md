# go-solr
solr go client from Sendgrid

## Usage
To start the client
```
solrClient := solr.NewSolrZK(...)
solrClient.Listen()
```
The Read and Update methods take a node list use the SolrLocator interface to return a node list

```
solr.GetSolrLocator(solr.NewSolrZK(...))
type SolrLocator interface {
	GetLeaders(docID string) ([]string, error)
	GetReplicaUris(baseURL string) ([]string, error)
	GetReplicasFromRoute(route string) ([]string, error)
	GetLeadersAndReplicas(docID string) ([]string, error)
}
```


To make requests
```
solrClient.Read(solr.GetReplicasFromRoute("shard!"),solr.FilterQuery("myfield:test"),solr.Route("shardkey!"))
```
To make updates
```
solrClient.Update(solr.GetLeadersAndReplicas("{anydocidtoroute}"),collectionName,callsSolrJsonDocs, docsMap)
```

## Tests
1. Run solr cloud on 8983 or `docker-compose up`
2. ``` go test ```