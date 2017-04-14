# solr-go
solr go client from Sendgrid

## Usage
To start the client
```
solrClient := solr.NewSolr()
solrClient.Listen()
```

To make requests
```
solrClient.Read(collectionName,solr.FilterQuery("myfield:test"),solr.Route("shardkey!"))
```
To make updates
```
solrClient.Update(firstDocId,collectionName,callsSolrJsonDocs, docsMap)
```

## Tests
1. Run solr cloud on 8983
2. ``` go test ```