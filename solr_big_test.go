package solr_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/sendgrid/solr-go"
	"log"
)

var _ = Describe("Solr Client", func() {
	var solrClient solr.SolrZK
	var solrHttp solr.SolrHTTP
	BeforeEach(func() {
		var err error
		solrClient = solr.NewSolrZK("zk:2181", "solr", "solrtest")
		err = solrClient.Listen()
		Expect(err).To(BeNil())
		solrHttp, err = solr.NewSolrHTTP(solrClient, "solrtest", solr.User("solr"), solr.Password("admin"), solr.MinRF(2))
		Expect(err).To(BeNil())

	})
	It("construct", func() {
		solrClient := solr.NewSolrZK("test", "solr", "solrtest")
		Expect(solrClient).To(Not(BeNil()))
		err := solrClient.Listen()
		Expect(err).To(Not(BeNil()))

	})

	Describe("Test Connection", func() {

		Describe("Test Requests", func() {

			It("can update requests and read with route many times for many shards", func() {
				const limit int = 10000
				uuid, _ := newUUID()
				for i := 0; i < limit; i++ {
					shardKey := "mycrazyshardkey" + string(i%10)
					iterationId, _ := newUUID()
					doc := map[string]interface{}{
						"id":         shardKey + "!rando" + iterationId,
						"email":      "rando" + iterationId + "@sendgrid.com",
						"first_name": "tester" + iterationId,
						"last_name":  uuid,
					}
					if i < limit-20 {
						err := solrHttp.Update(doc["id"].(string), true, doc, solr.Commit(false))
						if err != nil {
							log.Print(err)
						}
					} else {
						err := solrHttp.Update(doc["id"].(string), true, doc, solr.Commit(true))
						if err != nil {
							log.Print(err)
						}
					}

				}
				r, err := solrHttp.Read(solr.Query("*:*"), solr.FilterQuery("last_name:"+uuid), solr.Rows(1000))
				Expect(err).To(BeNil())
				Expect(r).To(Not(BeNil()))
				Expect(r.Response.NumFound).To(BeEquivalentTo(limit))
			})
		})
	})

})
