package solr_test

import (
	"fmt"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/sendgrid/solr-go"
	"strings"
)

var _ = Describe("Solr Client", func() {
	var solrClient solr.Solr

	BeforeEach(func() {
		var err error
		solrClient, err = solr.NewSolr("zk:2181", "solr", "solrtest", solr.User("solr"), solr.Password("admin"))
		Expect(err).To(BeNil())
		err = solrClient.Listen()
		Expect(err).To(BeNil())
	})
	It("construct", func() {
		solrClient, err := solr.NewSolr("test", "solr", "solrtest")
		Expect(err).To(BeNil())
		Expect(solrClient).To(Not(BeNil()))
		err = solrClient.Listen()
		Expect(err).To(Not(BeNil()))

	})

	Describe("Test Connection", func() {

		It("can get clusterstate", func() {
			state, err := solrClient.GetClusterState()
			Expect(err).To(BeNil())
			Expect(state).To(Not(BeNil()))
			Expect(len(state.Collections)).To(Equal(1))
		})

		It("can find a leader", func() {
			state, err := solrClient.GetClusterState()
			Expect(err).To(BeNil())
			Expect(state).To(Not(BeNil()))
			Expect(len(state.Collections)).To(Equal(1))
			leader, err := solrClient.GetLeader("mycrazyshardkey1!saurabh.kakkar@pearson.com")
			Expect(err).To(BeNil())
			Expect(leader).To(Not(BeNil()))
			Expect(leader).To(ContainSubstring(":8983/solr"))
			Expect(leader).To(ContainSubstring("http://"))
		})

		It("can find a replica", func() {
			state, err := solrClient.GetClusterState()
			fmt.Println(state)
			Expect(err).To(BeNil())
			Expect(state).To(Not(BeNil()))
			Expect(len(state.Collections)).To(Equal(1))
			replicas, err := solrClient.FindLiveReplicaUrls("mycrazyshardkey1!")
			Expect(err).To(BeNil())
			Expect(len(replicas)).To(Not(BeZero()))

			Expect(replicas[0]).To(ContainSubstring(":8983/solr"))
			Expect(replicas[0]).To(ContainSubstring("http://"))
		})
		It("can find a replica many times", func() {
			state, err := solrClient.GetClusterState()
			Expect(err).To(BeNil())
			Expect(state).To(Not(BeNil()))
			Expect(len(state.Collections)).To(Equal(1))
			counts := make(map[string]int)
			replicas, err := solrClient.FindLiveReplicaUrls("mycrazyshardkey1!")
			for i := 0; i < 1000; i++ {
				replica, err := solrClient.FindReplicaForRoute("mycrazyshardkey1!")
				Expect(err).To(BeNil())
				Expect(replica).To(Not(BeNil()))
				Expect(replica).To(ContainSubstring(":8983/solr"))
				Expect(replica).To(ContainSubstring("http://"))
				counts[replica] = counts[replica] + 1
			}

			for _, key := range replicas {
				Expect(counts[key] > 100).To(BeTrue())
			}

		})

		Describe("Test Requests", func() {
			It("can get requests", func() {
				r, err := solrClient.Read(solr.FilterQuery("*:*"), solr.Rows(10))
				Expect(err).To(BeNil())
				Expect(r).To(Not(BeNil()))
			})
			It("can update requests", func() {
				uuid, _ := newUUID()

				doc := map[string]interface{}{
					"id":         "mycrazyshardkey1!" + uuid,
					"email":      uuid + "feldman1@sendgrid.com",
					"first_name": "shawn1" + uuid,
					"last_name":  uuid + "feldman1",
				}
				err := solrClient.Update(doc["id"].(string), true, doc, solr.Commit(true))
				Expect(err).To(BeNil())
				r, err := solrClient.Read(solr.Query("*:*"), solr.FilterQuery("first_name:shawn1"+uuid), solr.Rows(10))
				Expect(err).To(BeNil())
				Expect(r).To(Not(BeNil()))
				fmt.Println(r.Response.Docs)
				Expect(r.Response.NumFound).To(BeEquivalentTo(1))
			})
			It("can update requests and read with route", func() {
				uuid, _ := newUUID()

				doc := map[string]interface{}{
					"id":         "mycrazyshardkey3!" + uuid,
					"email":      uuid + "feldman@sendgrid.com",
					"first_name": "shawn3" + uuid,
					"last_name":  uuid,
				}
				err := solrClient.Update(doc["id"].(string), true, doc, solr.Commit(true))
				Expect(err).To(BeNil())
				r, err := solrClient.Read(solr.Route("mycrazyshardkey2!"), solr.Query("*:*"), solr.FilterQuery("last_name:"+uuid), solr.Rows(10))
				Expect(err).To(BeNil())
				Expect(r).To(Not(BeNil()))
				Expect(r.Response.NumFound).To(BeEquivalentTo(1))
			})

			It("can update requests and read with route many times", func() {
				const limit int = 100
				uuid, _ := newUUID()
				for i := 0; i < limit; i++ {
					iterationId, _ := newUUID()
					doc := map[string]interface{}{
						"id":         "mycrazyshardkey4!rando" + iterationId,
						"email":      "rando" + iterationId + "@sendgrid.com",
						"first_name": "tester" + iterationId,
						"last_name":  uuid,
					}
					if i < limit-1 {
						err := solrClient.Update(doc["id"].(string), true, doc, solr.Commit(false))
						Expect(err).To(BeNil())
					} else {
						err := solrClient.Update(doc["id"].(string), true, doc, solr.Commit(true))
						Expect(err).To(BeNil())
					}

				}
				r, err := solrClient.Read(solr.Route("mycrazyshardkey4!"), solr.Query("*:*"), solr.FilterQuery("last_name:"+uuid), solr.Rows(1000))
				Expect(err).To(BeNil())
				Expect(r).To(Not(BeNil()))
				Expect(r.Response.NumFound).To(BeEquivalentTo(limit))
			})
		})
		Describe("Basic Auth Fails", func() {

			It("can get requests", func() {
				solrNoAuthClient, err := solr.NewSolr("zk:2181", "solr", "solrtest")
				Expect(err).To(BeNil())
				err = solrNoAuthClient.Listen()
				Expect(err).To(BeNil())
				r, err := solrNoAuthClient.Read(solr.FilterQuery("*:*"), solr.Rows(10))
				Expect(strings.Contains(err.Error(), "401")).To(BeTrue())
				Expect(r.Status).To(BeEquivalentTo(401))
			})

		})
	})

})
