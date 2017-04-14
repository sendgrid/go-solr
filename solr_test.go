package solr_test

import (
	"fmt"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/sendgrid/solr-go"
)

var _ = Describe("Solr Client", func() {
	var solrClient solr.Solr
	BeforeEach(func() {
		var err error
		solrClient, err = solr.NewSolr("zk:2181", "solr", "solrtest")
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
			leader, err := solrClient.GetLeader("list_segmentation_4161153_recipients__1!saurabh.kakkar@pearson.com")
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
			replicas, err := solrClient.FindLiveReplicaUrls("list_segmentation_4161153_recipients__1!")
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
			replicas, err := solrClient.FindLiveReplicaUrls("list_segmentation_4161153_recipients__1!")
			for i := 0; i < 1000; i++ {
				replica, err := solrClient.FindReplicaForRoute("list_segmentation_4161153_recipients__1!")
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
		})
	})

})
