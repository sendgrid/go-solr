package solr_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/sendgrid/solr-go"
)

var _ = Describe("Solr Client", func() {
	var solrClient solr.Solr
	BeforeEach(func() {
		var err error
		solrClient, err = solr.NewSolr("localhost:2181", "solr")
		Expect(err).To(BeNil())
	})
	It("construct", func() {
		solrClient, err := solr.NewSolr("test", "solr")
		Expect(err).To(Not(BeNil()))
		Expect(solrClient).To(Not(BeNil()))

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
			leader, err := solrClient.GetLeader("goseg", "list_segmentation_4161153_recipients__1!saurabh.kakkar@pearson.com")
			Expect(err).To(BeNil())
			Expect(leader).To(Not(BeNil()))
			Expect(leader).To(ContainSubstring(":8983/solr"))
			Expect(leader).To(ContainSubstring("http://"))
		})
	})

})
