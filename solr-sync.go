package solr

import (
	"github.com/samuel/go-zookeeper/zk"
)

func (s *solrZkInstance) Listen() error {
	err := s.zookeeper.Connect()
	s.currentNode = 0
	if err != nil {
		return err
	}
	s.clusterState = ClusterState{}

	collectionsEvents, err := s.initCollectionsListener()
	if err != nil {
		return err
	}
	liveNodeEvents, err := s.initLiveNodesListener()
	if err != nil {
		return err
	}
	//loop forever
	go func() {
		for {
			select {
			case cEvent := <-collectionsEvents:
				// do something if its not a session or disconnect
				if cEvent.Type > zk.EventSession {
					collections, err := s.zookeeper.GetClusterState()
					if err != nil {
						continue
					}
					s.setCollections(collections)
				}
				if cEvent.State < zk.StateConnected {
					s.logger.Printf("[Error] solr cluster zk disconnected  %v", cEvent)
				} else {
					s.logger.Printf("Solr-go: solr cluster zk state changed zkType: %d zkState: %d", cEvent.Type, cEvent.State)
				}
			case nEvent := <-liveNodeEvents:
				// do something if its not a session or disconnect
				if nEvent.Type > zk.EventSession {
					liveNodes, err := s.zookeeper.GetLiveNodes()
					if err != nil {
						continue
					}
					s.setLiveNodes(liveNodes)
				}
				if nEvent.State < zk.StateConnected {
					s.logger.Printf("[Error] solr cluster zk live nodes disconnected zkType: %v ", nEvent)
				} else {
					s.logger.Printf("Solr-go: solr cluster zk live nodes state changed zkType: %d zkState: %d", nEvent.Type, nEvent.State)
				}
			}
		}
	}()
	s.listening = true
	return nil
}

func (s *solrZkInstance) initCollectionsListener() (<-chan zk.Event, error) {
	s.clusterState = ClusterState{}
	collections, collectionsEvents, err := s.zookeeper.GetClusterStateW()
	if err != nil {
		return nil, err
	}
	s.setCollections(collections)
	return collectionsEvents, nil
}

func (s *solrZkInstance) initLiveNodesListener() (<-chan zk.Event, error) {
	liveNodes, liveNodeEvents, err := s.zookeeper.GetLiveNodesW()
	if err != nil {
		return nil, err
	}
	s.setLiveNodes(liveNodes)
	return liveNodeEvents, nil
}

// GetClusterState Intentionally return a copy vs a pointer want to be thread safe
func (s *solrZkInstance) GetClusterState() (ClusterState, error) {
	s.clusterStateMutex.Lock()
	defer s.clusterStateMutex.Unlock()
	return s.clusterState, nil
}

func (s *solrZkInstance) setLiveNodes(nodes []string) {
	s.clusterStateMutex.Lock()
	defer s.clusterStateMutex.Unlock()
	s.clusterState.LiveNodes = nodes
	s.logger.Printf("Solr-go: zk livenodes updated %v ", s.clusterState.LiveNodes)
}

func (s *solrZkInstance) setCollections(collections map[string]Collection) {
	s.clusterStateMutex.Lock()
	defer s.clusterStateMutex.Unlock()
	s.clusterState.Collections = collections
	s.logger.Printf("Solr-go: zk collections updated %v ", s.clusterState.Collections)
}

func (s *solrZkInstance) GetNextReadHost() string {
	s.currentNodeMutex.Lock()
	defer s.currentNodeMutex.Unlock()
	node := s.clusterState.LiveNodes[s.currentNode]
	if s.currentNode >= len(s.clusterState.LiveNodes)-1 {
		s.currentNode = 0
	} else {
		s.currentNode++
	}

	return node
}
