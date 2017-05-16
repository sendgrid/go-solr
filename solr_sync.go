package solr

import (
	"github.com/samuel/go-zookeeper/zk"
	"time"
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
	leaderEvents, err := s.initLeaderElectListener()
	if err != nil {
		return err
	}
	//loop forever
	go func() {
		errCount := 0
		for {
			select {
			case cEvent := <-collectionsEvents:
				// do something if its not a session or disconnect
				if cEvent.Type == zk.EventNodeDataChanged {
					collections, version, err := s.zookeeper.GetClusterState()
					if err != nil {
						errCount++
						time.Sleep(time.Duration(errCount*500) * time.Millisecond)
						continue
					}
					errCount = 0
					s.setCollections(collections, version)
				}
				if cEvent.State < zk.StateConnected {
					s.logger.Printf("[Error] solr cluster zk disconnected  %v", cEvent)
				} else {
					s.logger.Printf("go-solr: solr cluster zk state changed zkType: %d zkState: %d", cEvent.Type, cEvent.State)
				}
			case lEvent := <-leaderEvents:
				if lEvent.Type == zk.EventNodeChildrenChanged || lEvent.Type == zk.EventNodeDataChanged {
					// s.Logger().Printf("Leader changed pausing")
				}
			case nEvent := <-liveNodeEvents:
				// do something if its not a session or disconnect
				if nEvent.Type == zk.EventNodeDataChanged || nEvent.Type == zk.EventNodeChildrenChanged {
					liveNodes, err := s.zookeeper.GetLiveNodes()
					if err != nil {
						errCount++
						time.Sleep(time.Duration(errCount*500) * time.Millisecond)
						continue
					}
					errCount = 0
					s.setLiveNodes(liveNodes)
				}
				if nEvent.State < zk.StateConnected {
					s.logger.Printf("[Error] solr cluster zk live nodes disconnected zkType: %v ", nEvent)
				} else {
					s.logger.Printf("go-solr: solr cluster zk live nodes state changed zkType: %d zkState: %d", nEvent.Type, nEvent.State)
				}
			}
		}
	}()
	s.listening = true
	return nil
}

func (s *solrZkInstance) initCollectionsListener() (<-chan zk.Event, error) {
	s.clusterState = ClusterState{}
	collections, version, collectionsEvents, err := s.zookeeper.GetClusterStateW()
	if err != nil {
		return nil, err
	}
	s.setCollections(collections, version)
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

func (s *solrZkInstance) initLeaderElectListener() (<-chan zk.Event, error) {
	leaderEvents, err := s.zookeeper.GetLeaderElectW()
	if err != nil {
		return nil, err
	}
	return leaderEvents, nil
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
	s.logger.Printf("go-solr: zk livenodes updated %v ", s.clusterState.LiveNodes)
}

func (s *solrZkInstance) setCollections(collections map[string]Collection, version int) {
	s.clusterStateMutex.Lock()
	defer s.clusterStateMutex.Unlock()
	s.clusterState.Collections = collections
	s.clusterState.Version = version
	s.logger.Printf("go-solr: zk collections updated %v ", s.clusterState.Collections)
}
