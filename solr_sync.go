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
	var collectionsEvents <-chan zk.Event
	var liveNodeEvents <-chan zk.Event
	connect := func() error {
		collectionsEvents, err = s.initCollectionsListener()
		if err != nil {
			return err
		}
		liveNodeEvents, err = s.initLiveNodesListener()
		if err != nil {
			return err
		}
		return nil
	}
	err = connect()
	if err != nil {
		return err
	}
	//loop forever
	go func() {
		log := s.logger
		sleepTime := s.sleepTimeMS
		logErr := func() {
			log.Printf("[Solr zk]Error connecting to zk %v sleeping: %d", err, sleepTime)
		}
		for {
			select {
			case cEvent := <-collectionsEvents:
				if cEvent.Err != nil {
					log.Printf("[Go-Solr] error on cevent %v", cEvent)
					logErr()
					sleepTime = backoff(sleepTime)
					continue
				}
				// do something if its not a session or disconnect
				if cEvent.Type == zk.EventNodeDataChanged {
					collections, version, err := s.zookeeper.GetClusterState()
					if err != nil {
						logErr()
						sleepTime = backoff(sleepTime)
						continue
					}
					s.setCollections(collections, version)
				}
				if cEvent.State < zk.StateConnected {
					s.logger.Printf("[Error] solr cluster zk disconnected  %v", cEvent)
				}
				sleepTime = s.sleepTimeMS

			case nEvent := <-liveNodeEvents:
				if nEvent.Err != nil {
					logErr()
					log.Printf("[Go-Solr] error on nevent %v", nEvent)
					sleepTime = backoff(sleepTime)
					continue
				}
				// do something if its not a session or disconnect
				if nEvent.Type == zk.EventNodeDataChanged || nEvent.Type == zk.EventNodeChildrenChanged {
					liveNodes, err := s.zookeeper.GetLiveNodes()
					if err != nil {
						logErr()
						sleepTime = backoff(sleepTime)
						continue
					}
					s.setLiveNodes(liveNodes)
				}
				if nEvent.State < zk.StateConnected {
					s.logger.Printf("[Error] solr cluster zk live nodes disconnected zkType: %v ", nEvent)
				} else {
					s.logger.Printf("go-solr: solr cluster zk live nodes state changed zkType: %d zkState: %d", nEvent.Type, nEvent.State)
				}
				sleepTime = s.sleepTimeMS
			}
			if !s.zookeeper.IsConnected() {
				err = connect()
				if err != nil {
					s.logger.Printf("[Error] zk connect err %v, sleeping %d", err, sleepTime)
					sleepTime = backoff(sleepTime)
				} else {
					sleepTime = s.sleepTimeMS
				}
			}
		}
	}()
	s.listening = true
	return nil
}

func backoff(sleepTime int) int {
	time.Sleep(time.Duration(sleepTime) * time.Millisecond)
	return sleepTime * 2
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
