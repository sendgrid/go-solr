package solr

func (s *solrInstance) pollForState() {
	s.zookeeper.PollForClusterState(s.zkRoot, func(clusterState *ClusterState, err error) {
		if err != nil {
			return
		}
		s.setClusterState(clusterState)
	})
}
func (s *solrInstance) GetClusterState() (*ClusterState, error) {
	s.clusterStateMutex.Lock()
	defer s.clusterStateMutex.Unlock()
	return s.zookeeper.GetClusterState(s.zkRoot)
}

func (s *solrInstance) setClusterState(cs *ClusterState) {
	s.clusterStateMutex.Lock()
	defer s.clusterStateMutex.Unlock()
	s.clusterState = cs
}

func (s *solrInstance) getNextNode() string {
	s.currentNodeMutex.Lock()
	defer s.currentNodeMutex.Unlock()
	node := s.clusterState.LiveNodes[s.currentNode]
	if s.currentNode == len(s.clusterState.LiveNodes)-1 {
		s.currentNode = 0
	} else {
		s.currentNode++
	}

	return node
}
