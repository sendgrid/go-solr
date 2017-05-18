package solr

import ()

const (
	activeState string = "active"
)

func findLeader(key string, cs *Collection) (string, error) {
	replicas, err := findReplicas(key, cs)
	if err != nil {
		return "", err
	}
	return findLeaderFromReplicas(replicas), nil
}

func findLiveReplicaUrls(key string, cs *Collection) ([]string, error) {
	replicas, err := findReplicas(key, cs)
	if err != nil {
		return nil, err
	}
	var replcaUrls []string = make([]string, 0, len(replicas))
	for _, replica := range replicas {
		if replica.State == activeState {
			replcaUrls = append(replcaUrls, replica.BaseURL)
		}
	}
	return replcaUrls, nil
}

func findReplicas(key string, cs *Collection) (map[string]Replica, error) {
	composite, err := NewCompositeKey(key)
	if err != nil {
		return nil, err
	}
	shardKeyHash, err := Hash(composite)
	var replicas map[string]Replica
	for _, shard := range cs.Shards {
		hashRange, err := ConvertToHashRange(shard.Range)
		if err != nil {
			return nil, err
		}
		if shardKeyHash >= hashRange.Low && shardKeyHash <= hashRange.High {
			replicas = shard.Replicas
			break
		}
	}
	return replicas, nil
}

func findLeaderFromReplicas(replicas map[string]Replica) string {
	leader := ""
	for _, replica := range replicas {
		if replica.Leader == "true" {
			leader = replica.BaseURL
			break
		}
	}
	return leader
}
