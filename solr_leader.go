package solr

import ()

func findLeader(key string, cs *Collection) (string, error) {
	composite, err := NewCompositeKey(key)
	if err != nil {
		return "", err
	}
	shardKeyHash, err := Hash(composite)
	leader := ""
	for _, shard := range cs.Shards {
		hashRange, err := ConvertToHashRange(shard.Range)
		if err != nil {
			return leader, err
		}
		if shardKeyHash > hashRange.Low && shardKeyHash < hashRange.High {
			leader = findLeaderFromReplicas(shard.Replicas)
		}
	}
	return leader, nil
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
