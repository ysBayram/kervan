package session

import (
	"hash/fnv"
)

const DefaultShardCount = 256

func ShardIndex(clientID string, mask uint32) uint32 {
	h := fnv.New32a()
	h.Write([]byte(clientID))
	return h.Sum32() & mask
}
