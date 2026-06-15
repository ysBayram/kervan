package node

import (
	"os"
	"time"
)

type Identity struct {
	NodeID    string
	StartTime int64
	Addr      string
}

func LoadIdentity(nodeID string) *Identity {
	if nodeID == "" {
		nodeID = os.Getenv("POD_NAME")
	}
	if nodeID == "" {
		host, _ := os.Hostname()
		nodeID = host
	}
	return &Identity{
		NodeID:    nodeID,
		StartTime: time.Now().Unix(),
	}
}
