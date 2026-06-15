package target

import (
	"errors"
	"hash/fnv"
	"sort"
	"sync"
)

const defaultReplicas = 100

var ErrNoHealthyTargets = errors.New("no healthy targets available")

type ringNode struct {
	hash uint32
	id   TargetID
}

type Router struct {
	mu       sync.RWMutex
	nodes    []ringNode
	routeMap map[string][]TargetID
}

func NewRouter() *Router {
	return &Router{
		routeMap: make(map[string][]TargetID),
	}
}

func (rt *Router) UpdateRoute(route string, targets []TargetID) {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	rt.routeMap[route] = targets

	rt.nodes = rt.nodes[:0]
	for _, id := range targets {
		for i := 0; i < defaultReplicas; i++ {
			h := fnv.New32a()
			key := string(id) + ":" + string(rune(i))
			h.Write([]byte(key))
			rt.nodes = append(rt.nodes, ringNode{
				hash: h.Sum32(),
				id:   id,
			})
		}
	}
	sort.Slice(rt.nodes, func(i, j int) bool {
		return rt.nodes[i].hash < rt.nodes[j].hash
	})
}

func (rt *Router) Select(route, clientID string) (TargetID, error) {
	rt.mu.RLock()
	defer rt.mu.RUnlock()

	if len(rt.nodes) == 0 {
		return "", ErrNoHealthyTargets
	}

	h := fnv.New32a()
	h.Write([]byte(route + ":" + clientID))
	key := h.Sum32()

	idx := sort.Search(len(rt.nodes), func(i int) bool {
		return rt.nodes[i].hash >= key
	})
	if idx == len(rt.nodes) {
		idx = 0
	}

	return rt.nodes[idx].id, nil
}

func (rt *Router) SelectHealthy(route, clientID string, isHealthy func(TargetID) bool) (TargetID, error) {
	rt.mu.RLock()
	defer rt.mu.RUnlock()

	if len(rt.nodes) == 0 {
		return "", ErrNoHealthyTargets
	}

	h := fnv.New32a()
	h.Write([]byte(route + ":" + clientID))
	key := h.Sum32()

	start := sort.Search(len(rt.nodes), func(i int) bool {
		return rt.nodes[i].hash >= key
	})
	if start == len(rt.nodes) {
		start = 0
	}

	for i := 0; i < len(rt.nodes); i++ {
		idx := (start + i) % len(rt.nodes)
		if isHealthy(rt.nodes[idx].id) {
			return rt.nodes[idx].id, nil
		}
	}
	return "", ErrNoHealthyTargets
}
