//go:build k8s

package discovery

import (
	"context"
	"fmt"

	"github.com/ysBayram/kervan/internal/target"
)

type KubernetesDiscovery struct {
	namespace string
	service   string
}

func NewKubernetesDiscovery(namespace, service string) *KubernetesDiscovery {
	return &KubernetesDiscovery{
		namespace: namespace,
		service:   service,
	}
}

func (kd *KubernetesDiscovery) Watch(ctx context.Context) (<-chan DiscoveryEvent, error) {
	ch := make(chan DiscoveryEvent, 16)
	go kd.watchLoop(ctx, ch)
	return ch, nil
}

func (kd *KubernetesDiscovery) watchLoop(ctx context.Context, ch chan<- DiscoveryEvent) {
	// Stub: watch core/v1 Endpoints for namespace/service
	// Map EndpointSubset → Target{ID: podIP:port, Addr: ...}
	_ = fmt.Sprintf("%s/%s", kd.namespace, kd.service)

	<-ctx.Done()
}
