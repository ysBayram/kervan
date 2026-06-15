package discovery

import (
	"context"

	"github.com/ysBayram/kervan/internal/target"
)

type Discovery interface {
	Watch(ctx context.Context) (<-chan DiscoveryEvent, error)
}

type DiscoveryEvent struct {
	Targets []target.Target
	Route   string
}
