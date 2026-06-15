package discovery

import (
	"context"
	"os"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/ysBayram/kervan/internal/target"
)

type StaticDiscovery struct {
	path    string
	pollInterval time.Duration
}

type staticConfig struct {
	Targets []struct {
		ID     string `yaml:"id"`
		Addr   string `yaml:"addr"`
		Route  string `yaml:"route"`
		Weight int32  `yaml:"weight"`
	} `yaml:"targets"`
}

func NewStaticDiscovery(path string) *StaticDiscovery {
	return &StaticDiscovery{
		path:         path,
		pollInterval: 30 * time.Second,
	}
}

func (sd *StaticDiscovery) Watch(ctx context.Context) (<-chan DiscoveryEvent, error) {
	ch := make(chan DiscoveryEvent, 1)

	go func() {
		ticker := time.NewTicker(sd.pollInterval)
		defer ticker.Stop()

		for {
			events, err := sd.load()
			if err == nil && len(events) > 0 {
				for _, e := range events {
					select {
					case ch <- e:
					case <-ctx.Done():
						return
					}
				}
			}

			select {
			case <-ticker.C:
			case <-ctx.Done():
				return
			}
		}
	}()

	return ch, nil
}

func (sd *StaticDiscovery) load() ([]DiscoveryEvent, error) {
	data, err := os.ReadFile(sd.path)
	if err != nil {
		return nil, err
	}

	var cfg staticConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	routeMap := make(map[string][]target.Target)
	for _, t := range cfg.Targets {
		routeMap[t.Route] = append(routeMap[t.Route], target.Target{
			ID:     target.TargetID(t.ID),
			Addr:   t.Addr,
			Route:  t.Route,
			Weight: t.Weight,
		})
	}

	var events []DiscoveryEvent
	for route, targets := range routeMap {
		events = append(events, DiscoveryEvent{
			Route:   route,
			Targets: targets,
		})
	}
	return events, nil
}
