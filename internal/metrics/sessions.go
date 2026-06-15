package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	SessionsActive = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "kervan_sessions_active",
		Help: "Current active client sessions",
	})
)

func init() {
	prometheus.MustRegister(SessionsActive)
}
