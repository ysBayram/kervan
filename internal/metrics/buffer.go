package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	BufferDepth = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "kervan_buffer_depth",
		Help:    "Ring buffer depth",
		Buckets: prometheus.DefBuckets,
	})

	BufferDroppedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kervan_buffer_dropped_total",
		Help: "Payloads dropped by backpressure policy",
	}, []string{"policy"})

	FlushTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "kervan_flush_total",
		Help: "Total payloads flushed to upstream",
	})

	FlushLatencySeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "kervan_flush_latency_seconds",
		Help:    "Flush latency from enqueue to successful write",
		Buckets: prometheus.DefBuckets,
	})

	PoolHitRatio = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "kervan_pool_hit_ratio",
		Help: "sync.Pool reuse effectiveness",
	}, func() float64 { return 1.0 })
)

func init() {
	prometheus.MustRegister(BufferDepth, BufferDroppedTotal, FlushTotal, FlushLatencySeconds, PoolHitRatio)
}
