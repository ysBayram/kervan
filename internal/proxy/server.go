package proxy

import (
	"context"
	"errors"
	"net"
	"runtime"
	"strconv"
	"sync"

	"github.com/ysBayram/kervan/internal/config"
	"github.com/ysBayram/kervan/internal/metrics"
	"github.com/ysBayram/kervan/internal/netpoll"
	"github.com/ysBayram/kervan/internal/session"
)

type workerLocal struct {
	readBuf []byte
}

type Server struct {
	cfg      *config.Config
	poller   netpoll.Poller
	sessions *session.SessionManager
	fdReg    *netpoll.FDRegistry
	metrics  *metrics.Registry
	nodeID   string
	mu       sync.Mutex
	listeners []net.Listener
}

func NewServer(cfg *config.Config) (*Server, error) {
	reg := metrics.NewRegistry()

	poller, err := netpoll.New(netpoll.Config{BatchSize: 512})
	if err != nil {
		return nil, err
	}

	rid, err := raiseFDLimit()
	if err != nil {
		return nil, err
	}

	return &Server{
		cfg:      cfg,
		poller:   poller,
		sessions: session.NewManager(cfg.Session),
		fdReg:    netpoll.NewFDRegistry(int(rid.Cur)),
		metrics:  reg,
		nodeID:   cfg.NodeID,
	}, nil
}

func raiseFDLimit() (*netpoll.Rlimit, error) {
	return netpoll.RaiseFDLimit()
}

func generateClientID(fd int, nodeID string) string {
	return strconv.Itoa(fd) + "@" + nodeID
}

func (s *Server) Run(ctx context.Context) error {
	// Start listener for each config
	var listenerWg sync.WaitGroup
	for _, lc := range s.cfg.Listeners {
		ln, err := net.Listen("tcp", lc.Bind)
		if err != nil {
			return err
		}
		s.mu.Lock()
		s.listeners = append(s.listeners, ln)
		s.mu.Unlock()

		proto := session.ProtocolTCP
		if lc.Protocol == "websocket" || lc.Protocol == "ws" {
			proto = session.ProtocolWebSocket
		}

		listenerWg.Add(1)
		go func(ln net.Listener, proto session.Protocol) {
			defer listenerWg.Done()
			s.acceptLoop(ctx, ln, proto)
		}(ln, proto)
	}

	// Start poller workers
	for i := 0; i < runtime.GOMAXPROCS(0); i++ {
		go s.pollerWorker(ctx, i)
	}

	<-ctx.Done()
	return s.Shutdown()
}

func (s *Server) Shutdown() error {
	s.mu.Lock()
	listeners := s.listeners
	s.listeners = nil
	s.mu.Unlock()

	for _, ln := range listeners {
		ln.Close()
	}
	return s.poller.Close()
}

func (s *Server) pollerWorker(ctx context.Context, id int) {
	local := workerLocal{readBuf: make([]byte, s.cfg.Session.ReadBufferSize)}
	events := make([]netpoll.Event, 512)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		n, err := s.poller.Wait(events)
		if err != nil {
			if errors.Is(err, netpoll.ErrPollClosed) {
				return
			}
			continue
		}
		for i := 0; i < n; i++ {
			s.dispatch(events[i], &local)
		}
	}
}

func (s *Server) dispatch(ev netpoll.Event, local *workerLocal) {
	switch {
	case ev.Op&netpoll.PollRead != 0:
		if ev.Flags&0x01 != 0 {
			sess := s.fdReg.Lookup(int(ev.FD))
			if sess != nil {
				if cs, ok := sess.(*session.ClientSession); ok {
					s.closeSession(cs)
				}
			}
			return
		}
		s.handleClientRead(ev, local)
	case ev.Op&netpoll.PollWrite != 0:
		s.handleUpstreamWrite(ev)
	}
}
