package proxy

import (
	"log/slog"
	"time"

	"errors"

	"github.com/ysBayram/kervan/internal/buffer"
	"github.com/ysBayram/kervan/internal/netpoll"
	"github.com/ysBayram/kervan/internal/session"
	"github.com/ysBayram/kervan/internal/protocol/ws"
	"golang.org/x/sys/unix"
)

func (s *Server) handleClientRead(ev netpoll.Event, local *workerLocal) {
	sess := s.fdReg.Lookup(int(ev.FD))
	if sess == nil {
		return
	}
	cs, ok := sess.(*session.ClientSession)
	if !ok {
		return
	}

	n, err := unix.Read(cs.ClientFD, local.readBuf)
	if err == unix.EAGAIN {
		return
	}
	if n == 0 || err != nil {
		if err != nil && err != unix.EAGAIN && err != unix.ECONNRESET {
			slog.Error("client read error", "clientID", cs.ClientID, "error", err)
		}
		s.closeSession(cs)
		return
	}

	cs.Touch()
	s.forwardClientPayload(cs, local.readBuf[:n])
}

func (s *Server) handleUpstreamWrite(ev netpoll.Event) {
	sess := s.fdReg.Lookup(int(ev.FD))
	if sess == nil {
		return
	}
	cs, ok := sess.(*session.ClientSession)
	if !ok || cs.RingBuffer == nil {
		return
	}
	if err := s.flushScheduler.Drain(cs, s.bytePool); err != nil {
		slog.Debug("flush drain error", "clientID", cs.ClientID, "error", err)
	}
}

func (s *Server) forwardClientPayload(cs *session.ClientSession, payload []byte) error {
	state := cs.GetState()
	switch state {
	case session.StateActive:
		return s.writeUpstream(cs, payload)
	case session.StateFrozen, session.StateDraining:
		return s.enqueueBuffer(cs, payload)
	default:
		return session.ErrSessionClosed
	}
}

func (s *Server) enqueueBuffer(cs *session.ClientSession, payload []byte) error {
	if cs.RingBuffer == nil {
		slog.Debug("no ring buffer attached", "clientID", cs.ClientID)
		return nil
	}
	err := cs.RingBuffer.Enqueue(payload)
	if errors.Is(err, buffer.ErrWouldBlock) {
		slog.Debug("buffer full, parking client read", "clientID", cs.ClientID)
	}
	if errors.Is(err, buffer.ErrBufferFull) && cs.Protocol == session.ProtocolWebSocket {
		ws.SendClose(nil, 1013)
	}
	return err
}

func (s *Server) writeUpstream(cs *session.ClientSession, payload []byte) error {
	if cs.UpstreamFD < 0 {
		return unix.EBADF
	}
	_, err := unix.Write(cs.UpstreamFD, payload)
	if err != nil {
		slog.Error("upstream write error", "clientID", cs.ClientID, "error", err)
		return err
	}
	return nil
}

func (s *Server) closeSession(cs *session.ClientSession) {
	cs.State.Store(uint32(session.StateClosed))

	if cs.UpstreamFD >= 0 {
		unix.Close(cs.UpstreamFD)
	}
	unix.Close(cs.ClientFD)

	s.fdReg.Unregister(cs.ClientFD)
	s.fdReg.Unregister(cs.UpstreamFD)
	s.sessions.Remove(cs.ClientID)
	s.metrics.SessionsActive.Dec()

	slog.Debug("session closed", "clientID", cs.ClientID, "duration", time.Since(time.Unix(cs.CreatedAt, 0)), "createdAt", cs.CreatedAt)
}

func (s *Server) handleUpstreamError(cs *session.ClientSession, err error) {
	slog.Error("upstream error", "clientID", cs.ClientID, "error", err)
	s.closeSession(cs)
}

func (s *Server) DialUpstream(addr string) (int, error) {
	// Phase 1: simple TCP dial
	// Phase 3: TargetRegistry-based with health
	return 0, unix.EOPNOTSUPP
}
