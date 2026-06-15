package proxy

import (
	"context"
	"log/slog"
	"net"
	"syscall"

	"time"

	"github.com/ysBayram/kervan/internal/netpoll"
	"github.com/ysBayram/kervan/internal/session"
	"github.com/ysBayram/kervan/internal/protocol/ws"
)

func (s *Server) acceptLoop(ctx context.Context, ln net.Listener, proto session.Protocol) error {
	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return nil
			default:
				slog.Error("accept error", "error", err)
				continue
			}
		}

		// Extract fd from connection
		fd, err := extractFD(conn)
		if err != nil {
			conn.Close()
			slog.Error("extract fd", "error", err)
			continue
		}

		if err := netpoll.SetNonblock(fd); err != nil {
			conn.Close()
			slog.Error("set nonblock", "error", err)
			continue
		}

		clientID := generateClientID(fd, s.nodeID)

		// WebSocket upgrade before session creation
		if proto == session.ProtocolWebSocket {
			id, err := ws.Upgrade(conn, nil)
			if err != nil {
				conn.Close()
				slog.Error("ws upgrade", "error", err)
				continue
			}
			clientID = id
		}

		sess := s.createSession(fd, clientID, proto)
		if err := s.sessions.Insert(sess); err != nil {
			conn.Close()
			slog.Error("insert session", "error", err)
			continue
		}

		s.fdReg.Register(fd, sess)
		s.metrics.SessionsActive.Inc()

		if err := s.poller.Add(fd, netpoll.PollRead, uintptr(fd)); err != nil {
			slog.Error("poller add", "error", err)
			s.closeSession(sess)
			continue
		}

		slog.Debug("session accepted",
			"clientID", clientID,
			"fd", fd,
			"protocol", proto,
		)
	}
}

func extractFD(conn net.Conn) (int, error) {
	rawConn, err := conn.(syscall.Conn).SyscallConn()
	if err != nil {
		return 0, err
	}
	var fd int
	rawConn.Control(func(fdPtr uintptr) {
		fd = int(fdPtr)
	})
	return fd, nil
}

func (s *Server) createSession(fd int, clientID string, proto session.Protocol) *session.ClientSession {
	return &session.ClientSession{
		ClientID:   clientID,
		ClientFD:   fd,
		State:      session.StateCreated,
		Protocol:   proto,
		CreatedAt:  time.Now().Unix(),
	}
}
