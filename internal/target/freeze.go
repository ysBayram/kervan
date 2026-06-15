package target

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/ysBayram/kervan/internal/flush"
	"github.com/ysBayram/kervan/internal/session"
	"github.com/ysBayram/kervan/pkg/buffer"
)

var ErrTargetNotFound = errors.New("target not found")

type FreezeReason int

const (
	reasonProbe  FreezeReason = iota
	reasonPassive
	reasonAdmin
	reasonRemote
)

type FreezeController struct {
	registry *Registry
	sessions *session.SessionManager
	flush    *flush.Scheduler
	pool     *buffer.Pool
}

func NewFreezeController(registry *Registry, sessions *session.SessionManager, flush *flush.Scheduler, pool *buffer.Pool) *FreezeController {
	return &FreezeController{
		registry: registry,
		sessions: sessions,
		flush:    flush,
		pool:     pool,
	}
}

func (fc *FreezeController) FreezeTarget(targetID TargetID) {
	t, ok := fc.registry.GetTarget(targetID)
	if !ok {
		return
	}

	t.CASState(TargetHealthy, TargetUnhealthy)
	slog.Info("freeze target", "targetID", targetID)

	fc.sessions.RangeAll(func(sess *session.ClientSession) bool {
		if sess.TargetID != string(targetID) {
			return true
		}
		fc.freezeSession(sess)
		return true
	})
}

func (fc *FreezeController) freezeSession(sess *session.ClientSession) {
	if !sess.CASState(session.StateActive, session.StateFrozen) {
		return
	}
	if sess.UpstreamFD >= 0 {
		slog.Debug("freeze session: upstream fd closed", "clientID", sess.ClientID)
		sess.UpstreamFD = -1
	}
}

func (fc *FreezeController) ThawTarget(oldTargetID TargetID, newTargetAddr string) error {
	t, ok := fc.registry.GetTarget(oldTargetID)
	if !ok {
		return ErrTargetNotFound
	}

	t.CASState(TargetUnhealthy, TargetHealthy)
	slog.Info("thaw target", "targetID", oldTargetID, "newAddr", newTargetAddr)

	fc.sessions.RangeAll(func(sess *session.ClientSession) bool {
		if sess.TargetID != string(oldTargetID) {
			return true
		}
		fc.thawSession(sess, newTargetAddr)
		return true
	})
	return nil
}

func (fc *FreezeController) thawSession(sess *session.ClientSession, addr string) {
	if !sess.CASState(session.StateFrozen, session.StateDraining) {
		return
	}

	slog.Debug("thaw session: draining buffer", "clientID", sess.ClientID)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- fc.flush.Drain(sess, fc.pool)
	}()

	select {
	case err := <-done:
		if err != nil {
			slog.Error("drain error, returning to frozen", "clientID", sess.ClientID, "error", err)
			sess.CASState(session.StateDraining, session.StateFrozen)
			return
		}
	case <-ctx.Done():
		slog.Error("drain timeout, returning to frozen", "clientID", sess.ClientID)
		sess.CASState(session.StateDraining, session.StateFrozen)
		return
	}

	if sess.RingBuffer.Len() == 0 {
		sess.CASState(session.StateDraining, session.StateActive)
		slog.Info("session thawed", "clientID", sess.ClientID)
	}
}
