package api

import "errors"

type SessionState uint32

const (
	SessionCreated  SessionState = 0
	SessionActive   SessionState = 1
	SessionFrozen   SessionState = 2
	SessionDraining SessionState = 3
	SessionClosed   SessionState = 4
)

var (
	ErrSessionClosed  = errors.New("session closed")
	ErrBufferFull     = errors.New("buffer full")
	ErrFrameTooLarge  = errors.New("frame exceeds max size")
	ErrTargetNotFound = errors.New("target not found")
	ErrLeaseHeldByOther = errors.New("lease held by another node")
	ErrSplitBrain     = errors.New("split-brain: client ID owned by another node")
	ErrWouldBlock     = errors.New("operation would block")
)
