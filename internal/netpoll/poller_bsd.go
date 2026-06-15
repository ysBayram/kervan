//go:build darwin || freebsd || openbsd

package netpoll

import (
	"golang.org/x/sys/unix"
)

type kqueuePoller struct {
	kq     int
	events []unix.Kevent_t
	buf    []Event
}

func New(cfg Config) (Poller, error) {
	kq, err := unix.Kqueue()
	if err != nil {
		return nil, err
	}
	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = 512
	}
	return &kqueuePoller{
		kq:     kq,
		events: make([]unix.Kevent_t, batchSize),
		buf:    make([]Event, batchSize),
	}, nil
}

func opToKevent(op PollOp) uint16 {
	var e uint16
	if op&PollRead != 0 {
		e |= unix.EVFILT_READ
	}
	if op&PollWrite != 0 {
		e |= unix.EVFILT_WRITE
	}
	return e
}

func (p *kqueuePoller) Add(fd int, op PollOp, userData uintptr) error {
	changes := make([]unix.Kevent_t, 0, 2)
	if op&PollRead != 0 {
		changes = append(changes, unix.Kevent_t{
			Ident:  uint64(fd),
			Filter: unix.EVFILT_READ,
			Flags:  unix.EV_ADD | unix.EV_CLEAR,
			Udata:  (*byte)(nil),
		})
	}
	if op&PollWrite != 0 {
		changes = append(changes, unix.Kevent_t{
			Ident:  uint64(fd),
			Filter: unix.EVFILT_WRITE,
			Flags:  unix.EV_ADD | unix.EV_CLEAR,
			Udata:  (*byte)(nil),
		})
	}
	_, err := unix.Kevent(p.kq, changes, nil, nil)
	return err
}

func (p *kqueuePoller) Mod(fd int, op PollOp) error {
	return p.Add(fd, op, 0)
}

func (p *kqueuePoller) Del(fd int) error {
	changes := []unix.Kevent_t{
		{Ident: uint64(fd), Filter: unix.EVFILT_READ, Flags: unix.EV_DELETE},
		{Ident: uint64(fd), Filter: unix.EVFILT_WRITE, Flags: unix.EV_DELETE},
	}
	_, err := unix.Kevent(p.kq, changes, nil, nil)
	return err
}

func (p *kqueuePoller) Wait(batch []Event) (n int, err error) {
	nevents, err := unix.Kevent(p.kq, nil, p.events, nil)
	if err != nil {
		return 0, err
	}
	for i := 0; i < nevents; i++ {
		ev := &p.events[i]
		var op PollOp
		if ev.Filter == unix.EVFILT_READ {
			op |= PollRead
		}
		if ev.Filter == unix.EVFILT_WRITE {
			op |= PollWrite
		}
		var flags uint32
		if ev.Flags&unix.EV_EOF != 0 {
			flags |= 0x01
		}
		batch[i] = Event{
			FD:    int32(ev.Ident),
			Op:    op,
			Flags: flags,
		}
	}
	return nevents, nil
}

func (p *kqueuePoller) Close() error {
	return unix.Close(p.kq)
}
