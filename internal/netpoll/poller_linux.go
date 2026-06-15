//go:build linux

package netpoll

import (
	"golang.org/x/sys/unix"
)

type epollPoller struct {
	epfd   int
	events []unix.EpollEvent
}

func New(cfg Config) (Poller, error) {
	epfd, err := unix.EpollCreate1(unix.EPOLL_CLOEXEC)
	if err != nil {
		return nil, err
	}
	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = 512
	}
	return &epollPoller{
		epfd:   epfd,
		events: make([]unix.EpollEvent, batchSize),
	}, nil
}

func opToEpoll(op PollOp) uint32 {
	var e uint32
	if op&PollRead != 0 {
		e |= unix.EPOLLIN
	}
	if op&PollWrite != 0 {
		e |= unix.EPOLLOUT
	}
	return e
}

func (p *epollPoller) Add(fd int, op PollOp, userData uintptr) error {
	ev := unix.EpollEvent{
		Events: unix.EPOLLET | opToEpoll(op),
		Fd:     int32(fd),
		Pad:    int32(userData),
	}
	return unix.EpollCtl(p.epfd, unix.EPOLL_CTL_ADD, fd, &ev)
}

func (p *epollPoller) Mod(fd int, op PollOp) error {
	ev := unix.EpollEvent{
		Events: unix.EPOLLET | opToEpoll(op),
		Fd:     int32(fd),
	}
	return unix.EpollCtl(p.epfd, unix.EPOLL_CTL_MOD, fd, &ev)
}

func (p *epollPoller) Del(fd int) error {
	return unix.EpollCtl(p.epfd, unix.EPOLL_CTL_DEL, fd, nil)
}

func (p *epollPoller) Wait(batch []Event) (n int, err error) {
	nevents, err := unix.EpollWait(p.epfd, p.events, -1)
	if err != nil {
		return 0, err
	}
	for i := 0; i < nevents; i++ {
		ev := &p.events[i]
		var op PollOp
		if ev.Events&unix.EPOLLIN != 0 {
			op |= PollRead
		}
		if ev.Events&unix.EPOLLOUT != 0 {
			op |= PollWrite
		}
		batch[i] = Event{
			FD:       ev.Fd,
			Op:       op,
			Flags:    ev.Events,
			UserData: uintptr(ev.Pad),
		}
	}
	return nevents, nil
}

func (p *epollPoller) Close() error {
	return unix.Close(p.epfd)
}
