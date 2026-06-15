package netpoll

import "golang.org/x/sys/unix"

func SetNonblock(fd int) error {
	return unix.SetNonblock(fd, true)
}

func RaiseFDLimit() error {
	var lim unix.Rlimit
	if err := unix.Getrlimit(unix.RLIMIT_NOFILE, &lim); err != nil {
		return err
	}
	lim.Cur = lim.Max
	return unix.Setrlimit(unix.RLIMIT_NOFILE, &lim)
}
