package tcp

import (
	"errors"
	"golang.org/x/sys/unix"
)

var ErrShortRead = errors.New("short read")

func ReadExact(fd int, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := unix.Read(fd, buf[total:])
		if err != nil {
			if err == unix.EAGAIN {
				return total, nil
			}
			return total, err
		}
		if n == 0 {
			return total, nil
		}
		total += n
	}
	return total, nil
}

func WriteExact(fd int, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := unix.Write(fd, buf[total:])
		if err != nil {
			if err == unix.EAGAIN {
				return total, nil
			}
			return total, err
		}
		total += n
	}
	return total, nil
}
