package ws

import (
	"io"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
)

func ReadFrame(r io.Reader, buf []byte) (ws.OpCode, []byte, error) {
	h, err := ws.ReadHeader(r)
	if err != nil {
		return 0, nil, err
	}
	if h.Length > uint64(len(buf)) {
		return 0, nil, ws.ErrHeaderLengthUnexpected
	}
	payload := buf[:h.Length]
	if _, err := io.ReadFull(r, payload); err != nil {
		return 0, nil, err
	}
	if h.Masked {
		ws.Cipher(payload, h.Mask, 0)
	}
	return h.OpCode, payload, nil
}

func WriteFrame(w io.Writer, op ws.OpCode, payload []byte) error {
	return wsutil.WriteServerMessage(w, op, payload)
}

func SendClose(w io.Writer, code ws.StatusCode) error {
	return wsutil.WriteServerMessage(w, ws.OpClose, []byte{byte(code >> 8), byte(code)})
}
