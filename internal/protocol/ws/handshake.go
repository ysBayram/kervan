package ws

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"net"

	"github.com/gobwas/ws"
)

func GenerateClientID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return fmt.Sprintf("ws-%s", base64.RawURLEncoding.EncodeToString(b))
}

func Upgrade(conn net.Conn, buf []byte) (string, error) {
	_, err := ws.Upgrade(conn)
	if err != nil {
		return "", fmt.Errorf("ws upgrade: %w", err)
	}
	return GenerateClientID(), nil
}
