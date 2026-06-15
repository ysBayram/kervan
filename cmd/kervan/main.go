package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/ysBayram/kervan/internal/config"
	"github.com/ysBayram/kervan/internal/proxy"
)

func main() {
	cfgPath := "configs/kervan.yaml"
	if len(os.Args) > 1 {
		cfgPath = os.Args[1]
	}

	cfg, err := config.Load(cfgPath)
	if err != nil {
		log.Fatalf("config load: %v", err)
	}

	srv, err := proxy.NewServer(cfg)
	if err != nil {
		log.Fatalf("server init: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := srv.Run(ctx); err != nil {
		log.Fatalf("server run: %v", err)
	}
}
