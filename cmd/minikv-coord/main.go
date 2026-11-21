package main

import (
	"context"
	"embed"
	"flag"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/lucky23-afk/minikv/internal/api"
)

//go:embed web/*
var embeddedWeb embed.FS

func main() {
	httpAddr := flag.String("addr", ":8080", "HTTP dashboard and API listen address")
	initialNodes := flag.String("nodes", "", "Comma-separated initial nodes in id=tcp_addr=http_addr format")
	vnodes := flag.Int("vnodes", 128, "Virtual nodes per storage node")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	// Strip "web" subfolder from embedded filesystem
	subFS, err := fs.Sub(embeddedWeb, "web")
	if err != nil {
		logger.Error("failed to load embedded dashboard", "err", err)
		os.Exit(1)
	}

	coord := api.NewCoordinator(api.Config{
		VNodesPerNode: *vnodes,
		Replicas:      3,
		DashboardFS:   http.FS(subFS),
	}, logger)

	// Parse initial nodes e.g. "node-1=127.0.0.1:7000=127.0.0.1:7001,node-2=127.0.0.1:7010=127.0.0.1:7011"
	if *initialNodes != "" {
		for _, nStr := range strings.Split(*initialNodes, ",") {
			parts := strings.Split(strings.TrimSpace(nStr), "=")
			if len(parts) == 3 {
				coord.AddNode(parts[0], parts[1], parts[2])
				logger.Info("registered initial node", "id", parts[0], "tcp", parts[1], "http", parts[2])
			}
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start Gossip failure detector in background
	go coord.Gossiper().Start(ctx)

	server := &http.Server{
		Addr:    *httpAddr,
		Handler: coord.Routes(),
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		logger.Info("shutting down coordinator")
		cancel()
		shutdownCtx, sCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer sCancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	logger.Info("miniKV coordinator and dashboard running", "addr", *httpAddr)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		fmt.Fprintf(os.Stderr, "Server error: %v\n", err)
		os.Exit(1)
	}
}
