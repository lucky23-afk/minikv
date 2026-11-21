package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/lucky23-afk/minikv/internal/node"
	"github.com/lucky23-afk/minikv/internal/wal"
)

func main() {
	id := flag.String("id", "node-1", "Unique node ID")
	tcpAddr := flag.String("addr", "127.0.0.1:7000", "TCP address to listen on")
	httpAddr := flag.String("http-addr", "127.0.0.1:7001", "Internal HTTP address to listen on")
	dataDir := flag.String("data-dir", "./data/node-1", "Directory for WAL and snapshots")
	maxEntries := flag.Int("max-entries", 0, "Max entries per shard for LRU (0=unlimited)")
	maxBytes := flag.Int64("max-bytes", 0, "Max bytes per shard for LRU (0=unlimited)")
	fsync := flag.String("fsync", "every-second", "WAL fsync policy: always, every-second, none")
	flag.Parse()

	var policy wal.FsyncPolicy
	switch strings.ToLower(*fsync) {
	case "always":
		policy = wal.FsyncAlways
	case "none":
		policy = wal.FsyncNone
	default:
		policy = wal.FsyncEverySecond
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	cfg := node.Config{
		ID:          *id,
		TCPAddr:     *tcpAddr,
		HTTPAddr:    *httpAddr,
		DataDir:     *dataDir,
		MaxEntries:  *maxEntries,
		MaxBytes:    *maxBytes,
		FsyncPolicy: policy,
	}

	logger.Info("starting minikv storage node", "id", cfg.ID, "tcp", cfg.TCPAddr, "http", cfg.HTTPAddr)
	if err := node.RunUntilSignal(cfg, logger); err != nil {
		fmt.Fprintf(os.Stderr, "Node error: %v\n", err)
		os.Exit(1)
	}
}
