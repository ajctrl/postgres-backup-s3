package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"

	"github.com/bartels/postgres-backup-s3/internal/backup"
	"github.com/bartels/postgres-backup-s3/internal/storage"
)

func main() { os.Exit(run()) }

func run() int {
	args := os.Args[1:]
	command := "run"
	if len(args) > 0 {
		command, args = args[0], args[1:]
	}
	if command == "--help" || command == "-h" || command == "help" {
		fmt.Println("Usage: postgres-backup-s3 [run | backup | restore [timestamp | --version-id ID]]")
		return 0
	}
	if command != "run" && command != "backup" && command != "restore" || command != "restore" && len(args) != 0 {
		fmt.Fprintln(os.Stderr, "Usage: postgres-backup-s3 [run | backup | restore [timestamp | --version-id ID]]")
		return 2
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(signals)
	var signalCode atomic.Int32
	go func() {
		select {
		case sig := <-signals:
			signalCode.Store(int32(128 + sig.(syscall.Signal)))
			cancel()
		case <-ctx.Done():
		}
	}()
	c, err := backup.LoadConfig()
	if err == nil {
		var store *storage.S3
		store, err = storage.New(ctx, c.Storage)
		if err == nil {
			app := backup.New(c, store, os.Stdout, os.Stderr)
			switch command {
			case "run":
				err = app.Run(ctx)
			case "backup":
				err = app.Backup(ctx)
			case "restore":
				err = app.Restore(ctx, args)
			}
		}
	}
	if code := signalCode.Load(); code != 0 {
		return int(code)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}
	return 0
}
