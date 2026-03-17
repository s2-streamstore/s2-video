package main

import (
	"context"
	"log"
	"log/slog"
	"math"
	"net/http"
	"os"
	"strings"
	"os/signal"
	"syscall"
	"time"

	"github.com/s2-streamstore/s2-sdk-go/s2"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	basin := os.Getenv("S2_BASIN")
	if basin == "" {
		log.Fatal("S2_BASIN environment variable is required")
	}

	client := s2.NewFromEnvironment(&s2.ClientOptions{
		Logger:         logger,
		RequestTimeout: 30 * time.Second,
		RetryConfig: &s2.RetryConfig{
			MaxAttempts:       math.MaxInt,
			AppendRetryPolicy: s2.AppendRetryPolicyAll,
		},
	})

	basinClient := client.Basin(basin)
	hub := NewHub(basinClient, logger)
	registry := NewRoomRegistry()
	hub.registry = registry

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
	mux.HandleFunc("/ws", hub.HandleWebSocket)
	mux.HandleFunc("/ws/inspect", hub.HandleInspect)
	mux.HandleFunc("/api/rooms", registry.HandleCreateRoom)
	mux.HandleFunc("/api/rooms/", func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/export") {
			hub.HandleExport(w, r)
		} else {
			hub.HandleTimeline(w, r)
		}
	})
	mux.Handle("/", http.FileServer(http.Dir("static")))

	addr := os.Getenv("ADDR")
	if addr == "" {
		addr = ":8080"
	}

	srv := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		logger.Info("server starting", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	hub.Shutdown()
	srv.Shutdown(shutdownCtx)
}
