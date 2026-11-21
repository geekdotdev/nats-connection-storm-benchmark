// SPDX-License-Identifier: Apache-2.0

// Command callout is a minimal NATS auth-callout responder for benchmarking nats-server in operator mode.
//
//	callout serve   answer authorization requests
//
// A client presents a ticket: a user JWT signed by the AUTH account's scoped signing key, with the role names it asks for as
// tags. The broker has verified it and hands it to this responder, which maps the roles to subjects and answers with a user JWT
// for the tenant account. The keys come from components/provision (an init container of the callout pod makes them). Secrets
// are read from the environment or, for each NAME, from the file named by NAME_FILE (a mounted Kubernetes Secret); they are
// never taken from flags, which show up in process listings:
//
//	CALLOUT_CREDS                the callout's own login: a user JWT and seed in .creds format
//	CALLOUT_AUTH_SIGNING_SEED    the AUTH account's signing key, which signs the response claim
//	CALLOUT_TENANT_SIGNING_SEED  the tenant account's signing key, which signs the user JWT it mints
//	CALLOUT_TENANT_PUBLIC        the tenant account's public key (not secret)
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
)

func main() {
	if len(os.Args) < 2 || os.Args[1] != "serve" {
		usage()
	}
	if err := serve(os.Args[2:]); err != nil {
		fmt.Fprintln(os.Stderr, "callout:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: callout serve [--url URL] [--workers N] [--ttl DURATION] [--health-addr ADDR]")
	os.Exit(2)
}

// secret returns the value of NAME, or the contents of the file named by NAME_FILE.
func secret(name string) (string, error) {
	if v := os.Getenv(name); v != "" {
		return v, nil
	}
	if path := os.Getenv(name + "_FILE"); path != "" {
		b, err := os.ReadFile(path)
		if err != nil {
			return "", fmt.Errorf("%s_FILE: %w", name, err)
		}
		return strings.TrimRight(string(b), "\r\n"), nil
	}
	return "", fmt.Errorf("set %s or %s_FILE", name, name)
}

type config struct {
	url               string
	workers           int
	healthAddr        string
	creds             string // CALLOUT_CREDS
	authSigningSeed   string // CALLOUT_AUTH_SIGNING_SEED
	tenantSigningSeed string // CALLOUT_TENANT_SIGNING_SEED
	tenantPublic      string // CALLOUT_TENANT_PUBLIC
	ttl               time.Duration

	// ready, if set, is closed once the subscriptions are in place (used by tests).
	ready chan<- struct{}
}

func serve(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	cfg := config{}
	fs.StringVar(&cfg.url, "url", envOr("NATS_URL", "nats://127.0.0.1:4222"), "broker URL (env NATS_URL)")
	fs.IntVar(&cfg.workers, "workers", 16, "queue subscribers on the callout subject")
	fs.StringVar(&cfg.healthAddr, "health-addr", ":8080", "address for GET /healthz, empty to disable")
	fs.DurationVar(&cfg.ttl, "ttl", DefaultTTL, "how long a minted user JWT is valid; the broker then disconnects the client")
	if err := fs.Parse(args); err != nil {
		return err
	}
	for _, s := range []struct {
		dst  *string
		name string
	}{
		{&cfg.creds, "CALLOUT_CREDS"},
		{&cfg.authSigningSeed, "CALLOUT_AUTH_SIGNING_SEED"},
		{&cfg.tenantSigningSeed, "CALLOUT_TENANT_SIGNING_SEED"},
		{&cfg.tenantPublic, "CALLOUT_TENANT_PUBLIC"},
	} {
		v, err := secret(s.name)
		if err != nil {
			return err
		}
		*s.dst = v
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	return run(ctx, cfg)
}

func envOr(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

// run connects to the broker, answers requests until ctx is cancelled, then drains.
func run(ctx context.Context, cfg config) error {
	az, err := NewOperatorAuthorizer([]byte(cfg.authSigningSeed), []byte(cfg.tenantSigningSeed), cfg.tenantPublic, cfg.ttl)
	if err != nil {
		return err
	}
	// The creds are parsed here, so a malformed value fails at startup, not at the first connect attempt.
	if _, err := jwt.ParseDecoratedJWT([]byte(cfg.creds)); err != nil {
		return fmt.Errorf("CALLOUT_CREDS: %w", err)
	}
	if _, err := jwt.ParseDecoratedNKey([]byte(cfg.creds)); err != nil {
		return fmt.Errorf("CALLOUT_CREDS: %w", err)
	}
	if cfg.workers < 1 {
		return errors.New("workers must be at least 1")
	}
	// Retry so the callout can start before the broker does.
	nc, err := nats.Connect(cfg.url,
		nats.UserCredentialBytes([]byte(cfg.creds)),
		nats.Name("storm-callout"),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(500*time.Millisecond))
	if err != nil {
		return err
	}
	defer nc.Close()

	handler := func(m *nats.Msg) {
		if resp := az.Respond(m.Data); resp != nil {
			_ = m.Respond(resp)
		}
	}
	for i := 0; i < cfg.workers; i++ {
		sub, err := nc.QueueSubscribe(AuthCalloutSubject, "auth", handler)
		if err != nil {
			return err
		}
		// A burst of connections must not be dropped as a slow consumer.
		if err := sub.SetPendingLimits(-1, -1); err != nil {
			return err
		}
	}
	// Subscriptions made while the broker is unreachable are sent when the connection comes up, so only flush when
	// connected: a flush while the connection is still retrying fails after its timeout and would end the process.
	if nc.IsConnected() {
		if err := nc.Flush(); err != nil {
			return err
		}
	}
	var ready atomic.Bool
	ready.Store(true)
	if cfg.ready != nil {
		close(cfg.ready)
	}

	var health *http.Server
	if cfg.healthAddr != "" {
		mux := http.NewServeMux()
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
			if ready.Load() && nc.IsConnected() {
				fmt.Fprintln(w, "ok")
				return
			}
			http.Error(w, "not connected", http.StatusServiceUnavailable)
		})
		health = &http.Server{Addr: cfg.healthAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
		go func() {
			if err := health.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				slog.Error("health server", "err", err)
			}
		}()
	}

	slog.Info("answering authorization requests", "subject", AuthCalloutSubject, "workers", cfg.workers, "tenant", cfg.tenantPublic, "ttl", cfg.ttl.String())
	<-ctx.Done()

	if health != nil {
		shutdown, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = health.Shutdown(shutdown)
		cancel()
	}
	_ = nc.Drain()
	accepted, rejected, dropped := az.Counts()
	slog.Info("stopped", "accepted", accepted, "rejected", rejected, "dropped", dropped)
	return nil
}
