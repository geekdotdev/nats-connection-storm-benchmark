// SPDX-License-Identifier: Apache-2.0

// Command callout is a minimal NATS auth-callout responder for benchmarking nats-server.
//
//	callout keygen --seed-file PATH   create the issuer account key; print its public key
//	callout keygen --json             the same, printing {"seed","public"} on stdout for a program to capture
//	callout serve                     answer authorization requests
//
// Secrets are read from the environment or, for each NAME, from the file named by NAME_FILE
// (a mounted Kubernetes Secret); they are never taken from flags, which show up in process
// listings:
//
//	CALLOUT_SERVICE_USER, CALLOUT_SERVICE_PASSWORD  the callout's own login to the broker (AUTH account)
//	CALLOUT_ISSUER_SEED                             account seed whose public key is auth_callout.issuer
//	CALLOUT_USER_PASSWORD                           the password every benchmark user presents
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	var err error
	switch os.Args[1] {
	case "keygen":
		err = keygen(os.Args[2:], os.Stdout)
	case "serve":
		err = serve(os.Args[2:])
	default:
		usage()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "callout:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: callout keygen --seed-file PATH | callout serve [flags]")
	os.Exit(2)
}

// keygen creates the issuer account key. With --seed-file it writes the seed to a file (mode 0600, never
// overwriting) and prints only the public key, so no secret reaches the terminal or a log. With --json it prints
// {"seed": ..., "public": ...} to out, for a program that captures it (the benchmark driver, which puts the seed
// straight into a Kubernetes Secret); do not use --json on a terminal.
func keygen(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("keygen", flag.ContinueOnError)
	path := fs.String("seed-file", "", "where to write the new seed (must not exist)")
	asJSON := fs.Bool("json", false, "print the seed and the public key as JSON on stdout, for a program to capture")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if (*path == "") == !*asJSON {
		return errors.New("give exactly one of --seed-file and --json")
	}
	kp, err := nkeys.CreateAccount()
	if err != nil {
		return err
	}
	seed, err := kp.Seed()
	if err != nil {
		return err
	}
	pub, err := kp.PublicKey()
	if err != nil {
		return err
	}
	if *asJSON {
		return json.NewEncoder(out).Encode(map[string]string{"seed": string(seed), "public": pub})
	}
	f, err := os.OpenFile(*path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(append(seed, '\n')); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	fmt.Fprintln(out, pub)
	return nil
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
	url             string
	serviceUser     string
	servicePassword string
	issuerSeed      string
	userPassword    string
	workers         int
	accounts        int
	healthAddr      string

	// ready, if set, is closed once the subscriptions are in place (used by tests).
	ready chan<- struct{}
}

func serve(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	cfg := config{}
	fs.StringVar(&cfg.url, "url", envOr("NATS_URL", "nats://127.0.0.1:4222"), "broker URL (env NATS_URL)")
	fs.IntVar(&cfg.workers, "workers", 16, "queue subscribers on the callout subject")
	fs.IntVar(&cfg.accounts, "accounts", 16, "number of accounts A0..A<n-1> the broker has; user u<i> goes to account A<i>")
	fs.StringVar(&cfg.healthAddr, "health-addr", ":8080", "address for GET /healthz, empty to disable")
	if err := fs.Parse(args); err != nil {
		return err
	}
	for _, s := range []struct {
		dst  *string
		name string
	}{
		{&cfg.serviceUser, "CALLOUT_SERVICE_USER"},
		{&cfg.servicePassword, "CALLOUT_SERVICE_PASSWORD"},
		{&cfg.issuerSeed, "CALLOUT_ISSUER_SEED"},
		{&cfg.userPassword, "CALLOUT_USER_PASSWORD"},
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
	az, err := NewAuthorizer([]byte(cfg.issuerSeed), cfg.userPassword, cfg.accounts)
	if err != nil {
		return err
	}
	if cfg.workers < 1 {
		return errors.New("workers must be at least 1")
	}
	// Retry so the callout can start before the broker does.
	nc, err := nats.Connect(cfg.url,
		nats.UserInfo(cfg.serviceUser, cfg.servicePassword),
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

	slog.Info("answering authorization requests",
		"subject", AuthCalloutSubject, "workers", cfg.workers, "accounts", cfg.accounts,
		"issuer", az.IssuerPublicKey())
	<-ctx.Done()

	if health != nil {
		shutdown, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = health.Shutdown(shutdown)
		cancel()
	}
	_ = nc.Drain()
	slog.Info("stopped", "accepted", az.Accepted.Load(), "rejected", az.Rejected.Load(), "dropped", az.Dropped.Load())
	return nil
}
