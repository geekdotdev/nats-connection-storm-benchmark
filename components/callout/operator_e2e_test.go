// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

func writeFile(path, content string) error { return os.WriteFile(path, []byte(content), 0o600) }

// provisioned is the output of components/provision, written as files: one directory per object.
type provisioned struct {
	configMap, calloutSecret, generatorSecret string // directories
}

func (p provisioned) read(t *testing.T, dir, name string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(string(b))
}

// provision builds components/provision and runs it with --out-dir, so the callout is tested against the
// files the real init container would write (the contract between the two components), not a copy of them.
func provision(t *testing.T) provisioned {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "provision")
	build := exec.Command("go", "build", "-o", bin, ".")
	build.Dir = filepath.Join("..", "provision")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("building components/provision: %v\n%s", err, out)
	}
	out := t.TempDir()
	// The ConfigMap's files go in one directory with the server configuration, so the includes resolve beside it.
	run := exec.Command(bin, "--out-dir", out, "--mount-dir", filepath.Join(out, "configmap-storm-operator"))
	if b, err := run.CombinedOutput(); err != nil {
		t.Fatalf("provision: %v\n%s", err, b)
	}
	return provisioned{
		configMap:       filepath.Join(out, "configmap-storm-operator"),
		calloutSecret:   filepath.Join(out, "secret-storm-callout-auth"),
		generatorSecret: filepath.Join(out, "secret-storm-generator-auth"),
	}
}

func startOperatorBroker(t *testing.T, p provisioned) *server.Server {
	t.Helper()
	conf := filepath.Join(p.configMap, "server.conf")
	body := "listen: \"127.0.0.1:-1\"\nserver_name: T\nwebsocket { listen: \"127.0.0.1:-1\"\n  no_tls: true }\ninclude \"operator.conf\"\ninclude \"resolver-memory.conf\"\nauthorization { timeout: 10s }\n"
	if err := writeFile(conf, body); err != nil {
		t.Fatal(err)
	}
	opts, err := server.ProcessConfigFile(conf)
	if err != nil {
		t.Fatal(err)
	}
	opts.NoLog, opts.NoSigs = true, true
	s, err := server.NewServer(opts)
	if err != nil {
		t.Fatal(err)
	}
	go s.Start()
	t.Cleanup(s.Shutdown)
	if !s.ReadyForConnections(10 * time.Second) {
		t.Fatal("broker not ready")
	}
	return s
}

func startOperatorCallout(t *testing.T, p provisioned, url string, ttl time.Duration) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, config{
			url: url, workers: 4, ttl: ttl, ready: ready,
			creds:             p.read(t, p.calloutSecret, "callout.creds"),
			authSigningSeed:   p.read(t, p.calloutSecret, "auth-signing.seed"),
			tenantSigningSeed: p.read(t, p.calloutSecret, "tenant-signing.seed"),
			tenantPublic:      p.read(t, p.calloutSecret, "tenant.pub"),
		})
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("callout: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("callout did not stop")
		}
	})
	select {
	case <-ready:
	case err := <-done:
		t.Fatalf("callout exited: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("callout not ready")
	}
}

// ticket is what the load generator presents: a user JWT signed by the scoped key, with no permissions of its own.
func ticket(t *testing.T, p provisioned, tags ...string) nats.Option {
	t.Helper()
	scoped, err := nkeys.FromSeed([]byte(p.read(t, p.generatorSecret, "scoped.seed")))
	if err != nil {
		t.Fatal(err)
	}
	ukp, _ := nkeys.CreateUser()
	upub, _ := ukp.PublicKey()
	uc := jwt.NewUserClaims(upub)
	uc.SetScoped(true)
	uc.IssuerAccount = p.read(t, p.generatorSecret, "auth.pub")
	uc.Name = "storm-0"
	uc.Tags.Add(tags...)
	tok, err := uc.Encode(scoped)
	if err != nil {
		t.Fatal(err)
	}
	return nats.UserJWT(
		func() (string, error) { return tok, nil },
		func(nonce []byte) ([]byte, error) { return ukp.Sign(nonce) },
	)
}

func accountOf(t *testing.T, s *server.Server, clientName string) string {
	t.Helper()
	ci, err := s.Connz(&server.ConnzOptions{Username: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range ci.Conns {
		if c.Name == clientName {
			return c.Account
		}
	}
	t.Fatalf("the broker has no client named %q", clientName)
	return ""
}

// TestOperatorModeEndToEnd runs the callout in operator mode against a real nats-server configured from the
// files components/provision writes, and sends tickets through it.
func TestOperatorModeEndToEnd(t *testing.T) {
	p := provision(t)
	s := startOperatorBroker(t, p)
	startOperatorCallout(t, p, s.ClientURL(), DefaultTTL)

	t.Run("a ticket's roles become the user's permissions in the tenant account", func(t *testing.T) {
		errs := make(chan string, 4)
		nc, err := nats.Connect(s.ClientURL(), ticket(t, p, "app-feed-publish-orders", "app-feed-subscribe-alarms"),
			nats.Name("zp-1"), nats.NoReconnect(),
			nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) { errs <- err.Error() }))
		if err != nil {
			t.Fatalf("connect: %v", err)
		}
		defer nc.Close()
		if got, want := accountOf(t, s, "zp-1"), p.read(t, p.calloutSecret, "tenant.pub"); got != want {
			t.Fatalf("account %q, want the tenant account %q", got, want)
		}

		if _, err := nc.SubscribeSync("app.alarms"); err != nil {
			t.Fatal(err)
		}
		if err := nc.Publish("app.orders", []byte("x")); err != nil {
			t.Fatal(err)
		}
		if err := nc.Flush(); err != nil {
			t.Fatal(err)
		}
		select {
		case e := <-errs:
			t.Fatalf("an allowed operation was refused: %s", e)
		case <-time.After(300 * time.Millisecond):
		}

		// Everything else is refused: publishing where only subscribing is granted, and the reverse.
		_ = nc.Publish("app.alarms", []byte("x"))
		_, _ = nc.SubscribeSync("app.orders")
		_ = nc.Flush()
		for i := 0; i < 2; i++ {
			select {
			case e := <-errs:
				if !strings.Contains(strings.ToLower(e), "permissions violation") {
					t.Fatalf("unexpected error %q", e)
				}
			case <-time.After(2 * time.Second):
				t.Fatalf("only %d of 2 forbidden operations were refused", i)
			}
		}
	})

	t.Run("a ticket with no recognized roles connects and can do nothing", func(t *testing.T) {
		errs := make(chan string, 4)
		nc, err := nats.Connect(s.ClientURL(), ticket(t, p, "admin"), nats.NoReconnect(),
			nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) { errs <- err.Error() }))
		if err != nil {
			t.Fatalf("connect: %v", err)
		}
		defer nc.Close()
		_ = nc.Publish("app.orders", []byte("x"))
		_ = nc.Flush()
		select {
		case e := <-errs:
			if !strings.Contains(strings.ToLower(e), "permissions violation") {
				t.Fatalf("unexpected error %q", e)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("a user with no roles could publish")
		}
	})
}

// The broker enforces exp: with a short TTL the minted JWT expires and the broker closes the connection.
func TestMintedJWTExpiryIsEnforcedByTheBroker(t *testing.T) {
	p := provision(t)
	s := startOperatorBroker(t, p)
	startOperatorCallout(t, p, s.ClientURL(), 2*time.Second)

	closed := make(chan struct{})
	nc, err := nats.Connect(s.ClientURL(), ticket(t, p, "app-feed-publish-orders"), nats.NoReconnect(),
		nats.ClosedHandler(func(*nats.Conn) { close(closed) }))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	select {
	case <-closed:
	case <-time.After(8 * time.Second):
		t.Fatal("the broker did not close the connection after the JWT's exp")
	}
}

func TestOperatorModeFailsFastOnBadSecrets(t *testing.T) {
	p := provision(t)
	good := config{
		workers: 1, ttl: time.Hour, url: "nats://127.0.0.1:1",
		creds:             p.read(t, p.calloutSecret, "callout.creds"),
		authSigningSeed:   p.read(t, p.calloutSecret, "auth-signing.seed"),
		tenantSigningSeed: p.read(t, p.calloutSecret, "tenant-signing.seed"),
		tenantPublic:      p.read(t, p.calloutSecret, "tenant.pub"),
	}
	for name, mutate := range map[string]func(*config){
		"empty creds": func(c *config) { c.creds = "" },
		"creds without seed": func(c *config) {
			c.creds = strings.Split(c.creds, "------END NATS USER JWT------")[0] + "------END NATS USER JWT------\n"
		},
		"bad AUTH seed":     func(c *config) { c.authSigningSeed = "junk" },
		"bad tenant seed":   func(c *config) { c.tenantSigningSeed = "junk" },
		"bad tenant public": func(c *config) { c.tenantPublic = "junk" },
	} {
		t.Run(name, func(t *testing.T) {
			c := good
			mutate(&c)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			start := time.Now()
			if err := run(ctx, c); err == nil {
				t.Fatal("run accepted bad secrets")
			}
			if time.Since(start) > 2*time.Second {
				t.Fatal("it took a connection attempt to find out")
			}
		})
	}
}
