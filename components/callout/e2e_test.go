// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

func writeFile(path, content string) error { return os.WriteFile(path, []byte(content), 0o600) }

// TestEndToEnd runs a real nats-server configured the way the benchmark broker is, with this
// callout answering for it, and checks who gets in and into which account.
func TestEndToEnd(t *testing.T) {
	kp, err := nkeys.CreateAccount()
	if err != nil {
		t.Fatal(err)
	}
	seed, _ := kp.Seed()
	issuer, _ := kp.PublicKey()

	conf := filepath.Join(t.TempDir(), "server.conf")
	if err := writeFile(conf, fmt.Sprintf(`
		listen: "127.0.0.1:-1"
		server_name: T
		system_account: SYS
		accounts {
			SYS {}
			AUTH { users [ {user: "auth", password: "svc"} ] }
			A0 {}
			A1 {}
		}
		authorization {
			timeout: 10s
			auth_callout {
				issuer: %q
				account: AUTH
				auth_users: [ auth ]
			}
		}
	`, issuer)); err != nil {
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
	defer s.Shutdown()
	if !s.ReadyForConnections(10 * time.Second) {
		t.Fatal("server not ready")
	}
	url := s.ClientURL()

	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, config{
			url: url, serviceUser: "auth", servicePassword: "svc",
			issuerSeed: string(seed), userPassword: "pw",
			workers: 4, accounts: 2, ready: ready,
		})
	}()
	select {
	case <-ready:
	case err := <-done:
		t.Fatalf("callout exited before it was ready: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("callout not ready")
	}

	for i, acct := range []string{"A0", "A1"} {
		nc, err := nats.Connect(url, nats.UserInfo(fmt.Sprintf("u%d", i), "pw"), nats.Timeout(5*time.Second))
		if err != nil {
			t.Fatalf("u%d: %v", i, err)
		}
		defer nc.Close()
		a, err := s.LookupAccount(acct)
		if err != nil {
			t.Fatal(err)
		}
		if n := a.NumConnections(); n != 1 {
			t.Errorf("u%d: account %s has %d connections, want 1", i, acct, n)
		}
	}

	for name, c := range map[string][2]string{
		"wrong password":  {"u0", "nope"},
		"unknown user":    {"alice", "pw"},
		"no such account": {"u2", "pw"},
	} {
		start := time.Now()
		nc, err := nats.Connect(url, nats.UserInfo(c[0], c[1]), nats.Timeout(5*time.Second), nats.NoReconnect())
		if err == nil {
			nc.Close()
			t.Errorf("%s: connected", name)
			continue
		}
		if !strings.Contains(strings.ToLower(err.Error()), "authorization") {
			t.Errorf("%s: unexpected error %v", name, err)
		}
		if d := time.Since(start); d > 3*time.Second {
			t.Errorf("%s: refusal took %v, the callout should answer an error, not time out", name, d)
		}
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run returned %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run did not stop after cancel")
	}
}

// The callout is started before the broker, as a Kubernetes rollout can do. It must keep retrying instead of exiting, and
// answer once the broker is up.
func TestStartsBeforeTheBroker(t *testing.T) {
	kp, _ := nkeys.CreateAccount()
	seed, _ := kp.Seed()
	issuer, _ := kp.PublicKey()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close() // nothing listens on the port yet

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, config{
			url: fmt.Sprintf("nats://127.0.0.1:%d", port), serviceUser: "auth", servicePassword: "svc",
			issuerSeed: string(seed), userPassword: "pw", workers: 2, accounts: 1,
		})
	}()
	select {
	case err := <-done:
		t.Fatalf("the callout exited while the broker was down: %v", err)
	case <-time.After(12 * time.Second): // longer than the flush timeout that used to end it
	}

	conf := filepath.Join(t.TempDir(), "server.conf")
	if err := writeFile(conf, fmt.Sprintf(`
		listen: "127.0.0.1:%d"
		system_account: SYS
		accounts { SYS {}, AUTH { users [ {user: "auth", password: "svc"} ] }, A0 {} }
		authorization { timeout: 10s, auth_callout { issuer: %q, account: AUTH, auth_users: [ auth ] } }
	`, port, issuer)); err != nil {
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
	defer s.Shutdown()
	if !s.ReadyForConnections(10 * time.Second) {
		t.Fatal("server not ready")
	}

	var last error
	for deadline := time.Now().Add(15 * time.Second); time.Now().Before(deadline); time.Sleep(250 * time.Millisecond) {
		nc, err := nats.Connect(s.ClientURL(), nats.UserInfo("u0", "pw"), nats.Timeout(3*time.Second), nats.NoReconnect())
		if err == nil {
			nc.Close()
			return
		}
		last = err
	}
	t.Fatalf("the callout never answered after the broker came up: %v", last)
}
