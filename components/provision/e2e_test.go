// SPDX-License-Identifier: Apache-2.0

package main

import (
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

// writeBrokerFiles lays the ConfigMap out as files in dir, the way the broker pod mounts it, and
// returns a server configuration that includes the two fragments.
func writeBrokerFiles(t *testing.T, b *Bundle) string {
	t.Helper()
	dir := t.TempDir()
	for k, v := range b.Objects(dir)[2].Data {
		if err := os.WriteFile(filepath.Join(dir, k), []byte(v), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	conf := filepath.Join(dir, "server.conf")
	body := "listen: \"127.0.0.1:-1\"\nserver_name: T\ninclude \"operator.conf\"\ninclude \"resolver-memory.conf\"\n"
	if err := os.WriteFile(conf, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return conf
}

func startBroker(t *testing.T, conf string) *server.Server {
	t.Helper()
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

// startCallout is the smallest responder that exercises the trust chain: it logs in with the creds
// from the bundle, reads the role tags from the ticket the client presented, and answers with a
// user JWT for the tenant account signed by the tenant signing key, wrapped in a response claim
// signed by the AUTH signing key. A ticket tagged "role:deny" is refused.
func startCallout(t *testing.T, b *Bundle, url string) *atomic.Int64 {
	t.Helper()
	creds := filepath.Join(t.TempDir(), "callout.creds")
	if err := os.WriteFile(creds, []byte(b.CalloutCreds), 0o600); err != nil {
		t.Fatal(err)
	}
	authSign, err := nkeys.FromSeed([]byte(b.AuthSigningSeed))
	if err != nil {
		t.Fatal(err)
	}
	tenantSign, err := nkeys.FromSeed([]byte(b.TenantSigningSeed))
	if err != nil {
		t.Fatal(err)
	}
	nc, err := nats.Connect(url, nats.UserCredentials(creds), nats.Name("callout"))
	if err != nil {
		t.Fatalf("the callout could not log in with its creds: %v", err)
	}
	t.Cleanup(nc.Close)

	var requests atomic.Int64
	_, err = nc.Subscribe("$SYS.REQ.USER.AUTH", func(m *nats.Msg) {
		requests.Add(1)
		rc, err := jwt.DecodeAuthorizationRequestClaims(string(m.Data))
		if err != nil {
			return
		}
		resp := jwt.NewAuthorizationResponseClaims(rc.UserNkey)
		resp.Audience = rc.Server.ID
		ticket, err := jwt.DecodeUserClaims(rc.ConnectOptions.JWT)
		if err != nil || ticket.Tags.Contains("role:deny") {
			resp.Error = "denied"
		} else {
			uc := jwt.NewUserClaims(rc.UserNkey)
			uc.Name = "benchmark-user"
			uc.IssuerAccount = b.TenantPub
			uc.Expires = time.Now().Add(time.Hour).Unix()
			uc.Pub.Allow.Add("app.>")
			uc.Sub.Allow.Add("app.>")
			if resp.Jwt, err = uc.Encode(tenantSign); err != nil {
				resp.Error = err.Error()
			}
		}
		token, err := resp.Encode(authSign)
		if err != nil {
			return
		}
		m.Respond([]byte(token))
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	return &requests
}

// ticket makes what the load generator would: a user JWT signed by the scoped key, with no
// permissions of its own, the given role tags and an expiry an hour out.
func ticket(t *testing.T, b *Bundle, perms func(*jwt.UserClaims), tags ...string) nats.Option {
	t.Helper()
	scoped, err := nkeys.FromSeed([]byte(b.ScopedSeed))
	if err != nil {
		t.Fatal(err)
	}
	ukp, _ := nkeys.CreateUser()
	upub, _ := ukp.PublicKey()
	uc := jwt.NewUserClaims(upub)
	uc.SetScoped(true)
	uc.IssuerAccount = b.AuthPub
	uc.Expires = time.Now().Add(time.Hour).Unix()
	uc.Tags.Add(tags...)
	if perms != nil {
		perms(uc)
	}
	token, err := uc.Encode(scoped)
	if err != nil {
		t.Fatal(err)
	}
	return nats.UserJWT(
		func() (string, error) { return token, nil },
		func(nonce []byte) ([]byte, error) { return ukp.Sign(nonce) },
	)
}

// accountOf returns the account the broker has the client with this name in. Connz reports the
// account only when asked for auth detail (Username).
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

// TestTrustChainAgainstARealBroker starts nats-server in operator mode from the files provision
// writes (the MEMORY resolver, the included fragments) and walks a ticket through it.
func TestTrustChainAgainstARealBroker(t *testing.T) {
	b := mustGenerate(t)
	s := startBroker(t, writeBrokerFiles(t, b))
	requests := startCallout(t, b, s.ClientURL())

	t.Run("a scoped ticket reaches the callout and is placed in the tenant account", func(t *testing.T) {
		before := requests.Load()
		got := make(chan string, 1)
		nc, err := nats.Connect(s.ClientURL(), ticket(t, b, nil, "role:reader"), nats.Name("zp-client"), nats.NoReconnect(),
			nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) { got <- err.Error() }))
		if err != nil {
			t.Fatalf("connect: %v", err)
		}
		defer nc.Close()
		if requests.Load() != before+1 {
			t.Fatalf("the callout saw %d new requests, want 1", requests.Load()-before)
		}

		account := accountOf(t, s, "zp-client")
		if account != b.TenantPub {
			t.Fatalf("the client is in account %q, want the tenant account %q", account, b.TenantPub)
		}

		// The minted permissions apply, not the ticket's: app.> works, anything else is refused.
		sub, err := nc.SubscribeSync("app.ping")
		if err != nil {
			t.Fatal(err)
		}
		if err := nc.Publish("app.ping", []byte("x")); err != nil {
			t.Fatal(err)
		}
		if _, err := sub.NextMsg(2 * time.Second); err != nil {
			t.Fatalf("a message on app.ping did not arrive: %v", err)
		}
		_ = nc.Publish("other.subject", []byte("x"))
		_ = nc.Flush()
		select {
		case e := <-got:
			if !strings.Contains(strings.ToLower(e), "permissions violation") {
				t.Fatalf("unexpected error %q", e)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("publishing outside app.> was not refused")
		}
	})

	t.Run("a refusal from the callout refuses the connection", func(t *testing.T) {
		before := requests.Load()
		_, err := nats.Connect(s.ClientURL(), ticket(t, b, nil, "role:deny"), nats.NoReconnect())
		if err == nil {
			t.Fatal("a denied ticket connected")
		}
		if requests.Load() != before+1 {
			t.Fatal("the callout was not asked")
		}
	})

	// The broker refuses it without asking the callout. It does so only after its authorization timeout (2s by default): the failed
	// ticket is handed to the callout of an account that has none, and the server waits for an answer that never comes.
	t.Run("a ticket with permissions of its own is refused without asking the callout", func(t *testing.T) {
		before := requests.Load()
		_, err := nats.Connect(s.ClientURL(), ticket(t, b, func(uc *jwt.UserClaims) { uc.Pub.Allow.Add(">") }), nats.NoReconnect())
		if err == nil {
			t.Fatal("a scoped ticket carrying permissions connected")
		}
		if requests.Load() != before {
			t.Fatal("the callout was asked about a ticket the broker should have refused")
		}
	})

	t.Run("a ticket signed by the wrong key is refused", func(t *testing.T) {
		other := mustGenerate(t) // a different operator and AUTH account
		_, err := nats.Connect(s.ClientURL(), ticket(t, other, nil), nats.NoReconnect())
		if err == nil {
			t.Fatal("a ticket from another operator connected")
		}
	})

	t.Run("no credentials are refused", func(t *testing.T) {
		if _, err := nats.Connect(s.ClientURL(), nats.NoReconnect()); err == nil {
			t.Fatal("an anonymous client connected to an operator-mode broker")
		}
	})
}

func TestBrokerFragmentsAreWhatTheServerReads(t *testing.T) {
	b := mustGenerate(t)
	opts, err := server.ProcessConfigFile(writeBrokerFiles(t, b))
	if err != nil {
		t.Fatal(err)
	}
	if opts.SystemAccount != b.SysPub {
		t.Fatalf("system_account = %q, want %q", opts.SystemAccount, b.SysPub)
	}
	if len(opts.TrustedOperators) != 1 || opts.TrustedOperators[0].Subject != b.OperatorPub {
		t.Fatalf("the trusted operator is not the generated one: %v", opts.TrustedOperators)
	}
	if opts.AccountResolver == nil {
		t.Fatal("no account resolver configured")
	}
}
