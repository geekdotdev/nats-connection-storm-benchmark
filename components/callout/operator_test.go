// SPDX-License-Identifier: Apache-2.0

package main

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
)

type operatorFixture struct {
	az                         *OperatorAuthorizer
	authSignPub, tenantSignPub string
	tenantPub                  string
	scoped                     nkeys.KeyPair // stands for the generator's scoped key
}

func newOperatorFixture(t *testing.T, ttl time.Duration) *operatorFixture {
	t.Helper()
	mk := func() (nkeys.KeyPair, []byte, string) {
		kp, err := nkeys.CreateAccount()
		if err != nil {
			t.Fatal(err)
		}
		seed, _ := kp.Seed()
		pub, _ := kp.PublicKey()
		return kp, seed, pub
	}
	_, authSeed, authPub := mk()
	_, tenantSeed, tenantSignPub := mk()
	_, _, tenantPub := mk()
	scoped, _, _ := mk()
	az, err := NewOperatorAuthorizer(authSeed, tenantSeed, tenantPub, ttl)
	if err != nil {
		t.Fatal(err)
	}
	return &operatorFixture{az: az, authSignPub: authPub, tenantSignPub: tenantSignPub, tenantPub: tenantPub, scoped: scoped}
}

// ticketRequest builds an authorization request carrying a ticket with the given name and tags, the way a server would.
func (f *operatorFixture) ticketRequest(t *testing.T, name string, tags ...string) (encoded []byte, userNkey, serverID string) {
	t.Helper()
	u, _ := nkeys.CreateUser()
	userNkey, _ = u.PublicKey()
	tc := jwt.NewUserClaims(userNkey)
	tc.SetScoped(true)
	tc.Name = name
	tc.Tags.Add(tags...)
	ticket, err := tc.Encode(f.scoped)
	if err != nil {
		t.Fatal(err)
	}
	return f.requestWithJWT(t, userNkey, ticket)
}

func (f *operatorFixture) requestWithJWT(t *testing.T, userNkey, ticket string) ([]byte, string, string) {
	t.Helper()
	srv, _ := nkeys.CreateServer()
	srvPub, _ := srv.PublicKey()
	rc := jwt.NewAuthorizationRequestClaims(userNkey)
	rc.UserNkey = userNkey
	rc.Server.ID = srvPub
	rc.ConnectOptions.JWT = ticket
	tok, err := rc.Encode(srv)
	if err != nil {
		t.Fatal(err)
	}
	return []byte(tok), userNkey, srvPub
}

func TestOperatorRespondMintsTheTenantUser(t *testing.T) {
	f := newOperatorFixture(t, time.Hour)
	fixed := time.Unix(1_800_000_000, 0)
	f.az.now = func() time.Time { return fixed }

	req, userNkey, serverID := f.ticketRequest(t, "storm-7", "app-feed-publish-orders", "app-feed-subscribe-alarms")
	out := f.az.Respond(req)
	if out == nil {
		t.Fatal("no response")
	}
	rc, err := jwt.DecodeAuthorizationResponseClaims(string(out))
	if err != nil {
		t.Fatal(err)
	}
	if rc.Error != "" || rc.Jwt == "" {
		t.Fatalf("expected a user JWT, got error %q", rc.Error)
	}
	if rc.Subject != userNkey || rc.Audience != serverID {
		t.Fatalf("response addressed to %q / %q, want %q / %q", rc.Subject, rc.Audience, userNkey, serverID)
	}
	if rc.Issuer != f.authSignPub {
		t.Fatalf("the response is signed by %q, want the AUTH signing key %q", rc.Issuer, f.authSignPub)
	}

	uc, err := jwt.DecodeUserClaims(rc.Jwt)
	if err != nil {
		t.Fatal(err)
	}
	if uc.Subject != userNkey {
		t.Fatalf("user JWT subject %q, want %q", uc.Subject, userNkey)
	}
	if uc.Issuer != f.tenantSignPub {
		t.Fatalf("user JWT signed by %q, want the tenant signing key %q", uc.Issuer, f.tenantSignPub)
	}
	if uc.IssuerAccount != f.tenantPub {
		t.Fatalf("issuer_account %q, want the tenant account %q", uc.IssuerAccount, f.tenantPub)
	}
	if uc.Name != "storm-7" {
		t.Fatalf("name %q, want the ticket's", uc.Name)
	}
	if want := fixed.Add(time.Hour).Unix(); uc.Expires != want {
		t.Fatalf("exp %d, want mint time + 1h = %d", uc.Expires, want)
	}
	if uc.NotBefore != 0 {
		t.Fatalf("nbf is %d, want it unset", uc.NotBefore)
	}
	if !reflect.DeepEqual([]string(uc.Pub.Allow), []string{"app.orders"}) || !reflect.DeepEqual([]string(uc.Sub.Allow), []string{"app.alarms"}) {
		t.Fatalf("permissions pub %v sub %v", uc.Pub.Allow, uc.Sub.Allow)
	}
	if len(uc.Pub.Deny) != 0 || len(uc.Sub.Deny) != 0 {
		t.Fatalf("a direction with subjects must not also deny: %v %v", uc.Pub.Deny, uc.Sub.Deny)
	}
	var vr jwt.ValidationResults
	uc.Validate(&vr)
	if vr.IsBlocking(true) {
		t.Fatalf("the minted JWT does not validate: %v", vr.Errors())
	}
	if a, r, d := f.az.Counts(); a != 1 || r != 0 || d != 0 {
		t.Fatalf("counts %d/%d/%d", a, r, d)
	}
}

// An empty allow list restricts nothing in NATS, so a direction with no subjects must be denied outright.
func TestOperatorUserWithNoRolesCanDoNothing(t *testing.T) {
	f := newOperatorFixture(t, time.Hour)
	for name, tags := range map[string][]string{
		"no tags":             nil,
		"unrecognized tags":   {"admin", "role:reader"},
		"malformed stems":     {"app-feed-publish->", "app-feed-subscribe-a.b"},
		"publish role only":   {"app-feed-publish-orders"},
		"subscribe role only": {"app-feed-subscribe-orders"},
	} {
		t.Run(name, func(t *testing.T) {
			req, _, _ := f.ticketRequest(t, "x", tags...)
			rc, err := jwt.DecodeAuthorizationResponseClaims(string(f.az.Respond(req)))
			if err != nil || rc.Jwt == "" {
				t.Fatalf("expected a user JWT: %v %q", err, rc.Error)
			}
			uc, _ := jwt.DecodeUserClaims(rc.Jwt)
			pubOpen, subOpen := len(uc.Pub.Allow) > 0, len(uc.Sub.Allow) > 0
			if !pubOpen && !reflect.DeepEqual([]string(uc.Pub.Deny), []string{">"}) {
				t.Fatalf("publish is neither allowed nor denied outright: %v", uc.Pub)
			}
			if !subOpen && !reflect.DeepEqual([]string(uc.Sub.Deny), []string{">"}) {
				t.Fatalf("subscribe is neither allowed nor denied outright: %v", uc.Sub)
			}
		})
	}
}

func TestOperatorRejectsWithAnErrorResponse(t *testing.T) {
	f := newOperatorFixture(t, time.Hour)
	u, _ := nkeys.CreateUser()
	userNkey, _ := u.PublicKey()
	for name, ticket := range map[string]string{"empty": "", "not a JWT": "not.a.jwt", "an account JWT is not a ticket": f.accountJWT(t)} {
		t.Run(name, func(t *testing.T) {
			req, _, _ := f.requestWithJWT(t, userNkey, ticket)
			out := f.az.Respond(req)
			if out == nil {
				t.Fatal("a refusal must be answered, so the client is refused at once instead of waiting for the broker's timeout")
			}
			rc, err := jwt.DecodeAuthorizationResponseClaims(string(out))
			if err != nil {
				t.Fatal(err)
			}
			if rc.Jwt != "" || rc.Error == "" {
				t.Fatalf("want an error response, got jwt=%q error=%q", rc.Jwt, rc.Error)
			}
			if rc.Issuer != f.authSignPub {
				t.Fatalf("the refusal is signed by %q, want %q", rc.Issuer, f.authSignPub)
			}
			if strings.Contains(rc.Error, "tenant") || strings.Contains(rc.Error, f.tenantPub) {
				t.Fatalf("the error reveals the setup: %q", rc.Error)
			}
		})
	}
	if _, r, _ := f.az.Counts(); r != 3 {
		t.Fatalf("rejected = %d, want 3", r)
	}
}

func (f *operatorFixture) accountJWT(t *testing.T) string {
	t.Helper()
	ac := jwt.NewAccountClaims(f.tenantPub)
	op, _ := nkeys.CreateOperator()
	tok, err := ac.Encode(op)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

func TestOperatorDropsAMalformedRequest(t *testing.T) {
	f := newOperatorFixture(t, time.Hour)
	if f.az.Respond([]byte("not a jwt")) != nil {
		t.Fatal("a malformed request was answered")
	}
	if _, _, d := f.az.Counts(); d != 1 {
		t.Fatalf("dropped = %d, want 1", d)
	}
}

func TestNewOperatorAuthorizerValidates(t *testing.T) {
	user, _ := nkeys.CreateUser()
	userSeed, _ := user.Seed()
	acct, _ := nkeys.CreateAccount()
	acctSeed, _ := acct.Seed()
	acctPub, _ := acct.PublicKey()
	userPub, _ := user.PublicKey()
	cases := []struct {
		name         string
		auth, tenant []byte
		tenantPub    string
		ttl          time.Duration
		want         string
	}{
		{"ok", acctSeed, acctSeed, acctPub, time.Hour, ""},
		{"bad AUTH seed", []byte("junk"), acctSeed, acctPub, time.Hour, "AUTH signing seed is not a valid"},
		{"AUTH seed of the wrong type", userSeed, acctSeed, acctPub, time.Hour, "AUTH signing seed must be an account"},
		{"bad tenant seed", acctSeed, []byte("junk"), acctPub, time.Hour, "tenant signing seed is not a valid"},
		{"tenant seed of the wrong type", acctSeed, userSeed, acctPub, time.Hour, "tenant signing seed must be an account"},
		{"tenant public key of the wrong type", acctSeed, acctSeed, userPub, time.Hour, "tenant account public key"},
		{"no tenant public key", acctSeed, acctSeed, "", time.Hour, "tenant account public key"},
		{"zero ttl", acctSeed, acctSeed, acctPub, 0, "ttl"},
		{"negative ttl", acctSeed, acctSeed, acctPub, -time.Second, "ttl"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := NewOperatorAuthorizer(c.auth, c.tenant, c.tenantPub, c.ttl)
			if c.want == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("got %v, want an error containing %q", err, c.want)
			}
		})
	}
}

func mustAccountKey(t *testing.T) (pub string, seed []byte) {
	t.Helper()
	kp, err := nkeys.CreateAccount()
	if err != nil {
		t.Fatal(err)
	}
	seed, _ = kp.Seed()
	pub, _ = kp.PublicKey()
	return pub, seed
}
