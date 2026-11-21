// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
)

func newTestAuthorizer(t *testing.T, accounts int) (*Authorizer, []byte) {
	t.Helper()
	kp, err := nkeys.CreateAccount()
	if err != nil {
		t.Fatal(err)
	}
	seed, err := kp.Seed()
	if err != nil {
		t.Fatal(err)
	}
	az, err := NewAuthorizer(seed, "pw", accounts)
	if err != nil {
		t.Fatal(err)
	}
	return az, seed
}

// request builds an encoded authorization request the way a server would.
func request(t *testing.T, user, pass string) (encoded []byte, userNkey, serverID string) {
	t.Helper()
	srv, err := nkeys.CreateServer()
	if err != nil {
		t.Fatal(err)
	}
	srvPub, _ := srv.PublicKey()
	u, err := nkeys.CreateUser()
	if err != nil {
		t.Fatal(err)
	}
	userNkey, _ = u.PublicKey()
	rc := jwt.NewAuthorizationRequestClaims(userNkey)
	rc.UserNkey = userNkey // a server sets this; the claims' subject alone is not enough
	rc.Server.ID = srvPub
	rc.ConnectOptions.Username = user
	rc.ConnectOptions.Password = pass
	tok, err := rc.Encode(srv)
	if err != nil {
		t.Fatal(err)
	}
	return []byte(tok), userNkey, srvPub
}

func TestRespondAcceptsAndPlacesUserInItsAccount(t *testing.T) {
	az, _ := newTestAuthorizer(t, 4)
	req, userNkey, serverID := request(t, "u3", "pw")

	out := az.Respond(req)
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
		t.Fatalf("response addressed to subject %q audience %q, want %q and %q", rc.Subject, rc.Audience, userNkey, serverID)
	}
	if rc.Issuer != az.IssuerPublicKey() {
		t.Fatalf("response signed by %q, want the issuer %q", rc.Issuer, az.IssuerPublicKey())
	}
	uc, err := jwt.DecodeUserClaims(rc.Jwt)
	if err != nil {
		t.Fatal(err)
	}
	if uc.Subject != userNkey || uc.Audience != "A3" || uc.Issuer != az.IssuerPublicKey() {
		t.Fatalf("user claims: subject %q audience %q issuer %q", uc.Subject, uc.Audience, uc.Issuer)
	}
	if az.Accepted.Load() != 1 || az.Rejected.Load() != 0 {
		t.Fatalf("counters: accepted %d rejected %d", az.Accepted.Load(), az.Rejected.Load())
	}
}

func TestRespondRejectsWithAnErrorResponse(t *testing.T) {
	az, _ := newTestAuthorizer(t, 4)
	for name, c := range map[string][2]string{
		"wrong password":       {"u1", "nope"},
		"unknown user":         {"alice", "pw"},
		"account out of range": {"u4", "pw"},
		"leading zero":         {"u01", "pw"},
		"empty":                {"", ""},
	} {
		req, userNkey, serverID := request(t, c[0], c[1])
		out := az.Respond(req)
		if out == nil {
			t.Fatalf("%s: no response, the client would wait for the timeout", name)
		}
		rc, err := jwt.DecodeAuthorizationResponseClaims(string(out))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if rc.Error == "" || rc.Jwt != "" {
			t.Fatalf("%s: want an error and no JWT, got error %q jwt %q", name, rc.Error, rc.Jwt)
		}
		if rc.Subject != userNkey || rc.Audience != serverID {
			t.Fatalf("%s: error response is not addressed to the requester", name)
		}
	}
	if az.Accepted.Load() != 0 || az.Rejected.Load() != 5 {
		t.Fatalf("counters: accepted %d rejected %d", az.Accepted.Load(), az.Rejected.Load())
	}
}

func TestRespondDropsAMalformedRequest(t *testing.T) {
	az, _ := newTestAuthorizer(t, 1)
	if out := az.Respond([]byte("not a jwt")); out != nil {
		t.Fatalf("answered a malformed request: %q", out)
	}
	if az.Dropped.Load() != 1 {
		t.Fatalf("dropped = %d", az.Dropped.Load())
	}
}

// A request with no user nkey made the jwt constructors return nil, and the callout crashed.
func TestRespondDropsARequestWithoutAUserNkey(t *testing.T) {
	az, _ := newTestAuthorizer(t, 1)
	srv, _ := nkeys.CreateServer()
	u, _ := nkeys.CreateUser()
	pub, _ := u.PublicKey()
	rc := jwt.NewAuthorizationRequestClaims(pub) // subject set, nats.user_nkey left empty
	rc.ConnectOptions.Username, rc.ConnectOptions.Password = "u0", "pw"
	tok, err := rc.Encode(srv)
	if err != nil {
		t.Fatal(err)
	}
	if out := az.Respond([]byte(tok)); out != nil {
		t.Fatalf("answered a request with no user nkey: %q", out)
	}
	if az.Dropped.Load() != 1 {
		t.Fatalf("dropped = %d", az.Dropped.Load())
	}
}

func TestNewAuthorizerValidates(t *testing.T) {
	_, seed := newTestAuthorizer(t, 1)
	user, _ := nkeys.CreateUser()
	userSeed, _ := user.Seed()
	for name, c := range map[string]struct {
		seed     []byte
		pw       string
		accounts int
	}{
		"not a seed":     {[]byte("junk"), "pw", 1},
		"user seed":      {userSeed, "pw", 1},
		"empty password": {seed, "", 1},
		"no accounts":    {seed, "pw", 0},
	} {
		if _, err := NewAuthorizer(c.seed, c.pw, c.accounts); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestSecretReadsEnvThenFile(t *testing.T) {
	t.Setenv("X_SECRET", "")
	t.Setenv("X_SECRET_FILE", "")
	if _, err := secret("X_SECRET"); err == nil || !strings.Contains(err.Error(), "X_SECRET") {
		t.Fatalf("want an error naming the variable, got %v", err)
	}
	t.Setenv("X_SECRET", "from-env")
	if v, _ := secret("X_SECRET"); v != "from-env" {
		t.Fatalf("got %q", v)
	}
	t.Setenv("X_SECRET", "")
	f := t.TempDir() + "/s"
	if err := writeFile(f, "from-file\n"); err != nil {
		t.Fatal(err)
	}
	t.Setenv("X_SECRET_FILE", f)
	if v, _ := secret("X_SECRET"); v != "from-file" {
		t.Fatalf("got %q", v)
	}
}

func TestKeygenJSONGivesAUsableAccountKey(t *testing.T) {
	var out bytes.Buffer
	if err := keygen([]string{"--json"}, &out); err != nil {
		t.Fatal(err)
	}
	var k struct{ Seed, Public string }
	if err := json.Unmarshal(out.Bytes(), &k); err != nil {
		t.Fatal(err)
	}
	az, err := NewAuthorizer([]byte(k.Seed), "pw", 1)
	if err != nil {
		t.Fatalf("the seed is not usable: %v", err)
	}
	if az.IssuerPublicKey() != k.Public {
		t.Fatalf("public key %q does not belong to the seed (%q)", k.Public, az.IssuerPublicKey())
	}
}

func TestKeygenSeedFileModeNeverPrintsTheSeedAndNeverOverwrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "seed")
	var out bytes.Buffer
	if err := keygen([]string{"--seed-file", path}, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(out.String(), "A") || strings.Contains(out.String(), "SA") {
		t.Fatalf("expected only a public key on stdout, got %q", out.String())
	}
	if err := keygen([]string{"--seed-file", path}, &out); err == nil {
		t.Fatal("overwrote an existing seed file")
	}
}

func TestKeygenNeedsExactlyOneOutput(t *testing.T) {
	var out bytes.Buffer
	if err := keygen(nil, &out); err == nil {
		t.Fatal("accepted no output option")
	}
	if err := keygen([]string{"--json", "--seed-file", "x"}, &out); err == nil {
		t.Fatal("accepted both output options")
	}
	if out.Len() != 0 {
		t.Fatalf("wrote output on error: %q", out.String())
	}
}
