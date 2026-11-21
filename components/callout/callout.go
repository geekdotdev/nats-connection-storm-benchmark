// SPDX-License-Identifier: Apache-2.0

package main

import (
	"crypto/subtle"
	"errors"
	"regexp"
	"strconv"
	"sync/atomic"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
)

// AuthCalloutSubject is the subject the server sends authorization requests to.
const AuthCalloutSubject = "$SYS.REQ.USER.AUTH"

// A client authenticates as "u<N>" and is placed in account "A<N>", for N below the number of
// accounts the broker was configured with. Every user shares one password.
var userName = regexp.MustCompile(`^u(0|[1-9][0-9]*)$`)

// Authorizer turns an authorization request into a signed response. It does what the in-tree
// nats-server benchmark's handler does and nothing more: no database, no identity provider, no
// network calls, so a benchmark measures the server and not the callout.
//
// It does not check that the request was signed by the expected server; the connection to the
// broker is the trust boundary in this test setup. Do not use it as a production callout.
type Authorizer struct {
	issuer       nkeys.KeyPair
	userPassword []byte
	accounts     int

	Accepted atomic.Int64 // requests answered with a user JWT
	Rejected atomic.Int64 // requests answered with an error
	Dropped  atomic.Int64 // requests that could not be decoded or signed, so got no answer
}

// NewAuthorizer takes the seed of the account key the broker's auth_callout issuer is set to.
func NewAuthorizer(issuerSeed []byte, userPassword string, accounts int) (*Authorizer, error) {
	kp, err := nkeys.FromSeed(issuerSeed)
	if err != nil {
		return nil, errors.New("issuer seed is not a valid nkey seed")
	}
	pub, err := kp.PublicKey()
	if err != nil || !nkeys.IsValidPublicAccountKey(pub) {
		return nil, errors.New("issuer seed must be an account seed (public key starting with A)")
	}
	if userPassword == "" {
		return nil, errors.New("user password must not be empty")
	}
	if accounts < 1 {
		return nil, errors.New("accounts must be at least 1")
	}
	return &Authorizer{issuer: kp, userPassword: []byte(userPassword), accounts: accounts}, nil
}

// IssuerPublicKey is the value for auth_callout.issuer in the broker's configuration.
func (a *Authorizer) IssuerPublicKey() string {
	pub, _ := a.issuer.PublicKey()
	return pub
}

// Respond returns the encoded authorization response for an encoded authorization request, or
// nil if the request cannot be answered at all (it is malformed, so there is nobody to address).
func (a *Authorizer) Respond(request []byte) []byte {
	ac, err := jwt.DecodeAuthorizationRequestClaims(string(request))
	// The jwt constructors return nil for an empty subject, so a request without a user nkey
	// (malformed, or from something that is not a server) must be dropped before it reaches them.
	if err != nil || ac.UserNkey == "" {
		a.Dropped.Add(1)
		return nil
	}
	account, reason := a.authorize(ac.ConnectOptions.Username, ac.ConnectOptions.Password)
	if reason != "" {
		a.Rejected.Add(1)
		return a.sign(ac.UserNkey, ac.Server.ID, "", reason)
	}
	uc := jwt.NewUserClaims(ac.UserNkey)
	if uc == nil {
		a.Dropped.Add(1)
		return nil
	}
	uc.Name = ac.ConnectOptions.Username
	// The audience names the target account; this is how the callout places a user when the
	// broker is not running in operator mode.
	uc.Audience = account
	userJWT, err := uc.Encode(a.issuer)
	if err != nil {
		a.Dropped.Add(1)
		return nil
	}
	resp := a.sign(ac.UserNkey, ac.Server.ID, userJWT, "")
	if resp == nil {
		a.Dropped.Add(1)
		return nil
	}
	a.Accepted.Add(1)
	return resp
}

func (a *Authorizer) authorize(username, password string) (account, reason string) {
	m := userName.FindStringSubmatch(username)
	passOK := subtle.ConstantTimeCompare([]byte(password), a.userPassword) == 1
	if m == nil || !passOK {
		return "", "authorization failed"
	}
	n, err := strconv.Atoi(m[1])
	if err != nil || n >= a.accounts {
		return "", "authorization failed"
	}
	return "A" + m[1], ""
}

func (a *Authorizer) sign(userNkey, serverID, userJWT, reason string) []byte {
	rc := jwt.NewAuthorizationResponseClaims(userNkey)
	if rc == nil {
		return nil
	}
	rc.Audience = serverID
	rc.Jwt = userJWT
	rc.Error = reason
	token, err := rc.Encode(a.issuer)
	if err != nil {
		return nil
	}
	return []byte(token)
}
