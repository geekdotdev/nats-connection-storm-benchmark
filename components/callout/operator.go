// SPDX-License-Identifier: Apache-2.0

package main

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
)

// AuthCalloutSubject is the subject the server sends authorization requests to.
const AuthCalloutSubject = "$SYS.REQ.USER.AUTH"

// DefaultTTL is how long a minted user JWT is valid for. The broker disconnects a client whose JWT has expired.
const DefaultTTL = time.Hour

// OperatorAuthorizer answers authorization requests for a broker in operator mode (the only mode this callout has).
//
// A client presents a ticket: a user JWT signed by the AUTH account's scoped signing key, with no
// permissions of its own and its role names as tags. The broker has already verified the ticket and
// handed it to this responder, which maps the role names to subjects (SubjectsForRoles) and answers
// with a user JWT for the tenant account, signed by the tenant signing key and carrying those
// subjects, inside a response claim signed by the AUTH signing key. See components/provision for
// where the keys come from.
//
// It does no more than that: no database, no identity provider, no network calls,
// so a benchmark measures the broker and the callout round trip. It does not check that the request
// was signed by the expected server; the connection to the broker is the trust boundary in this test
// setup. The roles are self-asserted by whoever holds the scoped key. Do not use it as a production callout.
type OperatorAuthorizer struct {
	authSigning   nkeys.KeyPair // signs the response claim
	tenantSigning nkeys.KeyPair // signs the minted user JWT
	tenantPub     string        // the tenant account: the minted JWT's issuer_account
	ttl           time.Duration
	now           func() time.Time

	Accepted atomic.Int64 // requests answered with a user JWT
	Rejected atomic.Int64 // requests answered with an error
	Dropped  atomic.Int64 // requests that could not be decoded or signed, so got no answer
}

// NewOperatorAuthorizer takes the seeds of the AUTH account's plain signing key and the tenant
// account's signing key, the tenant account's public key, and the lifetime of a minted user JWT.
func NewOperatorAuthorizer(authSigningSeed, tenantSigningSeed []byte, tenantPub string, ttl time.Duration) (*OperatorAuthorizer, error) {
	account := func(what string, seed []byte) (nkeys.KeyPair, error) {
		kp, err := nkeys.FromSeed(seed)
		if err != nil {
			return nil, fmt.Errorf("%s is not a valid nkey seed", what)
		}
		pub, err := kp.PublicKey()
		if err != nil || !nkeys.IsValidPublicAccountKey(pub) {
			return nil, fmt.Errorf("%s must be an account seed (public key starting with A)", what)
		}
		return kp, nil
	}
	auth, err := account("the AUTH signing seed", authSigningSeed)
	if err != nil {
		return nil, err
	}
	tenant, err := account("the tenant signing seed", tenantSigningSeed)
	if err != nil {
		return nil, err
	}
	if !nkeys.IsValidPublicAccountKey(tenantPub) {
		return nil, errors.New("the tenant account public key is not a valid account public key")
	}
	if ttl <= 0 {
		return nil, errors.New("ttl must be positive")
	}
	return &OperatorAuthorizer{authSigning: auth, tenantSigning: tenant, tenantPub: tenantPub, ttl: ttl, now: time.Now}, nil
}

// Respond returns the encoded authorization response for an encoded authorization request, or
// nil if the request cannot be answered at all (it is malformed, so there is nobody to address).
func (a *OperatorAuthorizer) Respond(request []byte) []byte {
	rc, err := jwt.DecodeAuthorizationRequestClaims(string(request))
	// The jwt constructors return nil for an empty subject, so a request without a user nkey
	// (malformed, or from something that is not a server) must be dropped before it reaches them.
	if err != nil || rc.UserNkey == "" {
		a.Dropped.Add(1)
		return nil
	}
	ticket, err := jwt.DecodeUserClaims(rc.ConnectOptions.JWT)
	if err != nil || ticket == nil {
		a.Rejected.Add(1)
		return a.respond(rc.UserNkey, rc.Server.ID, "", "authorization failed")
	}
	userJWT, err := a.mintOperatorUser(rc.UserNkey, ticket.Name, ticket.Tags)
	if err != nil {
		a.Dropped.Add(1)
		return nil
	}
	resp := a.respond(rc.UserNkey, rc.Server.ID, userJWT, "")
	if resp == nil {
		a.Dropped.Add(1)
		return nil
	}
	a.Accepted.Add(1)
	return resp
}

// mintOperatorUser signs the user JWT for userNkey in the tenant account. It is signed by a signing
// key, so IssuerAccount names the account. A direction with no subjects is not left open: in NATS
// an empty allow list restricts nothing, so it gets a deny of ">" instead (the user connects but
// cannot publish, or subscribe, at all).
func (a *OperatorAuthorizer) mintOperatorUser(userNkey, name string, roles []string) (string, error) {
	uc := jwt.NewUserClaims(userNkey)
	if uc == nil {
		return "", errors.New("no user nkey")
	}
	uc.Name = name
	uc.IssuerAccount = a.tenantPub
	uc.Expires = a.now().Add(a.ttl).Unix()
	publish, subscribe := SubjectsForRoles(roles)
	allowOrDenyAll(&uc.Pub, publish)
	allowOrDenyAll(&uc.Sub, subscribe)
	return uc.Encode(a.tenantSigning)
}

func allowOrDenyAll(p *jwt.Permission, subjects []string) {
	if len(subjects) == 0 {
		p.Deny.Add(">")
		return
	}
	p.Allow.Add(subjects...)
}

func (a *OperatorAuthorizer) respond(userNkey, serverID, userJWT, reason string) []byte {
	return signResponse(a.authSigning, userNkey, serverID, userJWT, reason)
}

// signResponse wraps userJWT (or the refusal reason) in an authorization response claim addressed to
// the user and the server that asked, signed with signer. It returns nil if it cannot be built.
func signResponse(signer nkeys.KeyPair, userNkey, serverID, userJWT, reason string) []byte {
	rc := jwt.NewAuthorizationResponseClaims(userNkey)
	if rc == nil {
		return nil
	}
	rc.Audience = serverID
	rc.Jwt = userJWT
	rc.Error = reason
	token, err := rc.Encode(signer)
	if err != nil {
		return nil
	}
	return []byte(token)
}

// Counts reports how many requests were accepted, rejected and dropped.
func (a *OperatorAuthorizer) Counts() (accepted, rejected, dropped int64) {
	return a.Accepted.Load(), a.Rejected.Load(), a.Dropped.Load()
}
