// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
)

// Object names. The broker mounts ConfigMapName, the callout mounts CalloutSecretName and the
// load generator mounts GeneratorSecretName. Nothing else reads another's Secret.
const (
	CalloutSecretName   = "storm-callout-auth"
	GeneratorSecretName = "storm-generator-auth"
	ConfigMapName       = "storm-operator"

	// DefaultMountDir is where the broker mounts ConfigMapName; operator.conf names the operator JWT there.
	DefaultMountDir = "/etc/storm/operator"
)

// Bundle is everything one provisioning run makes.
//
// The operator, SYS, AUTH and tenant identity seeds exist only inside Generate: they sign the
// JWTs and are then wiped, so no object holds them and the accounts cannot be edited afterwards.
// Only the seeds a runtime component needs are kept, and each goes to exactly one object.
type Bundle struct {
	OperatorPub, SysPub, AuthPub, TenantPub string
	OperatorJWT, SysJWT, AuthJWT, TenantJWT string

	// ScopedPub and ScopedSeed are the AUTH account's scoped signing key: it can only sign
	// user JWTs with no permissions of their own, and the load generator holds it.
	ScopedPub, ScopedSeed string
	// AuthSigningSeed is a plain AUTH signing key; the callout signs its response claim with it.
	AuthSigningSeed string
	// TenantSigningSeed is a plain tenant signing key; the callout signs the user JWT it mints with it.
	TenantSigningSeed string
	// CalloutCreds is the callout's own login (a user JWT in AUTH and its seed) in .creds format.
	CalloutCreds string
	// CalloutUserPub is that user's public key, listed in the AUTH account's auth_users.
	CalloutUserPub string
}

func newKey(create func() (nkeys.KeyPair, error)) (kp nkeys.KeyPair, pub string, err error) {
	kp, err = create()
	if err != nil {
		return nil, "", err
	}
	pub, err = kp.PublicKey()
	if err != nil {
		return nil, "", err
	}
	return kp, pub, nil
}

// zeroPermissionTemplate is what the nats-server applies to a user signed by the scoped key. An
// empty allow list means unrestricted in NATS, so both directions carry an explicit deny of ">".
func zeroPermissionTemplate(scope *jwt.UserScope) {
	scope.Template.Pub.Deny.Add(">")
	scope.Template.Sub.Deny.Add(">")
}

// Generate makes a complete, self-consistent set of operator, account and user material.
func Generate() (*Bundle, error) {
	b := &Bundle{}

	opKP, opPub, err := newKey(nkeys.CreateOperator)
	if err != nil {
		return nil, fmt.Errorf("operator key: %w", err)
	}
	defer opKP.Wipe()
	sysKP, sysPub, err := newKey(nkeys.CreateAccount)
	if err != nil {
		return nil, fmt.Errorf("SYS account key: %w", err)
	}
	defer sysKP.Wipe()
	authKP, authPub, err := newKey(nkeys.CreateAccount)
	if err != nil {
		return nil, fmt.Errorf("AUTH account key: %w", err)
	}
	defer authKP.Wipe()
	tenantKP, tenantPub, err := newKey(nkeys.CreateAccount)
	if err != nil {
		return nil, fmt.Errorf("tenant account key: %w", err)
	}
	defer tenantKP.Wipe()

	scopedKP, scopedPub, err := newKey(nkeys.CreateAccount)
	if err != nil {
		return nil, fmt.Errorf("scoped signing key: %w", err)
	}
	defer scopedKP.Wipe()
	authSignKP, authSignPub, err := newKey(nkeys.CreateAccount)
	if err != nil {
		return nil, fmt.Errorf("AUTH signing key: %w", err)
	}
	defer authSignKP.Wipe()
	tenantSignKP, tenantSignPub, err := newKey(nkeys.CreateAccount)
	if err != nil {
		return nil, fmt.Errorf("tenant signing key: %w", err)
	}
	defer tenantSignKP.Wipe()
	calloutKP, calloutPub, err := newKey(nkeys.CreateUser)
	if err != nil {
		return nil, fmt.Errorf("callout user key: %w", err)
	}
	defer calloutKP.Wipe()

	b.OperatorPub, b.SysPub, b.AuthPub, b.TenantPub = opPub, sysPub, authPub, tenantPub
	b.ScopedPub, b.CalloutUserPub = scopedPub, calloutPub

	oc := jwt.NewOperatorClaims(opPub)
	oc.Name = "storm-bench"
	oc.SystemAccount = sysPub
	if b.OperatorJWT, err = oc.Encode(opKP); err != nil {
		return nil, fmt.Errorf("operator JWT: %w", err)
	}

	sc := jwt.NewAccountClaims(sysPub)
	sc.Name = "SYS"
	if b.SysJWT, err = sc.Encode(opKP); err != nil {
		return nil, fmt.Errorf("SYS account JWT: %w", err)
	}

	// The AUTH account turns the callout on: its users are sent to the callout, except the
	// callout's own user, and the callout may place them only in the tenant account.
	ac := jwt.NewAccountClaims(authPub)
	ac.Name = "AUTH"
	ac.Authorization.AuthUsers.Add(calloutPub)
	ac.Authorization.AllowedAccounts.Add(tenantPub)
	scope := jwt.NewUserScope()
	scope.Key = scopedPub
	scope.Role = "zero-permission"
	scope.Description = "load generator tickets: no permissions of their own, only presented to the auth callout"
	zeroPermissionTemplate(scope)
	ac.SigningKeys.AddScopedSigner(scope)
	ac.SigningKeys.Add(authSignPub)
	if b.AuthJWT, err = ac.Encode(opKP); err != nil {
		return nil, fmt.Errorf("AUTH account JWT: %w", err)
	}

	tc := jwt.NewAccountClaims(tenantPub)
	tc.Name = "TENANT"
	tc.SigningKeys.Add(tenantSignPub)
	if b.TenantJWT, err = tc.Encode(opKP); err != nil {
		return nil, fmt.Errorf("tenant account JWT: %w", err)
	}

	// The callout's own login, a user in AUTH signed by the AUTH signing key (so IssuerAccount is set).
	uc := jwt.NewUserClaims(calloutPub)
	uc.Name = "callout"
	uc.IssuerAccount = authPub
	calloutJWT, err := uc.Encode(authSignKP)
	if err != nil {
		return nil, fmt.Errorf("callout user JWT: %w", err)
	}
	calloutSeed, err := calloutKP.Seed()
	if err != nil {
		return nil, fmt.Errorf("callout user seed: %w", err)
	}
	creds, err := jwt.FormatUserConfig(calloutJWT, calloutSeed)
	if err != nil {
		return nil, fmt.Errorf("callout creds: %w", err)
	}
	b.CalloutCreds = string(creds)

	for _, s := range []struct {
		dst *string
		kp  nkeys.KeyPair
	}{{&b.ScopedSeed, scopedKP}, {&b.AuthSigningSeed, authSignKP}, {&b.TenantSigningSeed, tenantSignKP}} {
		seed, err := s.kp.Seed()
		if err != nil {
			return nil, err
		}
		*s.dst = string(seed)
	}
	return b, nil
}

// Object is one Kubernetes object to create: a Secret (Data goes in stringData) or a ConfigMap.
type Object struct {
	Kind string // "Secret" or "ConfigMap"
	Name string
	Data map[string]string
}

// Objects returns what to create, in the order to create it. The ConfigMap is last: it is the
// marker that a run finished, so a rerun that finds it has nothing to do.
func (b *Bundle) Objects(mountDir string) []Object {
	if mountDir == "" {
		mountDir = DefaultMountDir
	}
	return []Object{
		{Kind: "Secret", Name: CalloutSecretName, Data: map[string]string{
			"callout.creds":       b.CalloutCreds,
			"auth-signing.seed":   b.AuthSigningSeed,
			"tenant-signing.seed": b.TenantSigningSeed,
			"auth.pub":            b.AuthPub,
			"tenant.pub":          b.TenantPub,
		}},
		{Kind: "Secret", Name: GeneratorSecretName, Data: map[string]string{
			"scoped.seed": b.ScopedSeed,
			"scoped.pub":  b.ScopedPub,
			"auth.pub":    b.AuthPub,
		}},
		{Kind: "ConfigMap", Name: ConfigMapName, Data: map[string]string{
			"operator.jwt":         b.OperatorJWT,
			"sys.jwt":              b.SysJWT,
			"auth.jwt":             b.AuthJWT,
			"tenant.jwt":           b.TenantJWT,
			"operator.conf":        b.operatorConf(mountDir),
			"resolver-memory.conf": b.resolverMemoryConf(),
		}},
	}
}

// operatorConf is the part of the broker's configuration that names the operator and the system
// account. It does not depend on which resolver is used.
func (b *Bundle) operatorConf(mountDir string) string {
	return fmt.Sprintf("operator: %s\nsystem_account: %s\n", quote(strings.TrimRight(mountDir, "/")+"/operator.jwt"), b.SysPub)
}

// resolverMemoryConf is the MEMORY resolver with the three account JWTs preloaded. A later
// resolver replaces this one fragment; the JWTs are also in the ConfigMap as plain files.
func (b *Bundle) resolverMemoryConf() string {
	jwts := map[string]string{b.SysPub: b.SysJWT, b.AuthPub: b.AuthJWT, b.TenantPub: b.TenantJWT}
	keys := make([]string, 0, len(jwts))
	for k := range jwts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var sb strings.Builder
	sb.WriteString("resolver: MEMORY\nresolver_preload: {\n")
	for _, k := range keys {
		fmt.Fprintf(&sb, "  %s: %s\n", k, quote(jwts[k]))
	}
	sb.WriteString("}\n")
	return sb.String()
}

func quote(s string) string {
	// JSON string syntax is valid in the NATS configuration format for the characters in a JWT or a path.
	q, _ := json.Marshal(s)
	return string(q)
}

// Validate decodes every JWT in the bundle with the jwt library's own validators and checks the
// links between them. It is the check Generate's output must pass; main runs it before writing.
func (b *Bundle) Validate() error {
	var errs []error
	check := func(what, token string, validate func(c jwt.Claims) error) {
		c, err := jwt.Decode(token)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", what, err))
			return
		}
		var vr jwt.ValidationResults
		c.Validate(&vr)
		if vr.IsBlocking(true) {
			errs = append(errs, fmt.Errorf("%s does not validate: %v", what, vr.Errors()))
			return
		}
		if validate != nil {
			if err := validate(c); err != nil {
				errs = append(errs, fmt.Errorf("%s: %w", what, err))
			}
		}
	}
	signedBy := func(pub string) func(jwt.Claims) error {
		return func(c jwt.Claims) error {
			if c.Claims().Issuer != pub {
				return fmt.Errorf("issuer is %s, expected %s", c.Claims().Issuer, pub)
			}
			return nil
		}
	}
	check("operator JWT", b.OperatorJWT, signedBy(b.OperatorPub))
	check("SYS account JWT", b.SysJWT, signedBy(b.OperatorPub))
	check("AUTH account JWT", b.AuthJWT, func(c jwt.Claims) error {
		if err := signedBy(b.OperatorPub)(c); err != nil {
			return err
		}
		ac := c.(*jwt.AccountClaims)
		if len(ac.Authorization.AuthUsers) != 1 || ac.Authorization.AuthUsers[0] != b.CalloutUserPub {
			return errors.New("auth_users is not exactly the callout user")
		}
		if len(ac.Authorization.AllowedAccounts) != 1 || ac.Authorization.AllowedAccounts[0] != b.TenantPub {
			return errors.New("allowed_accounts is not exactly the tenant account")
		}
		scope, ok := ac.SigningKeys.GetScope(b.ScopedPub)
		if !ok || scope == nil {
			return errors.New("the scoped signing key is not in the account's signing keys")
		}
		us, ok := scope.(*jwt.UserScope)
		if !ok {
			return errors.New("the scoped signing key is not a user scope")
		}
		if len(us.Template.Pub.Deny) == 0 || len(us.Template.Sub.Deny) == 0 {
			return errors.New("the scope template does not deny both publish and subscribe")
		}
		if len(us.Template.Pub.Allow) != 0 || len(us.Template.Sub.Allow) != 0 {
			return errors.New("the scope template allows subjects")
		}
		return nil
	})
	check("tenant account JWT", b.TenantJWT, signedBy(b.OperatorPub))
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}
