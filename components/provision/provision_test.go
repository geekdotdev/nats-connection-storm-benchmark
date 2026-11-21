// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
)

func mustGenerate(t *testing.T) *Bundle {
	t.Helper()
	b, err := Generate()
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestGeneratedBundleValidates(t *testing.T) {
	b := mustGenerate(t)
	if err := b.Validate(); err != nil {
		t.Fatal(err)
	}
	// Two runs share nothing.
	c := mustGenerate(t)
	if b.OperatorPub == c.OperatorPub || b.AuthPub == c.AuthPub || b.ScopedPub == c.ScopedPub {
		t.Fatal("two runs produced the same keys")
	}
}

func TestValidateCatchesABrokenBundle(t *testing.T) {
	b := mustGenerate(t)
	other := mustGenerate(t)
	b.TenantJWT = other.TenantJWT // signed by a different operator
	if err := b.Validate(); err == nil || !strings.Contains(err.Error(), "tenant account JWT") {
		t.Fatalf("want a tenant JWT error, got %v", err)
	}
}

// A user JWT signed by the scoped key passes the library's check only if it has no permissions of
// its own. jwt.NewUserClaims sets limits, which fail it; SetScoped(true) clears them.
func TestScopedKeyAcceptsOnlyEmptyPermissions(t *testing.T) {
	b := mustGenerate(t)
	ac, err := jwt.DecodeAccountClaims(b.AuthJWT)
	if err != nil {
		t.Fatal(err)
	}
	scope, ok := ac.SigningKeys.GetScope(b.ScopedPub)
	if !ok || scope == nil {
		t.Fatal("no scope for the scoped key")
	}
	kp, err := nkeys.FromSeed([]byte(b.ScopedSeed))
	if err != nil {
		t.Fatal(err)
	}
	upub := func() string {
		u, _ := nkeys.CreateUser()
		p, _ := u.PublicKey()
		return p
	}

	good := jwt.NewUserClaims(upub())
	good.SetScoped(true)
	good.IssuerAccount = b.AuthPub
	good.Tags.Add("role:reader")
	good.Expires = time.Now().Add(time.Hour).Unix()
	token, err := good.Encode(kp)
	if err != nil {
		t.Fatal(err)
	}
	dec, err := jwt.DecodeUserClaims(token)
	if err != nil {
		t.Fatal(err)
	}
	if err := scope.ValidateScopedSigner(dec); err != nil {
		t.Fatalf("a scoped ticket with tags and an expiry was refused: %v", err)
	}

	withDefaults := jwt.NewUserClaims(upub()) // limits of -1, as NewUserClaims sets
	withDefaults.IssuerAccount = b.AuthPub
	token, _ = withDefaults.Encode(kp)
	dec, _ = jwt.DecodeUserClaims(token)
	if scope.ValidateScopedSigner(dec) == nil {
		t.Fatal("a ticket with default limits passed: the generator must call SetScoped(true)")
	}

	withPerms := jwt.NewUserClaims(upub())
	withPerms.SetScoped(true)
	withPerms.IssuerAccount = b.AuthPub
	withPerms.Pub.Allow.Add("foo")
	token, _ = withPerms.Encode(kp)
	dec, _ = jwt.DecodeUserClaims(token)
	if scope.ValidateScopedSigner(dec) == nil {
		t.Fatal("a ticket with its own permissions passed")
	}
}

// A seed is exactly 58 base32 characters starting S, then its kind. Anchoring both ends keeps a 56-character public key that happens
// to contain "SA..." or "SC..." from matching (an unanchored pattern flaked about once in 150 runs).
var seedRE = regexp.MustCompile(`(?:^|[^A-Z2-7])(S[OANUCX][A-Z2-7]{56})(?:$|[^A-Z2-7])`)

func seedsIn(o Object) []string {
	var out []string
	for _, v := range o.Data {
		for _, m := range seedRE.FindAllStringSubmatch(v, -1) {
			out = append(out, m[1])
		}
	}
	return out
}

func pubOf(t *testing.T, seed string) string {
	t.Helper()
	kp, err := nkeys.FromSeed([]byte(seed))
	if err != nil {
		t.Fatalf("not a seed: %v", err)
	}
	p, _ := kp.PublicKey()
	return p
}

// Each seed goes to exactly one object, the broker's ConfigMap holds none, and the identity seeds
// of the operator and the accounts are in no object at all.
func TestSeedsGoOnlyWhereTheyAreUsed(t *testing.T) {
	b := mustGenerate(t)
	objs := map[string]Object{}
	for _, o := range b.Objects("") {
		objs[o.Name] = o
	}
	if got := len(seedsIn(objs[ConfigMapName])); got != 0 {
		t.Fatalf("the broker's ConfigMap holds %d seeds", got)
	}

	gen := seedsIn(objs[GeneratorSecretName])
	if len(gen) != 1 || pubOf(t, gen[0]) != b.ScopedPub {
		t.Fatalf("the generator's Secret must hold exactly the scoped seed, has %v", gen)
	}

	callout := map[string]bool{}
	for _, s := range seedsIn(objs[CalloutSecretName]) {
		callout[pubOf(t, s)] = true
	}
	if len(callout) != 3 || !callout[b.CalloutUserPub] {
		t.Fatalf("the callout's Secret must hold three seeds, including its user's: %d", len(callout))
	}
	if callout[b.ScopedPub] {
		t.Fatal("the callout holds the scoped seed")
	}
	for _, id := range []string{b.OperatorPub, b.SysPub, b.AuthPub, b.TenantPub} {
		for name, o := range objs {
			for _, s := range seedsIn(o) {
				if pubOf(t, s) == id {
					t.Fatalf("an identity seed (%s) is stored in %s", id, name)
				}
			}
		}
	}
}

func TestObjectsOrderAndMarker(t *testing.T) {
	objs := mustGenerate(t).Objects("")
	if len(objs) != 3 || objs[2].Kind != "ConfigMap" || objs[2].Name != ConfigMapName {
		t.Fatalf("the ConfigMap must come last, as the completion marker: %+v", objs)
	}
}

func TestBrokerConfigFragments(t *testing.T) {
	b := mustGenerate(t)
	cm := b.Objects("/mnt/op/")[2].Data
	if want := `operator: "/mnt/op/operator.jwt"`; !strings.Contains(cm["operator.conf"], want) {
		t.Fatalf("operator.conf lacks %s:\n%s", want, cm["operator.conf"])
	}
	if !strings.Contains(cm["operator.conf"], "system_account: "+b.SysPub) {
		t.Fatalf("operator.conf does not name the system account:\n%s", cm["operator.conf"])
	}
	r := cm["resolver-memory.conf"]
	for _, want := range []string{"resolver: MEMORY", b.SysPub + `: "` + b.SysJWT + `"`, b.AuthPub + `: "` + b.AuthJWT + `"`, b.TenantPub + `: "` + b.TenantJWT + `"`} {
		if !strings.Contains(r, want) {
			t.Fatalf("resolver-memory.conf lacks %.60s", want)
		}
	}
	if cm["operator.jwt"] != b.OperatorJWT || cm["auth.jwt"] != b.AuthJWT {
		t.Fatal("the plain JWT files do not match the bundle")
	}
}

// fakeStore records what is created and can fail on request.
type fakeStore struct {
	mu       sync.Mutex
	objects  map[string]Object
	created  []string
	existsOn map[string]bool   // names that exist before the run
	failOn   map[string]error  // name -> error from Create
	hook     func(name string) // called before Create of name
}

func newFake() *fakeStore {
	return &fakeStore{objects: map[string]Object{}, existsOn: map[string]bool{}, failOn: map[string]error{}}
}

func (f *fakeStore) Exists(_ context.Context, kind, name string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.objects[kind+"/"+name]
	return ok || f.existsOn[kind+"/"+name], nil
}

func (f *fakeStore) Create(_ context.Context, o Object) error {
	if f.hook != nil {
		f.hook(o.Name)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	key := o.Kind + "/" + o.Name
	if err := f.failOn[o.Name]; err != nil {
		return err
	}
	if _, ok := f.objects[key]; ok || f.existsOn[key] {
		return ErrExists
	}
	f.objects[key] = o
	f.created = append(f.created, o.Name)
	return nil
}

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func TestRunCreatesAllThenSkips(t *testing.T) {
	s := newFake()
	var gens int
	gen := func() (*Bundle, error) { gens++; return Generate() }
	created, err := Run(context.Background(), s, gen, "", time.Second, quiet)
	if err != nil || !created {
		t.Fatalf("first run: created=%v err=%v", created, err)
	}
	want := []string{CalloutSecretName, GeneratorSecretName, ConfigMapName}
	if strings.Join(s.created, ",") != strings.Join(want, ",") {
		t.Fatalf("created %v, want %v", s.created, want)
	}
	before := s.objects["ConfigMap/"+ConfigMapName].Data["auth.jwt"]

	// A restart of the pod runs it again: nothing is generated and nothing changes.
	created, err = Run(context.Background(), s, gen, "", time.Second, quiet)
	if err != nil || created {
		t.Fatalf("second run: created=%v err=%v", created, err)
	}
	if gens != 1 {
		t.Fatalf("keys were generated %d times, want 1", gens)
	}
	if s.objects["ConfigMap/"+ConfigMapName].Data["auth.jwt"] != before || len(s.created) != 3 {
		t.Fatal("the second run changed the objects")
	}
}

func TestRunPartialStateIsAnErrorNotAnOverwrite(t *testing.T) {
	s := newFake()
	s.existsOn["Secret/"+CalloutSecretName] = true // an earlier run stopped after its first object
	_, err := Run(context.Background(), s, Generate, "", 300*time.Millisecond, quiet)
	if !errors.Is(err, ErrPartial) {
		t.Fatalf("want ErrPartial, got %v", err)
	}
	if len(s.created) != 0 {
		t.Fatalf("a run that found a partial state created %v", s.created)
	}
}

// Two callout pods start together: the second finds the first's object and waits for its marker.
func TestRunWaitsForAConcurrentRun(t *testing.T) {
	s := newFake()
	s.existsOn["Secret/"+CalloutSecretName] = true
	go func() {
		time.Sleep(400 * time.Millisecond)
		s.mu.Lock()
		s.objects["ConfigMap/"+ConfigMapName] = Object{}
		s.mu.Unlock()
	}()
	created, err := Run(context.Background(), s, Generate, "", 5*time.Second, quiet)
	if err != nil || created {
		t.Fatalf("created=%v err=%v", created, err)
	}
}

func TestRunStopsOnACreateError(t *testing.T) {
	s := newFake()
	s.failOn[GeneratorSecretName] = errors.New("forbidden")
	_, err := Run(context.Background(), s, Generate, "", time.Second, quiet)
	if err == nil || !strings.Contains(err.Error(), "forbidden") {
		t.Fatalf("got %v", err)
	}
	if _, ok := s.objects["ConfigMap/"+ConfigMapName]; ok {
		t.Fatal("the marker was written although a create failed")
	}
}

func TestRunDoesNotCreateAnythingIfGenerationFails(t *testing.T) {
	s := newFake()
	_, err := Run(context.Background(), s, func() (*Bundle, error) { return nil, errors.New("no entropy") }, "", time.Second, quiet)
	if err == nil || len(s.created) != 0 {
		t.Fatalf("err=%v created=%v", err, s.created)
	}
}

func TestOutDirWritesFilesAndRefusesToOverwrite(t *testing.T) {
	dir := t.TempDir()
	if err := cli(context.Background(), []string{"--out-dir", dir}, io.Discard); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{"secret-" + CalloutSecretName + "/callout.creds", "secret-" + GeneratorSecretName + "/scoped.seed", "configmap-" + ConfigMapName + "/operator.jwt"} {
		if _, err := os.Stat(filepath.Join(dir, p)); err != nil {
			t.Fatalf("missing %s: %v", p, err)
		}
	}
	before, _ := os.ReadFile(filepath.Join(dir, "configmap-"+ConfigMapName, "auth.jwt"))
	if err := cli(context.Background(), []string{"--out-dir", dir}, io.Discard); err != nil {
		t.Fatalf("a rerun over complete output should do nothing: %v", err)
	}
	after, _ := os.ReadFile(filepath.Join(dir, "configmap-"+ConfigMapName, "auth.jwt"))
	if string(before) != string(after) {
		t.Fatal("a rerun changed the output")
	}
	if err := cli(context.Background(), []string{"--out-dir", filepath.Join(dir, "nope")}, io.Discard); err == nil {
		t.Fatal("a missing --out-dir was accepted")
	}
}

func TestNotInAClusterIsAClearError(t *testing.T) {
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	t.Setenv("KUBERNETES_SERVICE_PORT", "")
	err := cli(context.Background(), nil, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "not running in a cluster") {
		t.Fatalf("got %v", err)
	}
}
