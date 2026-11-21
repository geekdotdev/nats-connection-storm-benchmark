// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"gopkg.in/yaml.v3"
)

const operatorOverlay = "../../deploy/stack/overlays/local"

// renderOverlay runs `kubectl kustomize` (no cluster needed) and returns the objects by "Kind/name".
func renderOverlay(t *testing.T, overlay string) map[string]map[string]any {
	t.Helper()
	kubectl, err := exec.LookPath("kubectl")
	if err != nil {
		t.Skip("kubectl is not installed")
	}
	cmd := exec.Command(kubectl, "kustomize", overlay)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("kubectl kustomize %s: %v\n%s", overlay, err, stderr.String())
	}
	objs := map[string]map[string]any{}
	dec := yaml.NewDecoder(bytes.NewReader(out))
	for {
		var doc map[string]any
		if err := dec.Decode(&doc); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		if doc == nil {
			continue
		}
		meta, _ := doc["metadata"].(map[string]any)
		objs[fmt.Sprint(doc["kind"], "/", meta["name"])] = doc
	}
	return objs
}

func brokerConfigData(t *testing.T, objs map[string]map[string]any) map[string]string {
	t.Helper()
	cm, ok := objs["ConfigMap/storm-broker-config"]
	if !ok {
		t.Fatal("the overlay has no storm-broker-config ConfigMap")
	}
	data := map[string]string{}
	for k, v := range cm["data"].(map[string]any) {
		data[k] = v.(string)
	}
	return data
}

// at walks a decoded YAML document: string keys into maps, ints into lists.
func at(t *testing.T, v any, path ...any) any {
	t.Helper()
	for _, p := range path {
		switch k := p.(type) {
		case string:
			m, ok := v.(map[string]any)
			if !ok {
				t.Fatalf("expected a map at %q, have %T", k, v)
			}
			v = m[k]
		case int:
			l, ok := v.([]any)
			if !ok || k >= len(l) {
				t.Fatalf("expected a list with index %d, have %T", k, v)
			}
			v = l[k]
		}
	}
	return v
}

func brokerContainer(t *testing.T, objs map[string]map[string]any) map[string]any {
	t.Helper()
	sts := objs["StatefulSet/broker"]
	if sts == nil {
		t.Fatal("no broker StatefulSet")
	}
	for _, c := range at(t, sts, "spec", "template", "spec", "containers").([]any) {
		if cm := c.(map[string]any); cm["name"] == "broker" {
			return cm
		}
	}
	t.Fatal("no broker container")
	return nil
}

// The manifests and provision must agree on where the ConfigMap is mounted and what it is called.
func TestManifestsAndProvisionAgreeOnTheOperatorConfigMap(t *testing.T) {
	objs := renderOverlay(t, operatorOverlay)
	conf := brokerConfigData(t, objs)["nats-server.conf"]
	// The config is mounted at /etc/nats and nats-server resolves an include against that directory, so the
	// include climbs to the mount: ../storm/operator, which is DefaultMountDir seen from /etc/nats.
	rel, err := filepath.Rel("/etc/nats", DefaultMountDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{"operator.conf", "resolver-memory.conf"} {
		if want := fmt.Sprintf("include %q", rel+"/"+f); !strings.Contains(conf, want) {
			t.Fatalf("the broker config does not have %s:\n%s", want, conf)
		}
	}

	var mounted bool
	for _, m := range at(t, brokerContainer(t, objs), "volumeMounts").([]any) {
		if m.(map[string]any)["mountPath"] == DefaultMountDir {
			mounted = true
			if m.(map[string]any)["readOnly"] != true {
				t.Fatal("the operator ConfigMap must be mounted read-only")
			}
		}
	}
	if !mounted {
		t.Fatalf("the broker container does not mount anything at %s", DefaultMountDir)
	}

	var volume string
	for _, v := range at(t, objs["StatefulSet/broker"], "spec", "template", "spec", "volumes").([]any) {
		if vm := v.(map[string]any); vm["name"] == "operator" {
			volume, _ = at(t, vm, "configMap", "name").(string)
			if opt := at(t, vm, "configMap").(map[string]any)["optional"]; opt != nil {
				t.Fatalf("the operator ConfigMap must not be optional, it is %v", opt)
			}
		}
	}
	if volume != ConfigMapName {
		t.Fatalf("the broker mounts ConfigMap %q, provision creates %q", volume, ConfigMapName)
	}
}

func TestOperatorBrokerHasNoConfigModeSecrets(t *testing.T) {
	objs := renderOverlay(t, operatorOverlay)
	conf := brokerConfigData(t, objs)["nats-server.conf"]
	for _, bad := range []string{"auth_callout", "accounts", "CALLOUT_", "A0 {", "password:"} {
		if strings.Contains(conf, bad) {
			t.Fatalf("the operator-mode config still has %q:\n%s", bad, conf)
		}
	}
	for _, e := range at(t, brokerContainer(t, objs), "env").([]any) {
		if name := e.(map[string]any)["name"].(string); strings.HasPrefix(name, "CALLOUT_") {
			t.Fatalf("the broker still has environment variable %s", name)
		}
	}
}

// writeTestTLS makes a CA and a server certificate for 127.0.0.1 and returns the CA file's path.
func writeTestTLS(t *testing.T, dir string) string {
	t.Helper()
	caKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	caTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test CA"},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caCert, _ := x509.ParseCertificate(caDER)
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "127.0.0.1"},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, _ := x509.MarshalECPrivateKey(key)
	write := func(name, typ string, b []byte) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: b}), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	write("tls.crt", "CERTIFICATE", der)
	write("tls.key", "EC PRIVATE KEY", keyDER)
	return write("ca.pem", "CERTIFICATE", caDER)
}

// TestRenderedBrokerConfigBootsWithProvisionOutput takes the broker configuration out of the rendered
// manifests, lays out the pod's directories under a temp root with ports changed to random ones, and starts a real nats-server with the
// files provision writes. A ticket then goes through the WebSocket listener the benchmark uses.
func TestRenderedBrokerConfigBootsWithProvisionOutput(t *testing.T) {
	objs := renderOverlay(t, operatorOverlay)
	files := brokerConfigData(t, objs)

	// The pod's layout under a temp root: /etc/nats, /etc/storm/operator and /etc/storm/tls.
	root := t.TempDir()
	confDir := filepath.Join(root, "etc", "nats")
	opDir := filepath.Join(root, "etc", "storm", "operator")
	tlsDir := filepath.Join(root, "etc", "storm", "tls")
	for _, d := range []string{confDir, opDir, tlsDir} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	caFile := writeTestTLS(t, tlsDir)

	b := mustGenerate(t)
	for name, data := range b.Objects(opDir)[2].Data {
		if err := os.WriteFile(filepath.Join(opDir, name), []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	rewrite := strings.NewReplacer(
		"0.0.0.0:4222", "127.0.0.1:-1",
		"0.0.0.0:8222", "127.0.0.1:-1",
		"0.0.0.0:8443", "127.0.0.1:-1",
		"/etc/storm/tls", tlsDir,
	)
	for _, name := range []string{"nats-server.conf", "cluster.conf", "extra.conf"} {
		if err := os.WriteFile(filepath.Join(confDir, name), []byte(rewrite.Replace(files[name])), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("POD_NAME", "broker-0")

	s := startBroker(t, filepath.Join(confDir, "nats-server.conf"))
	requests := startCallout(t, b, s.ClientURL())

	connect := func(t *testing.T, tags ...string) (*nats.Conn, error) {
		return nats.Connect(s.WebsocketURL(), ticket(t, b, nil, tags...), nats.RootCAs(caFile), nats.NoReconnect(), nats.Name("ws-client"))
	}

	t.Run("a ticket over the WebSocket listener is placed in the tenant account", func(t *testing.T) {
		before := requests.Load()
		nc, err := connect(t, "role:reader")
		if err != nil {
			t.Fatal(err)
		}
		defer nc.Close()
		if requests.Load() != before+1 {
			t.Fatal("the callout was not asked")
		}
		if got := accountOf(t, s, "ws-client"); got != b.TenantPub {
			t.Fatalf("account %q, want %q", got, b.TenantPub)
		}
	})

	t.Run("a long ticket (a CONNECT line past the default 4096 bytes) connects", func(t *testing.T) {
		var tags []string
		for i := 0; i < 70; i++ {
			tags = append(tags, fmt.Sprintf("role:%02d-%s", i, strings.Repeat("x", 55)))
		}
		nc, err := connect(t, tags...)
		if err != nil {
			t.Fatalf("a long ticket was refused: %v", err)
		}
		nc.Close()
	})
}

func podSpec(t *testing.T, objs map[string]map[string]any, kind, name string) map[string]any {
	t.Helper()
	o := objs[kind+"/"+name]
	if o == nil {
		t.Fatalf("no %s %s", kind, name)
	}
	return at(t, o, "spec", "template", "spec").(map[string]any)
}

func namedItem(t *testing.T, list any, name string) map[string]any {
	t.Helper()
	for _, it := range list.([]any) {
		if m := it.(map[string]any); m["name"] == name {
			return m
		}
	}
	t.Fatalf("no item named %q", name)
	return nil
}

// The callout pod: the init container is the only one that holds the ServiceAccount token, the callout reads the Secret
// the init container creates through its environment (a Secret volume would keep the pod from starting), and every key the
// callout asks for is one provision writes.
func TestCalloutPodUsesProvisionOutput(t *testing.T) {
	objs := renderOverlay(t, operatorOverlay)
	pod := podSpec(t, objs, "Deployment", "callout")

	if pod["automountServiceAccountToken"] != false {
		t.Fatal("the pod must not automount the ServiceAccount token")
	}
	if pod["serviceAccountName"] != "storm-provision" {
		t.Fatalf("serviceAccountName = %v", pod["serviceAccountName"])
	}
	init := namedItem(t, pod["initContainers"], "provision")
	mounts := func(c map[string]any) []string {
		var out []string
		ms, _ := c["volumeMounts"].([]any)
		for _, m := range ms {
			out = append(out, m.(map[string]any)["name"].(string))
		}
		return out
	}
	if got := mounts(init); len(got) != 1 || got[0] != "provision-api" {
		t.Fatalf("the init container mounts %v, want only provision-api", got)
	}
	callout := namedItem(t, pod["containers"], "callout")
	if got := mounts(callout); len(got) != 0 {
		t.Fatalf("the callout container mounts %v: it must not hold the ServiceAccount token", got)
	}

	// No volume refers to a Secret the init container creates.
	for _, v := range pod["volumes"].([]any) {
		vm := v.(map[string]any)
		if s, ok := vm["secret"].(map[string]any); ok {
			t.Fatalf("volume %v mounts Secret %v: a pod's volumes are set up before its init containers run", vm["name"], s["secretName"])
		}
	}

	provided := map[string]bool{}
	for _, o := range mustGenerate(t).Objects("") {
		if o.Name == CalloutSecretName {
			for k := range o.Data {
				provided[k] = true
			}
		}
	}
	source, err := os.ReadFile("../callout/main.go")
	if err != nil {
		t.Fatal(err)
	}
	seen := 0
	for _, e := range callout["env"].([]any) {
		em := e.(map[string]any)
		name := em["name"].(string)
		if !strings.Contains(string(source), `"`+name+`"`) {
			t.Fatalf("the manifest sets %s but components/callout/main.go does not read it", name)
		}
		ref, ok := at(t, em, "valueFrom").(map[string]any)["secretKeyRef"].(map[string]any)
		if !ok {
			t.Fatalf("%s does not come from a Secret", name)
		}
		if ref["name"] != CalloutSecretName {
			t.Fatalf("%s reads Secret %v, provision creates %s", name, ref["name"], CalloutSecretName)
		}
		if !provided[ref["key"].(string)] {
			t.Fatalf("%s reads key %v, which provision does not write", name, ref["key"])
		}
		seen++
	}
	if seen != 4 {
		t.Fatalf("the callout has %d environment variables, want the four operator-mode ones", seen)
	}

	args := fmt.Sprint(callout["args"])
	if strings.Contains(args, "--mode") || strings.Contains(args, "--accounts") || !strings.Contains(args, "--url") {
		t.Fatalf("the callout's arguments are %s: it has one mode, so no --mode or --accounts, and it needs --url", args)
	}
}

// The ServiceAccount may create the objects and look for the marker ConfigMap, and nothing else.
func TestProvisionRoleIsNarrow(t *testing.T) {
	objs := renderOverlay(t, operatorOverlay)
	role := objs["Role/storm-provision"]
	if role == nil {
		t.Fatal("no Role storm-provision")
	}
	type rule struct{ resources, names, verbs string }
	var got []rule
	for _, r := range role["rules"].([]any) {
		rm := r.(map[string]any)
		join := func(k string) string {
			var parts []string
			for _, x := range rm[k].([]any) {
				parts = append(parts, x.(string))
			}
			return strings.Join(parts, ",")
		}
		names := ""
		if _, ok := rm["resourceNames"]; ok {
			names = join("resourceNames")
		}
		got = append(got, rule{join("resources"), names, join("verbs")})
	}
	want := []rule{
		{"secrets,configmaps", "", "create"},
		{"configmaps", ConfigMapName, "get"},
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("the Role's rules are %v, want %v", got, want)
	}

	binding := objs["RoleBinding/storm-provision"]
	if binding == nil {
		t.Fatal("no RoleBinding")
	}
	subject := at(t, binding, "subjects", 0).(map[string]any)
	if subject["name"] != "storm-provision" || subject["namespace"] != "storm-bench" {
		t.Fatalf("the binding's subject is %v", subject)
	}
	for _, k := range []string{"Role/storm-provision", "RoleBinding/storm-provision", "ServiceAccount/storm-provision"} {
		if ns := at(t, objs[k], "metadata", "namespace"); ns != "storm-bench" {
			t.Fatalf("%s is in namespace %v", k, ns)
		}
	}
}
