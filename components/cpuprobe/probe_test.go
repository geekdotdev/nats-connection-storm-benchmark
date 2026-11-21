// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const statTmpl = "usage_usec %d\nuser_usec %d\nsystem_usec %d\ncore_sched.force_idle_usec 0\nnr_periods %d\nnr_throttled %d\nthrottled_usec %d\nnr_bursts 0\nburst_usec 0\n"

func writeCgroup(t *testing.T, dir string, usage, periods, throttled, throttledUsec int, max string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	stat := sprintf(statTmpl, usage, usage*6/10, usage*4/10, periods, throttled, throttledUsec)
	if err := os.WriteFile(filepath.Join(dir, "cpu.stat"), []byte(stat), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "cpu.max"), []byte(max+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestParseCPUReadsTheCountersAndTheLimit(t *testing.T) {
	c, err := ParseCPU(sprintf(statTmpl, 1812754, 1118932, 693821, 40, 7, 350000), "300000 100000")
	if err != nil {
		t.Fatal(err)
	}
	if c.UsageUsec != 1812754 || c.UserUsec != 1118932 || c.SystemUsec != 693821 || c.Periods != 40 || c.ThrottledPeriods != 7 || c.ThrottledUsec != 350000 {
		t.Fatalf("counters %+v", c)
	}
	if c.LimitCores == nil || *c.LimitCores != 3 {
		t.Fatalf("limit %v", c.LimitCores)
	}
	c, _ = ParseCPU(sprintf(statTmpl, 1, 1, 1, 0, 0, 0), "max 100000")
	if c.LimitCores != nil {
		t.Fatalf("max is no limit, got %v", *c.LimitCores)
	}
	c, _ = ParseCPU(sprintf(statTmpl, 1, 1, 1, 0, 0, 0), "50000 100000")
	if c.LimitCores == nil || *c.LimitCores != 0.5 {
		t.Fatalf("half a core: %v", c.LimitCores)
	}
}

func TestParseCPURefusesWhatIsNotCgroupV2(t *testing.T) {
	for _, s := range []string{"", "cpu 123\ncpu0 12\n", "nr_periods 4\nnr_throttled 1\n", "usage_usec abc\n"} {
		if _, err := ParseCPU(s, ""); err == nil {
			t.Errorf("%q was read as a cpu.stat", s)
		}
	}
}

func TestFindCgroupMatchesTheContainerIDInTheDirectoryName(t *testing.T) {
	root := t.TempDir()
	id := "0123456789abcdef0123456789abcdef"
	want := filepath.Join(root, "kubepods.slice", "kubepods-burstable.slice", "kubepods-burstable-podUID.slice", "cri-containerd-"+id+".scope")
	writeCgroup(t, want, 10, 0, 0, 0, "max 100000")
	writeCgroup(t, filepath.Join(root, "kubepods.slice", "cri-containerd-ffffffffffffffffffffffffffffffff.scope"), 99, 0, 0, 0, "max 100000")
	got, err := FindCgroup(root, id)
	if err != nil || got != want {
		t.Fatalf("got %q, %v; want %q", got, err, want)
	}
	if _, err := FindCgroup(root, "aaaaaaaaaaaaaaaaaaaaaaaa"); err == nil {
		t.Error("a container with no cgroup was found")
	}
	if _, err := FindCgroup(root, "short"); err == nil {
		t.Error("an id too short to search by was accepted")
	}
}

const podsJSON = `{"items":[
 {"metadata":{"name":"broker-0","namespace":"storm-bench","labels":{"app.kubernetes.io/name":"broker"}},
  "status":{"phase":"Running","containerStatuses":[{"name":"broker","containerID":"containerd://%[1]s","state":{"running":{}}}]}},
 {"metadata":{"name":"callout-x","namespace":"storm-bench","labels":{"app.kubernetes.io/name":"callout"}},
  "status":{"phase":"Running","containerStatuses":[{"name":"callout","containerID":"containerd://%[2]s","state":{"running":{}}}]}},
 {"metadata":{"name":"loadgen-done","namespace":"storm-bench","labels":{"app.kubernetes.io/name":"loadgen"}},
  "status":{"phase":"Succeeded","containerStatuses":[{"name":"loadgen","containerID":"containerd://%[3]s","state":{"terminated":{}}}]}},
 {"metadata":{"name":"starting","namespace":"storm-bench","labels":{}},
  "status":{"phase":"Pending","containerStatuses":[{"name":"c","containerID":"","state":{"waiting":{}}}]}}
]}`

func TestParsePodsKeepsOnlyRunningContainersWithAnID(t *testing.T) {
	cs, err := ParsePods([]byte(sprintf(podsJSON, "aaaaaaaaaaaa", "bbbbbbbbbbbb", "cccccccccccc")))
	if err != nil {
		t.Fatal(err)
	}
	if len(cs) != 2 || cs[0].Pod != "broker-0" || cs[0].Name != "broker" || cs[0].Component != "broker" || cs[0].ID != "aaaaaaaaaaaa" || cs[1].Component != "callout" {
		t.Fatalf("%+v", cs)
	}
	if _, err := ParsePods([]byte("not json")); err == nil {
		t.Error("garbage was read as a pod list")
	}
}

type fakeLister struct {
	cs    []Container
	err   error
	calls int
}

func (f *fakeLister) List(context.Context) ([]Container, error) { f.calls++; return f.cs, f.err }

func TestSnapshotReadsTheCountersFreshOnEveryCall(t *testing.T) {
	root := t.TempDir()
	id := "0123456789abcdef"
	dir := filepath.Join(root, "x", "cri-containerd-"+id+".scope")
	writeCgroup(t, dir, 1_000_000, 10, 1, 5000, "300000 100000")
	l := &fakeLister{cs: []Container{{Namespace: "storm-bench", Pod: "broker-0", Name: "broker", Component: "broker", ID: id}}}
	p := NewProbe(l, root, time.Hour)
	first := p.Snapshot(context.Background())
	if len(first) != 1 || first[0].UsageUsec != 1_000_000 || first[0].ThrottledPeriods != 1 {
		t.Fatalf("%+v", first)
	}
	writeCgroup(t, dir, 3_500_000, 30, 9, 90000, "300000 100000") // the counters moved on
	second := p.Snapshot(context.Background())
	if second[0].UsageUsec != 3_500_000 || second[0].ThrottledPeriods != 9 {
		t.Fatalf("a stale reading: %+v", second[0])
	}
	if l.calls != 1 {
		t.Fatalf("the pod list was asked for %d times within its refresh interval", l.calls)
	}
}

func TestAContainerThatRestartedIsFoundAgainByItsNewID(t *testing.T) {
	root := t.TempDir()
	oldID, newID := "aaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbb"
	writeCgroup(t, filepath.Join(root, "cri-containerd-"+oldID+".scope"), 500, 0, 0, 0, "max 100000")
	l := &fakeLister{cs: []Container{{Pod: "broker-0", Name: "broker", ID: oldID}}}
	now := time.Unix(1000, 0)
	p := NewProbe(l, root, time.Second)
	p.now = func() time.Time { return now }
	if s := p.Snapshot(context.Background()); len(s) != 1 || s[0].UsageUsec != 500 {
		t.Fatalf("%+v", s)
	}
	os.RemoveAll(filepath.Join(root, "cri-containerd-"+oldID+".scope"))
	writeCgroup(t, filepath.Join(root, "cri-containerd-"+newID+".scope"), 7, 0, 0, 0, "max 100000")
	l.cs = []Container{{Pod: "broker-0", Name: "broker", ID: newID}}
	now = now.Add(2 * time.Second) // past the refresh interval
	s := p.Snapshot(context.Background())
	if len(s) != 1 || s[0].UsageUsec != 7 {
		t.Fatalf("the restarted container was not picked up: %+v", s)
	}
	if _, kept := p.dirs[oldID]; kept {
		t.Error("the gone container's directory is still remembered")
	}
}

func TestAContainerWithNoCgroupYetIsLeftOutNotFailed(t *testing.T) {
	root := t.TempDir()
	l := &fakeLister{cs: []Container{{Pod: "new", Name: "c", ID: "cccccccccccccccc"}}}
	if s := NewProbe(l, root, time.Hour).Snapshot(context.Background()); len(s) != 0 {
		t.Fatalf("%+v", s)
	}
}

func TestAFailingPodListKeepsTheLastOne(t *testing.T) {
	root := t.TempDir()
	id := "dddddddddddddddd"
	writeCgroup(t, filepath.Join(root, id), 42, 0, 0, 0, "max 100000")
	l := &fakeLister{cs: []Container{{Pod: "p", Name: "c", ID: id}}}
	now := time.Unix(0, 0)
	p := NewProbe(l, root, time.Second)
	p.now = func() time.Time { return now }
	p.Snapshot(context.Background())
	l.err = http.ErrServerClosed
	now = now.Add(5 * time.Second)
	if s := p.Snapshot(context.Background()); len(s) != 1 || s[0].UsageUsec != 42 {
		t.Fatalf("the API being down must not blank the readings: %+v", s)
	}
}

func fixture(t *testing.T) (*Probe, string) {
	root := t.TempDir()
	id := "eeeeeeeeeeeeeeee"
	writeCgroup(t, filepath.Join(root, "cri-containerd-"+id+".scope"), 2_500_000, 100, 31, 1_220_000, "100000 100000")
	l := &fakeLister{cs: []Container{{Namespace: "storm-bench", Pod: "callout-x", Name: "callout", Component: "callout", ID: id}}}
	return NewProbe(l, root, time.Hour), id
}

func TestMetricsAreInPrometheusFormatInSeconds(t *testing.T) {
	p, _ := fixture(t)
	srv := httptest.NewServer(Handler(p, "node-1"))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var sb strings.Builder
	buf := make([]byte, 4096)
	for {
		n, e := resp.Body.Read(buf)
		sb.Write(buf[:n])
		if e != nil {
			break
		}
	}
	text := sb.String()
	labels := `{namespace="storm-bench",pod="callout-x",container="callout",component="callout",node="node-1"}`
	for _, want := range []string{
		"# TYPE storm_cpu_usage_seconds_total counter",
		"storm_cpu_usage_seconds_total" + labels + " 2.5\n",
		"storm_cpu_throttled_periods_total" + labels + " 31\n",
		"storm_cpu_throttled_seconds_total" + labels + " 1.22\n",
		"storm_cpu_periods_total" + labels + " 100\n",
		"# TYPE storm_cpu_limit_cores gauge",
		"storm_cpu_limit_cores" + labels + " 1\n",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("missing %q in\n%s", want, text)
		}
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Errorf("content type %q", ct)
	}
}

func TestAContainerWithNoLimitHasNoLimitSeries(t *testing.T) {
	root := t.TempDir()
	id := "ffffffffffffffff"
	writeCgroup(t, filepath.Join(root, id), 1, 0, 0, 0, "max 100000")
	p := NewProbe(&fakeLister{cs: []Container{{Pod: "p", Name: "c", ID: id}}}, root, time.Hour)
	if strings.Contains(Metrics("n", p.Snapshot(context.Background())), "storm_cpu_limit_cores{") {
		t.Error("a limit series for a container with no limit")
	}
}

func TestLabelValuesAreEscaped(t *testing.T) {
	out := Metrics("n", []Sample{{Container: Container{Pod: `a"b\c` + "\n", Name: "c"}}})
	if !strings.Contains(out, `pod="a\"b\\c\n"`) {
		t.Errorf("not escaped:\n%s", out)
	}
}

func TestCountersAreTheInstantAsJSON(t *testing.T) {
	p, _ := fixture(t)
	srv := httptest.NewServer(Handler(p, "node-1"))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/counters")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var doc countersDoc
	if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
		t.Fatal(err)
	}
	if doc.Node != "node-1" || len(doc.Containers) != 1 || doc.TimeMs < time.Now().Add(-time.Minute).UnixMilli() {
		t.Fatalf("%+v", doc)
	}
	c := doc.Containers[0]
	if c.Pod != "callout-x" || c.Container != "callout" || c.Component != "callout" || c.UsageUsec != 2_500_000 || c.ThrottledPeriods != 31 ||
		c.Periods != 100 || c.ThrottledUsec != 1_220_000 || c.LimitCores == nil || *c.LimitCores != 1 {
		t.Fatalf("%+v", c)
	}
	// the generator reads these keys: they are part of the contract
	raw, _ := json.Marshal(doc)
	for _, key := range []string{`"usageUsec"`, `"throttledPeriods"`, `"throttledUsec"`, `"periods"`, `"limitCores"`, `"timeMs"`, `"containers"`} {
		if !strings.Contains(string(raw), key) {
			t.Errorf("the JSON has no %s", key)
		}
	}
}

func TestHealthz(t *testing.T) {
	p, _ := fixture(t)
	rec := httptest.NewRecorder()
	Handler(p, "n").ServeHTTP(rec, httptest.NewRequest("GET", "/healthz", nil))
	if rec.Code != 200 {
		t.Fatal(rec.Code)
	}
}

func TestTheKubeListerAsksForThisNodesPodsWithTheTokenAndSelector(t *testing.T) {
	var gotPath, gotQuery, gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotQuery, gotAuth = r.URL.Path, r.URL.RawQuery, r.Header.Get("Authorization")
		w.Write([]byte(sprintf(podsJSON, "aaaaaaaaaaaa", "bbbbbbbbbbbb", "cccccccccccc")))
	}))
	defer srv.Close()
	tok := filepath.Join(t.TempDir(), "token")
	os.WriteFile(tok, []byte("s3cret\n"), 0o600)
	k := &kubeLister{base: srv.URL, namespace: "storm-bench", node: "node-1", selector: "app.kubernetes.io/part-of=storm-bench", tokenFile: tok, client: srv.Client()}
	cs, err := k.List(context.Background())
	if err != nil || len(cs) != 2 {
		t.Fatalf("%v %v", cs, err)
	}
	if gotPath != "/api/v1/namespaces/storm-bench/pods" || gotAuth != "Bearer s3cret" ||
		!strings.Contains(gotQuery, "fieldSelector=spec.nodeName%3Dnode-1") || !strings.Contains(gotQuery, "labelSelector=app.kubernetes.io%2Fpart-of%3Dstorm-bench") {
		t.Fatalf("%q %q %q", gotPath, gotQuery, gotAuth)
	}
}

func TestTheKubeListerReportsAnErrorStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "no", http.StatusForbidden) }))
	defer srv.Close()
	tok := filepath.Join(t.TempDir(), "token")
	os.WriteFile(tok, []byte("t"), 0o600)
	k := &kubeLister{base: srv.URL, namespace: "n", node: "x", selector: "a=b", tokenFile: tok, client: srv.Client()}
	if _, err := k.List(context.Background()); err == nil || !strings.Contains(err.Error(), "403") {
		t.Fatalf("%v", err)
	}
}
