// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type clock struct{ t time.Time }

func (c *clock) now() time.Time { return c.t }

func newHarvester(t *testing.T, cs []Container, get func(string) ([]byte, error)) (*Harvester, *clock) {
	t.Helper()
	clk := &clock{t: time.UnixMilli(1_700_000_000_000)}
	h := NewHarvester(&fakeLister{cs: cs}, t.TempDir(), 2*time.Second, 1<<20, 6060)
	h.Now = clk.now
	h.Get = func(_ context.Context, url string) ([]byte, error) { return get(url) }
	return h, clk
}

var broker = Container{Pod: "broker-0", Name: "broker", Component: "broker", UID: "aaaaaaaa-1111", PodIP: "10.0.0.7"}

func TestATickKeepsAnAddressedAndTimestampedBlockProfileOfEachBrokerOnTheNode(t *testing.T) {
	var asked []string
	h, clk := newHarvester(t, []Container{broker, {Pod: "callout-x", Component: "callout", UID: "bbbbbbbb-2222", PodIP: "10.0.0.8"}, broker}, func(u string) ([]byte, error) {
		asked = append(asked, u)
		return []byte("PROFILE"), nil
	})
	h.Tick(context.Background())
	clk.t = clk.t.Add(2 * time.Second)
	h.Tick(context.Background())
	if len(asked) != 4 || asked[0] != "http://10.0.0.7:6060/debug/pprof/block" || asked[1] != "http://10.0.0.7:6060/debug/pprof/mutex" {
		t.Fatalf("asked %v: a block and a mutex request per broker per tick (the callout is not profiled, a pod listed twice is asked once)", asked)
	}
	idx := h.Index()
	if len(idx) != 4 || idx[0].AtMs != 1_700_000_000_000 || idx[2].AtMs != 1_700_000_002_000 || idx[0].UID != "aaaaaaaa-1111" || idx[0].Pod != "broker-0" {
		t.Fatalf("index %+v", idx)
	}
	if idx[0].Kind != "block" || idx[1].Kind != "mutex" || idx[0].AtMs != idx[1].AtMs {
		t.Fatalf("a block and a mutex profile of the same instant: %+v", idx[:2])
	}
	if b, err := os.ReadFile(filepath.Join(h.Dir, idx[0].File)); err != nil || string(b) != "PROFILE" {
		t.Fatalf("file %v %q", err, b)
	}
}

func TestABrokerWithoutAProfilerIsLeftAloneForAWhileThenTriedAgain(t *testing.T) {
	calls := 0
	h, clk := newHarvester(t, []Container{broker}, func(string) ([]byte, error) { calls++; return nil, errors.New("connection refused") })
	h.Tick(context.Background())
	clk.t = clk.t.Add(2 * time.Second)
	h.Tick(context.Background())
	if calls != 1 {
		t.Fatalf("%d requests: a refusing broker must not be asked every tick", calls)
	}
	clk.t = clk.t.Add(31 * time.Second)
	h.Tick(context.Background())
	if calls != 2 || len(h.Index()) != 0 {
		t.Fatalf("calls %d, kept %d", calls, len(h.Index()))
	}
}

func TestAPodWithoutAnAddressOrUIDIsNotProfiled(t *testing.T) {
	h, _ := newHarvester(t, []Container{{Pod: "p", Component: "broker"}, {Pod: "q", Component: "broker", UID: "cccccccc-3333"}}, func(string) ([]byte, error) {
		t.Fatal("asked")
		return nil, nil
	})
	h.Tick(context.Background())
}

func TestTheOldestSnapshotsGoFirstBeyondTheCapAndTheNewestIsAlwaysKept(t *testing.T) {
	h, clk := newHarvester(t, []Container{broker}, func(string) ([]byte, error) { return []byte(strings.Repeat("x", 400)), nil })
	h.Kinds = []string{"block"}
	h.MaxBytes = 1000
	for i := 0; i < 5; i++ {
		h.Tick(context.Background())
		clk.t = clk.t.Add(time.Second)
	}
	idx := h.Index()
	if len(idx) != 2 || idx[1].AtMs != 1_700_000_004_000 {
		t.Fatalf("kept %+v", idx)
	}
	entries, _ := os.ReadDir(filepath.Join(h.Dir, broker.UID))
	if len(entries) != 2 {
		t.Fatalf("%d files on disk: evicted snapshots must be deleted", len(entries))
	}
}

func serve(h *Harvester) http.Handler {
	mux := http.NewServeMux()
	h.Mount(mux, "node-1")
	return mux
}

func TestTheIndexAndEachFileAreServedAndNoOtherPathIs(t *testing.T) {
	h, _ := newHarvester(t, []Container{broker}, func(string) ([]byte, error) { return []byte("PROFILE"), nil })
	h.Tick(context.Background())
	srv := serve(h)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest("GET", "/profiles", nil))
	var doc struct {
		Node     string     `json:"node"`
		Profiles []Snapshot `json:"profiles"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &doc); err != nil || doc.Node != "node-1" || len(doc.Profiles) != 2 {
		t.Fatalf("%v %s", err, rec.Body.String())
	}
	rec = httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest("GET", "/profiles/"+doc.Profiles[0].File, nil))
	if rec.Code != 200 || rec.Body.String() != "PROFILE" {
		t.Fatalf("%d %q", rec.Code, rec.Body.String())
	}
	for _, bad := range []string{"/profiles/../etc/passwd", "/profiles/aaaaaaaa-1111/../../x.block.pb.gz", "/profiles/aaaaaaaa-1111/notaprofile", "/profiles/%2e%2e/x"} {
		rec = httptest.NewRecorder()
		srv.ServeHTTP(rec, httptest.NewRequest("GET", bad, nil))
		if rec.Code == 200 {
			t.Fatalf("%s was served", bad)
		}
	}
}

func TestAnEmptyIndexIsAnEmptyListNotNull(t *testing.T) {
	h, _ := newHarvester(t, nil, func(string) ([]byte, error) { return nil, nil })
	rec := httptest.NewRecorder()
	serve(h).ServeHTTP(rec, httptest.NewRequest("GET", "/profiles", nil))
	if !strings.Contains(rec.Body.String(), `"profiles":[]`) {
		t.Fatalf("%s", rec.Body.String())
	}
}

func TestDeleteClearsTheSnapshotsSoANewRunStartsWithNone(t *testing.T) {
	h, _ := newHarvester(t, []Container{broker}, func(string) ([]byte, error) { return []byte("P"), nil })
	h.Tick(context.Background())
	rec := httptest.NewRecorder()
	serve(h).ServeHTTP(rec, httptest.NewRequest("DELETE", "/profiles", nil))
	entries, _ := os.ReadDir(filepath.Join(h.Dir, broker.UID))
	if rec.Code != 204 || len(h.Index()) != 0 || len(entries) != 0 {
		t.Fatalf("%d %d %d", rec.Code, len(h.Index()), len(entries))
	}
}

func TestParsePodsCarriesTheUIDAndTheIP(t *testing.T) {
	cs, err := ParsePods([]byte(`{"items":[{"metadata":{"name":"broker-0","uid":"u-1","namespace":"n","labels":{"app.kubernetes.io/name":"broker"}},
		"status":{"podIP":"10.1.2.3","containerStatuses":[{"name":"broker","containerID":"containerd://abc","state":{"running":{}}}]}}]}`))
	if err != nil || len(cs) != 1 || cs[0].UID != "u-1" || cs[0].PodIP != "10.1.2.3" {
		t.Fatalf("%v %+v", err, cs)
	}
}
