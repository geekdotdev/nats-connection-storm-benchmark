// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

// Harvester takes the broker's pprof block and mutex profiles every Interval, from inside the cluster, and keeps the snapshots until the run is over.
//
// A block profile is cumulative since the process started, so any two snapshots of the same pod can be subtracted to give what happened
// between them: the storms' times (the load generator's reports have them) pick the pair, afterwards. Nothing here knows when a storm
// starts, and the driver does not take part: it only reads the snapshots when the run has completed (as it does Prometheus's series).
//
// It profiles only the brokers on its own node (the lister is already node-scoped), and only those that answer: a broker not started
// with `prof_port` refuses, is left alone for `backoff`, and costs one refused connection each time.
type Harvester struct {
	Lister    PodLister
	Dir       string
	Interval  time.Duration
	MaxBytes  int64         // the snapshots kept: the oldest go first beyond this
	Port      int           // the broker's prof_port
	Component string        // the pods to profile (the pod label app.kubernetes.io/name)
	Kinds     []string      // the profiles taken: block (who waited) and mutex (who held the lock they waited for; empty unless the broker enables it)
	Backoff   time.Duration // how long a broker that refused is left alone
	Now       func() time.Time
	Get       func(ctx context.Context, url string) ([]byte, error)

	mu      sync.Mutex
	files   []Snapshot
	size    int64
	skipped map[string]time.Time // pod uid -> when to try again
}

// Snapshot is one kept profile.
type Snapshot struct {
	Pod   string `json:"pod"`
	UID   string `json:"uid"`
	Kind  string `json:"kind"`
	AtMs  int64  `json:"atMs"`
	Bytes int64  `json:"bytes"`
	File  string `json:"file"` // the path under /profiles/
}

func NewHarvester(l PodLister, dir string, every time.Duration, maxBytes int64, port int) *Harvester {
	return &Harvester{Lister: l, Dir: dir, Interval: every, MaxBytes: maxBytes, Port: port, Component: "broker", Kinds: []string{"block", "mutex"}, Backoff: 30 * time.Second,
		Now: time.Now, Get: httpGet, skipped: map[string]time.Time{}}
}

func httpGet(ctx context.Context, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s", resp.Status)
	}
	return io.ReadAll(io.LimitReader(resp.Body, 64<<20))
}

// Tick takes one snapshot of every broker on this node that has a profiler.
func (h *Harvester) Tick(ctx context.Context) {
	cs, err := h.Lister.List(ctx)
	if err != nil {
		return
	}
	seen := map[string]bool{}
	for _, c := range cs {
		if c.Component != h.Component || c.PodIP == "" || c.UID == "" || seen[c.UID] {
			continue
		}
		seen[c.UID] = true
		now := h.Now()
		h.mu.Lock()
		wait := h.skipped[c.UID]
		h.mu.Unlock()
		if now.Before(wait) {
			continue
		}
		bodies := map[string][]byte{}
		var failed error
		for _, kind := range h.Kinds {
			body, err := h.Get(ctx, fmt.Sprintf("http://%s:%d/debug/pprof/%s", c.PodIP, h.Port, kind))
			if err != nil {
				failed = err
				break
			}
			bodies[kind] = body
		}
		if failed != nil {
			h.mu.Lock()
			if _, told := h.skipped[c.UID]; !told {
				slog.Info("no profiler on the broker; leaving it alone for a while (give it prof_port to profile it)", "pod", c.Pod, "err", failed)
			}
			h.skipped[c.UID] = now.Add(h.Backoff)
			h.mu.Unlock()
			continue
		}
		h.mu.Lock()
		delete(h.skipped, c.UID)
		h.mu.Unlock()
		for _, kind := range h.Kinds { // both of one instant, or neither: a pair that does not match cannot be compared
			h.keep(c, kind, now, bodies[kind])
		}
	}
}

func (h *Harvester) keep(c Container, kind string, at time.Time, body []byte) {
	rel := filepath.Join(c.UID, fmt.Sprintf("%d.%s.pb.gz", at.UnixMilli(), kind))
	path := filepath.Join(h.Dir, rel)
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		slog.Error("cannot keep a profile", "err", err)
		return
	}
	if err := os.WriteFile(path, body, 0o640); err != nil {
		slog.Error("cannot keep a profile", "err", err)
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.files = append(h.files, Snapshot{Pod: c.Pod, UID: c.UID, Kind: kind, AtMs: at.UnixMilli(), Bytes: int64(len(body)), File: filepath.ToSlash(rel)})
	h.size += int64(len(body))
	for h.size > h.MaxBytes && len(h.files) > 1 { // the oldest go first; the newest is always kept
		old := h.files[0]
		_ = os.Remove(filepath.Join(h.Dir, old.File))
		h.size -= old.Bytes
		h.files = h.files[1:]
	}
}

// Run takes a snapshot every Interval until ctx ends.
func (h *Harvester) Run(ctx context.Context) {
	t := time.NewTicker(h.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			h.Tick(ctx)
		}
	}
}

// Index is the snapshots kept, oldest first.
func (h *Harvester) Index() []Snapshot {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := append(make([]Snapshot, 0, len(h.files)), h.files...) // never nil: an empty index is [], not null
	sort.SliceStable(out, func(i, j int) bool { return out[i].AtMs < out[j].AtMs })
	return out
}

// Clear deletes every snapshot: a new run starts with none.
func (h *Harvester) Clear() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, f := range h.files {
		_ = os.Remove(filepath.Join(h.Dir, f.File))
	}
	h.files, h.size = nil, 0
}

var fileName = regexp.MustCompile(`^[0-9a-f-]{8,64}/[0-9]+\.[a-z]+\.pb\.gz$`)

// Mount serves the snapshots: GET /profiles is the index, GET /profiles/<pod uid>/<file> one profile, DELETE /profiles clears them.
func (h *Harvester) Mount(mux *http.ServeMux, node string) {
	mux.HandleFunc("/profiles", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			h.Clear()
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"node": node, "profiles": h.Index()})
	})
	mux.HandleFunc("/profiles/", func(w http.ResponseWriter, r *http.Request) {
		rel := strings.TrimPrefix(r.URL.Path, "/profiles/")
		if !fileName.MatchString(rel) { // no path of the caller's choosing reaches the file system
			http.NotFound(w, r)
			return
		}
		body, err := os.ReadFile(filepath.Join(h.Dir, rel))
		if err != nil {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(body)
	})
}
