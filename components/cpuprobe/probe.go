// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"log/slog"
	"sort"
	"sync"
	"time"
)

// Sample is one container's counters at the moment it was read.
type Sample struct {
	Container
	Counters
}

// Probe reads the CPU counters of the benchmark's containers on this node, fresh on every call. The pod list
// is refreshed at most every `refresh`; each container's cgroup is found once and remembered until the
// container is gone.
type Probe struct {
	lister  PodLister
	root    string
	refresh time.Duration
	now     func() time.Time

	mu       sync.Mutex
	listedAt time.Time
	known    []Container
	dirs     map[string]string // container id -> cgroup directory
}

func NewProbe(lister PodLister, cgroupRoot string, refresh time.Duration) *Probe {
	return &Probe{lister: lister, root: cgroupRoot, refresh: refresh, now: time.Now, dirs: map[string]string{}}
}

func (p *Probe) containers(ctx context.Context) []Container {
	if p.now().Sub(p.listedAt) >= p.refresh || p.known == nil {
		if cs, err := p.lister.List(ctx); err != nil {
			slog.Warn("listing pods failed; using the last list", "err", err)
		} else {
			p.known = cs
			live := map[string]bool{}
			for _, c := range cs {
				live[c.ID] = true
			}
			for id := range p.dirs { // a container that is gone does not keep its entry
				if !live[id] {
					delete(p.dirs, id)
				}
			}
		}
		p.listedAt = p.now()
	}
	return p.known
}

// Snapshot reads every known container's counters now. A container whose cgroup cannot be found or read is
// left out and said once per container, not failed: a pod that has just started or just died is normal.
func (p *Probe) Snapshot(ctx context.Context) []Sample {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []Sample
	for _, c := range p.containers(ctx) {
		dir, ok := p.dirs[c.ID]
		if !ok {
			d, err := FindCgroup(p.root, c.ID)
			if err != nil {
				slog.Debug("no cgroup yet", "pod", c.Pod, "container", c.Name, "err", err)
				continue
			}
			dir = d
			p.dirs[c.ID] = dir
		}
		counters, err := ReadCounters(dir)
		if err != nil {
			delete(p.dirs, c.ID) // the directory may be gone (the container restarted): look again next time
			slog.Warn("reading counters failed", "pod", c.Pod, "container", c.Name, "err", err)
			continue
		}
		out = append(out, Sample{Container: c, Counters: counters})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Pod != out[j].Pod {
			return out[i].Pod < out[j].Pod
		}
		return out[i].Name < out[j].Name
	})
	return out
}
