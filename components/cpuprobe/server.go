// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

type counterView struct {
	Namespace string `json:"namespace"`
	Pod       string `json:"pod"`
	Container string `json:"container"`
	Component string `json:"component"`
	Counters
}

// countersDoc is what GET /counters returns: the instant, and every container's cumulative counters then.
type countersDoc struct {
	Node       string        `json:"node"`
	TimeMs     int64         `json:"timeMs"`
	Containers []counterView `json:"containers"`
}

func escape(s string) string {
	return strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace(s)
}

// Metrics renders samples in the Prometheus text format. Counters are in seconds, as Prometheus names them.
func Metrics(node string, samples []Sample) string {
	type metric struct{ name, typ, help string }
	var b strings.Builder
	counters := []metric{
		{"storm_cpu_usage_seconds_total", "counter", "CPU time used by the container, from its cgroup"},
		{"storm_cpu_user_seconds_total", "counter", "CPU time in user mode"},
		{"storm_cpu_system_seconds_total", "counter", "CPU time in kernel mode"},
		{"storm_cpu_periods_total", "counter", "CFS enforcement periods that have elapsed"},
		{"storm_cpu_throttled_periods_total", "counter", "CFS periods in which the container hit its CPU limit and was throttled"},
		{"storm_cpu_throttled_seconds_total", "counter", "total time the container was throttled"},
		{"storm_cpu_limit_cores", "gauge", "the container's CPU limit in cores (absent when it has none)"},
	}
	for _, m := range counters {
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s %s\n", m.name, m.help, m.name, m.typ)
		for _, s := range samples {
			var v float64
			switch m.name {
			case "storm_cpu_usage_seconds_total":
				v = float64(s.UsageUsec) / 1e6
			case "storm_cpu_user_seconds_total":
				v = float64(s.UserUsec) / 1e6
			case "storm_cpu_system_seconds_total":
				v = float64(s.SystemUsec) / 1e6
			case "storm_cpu_periods_total":
				v = float64(s.Periods)
			case "storm_cpu_throttled_periods_total":
				v = float64(s.ThrottledPeriods)
			case "storm_cpu_throttled_seconds_total":
				v = float64(s.ThrottledUsec) / 1e6
			case "storm_cpu_limit_cores":
				if s.LimitCores == nil {
					continue
				}
				v = *s.LimitCores
			}
			fmt.Fprintf(&b, "%s{namespace=\"%s\",pod=\"%s\",container=\"%s\",component=\"%s\",node=\"%s\"} %g\n", m.name,
				escape(s.Namespace), escape(s.Pod), escape(s.Name), escape(s.Component), escape(node), v)
		}
	}
	return b.String()
}

// Handler serves the probe: /metrics for Prometheus, /counters for a client that wants the instant (the load
// generator, at the start and end of a burst), /healthz, and, when h is not nil, the broker profiles it has kept (/profiles).
func Handler(p *Probe, node string, h ...*Harvester) http.Handler {
	mux := http.NewServeMux()
	if len(h) > 0 && h[0] != nil {
		h[0].Mount(mux, node)
	}
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprint(w, Metrics(node, p.Snapshot(r.Context())))
	})
	mux.HandleFunc("/counters", func(w http.ResponseWriter, r *http.Request) {
		samples := p.Snapshot(r.Context())
		doc := countersDoc{Node: node, TimeMs: time.Now().UnixMilli(), Containers: make([]counterView, 0, len(samples))}
		for _, s := range samples {
			doc.Containers = append(doc.Containers, counterView{s.Namespace, s.Pod, s.Name, s.Component, s.Counters})
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(doc)
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { fmt.Fprintln(w, "ok") })
	return mux
}
