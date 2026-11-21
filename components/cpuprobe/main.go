// SPDX-License-Identifier: Apache-2.0

// cpuprobe reads the CPU counters of the benchmark's containers on its node, straight from their cgroups, at
// the moment it is asked: no cache, no sampling interval. Prometheus scrapes /metrics; the load generator
// asks /counters at the start and end of every burst. Run it as a DaemonSet with the node's cgroup tree
// mounted read-only; it needs to list pods in one namespace and nothing else.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	listen := flag.String("listen", ":9100", "address to serve /metrics, /counters and /healthz on")
	root := flag.String("cgroup-root", "/host/sys/fs/cgroup", "the node's cgroup v2 tree, mounted read-only")
	namespace := flag.String("namespace", os.Getenv("POD_NAMESPACE"), "the benchmark's namespace")
	node := flag.String("node", os.Getenv("NODE_NAME"), "this node's name")
	selector := flag.String("selector", "app.kubernetes.io/part-of=storm-bench", "label selector of the pods to read")
	refresh := flag.Duration("refresh", 2*time.Second, "how often the pod list is refreshed at most")
	flag.Parse()
	if *namespace == "" || *node == "" {
		slog.Error("--namespace and --node are needed (POD_NAMESPACE and NODE_NAME from the downward API)")
		os.Exit(2)
	}
	lister, err := newKubeLister(defaultSADir, *namespace, *node, *selector)
	if err != nil {
		slog.Error("cannot talk to the Kubernetes API", "err", err)
		os.Exit(1)
	}
	probe := NewProbe(lister, *root, *refresh)
	srv := &http.Server{Addr: *listen, Handler: Handler(probe, *node), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("server", "err", err)
			os.Exit(1)
		}
	}()
	slog.Info("reading cgroups", "root", *root, "node", *node, "namespace", *namespace, "listen", *listen)
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
}
