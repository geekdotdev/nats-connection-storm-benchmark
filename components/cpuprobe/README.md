# cpuprobe

Reads the CPU counters of the benchmark's containers on its node, **straight from their cgroups, at the moment it is asked**. It exists because the kubelet's cAdvisor refreshes its numbers only every 10 to 11 seconds (measured on this cluster), which is too coarse for a burst that lasts under a second.

For each running container of the pods labelled `app.kubernetes.io/part-of=storm-bench` on its node it reads `cpu.stat` and `cpu.max` (cgroup v2) and reports cumulative `usage_usec`, `user_usec`, `system_usec`, `nr_periods`, `nr_throttled`, `throttled_usec` and the CPU limit. The use during an interval is the difference between two readings.

- `GET /metrics`: Prometheus text format (`storm_cpu_usage_seconds_total`, `storm_cpu_periods_total`, `storm_cpu_throttled_periods_total`, `storm_cpu_throttled_seconds_total`, `storm_cpu_limit_cores`, ...), labelled with namespace, pod, container, component and node. Prometheus scrapes it every second.
- `GET /counters`: the same counters as JSON with the instant they were read (`timeMs`). The load generator asks every probe at the start and the end of each burst, so the broker's and callout's CPU in a burst are in the generator's report, in the Job's log.
- `GET /healthz`.

A container's cgroup directory is found once, by searching the mounted tree for a directory whose name contains the container's runtime id (`cri-containerd-<id>.scope` with the systemd driver), and remembered until the container is gone. A container with no cgroup yet, or one that cannot be read, is left out, never an error: a pod that has just started or just died is normal. If the API cannot be reached the last pod list is kept.

```
cpuprobe [--listen :9100] [--cgroup-root /host/sys/fs/cgroup] [--namespace NS] [--node NODE] [--selector LABELS] [--refresh 2s]
```

`--namespace` and `--node` come from the downward API (`POD_NAMESPACE`, `NODE_NAME`). It runs as a DaemonSet (`deploy/monitoring`) with `/sys/fs/cgroup` of the node mounted **read-only**, and its ServiceAccount may get and list pods in the benchmark's namespace and nothing else.

`go test ./...` (no cluster): the counters and limit parsing, finding a cgroup by container id in a fake tree, counters read fresh on every call, a restarted container found again, the Prometheus and JSON output, and the Kubernetes request (path, selectors, token).
