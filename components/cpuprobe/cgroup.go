// SPDX-License-Identifier: Apache-2.0

package main

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Counters are one container's cumulative CPU counters, from its cgroup (v2): cpu.stat and cpu.max.
// They only grow, so the use between two readings is the difference of the two.
type Counters struct {
	UsageUsec        uint64   `json:"usageUsec"`
	UserUsec         uint64   `json:"userUsec"`
	SystemUsec       uint64   `json:"systemUsec"`
	Periods          uint64   `json:"periods"`
	ThrottledPeriods uint64   `json:"throttledPeriods"`
	ThrottledUsec    uint64   `json:"throttledUsec"`
	LimitCores       *float64 `json:"limitCores"` // nil: no limit
}

// ParseCPU reads the text of cpu.stat and of cpu.max. It fails when cpu.stat has no usage_usec
// (cgroup v1, or not a cpu.stat at all).
func ParseCPU(stat, max string) (Counters, error) {
	var c Counters
	seen := false
	for _, line := range strings.Split(stat, "\n") {
		f := strings.Fields(line)
		if len(f) != 2 {
			continue
		}
		v, err := strconv.ParseUint(f[1], 10, 64)
		if err != nil {
			continue
		}
		switch f[0] {
		case "usage_usec":
			c.UsageUsec, seen = v, true
		case "user_usec":
			c.UserUsec = v
		case "system_usec":
			c.SystemUsec = v
		case "nr_periods":
			c.Periods = v
		case "nr_throttled":
			c.ThrottledPeriods = v
		case "throttled_usec":
			c.ThrottledUsec = v
		}
	}
	if !seen {
		return Counters{}, errors.New("no usage_usec in cpu.stat: not cgroup v2")
	}
	if f := strings.Fields(max); len(f) == 2 && f[0] != "max" {
		quota, e1 := strconv.ParseFloat(f[0], 64)
		period, e2 := strconv.ParseFloat(f[1], 64)
		if e1 == nil && e2 == nil && period > 0 {
			l := quota / period
			c.LimitCores = &l
		}
	}
	return c, nil
}

// ReadCounters reads cpu.stat and cpu.max in a container's cgroup directory.
func ReadCounters(dir string) (Counters, error) {
	stat, err := os.ReadFile(filepath.Join(dir, "cpu.stat"))
	if err != nil {
		return Counters{}, err
	}
	max, _ := os.ReadFile(filepath.Join(dir, "cpu.max")) // missing is "no limit": the root cgroup has none
	return ParseCPU(string(stat), string(max))
}

// FindCgroup finds the cgroup directory of the container with this ID: the directory under root whose name
// contains it (with the systemd driver it is cri-containerd-<id>.scope, with cgroupfs it is <id>). The layout
// above that differs by runtime and QoS class, so it is searched for, once per container.
func FindCgroup(root, containerID string) (string, error) {
	if len(containerID) < 12 {
		return "", fmt.Errorf("container id %q is too short to search by", containerID)
	}
	found := ""
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil // a directory that vanished or cannot be read: keep looking
		}
		if d.IsDir() && strings.Contains(d.Name(), containerID) {
			found = path
			return fs.SkipAll
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	if found == "" {
		return "", fmt.Errorf("no cgroup for container %s under %s", containerID, root)
	}
	return found, nil
}
