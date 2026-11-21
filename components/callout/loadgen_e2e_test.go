// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// buildLoadgen builds the Rust generator (components/loadgen) once for these tests, offline.
func buildLoadgen(t *testing.T) string {
	t.Helper()
	cargo, err := exec.LookPath("cargo")
	if err != nil {
		t.Skip("cargo is not installed")
	}
	dir := filepath.Join("..", "loadgen")
	build := exec.Command(cargo, "build", "--release", "--offline")
	build.Dir = dir
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("building components/loadgen: %v\n%s", err, out)
	}
	bin, err := filepath.Abs(filepath.Join(dir, "target", "release", "auth-storm"))
	if err != nil {
		t.Fatal(err)
	}
	return bin
}

type stormReport struct {
	Outcomes map[string]int `json:"outcomes"`
	Failures map[string]int `json:"failures"`
	TotalMs  struct {
		N int `json:"n"`
	} `json:"totalMs"`
	Seconds float64 `json:"seconds"`
	Samples []struct {
		Outcome string   `json:"outcome"`
		StartMs float64  `json:"startMs"`
		SentMs  *float64 `json:"sentMs"`
		DoneMs  float64  `json:"doneMs"`
		AuthMs  *float64 `json:"authMs"`
	} `json:"samples"`
}

// runLoadgen runs the generator against url and returns its report and what it printed.
func runLoadgen(t *testing.T, bin, url string, env ...string) (stormReport, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin)
	cmd.Env = append([]string{"PATH=" + os.Getenv("PATH"), "STORM_URL=" + url, "STORM_JSON=-", "STORM_HOLD_MS=100", "STORM_RUNS=1"}, env...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	var report stormReport
	sc := bufio.NewScanner(&stdout)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		if line, ok := strings.CutPrefix(sc.Text(), "STORM_REPORT_JSON "); ok {
			if jerr := json.Unmarshal([]byte(line), &report); jerr != nil {
				t.Fatalf("bad report: %v\n%s", jerr, line)
			}
		}
	}
	if report.Outcomes == nil {
		t.Fatalf("no report (err=%v)\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
	}
	return report, stdout.String() + stderr.String()
}

// TestRustGeneratorAgainstARealBroker runs the Rust load generator in operator mode against a real nats-server and this
// callout, over the WebSocket listener. It is the check that the generator's tickets and nonce signatures (its own small
// implementation of nkeys and JWT v2) are what the broker accepts.
func TestRustGeneratorAgainstARealBroker(t *testing.T) {
	bin := buildLoadgen(t)
	p := provision(t)
	s := startOperatorBroker(t, p)
	startOperatorCallout(t, p, s.ClientURL(), DefaultTTL)

	operatorEnv := func(extra ...string) []string {
		return append([]string{
			"STORM_SCOPED_SEED_FILE=" + filepath.Join(p.generatorSecret, "scoped.seed"),
			"STORM_AUTH_ACCOUNT_FILE=" + filepath.Join(p.generatorSecret, "auth.pub"),
		}, extra...)
	}

	t.Run("every connection is admitted", func(t *testing.T) {
		rep, out := runLoadgen(t, bin, s.WebsocketURL(), operatorEnv("STORM_CONNECTIONS=50")...)
		if rep.Outcomes["admitted"] != 50 || len(rep.Failures) != 0 {
			t.Fatalf("report %+v\n%s", rep, out)
		}
		if rep.TotalMs.N != 50 {
			t.Fatalf("total timings for %d connections, want 50", rep.TotalMs.N)
		}
	})

	t.Run("samples carry each connection's start, CONNECT and answer times since the run began", func(t *testing.T) {
		rep, out := runLoadgen(t, bin, s.WebsocketURL(), operatorEnv("STORM_CONNECTIONS=50", "STORM_SAMPLES=1")...)
		if len(rep.Samples) != 50 || rep.Outcomes["admitted"] != 50 {
			t.Fatalf("%d samples, report %+v\n%s", len(rep.Samples), rep.Outcomes, out)
		}
		last := 0.0
		for i, x := range rep.Samples {
			if x.SentMs == nil || x.AuthMs == nil {
				t.Fatalf("sample %d has no CONNECT time: %+v", i, x)
			}
			if !(0 <= x.StartMs && x.StartMs <= *x.SentMs && *x.SentMs <= x.DoneMs) {
				t.Fatalf("sample %d is out of order: start %v sent %v done %v", i, x.StartMs, *x.SentMs, x.DoneMs)
			}
			// answer minus CONNECT is the auth time (each is rounded to 0.1 ms)
			if d := x.DoneMs - *x.SentMs - *x.AuthMs; d < -0.3 || d > 0.3 {
				t.Fatalf("sample %d: done-sent %v differs from authMs %v", i, x.DoneMs-*x.SentMs, *x.AuthMs)
			}
			last = max(last, x.DoneMs)
		}
		// the run's clock stops at the last answer, so it agrees with the latest doneMs (seconds is rounded to 10 ms)
		if d := last/1000 - rep.Seconds; d < -0.02 || d > 0.02 {
			t.Fatalf("last answer at %v ms, run took %v s", last, rep.Seconds)
		}
	})

	t.Run("tickets with no recognized roles are admitted too", func(t *testing.T) {
		// Admission does not depend on the roles: the callout places the user and the permissions are denied outright.
		rep, out := runLoadgen(t, bin, s.WebsocketURL(), operatorEnv("STORM_CONNECTIONS=10", "STORM_ROLES=admin")...)
		if rep.Outcomes["admitted"] != 10 {
			t.Fatalf("report %+v\n%s", rep, out)
		}
	})

	t.Run("the wrong scoped key is refused", func(t *testing.T) {
		other := provision(t) // a different operator and AUTH account
		env := []string{
			"STORM_SCOPED_SEED_FILE=" + filepath.Join(other.generatorSecret, "scoped.seed"),
			"STORM_AUTH_ACCOUNT_FILE=" + filepath.Join(other.generatorSecret, "auth.pub"),
			"STORM_CONNECTIONS=5", "STORM_TIMEOUT_MS=3000",
		}
		rep, out := runLoadgen(t, bin, s.WebsocketURL(), env...)
		if rep.Outcomes["admitted"] != 0 || rep.Outcomes["refused"] != 5 {
			t.Fatalf("a ticket from another operator was not refused: %+v\n%s", rep, out)
		}
	})

	t.Run("a bad seed fails before any connection", func(t *testing.T) {
		cmd := exec.Command(bin)
		cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "STORM_URL=" + s.WebsocketURL(), "STORM_SCOPED_SEED=junk",
			"STORM_AUTH_ACCOUNT=" + p.read(t, p.generatorSecret, "auth.pub")}
		out, err := cmd.CombinedOutput()
		if err == nil || !strings.Contains(string(out), "scoped seed") {
			t.Fatalf("want a scoped-seed error, got err=%v\n%s", err, out)
		}
	})

	t.Run("the generator needs its settings", func(t *testing.T) {
		cmd := exec.Command(bin)
		cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "STORM_URL=" + s.WebsocketURL()}
		out, err := cmd.CombinedOutput()
		if err == nil || !strings.Contains(string(out), "STORM_SCOPED_SEED") {
			t.Fatalf("want a missing-settings error, got err=%v\n%s", err, out)
		}
	})
}
