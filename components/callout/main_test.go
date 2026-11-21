// SPDX-License-Identifier: Apache-2.0

package main

import (
	"strings"
	"testing"
)

func TestSecretReadsEnvThenFile(t *testing.T) {
	t.Setenv("X_SECRET", "")
	t.Setenv("X_SECRET_FILE", "")
	if _, err := secret("X_SECRET"); err == nil || !strings.Contains(err.Error(), "X_SECRET") {
		t.Fatalf("want an error naming the variable, got %v", err)
	}
	t.Setenv("X_SECRET", "from-env")
	if v, _ := secret("X_SECRET"); v != "from-env" {
		t.Fatalf("got %q", v)
	}
	t.Setenv("X_SECRET", "")
	f := t.TempDir() + "/s"
	if err := writeFile(f, "from-file\n"); err != nil {
		t.Fatal(err)
	}
	t.Setenv("X_SECRET_FILE", f)
	if v, _ := secret("X_SECRET"); v != "from-file" {
		t.Fatalf("got %q", v)
	}
}

func TestServeNeedsAllFourSecrets(t *testing.T) {
	for _, name := range []string{"CALLOUT_CREDS", "CALLOUT_AUTH_SIGNING_SEED", "CALLOUT_TENANT_SIGNING_SEED", "CALLOUT_TENANT_PUBLIC"} {
		t.Setenv(name, "x")
	}
	t.Setenv("CALLOUT_TENANT_PUBLIC", "")
	t.Setenv("CALLOUT_TENANT_PUBLIC_FILE", "")
	err := serve([]string{"--health-addr", ""})
	if err == nil || !strings.Contains(err.Error(), "CALLOUT_TENANT_PUBLIC") {
		t.Fatalf("want an error naming the missing secret, got %v", err)
	}
}

func TestOldFlagsAreGone(t *testing.T) {
	for _, flag := range []string{"--mode", "--accounts"} {
		if err := serve([]string{flag, "x"}); err == nil || !strings.Contains(err.Error(), "flag provided but not defined") {
			t.Fatalf("%s should be gone, got %v", flag, err)
		}
	}
}
