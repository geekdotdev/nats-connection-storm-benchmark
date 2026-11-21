// SPDX-License-Identifier: Apache-2.0

// Command provision makes the operator, account and user material the benchmark's broker, auth
// callout and load generator share, and stores it as Kubernetes objects. It runs once, as an init
// container of the callout pod.
//
// It creates a Secret for the callout, a Secret for the load generator and a ConfigMap for the
// broker (see the constants in provision.go for the names). The identity seeds of the operator and
// the accounts sign the JWTs and are then discarded. If the objects already exist it does nothing,
// so restarting the pod keeps the same keys. It never updates or deletes anything: to get new keys,
// delete the three objects and restart the pods.
//
//	provision [--mount-dir DIR] [--sa-dir DIR] [--wait DURATION]   create the objects in the cluster
//	provision --out-dir DIR                                         write them as files instead, for trying it by hand
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := cli(ctx, os.Args[1:], os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "provision:", err)
		os.Exit(1)
	}
}

func cli(ctx context.Context, args []string, logOut io.Writer) error {
	fs := flag.NewFlagSet("provision", flag.ContinueOnError)
	fs.SetOutput(logOut)
	mountDir := fs.String("mount-dir", DefaultMountDir, "where the broker mounts the ConfigMap; written into operator.conf")
	saDir := fs.String("sa-dir", defaultSADir, "directory holding the ServiceAccount token, ca.crt and namespace")
	outDir := fs.String("out-dir", "", "write the objects as files under this existing directory instead of creating them in the cluster")
	wait := fs.Duration("wait", 30*time.Second, "how long to wait for another run's objects to complete before reporting a partial state")
	if err := fs.Parse(args); err != nil {
		return err
	}
	log := slog.New(slog.NewTextHandler(logOut, nil))

	var store Store
	if *outDir != "" {
		if st, err := os.Stat(*outDir); err != nil || !st.IsDir() {
			return fmt.Errorf("--out-dir %q must be an existing directory", *outDir)
		}
		store = dirStore{dir: *outDir}
	} else {
		ks, err := newKubeStore(*saDir)
		if err != nil {
			return err
		}
		store = ks
	}
	created, err := Run(ctx, store, Generate, *mountDir, *wait, log)
	if err != nil {
		return err
	}
	if created {
		log.Info("provisioned", "objects", []string{CalloutSecretName, GeneratorSecretName, ConfigMapName})
	} else {
		log.Info("already provisioned, nothing to do", "marker", ConfigMapName)
	}
	return nil
}

// ErrPartial means some of the objects exist but the completion marker does not: a previous run
// stopped part way. Nothing here may delete them, so a person has to.
var ErrPartial = errors.New("some provisioning objects exist but the run never finished: delete " +
	CalloutSecretName + ", " + GeneratorSecretName + " and " + ConfigMapName + " (bench secrets delete) and restart the pods")

// Run provisions once. It reports whether it created the objects (true) or found them already
// complete (false). The ConfigMap is created last and marks completion.
//
// Two runs at the same time (two callout pods) or a run that stopped part way both show up as an
// object that already exists. Run then waits up to wait for the marker to appear, which is the other
// run finishing, and returns ErrPartial if it does not.
func Run(ctx context.Context, store Store, gen func() (*Bundle, error), mountDir string, wait time.Duration, log *slog.Logger) (bool, error) {
	done, err := store.Exists(ctx, "ConfigMap", ConfigMapName)
	if err != nil {
		return false, fmt.Errorf("checking for %s: %w", ConfigMapName, err)
	}
	if done {
		return false, nil
	}

	b, err := gen()
	if err != nil {
		return false, fmt.Errorf("generating keys: %w", err)
	}
	if err := b.Validate(); err != nil {
		return false, fmt.Errorf("generated material failed validation: %w", err)
	}
	for _, o := range b.Objects(mountDir) {
		err := store.Create(ctx, o)
		if errors.Is(err, ErrExists) {
			log.Info("object already exists, waiting for the other run to finish", "kind", o.Kind, "name", o.Name)
			return false, waitForMarker(ctx, store, wait)
		}
		if err != nil {
			return false, fmt.Errorf("creating %s %s: %w", o.Kind, o.Name, err)
		}
	}
	return true, nil
}

func waitForMarker(ctx context.Context, store Store, wait time.Duration) error {
	deadline := time.Now().Add(wait)
	for {
		done, err := store.Exists(ctx, "ConfigMap", ConfigMapName)
		if err != nil {
			return fmt.Errorf("checking for %s: %w", ConfigMapName, err)
		}
		if done {
			return nil
		}
		if time.Now().After(deadline) {
			return ErrPartial
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
}
