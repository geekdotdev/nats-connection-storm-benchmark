// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// ErrExists is returned by Store.Create when the object is already there.
var ErrExists = errors.New("already exists")

// Store is the small part of the Kubernetes API provisioning needs: ask whether an object
// exists, and create one. There is deliberately no update or delete: the ServiceAccount that
// runs this has neither.
type Store interface {
	Exists(ctx context.Context, kind, name string) (bool, error)
	Create(ctx context.Context, o Object) error
}

// kubeStore talks to the API server of the cluster this pod runs in, over HTTPS, with the
// ServiceAccount token. It uses the standard library instead of client-go: two calls do not
// need its dependency tree.
type kubeStore struct {
	base      string
	namespace string
	tokenFile string
	client    *http.Client
}

const defaultSADir = "/var/run/secrets/kubernetes.io/serviceaccount"

// newKubeStore reads the in-cluster settings: KUBERNETES_SERVICE_HOST and _PORT, and under saDir
// the files ca.crt, namespace and token. The token is read again on each request because
// projected tokens are rotated.
func newKubeStore(saDir string) (*kubeStore, error) {
	host, port := os.Getenv("KUBERNETES_SERVICE_HOST"), os.Getenv("KUBERNETES_SERVICE_PORT")
	if host == "" || port == "" {
		return nil, errors.New("KUBERNETES_SERVICE_HOST / KUBERNETES_SERVICE_PORT are not set: not running in a cluster (use --out-dir to write files instead)")
	}
	ca, err := os.ReadFile(filepath.Join(saDir, "ca.crt"))
	if err != nil {
		return nil, fmt.Errorf("cluster CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return nil, errors.New("cluster CA file holds no certificate")
	}
	ns, err := os.ReadFile(filepath.Join(saDir, "namespace"))
	if err != nil {
		return nil, fmt.Errorf("namespace: %w", err)
	}
	return &kubeStore{
		base:      "https://" + net.JoinHostPort(host, port),
		namespace: strings.TrimSpace(string(ns)),
		tokenFile: filepath.Join(saDir, "token"),
		client: &http.Client{
			Timeout:   15 * time.Second,
			Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}},
		},
	}, nil
}

func resource(kind string) (string, error) {
	switch kind {
	case "Secret":
		return "secrets", nil
	case "ConfigMap":
		return "configmaps", nil
	}
	return "", fmt.Errorf("unsupported kind %q", kind)
}

func (s *kubeStore) do(ctx context.Context, method, path string, body []byte) (int, []byte, error) {
	token, err := os.ReadFile(s.tokenFile)
	if err != nil {
		return 0, nil, fmt.Errorf("service account token: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, method, s.base+path, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(token)))
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	out, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return resp.StatusCode, out, err
}

func (s *kubeStore) Exists(ctx context.Context, kind, name string) (bool, error) {
	res, err := resource(kind)
	if err != nil {
		return false, err
	}
	code, body, err := s.do(ctx, http.MethodGet, fmt.Sprintf("/api/v1/namespaces/%s/%s/%s", s.namespace, res, name), nil)
	if err != nil {
		return false, err
	}
	switch code {
	case http.StatusOK:
		return true, nil
	case http.StatusNotFound:
		return false, nil
	}
	return false, apiError("get", kind, name, code, body)
}

func (s *kubeStore) Create(ctx context.Context, o Object) error {
	res, err := resource(o.Kind)
	if err != nil {
		return err
	}
	manifest := map[string]any{
		"apiVersion": "v1",
		"kind":       o.Kind,
		"metadata": map[string]any{
			"name":      o.Name,
			"namespace": s.namespace,
			"labels":    map[string]string{"app.kubernetes.io/managed-by": "storm-provision"},
		},
	}
	if o.Kind == "Secret" {
		manifest["type"] = "Opaque"
		manifest["stringData"] = o.Data
	} else {
		manifest["data"] = o.Data
	}
	body, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	code, resp, err := s.do(ctx, http.MethodPost, fmt.Sprintf("/api/v1/namespaces/%s/%s", s.namespace, res), body)
	if err != nil {
		return err
	}
	switch code {
	case http.StatusCreated, http.StatusOK:
		return nil
	case http.StatusConflict:
		return ErrExists
	}
	return apiError("create", o.Kind, o.Name, code, resp)
}

// apiError reports an API failure without echoing the response body when it could hold object data.
func apiError(verb, kind, name string, code int, body []byte) error {
	var status struct {
		Message string `json:"message"`
	}
	msg := ""
	if json.Unmarshal(body, &status) == nil {
		msg = status.Message
	}
	return fmt.Errorf("%s %s %s: HTTP %d %s", verb, kind, name, code, msg)
}

// dirStore writes the objects as files under a directory, one subdirectory per object, for
// trying the program without a cluster. It refuses to overwrite, like the real thing.
type dirStore struct{ dir string }

func (d dirStore) path(kind, name string) string {
	return filepath.Join(d.dir, strings.ToLower(kind)+"-"+name)
}

func (d dirStore) Exists(_ context.Context, kind, name string) (bool, error) {
	_, err := os.Stat(d.path(kind, name))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return err == nil, err
}

func (d dirStore) Create(_ context.Context, o Object) error {
	p := d.path(o.Kind, o.Name)
	if err := os.Mkdir(p, 0o700); err != nil {
		if errors.Is(err, os.ErrExist) {
			return ErrExists
		}
		return err
	}
	for k, v := range o.Data {
		if err := os.WriteFile(filepath.Join(p, k), []byte(v), 0o600); err != nil {
			return err
		}
	}
	return nil
}
