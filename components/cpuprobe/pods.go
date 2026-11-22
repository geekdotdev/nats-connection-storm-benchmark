// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Container is one running container of the benchmark on this node.
type Container struct {
	Namespace string
	Pod       string
	Name      string // the container's name in its pod
	Component string // the pod's app.kubernetes.io/name: broker, callout, loadgen...
	ID        string // the runtime's id, without the "containerd://" prefix
	UID       string // the pod's UID: a replaced pod has a new one, so counters that restart from zero are told apart
	PodIP     string // where the pod can be reached on the pod network (the profile harvester's address for the broker)
}

// PodLister says which containers of the benchmark are on this node.
type PodLister interface {
	List(ctx context.Context) ([]Container, error)
}

type podList struct {
	Items []struct {
		Metadata struct {
			Name      string            `json:"name"`
			UID       string            `json:"uid"`
			Namespace string            `json:"namespace"`
			Labels    map[string]string `json:"labels"`
		} `json:"metadata"`
		Status struct {
			Phase             string `json:"phase"`
			PodIP             string `json:"podIP"`
			ContainerStatuses []struct {
				Name        string                 `json:"name"`
				ContainerID string                 `json:"containerID"`
				State       map[string]interface{} `json:"state"`
			} `json:"containerStatuses"`
		} `json:"status"`
	} `json:"items"`
}

// ParsePods reads a Pod list. Only running containers with a runtime id are returned: a container that has
// not started has no cgroup to read, and a finished one has none left.
func ParsePods(body []byte) ([]Container, error) {
	var pl podList
	if err := json.Unmarshal(body, &pl); err != nil {
		return nil, fmt.Errorf("pod list: %w", err)
	}
	var out []Container
	for _, p := range pl.Items {
		for _, cs := range p.Status.ContainerStatuses {
			if _, running := cs.State["running"]; !running || cs.ContainerID == "" {
				continue
			}
			id := cs.ContainerID
			if i := strings.Index(id, "://"); i >= 0 {
				id = id[i+3:]
			}
			out = append(out, Container{Namespace: p.Metadata.Namespace, Pod: p.Metadata.Name, Name: cs.Name,
				Component: p.Metadata.Labels["app.kubernetes.io/name"], ID: id, UID: p.Metadata.UID, PodIP: p.Status.PodIP})
		}
	}
	return out, nil
}

// kubeLister lists the benchmark's pods on one node with the Pod's ServiceAccount (it may get and list pods in
// the benchmark's namespace and nothing else). The token is read again on each request: it is rotated.
type kubeLister struct {
	base, namespace, node, selector, tokenFile string
	client                                     *http.Client
}

const defaultSADir = "/var/run/secrets/kubernetes.io/serviceaccount"

func newKubeLister(saDir, namespace, node, selector string) (*kubeLister, error) {
	host, port := os.Getenv("KUBERNETES_SERVICE_HOST"), os.Getenv("KUBERNETES_SERVICE_PORT")
	if host == "" || port == "" {
		return nil, errors.New("KUBERNETES_SERVICE_HOST / KUBERNETES_SERVICE_PORT are not set: not running in a cluster")
	}
	ca, err := os.ReadFile(filepath.Join(saDir, "ca.crt"))
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return nil, errors.New("ca.crt holds no certificate")
	}
	return &kubeLister{base: "https://" + host + ":" + port, namespace: namespace, node: node, selector: selector,
		tokenFile: filepath.Join(saDir, "token"),
		client:    &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}}}}, nil
}

func (k *kubeLister) List(ctx context.Context) ([]Container, error) {
	q := url.Values{"labelSelector": {k.selector}, "fieldSelector": {"spec.nodeName=" + k.node}}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, k.base+"/api/v1/namespaces/"+k.namespace+"/pods?"+q.Encode(), nil)
	if err != nil {
		return nil, err
	}
	tok, err := os.ReadFile(k.tokenFile)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(tok)))
	resp, err := k.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("listing pods: %s", resp.Status)
	}
	return ParsePods(body)
}
