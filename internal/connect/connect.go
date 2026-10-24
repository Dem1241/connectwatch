// Package connect talks to the Kafka Connect REST API: what the connectors
// and their tasks are doing, and the few things connectwatch can ask of them.
package connect

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"
)

// States a connector or task can be in.
const (
	Running    = "RUNNING"
	Failed     = "FAILED"
	Paused     = "PAUSED"
	Stopped    = "STOPPED" // Kafka 3.5+
	Unassigned = "UNASSIGNED"
	Restarting = "RESTARTING"
)

// Cluster is the Connect worker connectwatch talks to.
type Cluster struct {
	Name      string // where it is, like localhost:8083
	Version   string // the Kafka version of the worker
	ClusterID string // the Kafka cluster it stores its state in
}

// Task is one task of a connector.
type Task struct {
	ID     int
	State  string
	Worker string
	Trace  string // the Java stack trace when it failed
}

// Connector is a connector with its status and config.
type Connector struct {
	Name   string
	Type   string // source or sink
	Class  string // connector.class
	State  string
	Worker string
	Trace  string
	Tasks  []Task
	Config map[string]string
}

// Snapshot is everything read in one go.
type Snapshot struct {
	At         time.Time
	Cluster    Cluster
	Connectors []Connector
}

// Client is a Kafka Connect REST API, usually on port 8083.
type Client struct {
	base *url.URL
	http *http.Client
	name string
}

// New makes a client for a Connect URL like http://localhost:8083. Basic auth
// goes in the URL: http://user:pass@connect:8083.
func New(raw string) (*Client, error) {
	if !strings.Contains(raw, "://") {
		raw = "http://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("connect url: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("connect url: want http or https, got %q", u.Scheme)
	}
	if u.Host == "" {
		return nil, fmt.Errorf("connect url %q has no host", raw)
	}
	u.Path = strings.TrimSuffix(u.Path, "/")
	return &Client{base: u, http: &http.Client{Timeout: 10 * time.Second}, name: u.Host + u.Path}, nil
}

// Name is where the client points, without the password.
func (c *Client) Name() string { return c.name }

// Error is an error response from Connect.
type Error struct {
	Status  int
	Message string
}

func (e *Error) Error() string {
	if e.Message == "" {
		return fmt.Sprintf("connect answered %d %s", e.Status, http.StatusText(e.Status))
	}
	return fmt.Sprintf("connect answered %d: %s", e.Status, e.Message)
}

func (c *Client) do(ctx context.Context, method, path string, query url.Values, out any) error {
	// path comes escaped, so a connector called shop/eu stays one segment
	u := *c.base
	u.RawPath = c.base.EscapedPath() + path
	p, err := url.PathUnescape(u.RawPath)
	if err != nil {
		return err
	}
	u.Path = p
	u.RawQuery = query.Encode()
	req, err := http.NewRequestWithContext(ctx, method, u.String(), nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	if u.User != nil {
		pass, _ := u.User.Password()
		req.SetBasicAuth(u.User.Username(), pass)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode >= 300 {
		e := &Error{Status: resp.StatusCode}
		var msg struct {
			Message string `json:"message"`
		}
		if json.Unmarshal(body, &msg) == nil {
			e.Message = msg.Message
		}
		return e
	}
	if out == nil || len(bytes.TrimSpace(body)) == 0 {
		return nil
	}
	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("reading %s %s: %w", method, path, err)
	}
	return nil
}

type statusJSON struct {
	Name      string `json:"name"`
	Type      string `json:"type"`
	Connector struct {
		State    string `json:"state"`
		WorkerID string `json:"worker_id"`
		Trace    string `json:"trace"`
	} `json:"connector"`
	Tasks []struct {
		ID       int    `json:"id"`
		State    string `json:"state"`
		WorkerID string `json:"worker_id"`
		Trace    string `json:"trace"`
	} `json:"tasks"`
}

type infoJSON struct {
	Type   string            `json:"type"`
	Config map[string]string `json:"config"`
}

// Snapshot reads the worker and every connector with its status and config,
// in two requests.
func (c *Client) Snapshot(ctx context.Context) (Snapshot, error) {
	snap := Snapshot{At: time.Now(), Cluster: Cluster{Name: c.name}}
	var root struct {
		Version   string `json:"version"`
		ClusterID string `json:"kafka_cluster_id"`
	}
	if err := c.do(ctx, http.MethodGet, "/", nil, &root); err != nil {
		return snap, err
	}
	snap.Cluster.Version, snap.Cluster.ClusterID = root.Version, root.ClusterID

	var all map[string]struct {
		Status *statusJSON `json:"status"`
		Info   *infoJSON   `json:"info"`
	}
	q := url.Values{"expand": {"status", "info"}}
	if err := c.do(ctx, http.MethodGet, "/connectors", q, &all); err != nil {
		return snap, err
	}
	for name, e := range all {
		snap.Connectors = append(snap.Connectors, fromJSON(name, e.Status, e.Info))
	}
	slices.SortFunc(snap.Connectors, func(a, b Connector) int { return strings.Compare(a.Name, b.Name) })
	return snap, nil
}

func fromJSON(name string, st *statusJSON, info *infoJSON) Connector {
	c := Connector{Name: name}
	if info != nil {
		c.Type, c.Config = info.Type, info.Config
		c.Class = info.Config["connector.class"]
	}
	if st != nil {
		c.Type = cmp.Or(st.Type, c.Type)
		c.State, c.Worker, c.Trace = st.Connector.State, st.Connector.WorkerID, st.Connector.Trace
		for _, t := range st.Tasks {
			c.Tasks = append(c.Tasks, Task{ID: t.ID, State: t.State, Worker: t.WorkerID, Trace: t.Trace})
		}
		slices.SortFunc(c.Tasks, func(a, b Task) int { return cmp.Compare(a.ID, b.ID) })
	}
	return c
}

// Restart restarts a connector and the tasks of it that failed. Workers older
// than Kafka 3.0 ignore the options and only restart the connector itself.
func (c *Client) Restart(ctx context.Context, name string) error {
	q := url.Values{"includeTasks": {"true"}, "onlyFailed": {"true"}}
	return c.do(ctx, http.MethodPost, "/connectors/"+url.PathEscape(name)+"/restart", q, nil)
}

// Pause pauses a connector and its tasks.
func (c *Client) Pause(ctx context.Context, name string) error {
	return c.do(ctx, http.MethodPut, "/connectors/"+url.PathEscape(name)+"/pause", nil, nil)
}

// Resume resumes a paused or stopped connector.
func (c *Client) Resume(ctx context.Context, name string) error {
	return c.do(ctx, http.MethodPut, "/connectors/"+url.PathEscape(name)+"/resume", nil, nil)
}
