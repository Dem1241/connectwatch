package connect

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"testing"
	"time"
)

// fakeConnect answers like a Connect worker with two connectors, and records
// the requests it got.
func fakeConnect(t *testing.T) (*httptest.Server, *[]string) {
	t.Helper()
	var got []string
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"version":"3.9.0","commit":"a60e31147e6b01ee","kafka_cluster_id":"Mk3OEYBSD34fcwNTJENDM2Qk"}`))
	})
	mux.HandleFunc("GET /connectors", func(w http.ResponseWriter, r *http.Request) {
		if q := r.URL.Query()["expand"]; !slices.Equal(q, []string{"status", "info"}) {
			t.Errorf("expand = %v, want status and info", q)
		}
		w.Write([]byte(`{
			"shop": {
				"status": {"name": "shop", "type": "source",
					"connector": {"state": "RUNNING", "worker_id": "w1:8083"},
					"tasks": [
						{"id": 1, "state": "RUNNING", "worker_id": "w2:8083"},
						{"id": 0, "state": "FAILED", "worker_id": "w1:8083", "trace": "boom\n\tat x"}
					]},
				"info": {"name": "shop", "type": "source", "tasks": [],
					"config": {"connector.class": "io.debezium.connector.postgresql.PostgresConnector", "tasks.max": "2"}}
			},
			"a-sink": {
				"status": {"name": "a-sink", "type": "sink", "connector": {"state": "PAUSED", "worker_id": "w2:8083"}, "tasks": []},
				"info": {"name": "a-sink", "type": "sink", "tasks": [], "config": {"connector.class": "x.Sink"}}
			}
		}`))
	})
	for _, p := range []string{"POST /connectors/{name}/restart", "PUT /connectors/{name}/pause", "PUT /connectors/{name}/resume"} {
		mux.HandleFunc(p, func(w http.ResponseWriter, r *http.Request) {
			got = append(got, r.Method+" "+r.URL.RequestURI())
			if r.PathValue("name") == "missing" {
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte(`{"error_code":404,"message":"Connector missing not found"}`))
				return
			}
			w.WriteHeader(http.StatusAccepted)
		})
	}
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, &got
}

func TestSnapshot(t *testing.T) {
	srv, _ := fakeConnect(t)
	c, err := New(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	snap, err := c.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if snap.Cluster.Version != "3.9.0" || snap.Cluster.ClusterID != "Mk3OEYBSD34fcwNTJENDM2Qk" {
		t.Errorf("cluster = %+v", snap.Cluster)
	}
	if len(snap.Connectors) != 2 || snap.Connectors[0].Name != "a-sink" || snap.Connectors[1].Name != "shop" {
		t.Fatalf("connectors aren't sorted by name: %+v", snap.Connectors)
	}
	shop := snap.Connectors[1]
	if shop.Type != "source" || shop.Class != "io.debezium.connector.postgresql.PostgresConnector" || shop.State != Running || shop.Worker != "w1:8083" {
		t.Errorf("shop = %+v", shop)
	}
	if len(shop.Tasks) != 2 || shop.Tasks[0].ID != 0 || shop.Tasks[0].State != Failed || shop.Tasks[0].Trace != "boom\n\tat x" {
		t.Errorf("tasks aren't read or sorted by id: %+v", shop.Tasks)
	}
	if shop.Config["tasks.max"] != "2" {
		t.Errorf("config = %v", shop.Config)
	}
}

func TestActions(t *testing.T) {
	srv, got := fakeConnect(t)
	c, _ := New(srv.URL)
	ctx := context.Background()
	if err := c.Restart(ctx, "shop/eu"); err != nil {
		t.Fatal(err)
	}
	if err := c.Pause(ctx, "shop"); err != nil {
		t.Fatal(err)
	}
	if err := c.Resume(ctx, "shop"); err != nil {
		t.Fatal(err)
	}
	want := []string{
		"POST /connectors/shop%2Feu/restart?includeTasks=true&onlyFailed=true",
		"PUT /connectors/shop/pause",
		"PUT /connectors/shop/resume",
	}
	if !slices.Equal(*got, want) {
		t.Errorf("requests =\n%s\nwant\n%s", strings.Join(*got, "\n"), strings.Join(want, "\n"))
	}
}

func TestErrorMessage(t *testing.T) {
	srv, _ := fakeConnect(t)
	c, _ := New(srv.URL)
	err := c.Pause(context.Background(), "missing")
	var e *Error
	if !errors.As(err, &e) || e.Status != 404 || e.Message != "Connector missing not found" {
		t.Fatalf("err = %v", err)
	}
	if err.Error() != "connect answered 404: Connector missing not found" {
		t.Errorf("Error() = %q", err.Error())
	}
}

func TestBasicAuth(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if u, p, ok := r.BasicAuth(); !ok || u != "me" || p != "pw" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.Write([]byte(`{}`))
	}))
	defer srv.Close()
	c, err := New(strings.Replace(srv.URL, "http://", "http://me:pw@", 1))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(c.Name(), "pw") {
		t.Errorf("name shows the password: %s", c.Name())
	}
	if _, err := c.Snapshot(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestNew(t *testing.T) {
	for in, want := range map[string]string{
		"localhost:8083":             "localhost:8083",
		"http://connect:8083/":       "connect:8083",
		"https://gw.example/connect": "gw.example/connect",
	} {
		c, err := New(in)
		if err != nil {
			t.Errorf("New(%q): %v", in, err)
			continue
		}
		if c.Name() != want {
			t.Errorf("New(%q).Name() = %q, want %q", in, c.Name(), want)
		}
	}
	for _, bad := range []string{"ftp://connect", "http://"} {
		if _, err := New(bad); err == nil {
			t.Errorf("New(%q) should fail", bad)
		}
	}
}

// TestRealWorker runs against a real Connect worker when
// CONNECTWATCH_TEST_URL points at one. It creates a heartbeat connector,
// pauses, resumes and restarts it, and deletes it again.
func TestRealWorker(t *testing.T) {
	addr := os.Getenv("CONNECTWATCH_TEST_URL")
	if addr == "" {
		t.Skip("set CONNECTWATCH_TEST_URL to test against a real worker")
	}
	c, err := New(addr)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	name := "connectwatch-test"
	bootstrap := cmp.Or(os.Getenv("CONNECTWATCH_TEST_BOOTSTRAP"), "localhost:9092")
	cfg := fmt.Sprintf(`{"connector.class":"org.apache.kafka.connect.mirror.MirrorHeartbeatConnector","source.cluster.alias":"a","target.cluster.alias":"b","source.cluster.bootstrap.servers":%q,"target.cluster.bootstrap.servers":%q,"tasks.max":"1"}`, bootstrap, bootstrap)
	if err := c.put(ctx, "/connectors/"+name+"/config", cfg); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.do(context.Background(), http.MethodDelete, "/connectors/"+name, nil, nil) })

	waitFor := func(state string) Connector {
		t.Helper()
		deadline := time.Now().Add(60 * time.Second)
		for {
			snap, err := c.Snapshot(ctx)
			if err != nil {
				t.Fatal(err)
			}
			for _, k := range snap.Connectors {
				if k.Name == name && k.State == state && len(k.Tasks) > 0 && k.Tasks[0].State == state {
					return k
				}
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s never got to %s: %+v", name, state, snap.Connectors)
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
	k := waitFor(Running)
	if k.Type != "source" || k.Class != "org.apache.kafka.connect.mirror.MirrorHeartbeatConnector" || k.Worker == "" {
		t.Errorf("connector = %+v", k)
	}
	if err := c.Pause(ctx, name); err != nil {
		t.Fatal(err)
	}
	waitFor(Paused)
	if err := c.Resume(ctx, name); err != nil {
		t.Fatal(err)
	}
	waitFor(Running)
	if err := c.Restart(ctx, name); err != nil {
		t.Fatal(err)
	}
	if err := c.Pause(ctx, "connectwatch-missing"); err == nil {
		t.Error("pausing a connector that doesn't exist should fail")
	}
}

// put sends a json body, which connectwatch itself never needs to.
func (c *Client) put(ctx context.Context, path, body string) error {
	u := *c.base
	u.Path += path
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, u.String(), strings.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return &Error{Status: resp.StatusCode}
	}
	return nil
}
