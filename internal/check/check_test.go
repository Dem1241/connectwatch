package check

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/Dem1241/connectwatch/internal/connect"
	"github.com/Dem1241/connectwatch/internal/health"
)

var t0 = time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)

func cluster() connect.Snapshot {
	return connect.Snapshot{
		At:      t0,
		Cluster: connect.Cluster{Name: "connect:8083", Version: "3.9.0"},
		Connectors: []connect.Connector{
			{Name: "orders-s3", Type: "sink", Class: "io.confluent.connect.s3.S3SinkConnector", State: connect.Running,
				Tasks: []connect.Task{{ID: 0, State: connect.Running}}},
			{Name: "shop", Type: "source", Class: "io.debezium.connector.postgresql.PostgresConnector", State: connect.Running,
				Tasks:  []connect.Task{{ID: 0, State: connect.Failed, Trace: "x.Wrapper: outer\n\tat a\nCaused by: java.net.ConnectException: Connection refused\n\tat b"}},
				Config: map[string]string{"database.password": "hunter2"}},
		},
	}
}

func statuses(snap connect.Snapshot) []health.Status {
	return health.NewTracker(health.DefaultLimits).Update(snap)
}

func TestCode(t *testing.T) {
	snap := cluster()
	if got := Code(statuses(snap)); got != ExitDanger {
		t.Errorf("Code = %d, want %d", got, ExitDanger)
	}
	snap.Connectors = snap.Connectors[:1]
	if got := Code(statuses(snap)); got != ExitOK {
		t.Errorf("Code = %d, want %d", got, ExitOK)
	}
	snap.Connectors[0].Tasks = nil
	if got := Code(statuses(snap)); got != ExitWarn {
		t.Errorf("running with no tasks: Code = %d, want %d", got, ExitWarn)
	}
}

func TestText(t *testing.T) {
	var b bytes.Buffer
	if err := Text(&b, cluster(), statuses(cluster())); err != nil {
		t.Fatal(err)
	}
	out := b.String()
	for _, want := range []string{
		"LEVEL   CONNECTOR  TYPE    CLASS              STATE    TASKS",
		"danger  shop       source  PostgresConnector  RUNNING  0/1 running",
		"ok      orders-s3  sink    S3SinkConnector    RUNNING  1/1 running",
		"  - 1 of 1 task failed",
		"  task 0: ConnectException: Connection refused",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("report is missing %q:\n%s", want, out)
		}
	}
}

func TestTextNoConnectors(t *testing.T) {
	var b bytes.Buffer
	snap := cluster()
	snap.Connectors = nil
	if err := Text(&b, snap, nil); err != nil {
		t.Fatal(err)
	}
	if b.String() != "no connectors on connect:8083\n" {
		t.Errorf("got %q", b.String())
	}
}

func TestJSON(t *testing.T) {
	var b bytes.Buffer
	if err := JSON(&b, cluster(), statuses(cluster())); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(b.String(), "hunter2") {
		t.Errorf("json has the password in it:\n%s", b.String())
	}
	var out struct {
		Level      string `json:"level"`
		Connectors []struct {
			Name      string   `json:"name"`
			Level     string   `json:"level"`
			Reasons   []string `json:"reasons"`
			Cause     string   `json:"cause"`
			CauseFrom string   `json:"cause_from"`
			Advice    string   `json:"advice"`
			Tasks     []struct {
				State string `json:"state"`
			} `json:"tasks"`
		} `json:"connectors"`
	}
	if err := json.Unmarshal(b.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if out.Level != "danger" || len(out.Connectors) != 2 {
		t.Fatalf("%+v", out)
	}
	c := out.Connectors[0]
	if c.Name != "shop" || c.Cause != "java.net.ConnectException: Connection refused" || c.CauseFrom != "task 0" || c.Advice == "" || c.Tasks[0].State != "FAILED" {
		t.Errorf("shop = %+v", c)
	}
	if out.Connectors[1].Reasons == nil {
		t.Error("reasons should be [] rather than null")
	}
}
