package demo

import (
	"context"
	"testing"
	"time"

	"github.com/Dem1241/connectwatch/internal/connect"
	"github.com/Dem1241/connectwatch/internal/health"
)

func find(t *testing.T, snap connect.Snapshot, name string) connect.Connector {
	t.Helper()
	for _, c := range snap.Connectors {
		if c.Name == name {
			return c
		}
	}
	t.Fatalf("no %s in the demo", name)
	return connect.Connector{}
}

func TestDemoHasSomethingForEveryLevel(t *testing.T) {
	start := time.Now()
	c := New(start)
	tr := health.NewTracker(health.DefaultLimits)
	var sts []health.Status
	for i := range 60 {
		sts = tr.Update(c.At(start.Add(time.Duration(i) * 2 * time.Second)))
	}
	levels := map[string]health.Level{}
	for _, st := range sts {
		levels[st.Name] = st.Level
	}
	if levels["debezium-shop"] != health.Danger || levels["s3-sink-orders"] != health.Danger {
		t.Errorf("the failing ones aren't in danger: %v", levels)
	}
	if levels["es-sink-search"] == health.OK {
		t.Errorf("es-sink-search should be flapping: %v", levels)
	}
	if levels["mm2-heartbeats"] != health.OK || levels["jdbc-sink-legacy"] != health.OK {
		t.Errorf("the healthy ones aren't ok: %v", levels)
	}
}

func TestRestartFixesTheSinkButNotDebezium(t *testing.T) {
	start := time.Now()
	c := New(start)
	ctx := context.Background()
	c.At(start)
	for _, name := range []string{"debezium-shop", "s3-sink-orders"} {
		if err := c.Restart(ctx, name); err != nil {
			t.Fatal(err)
		}
	}
	snap := c.At(start.Add(5 * time.Second))
	if s := find(t, snap, "debezium-shop").Tasks[0].State; s != connect.Running {
		t.Errorf("debezium-shop after a restart = %s", s)
	}
	snap = c.At(start.Add(30 * time.Second))
	if s := find(t, snap, "debezium-shop").Tasks[0].State; s != connect.Failed {
		t.Errorf("debezium-shop should fail again, the record is still too large: %s", s)
	}
	if s := find(t, snap, "s3-sink-orders").Tasks[2].State; s != connect.Running {
		t.Errorf("s3-sink-orders after a restart = %s", s)
	}
}

func TestPauseAndResume(t *testing.T) {
	start := time.Now()
	c := New(start)
	ctx := context.Background()
	if err := c.Resume(ctx, "jdbc-sink-legacy"); err != nil {
		t.Fatal(err)
	}
	if k := find(t, c.At(start), "jdbc-sink-legacy"); k.State != connect.Running || k.Tasks[1].State != connect.Running {
		t.Errorf("resumed = %+v", k)
	}
	if err := c.Pause(ctx, "nope"); err == nil {
		t.Error("pausing a connector that doesn't exist should fail")
	}
}
