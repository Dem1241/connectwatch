package ui

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/charmbracelet/x/ansi"

	"github.com/Dem1241/connectwatch/internal/connect"
	"github.com/Dem1241/connectwatch/internal/health"
)

type fakeSource struct {
	snap    connect.Snapshot
	err     error
	actions []string
	failAct error
}

func (f *fakeSource) Snapshot(context.Context) (connect.Snapshot, error) { return f.snap, f.err }

func (f *fakeSource) Restart(_ context.Context, name string) error { return f.act("restart " + name) }
func (f *fakeSource) Pause(_ context.Context, name string) error   { return f.act("pause " + name) }
func (f *fakeSource) Resume(_ context.Context, name string) error  { return f.act("resume " + name) }

func (f *fakeSource) act(a string) error {
	f.actions = append(f.actions, a)
	return f.failAct
}

var t0 = time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)

const tooLarge = `org.apache.kafka.connect.errors.ConnectException: Unrecoverable exception from producer send callback
	at org.apache.kafka.connect.runtime.WorkerSourceTask.maybeThrowProducerSendException(WorkerSourceTask.java:340)
Caused by: org.apache.kafka.common.errors.RecordTooLargeException: The message is 1338210 bytes when serialized which is larger than 1048576
`

func cluster() connect.Snapshot {
	return connect.Snapshot{
		At:      t0,
		Cluster: connect.Cluster{Name: "connect:8083", Version: "3.9.0"},
		Connectors: []connect.Connector{
			{Name: "debezium-shop", Type: "source", Class: "io.debezium.connector.postgresql.PostgresConnector", State: connect.Running, Worker: "w1:8083",
				Tasks:  []connect.Task{{ID: 0, State: connect.Failed, Worker: "w1:8083", Trace: tooLarge}},
				Config: map[string]string{"connector.class": "io.debezium.connector.postgresql.PostgresConnector", "slot.name": "debezium_shop", "database.password": "hunter2", "topic.prefix": "shop", "database.user": "${file:/secrets/db.properties:user}"}},
			{Name: "jdbc-legacy", Type: "sink", Class: "io.confluent.connect.jdbc.JdbcSinkConnector", State: connect.Paused, Worker: "w2:8083",
				Tasks: []connect.Task{{ID: 0, State: connect.Paused, Worker: "w2:8083"}}},
			{Name: "orders-s3", Type: "sink", Class: "io.confluent.connect.s3.S3SinkConnector", State: connect.Running, Worker: "w2:8083",
				Tasks:  []connect.Task{{ID: 0, State: connect.Running, Worker: "w1:8083"}, {ID: 1, State: connect.Running, Worker: "w2:8083"}},
				Config: map[string]string{"topics": "shop.public.orders,shop.public.items"}},
		},
	}
}

// load builds a model that has seen one snapshot of src, at the given size.
func load(t *testing.T, src *fakeSource, w, h int) Model {
	t.Helper()
	var m tea.Model = New(src, health.DefaultLimits, time.Second, false)
	m, _ = m.Update(tea.WindowSizeMsg{Width: w, Height: h})
	m, _ = m.Update(m.(Model).Init()())
	return m.(Model)
}

// press sends keys, and runs whatever an action key started.
func press(t *testing.T, m Model, keys ...string) Model {
	t.Helper()
	var tm tea.Model = m
	for _, k := range keys {
		var msg tea.KeyPressMsg
		switch k {
		case "down":
			msg = tea.KeyPressMsg{Code: tea.KeyDown}
		case "up":
			msg = tea.KeyPressMsg{Code: tea.KeyUp}
		case "enter":
			msg = tea.KeyPressMsg{Code: tea.KeyEnter}
		case "esc":
			msg = tea.KeyPressMsg{Code: tea.KeyEscape}
		case "tab":
			msg = tea.KeyPressMsg{Code: tea.KeyTab}
		case "space":
			msg = tea.KeyPressMsg{Code: tea.KeySpace, Text: " "}
		case "R":
			msg = tea.KeyPressMsg{Code: 'r', ShiftedCode: 'R', Mod: tea.ModShift, Text: "R"}
		default:
			msg = tea.KeyPressMsg{Code: rune(k[0]), Text: k}
		}
		var cmd tea.Cmd
		tm, cmd = tm.Update(msg)
		if cmd != nil {
			if am, ok := cmd().(actionMsg); ok {
				tm, _ = tm.Update(am)
			}
		}
	}
	return tm.(Model)
}

func screen(m Model) string { return ansi.Strip(m.View().Content) }

func TestShowsTheWorstFirst(t *testing.T) {
	s := screen(load(t, &fakeSource{snap: cluster()}, 140, 40))
	for _, want := range []string{"⋈ connectwatch", "connect:8083", "kafka 3.9.0", "● live", "3 connectors, 4 tasks", "1 failing", "1 paused", "failing", "0/1", "2/2"} {
		if !strings.Contains(s, want) {
			t.Errorf("screen is missing %q:\n%s", want, s)
		}
	}
	deb, s3 := strings.Index(s, "debezium-shop"), strings.Index(s, "orders-s3")
	if deb < 0 || s3 < deb {
		t.Errorf("want debezium-shop before the healthy ones:\n%s", s)
	}
}

func TestDetailExplainsAndSuggests(t *testing.T) {
	s := screen(load(t, &fakeSource{snap: cluster()}, 140, 50))
	for _, want := range []string{
		"PostgresConnector", "w1:8083", "shop.*",
		"tasks · 0/1 running", "#0", "RecordTooLargeException",
		"why it's flagged", "1 of 1 task failed",
		"what went wrong", "task 0", "1338210 bytes when serialized",
		"what you can do", "producer.override.max.request.size",
		"replication slot debezium_shop holds on to WAL",
		"R restarts what failed",
	} {
		if !strings.Contains(s, want) {
			t.Errorf("detail is missing %q:\n%s", want, s)
		}
	}
}

func TestHealthyConnectorHasNoAdvice(t *testing.T) {
	m := press(t, load(t, &fakeSource{snap: cluster()}, 140, 40), "s") // by name: debezium-shop, jdbc-legacy, orders-s3
	m = press(t, m, "down", "down")
	s := screen(m)
	if m.selectedName() != "orders-s3" {
		t.Fatalf("selected %s", m.selectedName())
	}
	if strings.Contains(s, "what you can do") || strings.Contains(s, "why it's flagged") {
		t.Errorf("a healthy connector got advice:\n%s", s)
	}
	if !strings.Contains(s, "shop.public.orders, shop.public.items") {
		t.Errorf("topics aren't shown:\n%s", s)
	}
}

func TestTraceTab(t *testing.T) {
	s := screen(press(t, load(t, &fakeSource{snap: cluster()}, 140, 40), "tab"))
	for _, want := range []string{"trace of task 0", "ConnectException: Unrecoverable", "at org.apache.kafka.connect.runtime.WorkerSourceTask", "Caused by: org.apache.kafka.common.errors.RecordTooLargeException"} {
		if !strings.Contains(s, want) {
			t.Errorf("trace tab is missing %q:\n%s", want, s)
		}
	}
}

func TestConfigTabMasksSecrets(t *testing.T) {
	s := screen(press(t, load(t, &fakeSource{snap: cluster()}, 140, 40), "tab", "tab"))
	if strings.Contains(s, "hunter2") {
		t.Errorf("the password is on screen:\n%s", s)
	}
	for _, want := range []string{"database.password", "••••••", "slot.name", "debezium_shop", "${file:/secrets/db.properties:user}"} {
		if !strings.Contains(s, want) {
			t.Errorf("config tab is missing %q:\n%s", want, s)
		}
	}
}

func TestRestartAsksFirst(t *testing.T) {
	src := &fakeSource{snap: cluster()}
	m := press(t, load(t, src, 140, 40), "R")
	if !strings.Contains(screen(m), "restart the failed tasks of debezium-shop?  y yes") {
		t.Errorf("no question:\n%s", screen(m))
	}
	m = press(t, m, "n")
	if len(src.actions) != 0 {
		t.Fatalf("n still did %v", src.actions)
	}
	m = press(t, m, "R", "y")
	if !slices.Equal(src.actions, []string{"restart debezium-shop"}) {
		t.Errorf("actions = %v", src.actions)
	}
	if !strings.Contains(screen(m), "restarted the failed tasks of debezium-shop") {
		t.Errorf("no notice:\n%s", screen(m))
	}
}

func TestRestartNeedsSomethingFailed(t *testing.T) {
	src := &fakeSource{snap: cluster()}
	m := press(t, load(t, src, 140, 40), "s", "down", "down", "R")
	if len(src.actions) != 0 {
		t.Errorf("restarted a healthy connector: %v", src.actions)
	}
	if !strings.Contains(screen(m), "nothing has failed in orders-s3") {
		t.Errorf("no notice:\n%s", screen(m))
	}
}

func TestPauseAndResume(t *testing.T) {
	src := &fakeSource{snap: cluster()}
	m := press(t, load(t, src, 140, 40), "s") // debezium-shop, jdbc-legacy, orders-s3
	press(t, m, "p", "y", "down", "p", "y")
	if want := []string{"pause debezium-shop", "resume jdbc-legacy"}; !slices.Equal(src.actions, want) {
		t.Errorf("actions = %v, want %v", src.actions, want)
	}
}

func TestFailedAction(t *testing.T) {
	src := &fakeSource{snap: cluster(), failAct: errors.New("connect answered 409: rebalance in progress")}
	s := screen(press(t, load(t, src, 140, 40), "p", "y"))
	if !strings.Contains(s, "couldn't pause debezium-shop: connect answered 409: rebalance in progress") {
		t.Errorf("error isn't shown:\n%s", s)
	}
}

func TestReadOnly(t *testing.T) {
	src := &fakeSource{snap: cluster()}
	var tm tea.Model = New(src, health.DefaultLimits, time.Second, true)
	tm, _ = tm.Update(tea.WindowSizeMsg{Width: 140, Height: 40})
	tm, _ = tm.Update(tm.(Model).Init()())
	m := press(t, tm.(Model), "R", "y", "p", "y")
	if len(src.actions) != 0 {
		t.Errorf("read-only did %v", src.actions)
	}
	s := screen(m)
	if strings.Contains(s, "R restart") || strings.Contains(s, "R restarts") {
		t.Errorf("read-only still offers restarting:\n%s", s)
	}
	if !strings.Contains(s, "read-only") {
		t.Errorf("header doesn't say read-only:\n%s", s)
	}
}

func TestFilter(t *testing.T) {
	m := press(t, load(t, &fakeSource{snap: cluster()}, 140, 40), "/", "s", "3", "enter")
	s := screen(m)
	if !strings.Contains(s, "orders-s3") || strings.Contains(s, "debezium-shop") {
		t.Errorf("filter for s3 should show only orders-s3:\n%s", s)
	}
	m = press(t, m, "esc")
	if s := screen(m); !strings.Contains(s, "debezium-shop") {
		t.Errorf("esc should clear the filter:\n%s", s)
	}
}

func TestFilterByState(t *testing.T) {
	m := press(t, load(t, &fakeSource{snap: cluster()}, 140, 40), "/", "p", "a", "u", "s", "enter")
	if v := m.visible(); len(v) != 1 || v[0].Name != "jdbc-legacy" {
		t.Errorf("filter for paus = %v", v)
	}
}

func TestSortKeepsTheSelection(t *testing.T) {
	m := press(t, load(t, &fakeSource{snap: cluster()}, 140, 40), "down") // the next one by health
	before := m.selectedName()
	m = press(t, m, "s", "s") // by type
	if got := m.selectedName(); got != before {
		t.Errorf("selected %q after sorting, want %q", got, before)
	}
	var names []string
	for _, st := range m.visible() {
		names = append(names, st.Name)
	}
	if got := strings.Join(names, " "); got != "jdbc-legacy orders-s3 debezium-shop" {
		t.Errorf("sorted by type = %s", got)
	}
}

func TestPauseStopsRefreshing(t *testing.T) {
	m := press(t, load(t, &fakeSource{snap: cluster()}, 140, 40), "space")
	if !strings.Contains(screen(m), "paused") {
		t.Error("header doesn't say paused")
	}
	tm, _ := m.Update(tickMsg{})
	if tm.(Model).loading {
		t.Error("a tick while paused started a refresh")
	}
}

func TestConnectError(t *testing.T) {
	src := &fakeSource{snap: cluster()}
	m := load(t, src, 140, 40)
	src.err = errors.New("connection refused")
	tm, _ := m.Update(m.Init()())
	s := screen(tm.(Model))
	if !strings.Contains(s, "can't reach connect") || !strings.Contains(s, "connection refused") {
		t.Errorf("error isn't shown:\n%s", s)
	}
	if !strings.Contains(s, "debezium-shop") {
		t.Errorf("the last good snapshot should stay on screen:\n%s", s)
	}
}

func TestNoConnectors(t *testing.T) {
	snap := cluster()
	snap.Connectors = nil
	s := screen(load(t, &fakeSource{snap: snap}, 140, 40))
	if !strings.Contains(s, "no connectors yet") {
		t.Errorf("empty cluster isn't explained:\n%s", s)
	}
}

func TestNarrowTerminalStacksThePanes(t *testing.T) {
	for _, tb := range []string{"", "tab", "tab"} {
		m := load(t, &fakeSource{snap: cluster()}, 80, 40)
		if tb != "" {
			m = press(t, m, tb)
		}
		lines := strings.Split(screen(m), "\n")
		if len(lines) != 40 {
			t.Errorf("screen has %d lines, want 40", len(lines))
		}
		for i, l := range lines {
			if w := ansi.StringWidth(l); w > 80 {
				t.Errorf("line %d is %d wide, more than the terminal: %q", i, w, l)
			}
		}
	}
}

func TestWrapBreaksLongWords(t *testing.T) {
	for _, l := range wrap("short "+strings.Repeat("x", 25)+" end", 10) {
		if len(l) > 10 {
			t.Errorf("line %q is longer than 10", l)
		}
	}
}
