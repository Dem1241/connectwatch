// Package health decides how worried to be about each connector, and
// remembers enough history to tell a connector that keeps failing from one
// that failed once.
package health

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/Dem1241/connectwatch/internal/connect"
)

// Level is how bad a connector looks.
type Level int

const (
	OK Level = iota
	Warn
	Danger
)

func (l Level) String() string {
	switch l {
	case Warn:
		return "warn"
	case Danger:
		return "danger"
	}
	return "ok"
}

// Limits are the thresholds a connector is judged by.
type Limits struct {
	Flaps      int           // failures within FlapWindow that make a connector flapping
	FlapWindow time.Duration // how far back failures are counted
	Unassigned time.Duration // how long a task may wait for a worker, rebalances take a moment
}

// DefaultLimits are a sensible start for most clusters.
var DefaultLimits = Limits{Flaps: 3, FlapWindow: 15 * time.Minute, Unassigned: time.Minute}

// Status is a connector with everything connectwatch worked out about it.
type Status struct {
	connect.Connector
	Level   Level
	Reasons []string // why it isn't ok, most important first

	Running, Failed int // tasks in those states

	// Cause is the root of the first failure: the connector's own, otherwise
	// the first failed task's. CauseFrom says which one it was.
	Cause     Cause
	CauseFrom string
	// Advice is what to do about Cause, when connectwatch recognises it.
	Advice string

	// Failures is how often the connector or one of its tasks went into
	// FAILED within the flap window, counting the first time it was seen so.
	Failures int
	// FailedFor is how long it has been failing without a break, as far as
	// connectwatch has seen.
	FailedFor time.Duration
}

// Tracker turns a stream of snapshots into statuses.
type Tracker struct {
	limits      Limits
	prev        map[string]map[string]string // connector -> "connector" or task id -> state
	failures    map[string][]time.Time
	failedSince map[string]time.Time
	waitSince   map[string]time.Time
}

// NewTracker starts with no history.
func NewTracker(limits Limits) *Tracker {
	return &Tracker{
		limits:      limits,
		prev:        map[string]map[string]string{},
		failures:    map[string][]time.Time{},
		failedSince: map[string]time.Time{},
		waitSince:   map[string]time.Time{},
	}
}

// Update adds a snapshot and returns the status of every connector in it,
// the worst first.
func (t *Tracker) Update(snap connect.Snapshot) []Status {
	seen := map[string]bool{}
	out := make([]Status, 0, len(snap.Connectors))
	for _, c := range snap.Connectors {
		seen[c.Name] = true
		st := Status{Connector: c}

		states := map[string]string{"connector": c.State}
		for _, task := range c.Tasks {
			states[fmt.Sprint(task.ID)] = task.State
			switch task.State {
			case connect.Running:
				st.Running++
			case connect.Failed:
				st.Failed++
			}
		}

		// a new failure is anything that is FAILED now and wasn't last time
		prev := t.prev[c.Name]
		for who, state := range states {
			if state == connect.Failed && prev[who] != connect.Failed {
				t.failures[c.Name] = append(t.failures[c.Name], snap.At)
			}
		}
		t.prev[c.Name] = states
		recent := t.failures[c.Name][:0]
		for _, at := range t.failures[c.Name] {
			if snap.At.Sub(at) <= t.limits.FlapWindow {
				recent = append(recent, at)
			}
		}
		t.failures[c.Name] = recent
		st.Failures = len(recent)

		failing := c.State == connect.Failed || st.Failed > 0
		since(t.failedSince, c.Name, failing, snap.At)
		if failing {
			st.FailedFor = snap.At.Sub(t.failedSince[c.Name])
		}
		waiting := c.State == connect.Unassigned || slices.ContainsFunc(c.Tasks, func(task connect.Task) bool { return task.State == connect.Unassigned })
		since(t.waitSince, c.Name, waiting, snap.At)

		st.Cause, st.CauseFrom, st.Advice = cause(c)
		st.Level, st.Reasons = t.judge(st, waiting, snap.At)
		out = append(out, st)
	}
	// forget connectors that were deleted
	for name := range t.prev {
		if !seen[name] {
			delete(t.prev, name)
			delete(t.failures, name)
			delete(t.failedSince, name)
			delete(t.waitSince, name)
		}
	}
	Sort(out)
	return out
}

// Sort puts the worst connectors first, then by name.
func Sort(sts []Status) {
	slices.SortStableFunc(sts, func(a, b Status) int {
		return cmp.Or(cmp.Compare(b.Level, a.Level), cmp.Compare(b.Failed, a.Failed), strings.Compare(a.Name, b.Name))
	})
}

// since keeps the time a condition started, and forgets it once it's over.
func since(m map[string]time.Time, name string, on bool, now time.Time) {
	if !on {
		delete(m, name)
	} else if _, ok := m[name]; !ok {
		m[name] = now
	}
}

func cause(c connect.Connector) (Cause, string, string) {
	trace, from := "", ""
	if c.State == connect.Failed && c.Trace != "" {
		trace, from = c.Trace, "connector"
	} else {
		for _, task := range c.Tasks {
			if task.State == connect.Failed && task.Trace != "" {
				trace, from = task.Trace, fmt.Sprintf("task %d", task.ID)
				break
			}
		}
	}
	if trace == "" {
		return Cause{}, "", ""
	}
	return RootCause(trace), from, Advice(trace)
}

func (t *Tracker) judge(st Status, waiting bool, now time.Time) (Level, []string) {
	level := OK
	var reasons []string
	raise := func(l Level, reason string) {
		level = max(level, l)
		reasons = append(reasons, reason)
	}

	if st.State == connect.Failed {
		raise(Danger, "the connector failed")
	}
	if st.Failed > 0 {
		raise(Danger, fmt.Sprintf("%d of %d task%s failed", st.Failed, len(st.Tasks), plural(len(st.Tasks))))
	}
	if st.Failures >= t.limits.Flaps {
		raise(Warn, fmt.Sprintf("failed %d times in the last %s", st.Failures, Duration(t.limits.FlapWindow)))
	}
	if waiting {
		if wait := now.Sub(t.waitSince[st.Name]); wait >= t.limits.Unassigned {
			raise(Warn, "waiting for a worker for "+Duration(wait))
		}
	}
	if st.State == connect.Running && len(st.Tasks) == 0 {
		raise(Warn, "running, but with no tasks")
	}
	return level, reasons
}

func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}

// Duration formats d to the minute, like 3h 12m.
func Duration(d time.Duration) string {
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh %dm", int(d.Hours()), int(d.Minutes())%60)
	}
	return fmt.Sprintf("%dd %dh", int(d.Hours())/24, int(d.Hours())%24)
}

// ShortClass is a connector class without its package:
// io.debezium.connector.postgresql.PostgresConnector is PostgresConnector.
func ShortClass(class string) string {
	if i := strings.LastIndexByte(class, '.'); i >= 0 {
		return class[i+1:]
	}
	return class
}
