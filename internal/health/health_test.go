package health

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Dem1241/connectwatch/internal/connect"
)

var t0 = time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)

const pgTrace = `org.apache.kafka.connect.errors.ConnectException: An exception occurred in the change event producer. This connector will be stopped.
	at io.debezium.pipeline.ErrorHandler.setProducerThrowable(ErrorHandler.java:67)
	at java.base/java.lang.Thread.run(Thread.java:1583)
Caused by: io.debezium.DebeziumException: Couldn't obtain encoding for database shop
	at io.debezium.connector.postgresql.connection.PostgresConnection.determineDatabaseCharset(PostgresConnection.java:615)
	... 9 more
Caused by: org.postgresql.util.PSQLException: ERROR: replication slot "debezium_shop" is active for PID 48211
	at org.postgresql.core.v3.QueryExecutorImpl.receiveErrorResponse(QueryExecutorImpl.java:2725)
	... 12 more
`

func TestRootCause(t *testing.T) {
	c := RootCause(pgTrace)
	if c.Class != "org.postgresql.util.PSQLException" || c.Message != `ERROR: replication slot "debezium_shop" is active for PID 48211` {
		t.Errorf("RootCause = %+v", c)
	}
	if c.Short() != "PSQLException" {
		t.Errorf("Short = %q", c.Short())
	}
}

func TestRootCauseWithoutCausedBy(t *testing.T) {
	c := RootCause("java.lang.NullPointerException\n\tat a.b(C.java:1)")
	if c.Class != "java.lang.NullPointerException" || c.Message != "" || c.String() != "NullPointerException" {
		t.Errorf("RootCause = %+v", c)
	}
}

func TestRootCauseMultilineMessage(t *testing.T) {
	c := RootCause("org.x.BadThing: first line\nsecond line\n\tat a.b(C.java:1)")
	if c.Message != "first line second line" {
		t.Errorf("message = %q", c.Message)
	}
}

func TestRootCauseNotJava(t *testing.T) {
	if c := RootCause("task was killed: out of luck"); c.Class != "" || c.Message != "task was killed: out of luck" {
		t.Errorf("RootCause = %+v", c)
	}
	if c := RootCause("  "); c != (Cause{}) {
		t.Errorf("empty trace gave %+v", c)
	}
}

func TestAdvice(t *testing.T) {
	for trace, want := range map[string]string{
		pgTrace: "something else is still reading the slot debezium_shop (pid 48211)",
		"Caused by: org.apache.kafka.common.errors.RecordTooLargeException: The message is 1338210 bytes when serialized":                                   "a record of 1338210 bytes",
		"Caused by: org.apache.kafka.common.errors.SerializationException: Unknown magic byte!":                                                             "aren't in the format the converter expects",
		"Caused by: org.postgresql.util.PSQLException: FATAL: password authentication failed for user \"cdc\"":                                              "credentials",
		"Caused by: java.net.ConnectException: Connection refused":                                                                                          "can't reach",
		"io.debezium.DebeziumException: The connector is trying to read binlog starting at mysql-bin.000003, but this is no longer available on the server": "mysql purged the binlog",
	} {
		if got := Advice(trace); !strings.Contains(got, want) {
			t.Errorf("Advice for %.60q = %q, want it to mention %q", trace, got, want)
		}
	}
	if got := Advice("java.lang.IllegalStateException: something nobody has seen"); got != "" {
		t.Errorf("unknown error got advice %q", got)
	}
}

func conn(name string, state string, tasks ...string) connect.Connector {
	c := connect.Connector{Name: name, Type: "source", Class: "io.debezium.connector.postgresql.PostgresConnector", State: state}
	for i, s := range tasks {
		task := connect.Task{ID: i, State: s, Worker: "w:8083"}
		if s == connect.Failed {
			task.Trace = pgTrace
		}
		c.Tasks = append(c.Tasks, task)
	}
	return c
}

func snap(at time.Time, cs ...connect.Connector) connect.Snapshot {
	return connect.Snapshot{At: at, Connectors: cs}
}

func TestFailedTask(t *testing.T) {
	sts := NewTracker(DefaultLimits).Update(snap(t0,
		conn("ok", connect.Running, connect.Running),
		conn("shop", connect.Running, connect.Running, connect.Failed),
	))
	if sts[0].Name != "shop" {
		t.Fatalf("worst should be first: %s, %s", sts[0].Name, sts[1].Name)
	}
	st := sts[0]
	if st.Level != Danger || st.Running != 1 || st.Failed != 1 {
		t.Errorf("shop = level %s, %d running, %d failed", st.Level, st.Running, st.Failed)
	}
	if !slices.Contains(st.Reasons, "1 of 2 tasks failed") {
		t.Errorf("reasons = %v", st.Reasons)
	}
	if st.CauseFrom != "task 1" || st.Cause.Short() != "PSQLException" || st.Advice == "" {
		t.Errorf("cause = %s %+v, advice %q", st.CauseFrom, st.Cause, st.Advice)
	}
	if sts[1].Level != OK || sts[1].Reasons != nil {
		t.Errorf("healthy connector = %s %v", sts[1].Level, sts[1].Reasons)
	}
}

func TestFailedConnectorBeatsTaskForTheCause(t *testing.T) {
	c := conn("shop", connect.Failed, connect.Failed)
	c.Trace = "org.x.ConfigException: Missing required configuration \"database.hostname\""
	st := NewTracker(DefaultLimits).Update(snap(t0, c))[0]
	if st.CauseFrom != "connector" || st.Cause.Short() != "ConfigException" {
		t.Errorf("cause = %s %+v", st.CauseFrom, st.Cause)
	}
	if !slices.Contains(st.Reasons, "the connector failed") {
		t.Errorf("reasons = %v", st.Reasons)
	}
}

func TestFlapping(t *testing.T) {
	tr := NewTracker(DefaultLimits)
	var st Status
	// fails and comes back three times in six minutes
	for i := range 6 {
		state := connect.Running
		if i%2 == 0 {
			state = connect.Failed
		}
		st = tr.Update(snap(t0.Add(time.Duration(i)*time.Minute), conn("es", connect.Running, state)))[0]
	}
	if st.Failed != 0 || st.Failures != 3 || st.Level != Warn {
		t.Fatalf("running again after 3 failures: level %s, %d failures", st.Level, st.Failures)
	}
	if !slices.Contains(st.Reasons, "failed 3 times in the last 15m") {
		t.Errorf("reasons = %v", st.Reasons)
	}
	// and once they are out of the window, it's fine again
	st = tr.Update(snap(t0.Add(30*time.Minute), conn("es", connect.Running, connect.Running)))[0]
	if st.Level != OK || st.Failures != 0 {
		t.Errorf("long after: level %s, %d failures", st.Level, st.Failures)
	}
}

func TestStillFailedIsOneFailure(t *testing.T) {
	tr := NewTracker(DefaultLimits)
	var st Status
	for i := range 10 {
		st = tr.Update(snap(t0.Add(time.Duration(i)*time.Minute), conn("shop", connect.Running, connect.Failed)))[0]
	}
	if st.Failures != 1 {
		t.Errorf("a task that stayed failed counted %d failures", st.Failures)
	}
	if st.FailedFor != 9*time.Minute {
		t.Errorf("FailedFor = %s", st.FailedFor)
	}
}

func TestUnassignedGetsAMinute(t *testing.T) {
	tr := NewTracker(DefaultLimits)
	st := tr.Update(snap(t0, conn("shop", connect.Running, connect.Unassigned)))[0]
	if st.Level != OK {
		t.Errorf("a rebalance shouldn't warn straight away: %v", st.Reasons)
	}
	st = tr.Update(snap(t0.Add(90*time.Second), conn("shop", connect.Running, connect.Unassigned)))[0]
	if st.Level != Warn || !slices.Contains(st.Reasons, "waiting for a worker for 1m") {
		t.Errorf("after 90s: %s %v", st.Level, st.Reasons)
	}
}

func TestRunningWithNoTasks(t *testing.T) {
	st := NewTracker(DefaultLimits).Update(snap(t0, conn("shop", connect.Running)))[0]
	if st.Level != Warn || !slices.Contains(st.Reasons, "running, but with no tasks") {
		t.Errorf("%s %v", st.Level, st.Reasons)
	}
	st = NewTracker(DefaultLimits).Update(snap(t0, conn("shop", connect.Paused)))[0]
	if st.Level != OK {
		t.Errorf("a paused connector with no tasks is fine: %v", st.Reasons)
	}
}

func TestDeletedConnectorsAreForgotten(t *testing.T) {
	tr := NewTracker(DefaultLimits)
	tr.Update(snap(t0, conn("shop", connect.Running, connect.Failed)))
	tr.Update(snap(t0.Add(time.Minute)))
	st := tr.Update(snap(t0.Add(2*time.Minute), conn("shop", connect.Running, connect.Failed)))[0]
	if st.FailedFor != 0 {
		t.Errorf("a recreated connector remembered %s of failing", st.FailedFor)
	}
}

func TestShortClass(t *testing.T) {
	if got := ShortClass("io.debezium.connector.postgresql.PostgresConnector"); got != "PostgresConnector" {
		t.Errorf("ShortClass = %q", got)
	}
	if !IsPostgresSource("io.debezium.connector.postgresql.PostgresConnector") || IsPostgresSource("io.confluent.connect.jdbc.PostgresSinkConnector") {
		t.Error("IsPostgresSource is wrong")
	}
}
