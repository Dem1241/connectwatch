package health

import (
	"regexp"
	"strings"
)

// hint is advice for an error connectwatch recognises.
type hint struct {
	re     *regexp.Regexp
	advice string // $1 and friends are filled in from re
}

// hints are the failures that come up again and again, most specific first.
// They match anywhere in the trace.
var hints = []hint{
	{regexp.MustCompile(`replication slot "([^"]+)" is active for PID (\d+)`),
		`something else is still reading the slot $1 (pid $2), often an old task that didn't shut down. find it in pg_stat_replication, stop it, then restart the connector.`},
	{regexp.MustCompile(`replication slot "([^"]+)" does not exist`),
		`the slot $1 was dropped. the connector needs a new snapshot: give it a new slot, or set snapshot.mode to when_needed, and restart it.`},
	{regexp.MustCompile(`(?i)requested WAL segment \S+ has already been removed|can no longer get the changes|replication slot "[^"]+" .*invalidated`),
		`postgres removed WAL the slot still needed, so the changes in between are gone. the connector needs a new snapshot (snapshot.mode when_needed), and max_slot_wal_keep_size may be set too low for how long it was down.`},
	{regexp.MustCompile(`binlog .*is no longer available on the server|Could not find first log file name in binary log index file`),
		`mysql purged the binlog the connector was at. it needs a new snapshot (snapshot.mode when_needed), and binlog_expire_logs_seconds may be shorter than it was down for.`},
	{regexp.MustCompile(`RecordTooLargeException.*?(\d+) bytes`),
		`a record of $1 bytes is bigger than the producer allows. raise producer.override.max.request.size on the connector (the worker needs connector.client.config.override.policy=All) and max.message.bytes on the topic.`},
	{regexp.MustCompile(`RecordTooLargeException`),
		`a record is bigger than the producer allows. raise producer.override.max.request.size on the connector and max.message.bytes on the topic.`},
	{regexp.MustCompile(`Unknown magic byte|Error deserializing|Converting byte\[\] to Kafka Connect data failed`),
		`the records on the topic aren't in the format the converter expects, like plain json read with the avro converter. check key.converter and value.converter against what the producer writes. errors.tolerance=all with a dead letter queue skips the bad ones.`},
	{regexp.MustCompile(`Schema being registered is incompatible`),
		`the schema registry refused a new schema version. check the subject's compatibility setting, or the change in the source table that caused it.`},
	{regexp.MustCompile(`Tolerance exceeded in error handler`),
		`a record failed to convert or transform, and errors.tolerance is none. fix the record or the converter, or set errors.tolerance=all with errors.deadletterqueue.topic.name to set bad records aside.`},
	{regexp.MustCompile(`(?i)password authentication failed|Access denied for user|Login failed for user|authentication failed`),
		`the credentials in the connector config were refused. check the user and password, and whether they were rotated.`},
	{regexp.MustCompile(`(?i)permission denied|must be superuser or replication role|must have (?:the )?(?:required )?(?:permissions|privileges)`),
		`the database user is missing a privilege the connector needs, like REPLICATION or SELECT on the captured tables.`},
	{regexp.MustCompile(`TopicAuthorizationException|ClusterAuthorizationException|GroupAuthorizationException`),
		`kafka refused access. the connector's principal needs ACLs on its topics (and on its consumer group for sinks).`},
	{regexp.MustCompile(`(?i)connection refused|UnknownHostException|No route to host|Connection timed out|Communications link failure|The connection attempt failed`),
		`the connector can't reach the system on the other end. check that it is up, and the host and port in the config.`},
	{regexp.MustCompile(`TimeoutException.*(?:metadata|Topic \S+ not present)`),
		`the worker can't get topic metadata from kafka in time. check that the brokers are up and the topic exists, or that topic creation is allowed.`},
	{regexp.MustCompile(`OutOfMemoryError`),
		`the worker ran out of memory. give it more heap (KAFKA_HEAP_OPTS), or lower batch sizes and tasks.max on the busy connectors.`},
	{regexp.MustCompile(`ClassNotFoundException|NoClassDefFoundError|Failed to find any class that implements Connector`),
		`a class is missing from the plugin path. check that the plugin is installed on every worker, and plugin.path.`},
}

// Advice is what to do about a failure, if connectwatch recognises it.
func Advice(trace string) string {
	for _, h := range hints {
		if m := h.re.FindStringSubmatchIndex(trace); m != nil {
			return string(h.re.ExpandString(nil, h.advice, trace, m))
		}
	}
	return ""
}

// IsPostgresSource is whether a connector reads postgres through a
// replication slot, which holds on to WAL while the connector is down.
func IsPostgresSource(class string) bool {
	return strings.Contains(strings.ToLower(class), "postgres") && !strings.Contains(strings.ToLower(class), "sink")
}
