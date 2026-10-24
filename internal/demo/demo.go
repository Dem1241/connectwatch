// Package demo is a pretend Kafka Connect cluster with a few connectors in
// trouble: a debezium task that failed hours ago and fails again when
// restarted, a sink task that hit a bad record, one that keeps flapping and
// one somebody paused. connectwatch can be tried with it without a cluster,
// and restarting, pausing and resuming work too.
package demo

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Dem1241/connectwatch/internal/connect"
)

// Cluster hands out snapshots that change over time.
type Cluster struct {
	mu    sync.Mutex
	start time.Time
	now   time.Time
	conns []connect.Connector
	// restarted is when a connector was last restarted, for the ones that
	// come back RESTARTING and then do something
	restarted map[string]time.Time
}

const (
	worker0 = "connect-0.connect:8083"
	worker1 = "connect-1.connect:8083"
	worker2 = "connect-2.connect:8083"
)

// New starts the pretend cluster at now.
func New(now time.Time) *Cluster {
	return &Cluster{
		start:     now,
		now:       now,
		restarted: map[string]time.Time{},
		conns: []connect.Connector{
			{
				Name: "debezium-shop", Type: "source", Class: "io.debezium.connector.postgresql.PostgresConnector",
				State: connect.Running, Worker: worker1,
				Tasks: []connect.Task{{ID: 0, State: connect.Failed, Worker: worker1, Trace: tooLarge}},
				Config: map[string]string{
					"connector.class":   "io.debezium.connector.postgresql.PostgresConnector",
					"database.hostname": "shop-db.internal", "database.port": "5432", "database.dbname": "shop",
					"database.user": "debezium", "database.password": "hunter2-but-longer",
					"slot.name": "debezium_shop", "plugin.name": "pgoutput", "topic.prefix": "shop",
					"table.include.list": "public.orders,public.customers,public.invoices", "tasks.max": "1",
				},
			},
			{
				Name: "debezium-inventory", Type: "source", Class: "io.debezium.connector.mysql.MySqlConnector",
				State: connect.Running, Worker: worker0,
				Tasks: []connect.Task{{ID: 0, State: connect.Running, Worker: worker2}},
				Config: map[string]string{
					"connector.class":   "io.debezium.connector.mysql.MySqlConnector",
					"database.hostname": "inventory-db.internal", "database.user": "cdc", "database.password": "s3cret",
					"database.server.id": "184054", "topic.prefix": "inventory", "tasks.max": "1",
					"schema.history.internal.kafka.topic": "schema-changes.inventory",
				},
			},
			{
				Name: "s3-sink-orders", Type: "sink", Class: "io.confluent.connect.s3.S3SinkConnector",
				State: connect.Running, Worker: worker2,
				Tasks: []connect.Task{
					{ID: 0, State: connect.Running, Worker: worker0},
					{ID: 1, State: connect.Running, Worker: worker1},
					{ID: 2, State: connect.Failed, Worker: worker2, Trace: badRecord},
				},
				Config: map[string]string{
					"connector.class": "io.confluent.connect.s3.S3SinkConnector", "topics": "shop.public.orders",
					"s3.bucket.name": "lake-raw", "s3.region": "eu-west-1", "flush.size": "10000", "tasks.max": "3",
					"value.converter":                     "io.confluent.connect.avro.AvroConverter",
					"value.converter.schema.registry.url": "http://schema-registry:8081",
					"aws.secret.access.key":               "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
				},
			},
			{
				Name: "es-sink-search", Type: "sink", Class: "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
				State: connect.Running, Worker: worker0,
				Tasks: []connect.Task{
					{ID: 0, State: connect.Running, Worker: worker0},
					{ID: 1, State: connect.Running, Worker: worker2},
				},
				Config: map[string]string{
					"connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
					"topics":          "shop.public.customers", "connection.url": "http://search-es:9200",
					"connection.password": "elastic", "tasks.max": "2",
				},
			},
			{
				Name: "jdbc-sink-legacy", Type: "sink", Class: "io.confluent.connect.jdbc.JdbcSinkConnector",
				State: connect.Paused, Worker: worker1,
				Tasks: []connect.Task{
					{ID: 0, State: connect.Paused, Worker: worker1},
					{ID: 1, State: connect.Paused, Worker: worker2},
				},
				Config: map[string]string{
					"connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector", "topics": "inventory.products",
					"connection.url": "jdbc:postgresql://legacy-db:5432/erp", "connection.password": "erp", "tasks.max": "2",
				},
			},
			{
				Name: "mm2-heartbeats", Type: "source", Class: "org.apache.kafka.connect.mirror.MirrorHeartbeatConnector",
				State: connect.Running, Worker: worker2,
				Tasks: []connect.Task{{ID: 0, State: connect.Running, Worker: worker1}},
				Config: map[string]string{
					"connector.class":      "org.apache.kafka.connect.mirror.MirrorHeartbeatConnector",
					"source.cluster.alias": "eu", "target.cluster.alias": "us", "emit.heartbeats.interval.seconds": "5",
				},
			},
		},
	}
}

// Snapshot is the cluster as it is now.
func (c *Cluster) Snapshot(context.Context) (connect.Snapshot, error) {
	return c.At(time.Now()), nil
}

// At is the snapshot at a given time, so tests and screenshots can play the
// demo faster than real time. Times must not go backwards.
func (c *Cluster) At(now time.Time) connect.Snapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = now
	for i := range c.conns {
		k := &c.conns[i]
		switch k.Name {
		case "debezium-shop":
			// the record is still too large, so a restart only buys a few seconds
			if at, ok := c.restarted[k.Name]; ok {
				switch since := now.Sub(at); {
				case since < 2*time.Second:
					k.Tasks[0].State = connect.Restarting
				case since < 12*time.Second:
					k.Tasks[0].State, k.Tasks[0].Trace = connect.Running, ""
				default:
					k.Tasks[0].State, k.Tasks[0].Trace = connect.Failed, tooLarge
					delete(c.restarted, k.Name)
				}
			}
		case "s3-sink-orders":
			if at, ok := c.restarted[k.Name]; ok {
				if now.Sub(at) < 2*time.Second {
					k.Tasks[2].State = connect.Restarting
				} else {
					k.Tasks[2].State, k.Tasks[2].Trace = connect.Running, ""
					delete(c.restarted, k.Name)
				}
			}
		case "es-sink-search":
			// elasticsearch drops out for a while every half minute and
			// something restarts the task once it is back
			if k.State != connect.Running {
				continue
			}
			if int(now.Sub(c.start).Seconds())%30 < 10 && now.Sub(c.start) > 5*time.Second {
				k.Tasks[1].State, k.Tasks[1].Trace = connect.Failed, refused
			} else {
				k.Tasks[1].State, k.Tasks[1].Trace = connect.Running, ""
			}
		}
	}
	snap := connect.Snapshot{
		At:      now,
		Cluster: connect.Cluster{Name: "demo/connect", Version: "3.9.0", ClusterID: "Mk3OEYBSD34fcwNTJENDM2Qk"},
	}
	for _, k := range c.conns {
		k.Tasks = append([]connect.Task(nil), k.Tasks...)
		snap.Connectors = append(snap.Connectors, k)
	}
	return snap
}

func (c *Cluster) find(name string) (*connect.Connector, error) {
	for i := range c.conns {
		if c.conns[i].Name == name {
			return &c.conns[i], nil
		}
	}
	return nil, &connect.Error{Status: 404, Message: fmt.Sprintf("Connector %s not found", name)}
}

// Restart restarts the failed tasks of a connector.
func (c *Cluster) Restart(_ context.Context, name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	k, err := c.find(name)
	if err != nil {
		return err
	}
	for i := range k.Tasks {
		if k.Tasks[i].State == connect.Failed {
			k.Tasks[i].State = connect.Restarting
			c.restarted[name] = c.now
		}
	}
	return nil
}

// Pause pauses a connector and its tasks.
func (c *Cluster) Pause(_ context.Context, name string) error { return c.set(name, connect.Paused) }

// Resume resumes a connector and its tasks.
func (c *Cluster) Resume(_ context.Context, name string) error { return c.set(name, connect.Running) }

func (c *Cluster) set(name, state string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	k, err := c.find(name)
	if err != nil {
		return err
	}
	k.State = state
	for i := range k.Tasks {
		if k.Tasks[i].State != connect.Failed {
			k.Tasks[i].State = state
		}
	}
	return nil
}

// The traces are trimmed copies of real ones.
const (
	tooLarge = `org.apache.kafka.connect.errors.ConnectException: Unrecoverable exception from producer send callback
	at org.apache.kafka.connect.runtime.WorkerSourceTask.maybeThrowProducerSendException(WorkerSourceTask.java:340)
	at org.apache.kafka.connect.runtime.AbstractWorkerSourceTask.sendRecords(AbstractWorkerSourceTask.java:395)
	at org.apache.kafka.connect.runtime.AbstractWorkerSourceTask.execute(AbstractWorkerSourceTask.java:354)
	at org.apache.kafka.connect.runtime.WorkerTask.doRun(WorkerTask.java:204)
	at org.apache.kafka.connect.runtime.WorkerTask.run(WorkerTask.java:259)
	at java.base/java.lang.Thread.run(Thread.java:1583)
Caused by: org.apache.kafka.common.errors.RecordTooLargeException: The message is 1338210 bytes when serialized which is larger than 1048576, which is the value of the max.request.size configuration.
`
	badRecord = `org.apache.kafka.connect.errors.ConnectException: Tolerance exceeded in error handler
	at org.apache.kafka.connect.runtime.errors.RetryWithToleranceOperator.execAndHandleError(RetryWithToleranceOperator.java:230)
	at org.apache.kafka.connect.runtime.WorkerSinkTask.convertAndTransformRecord(WorkerSinkTask.java:536)
	at org.apache.kafka.connect.runtime.WorkerSinkTask.poll(WorkerSinkTask.java:340)
	at java.base/java.lang.Thread.run(Thread.java:1583)
Caused by: org.apache.kafka.connect.errors.DataException: Failed to deserialize data for topic shop.public.orders to Avro:
	at io.confluent.connect.avro.AvroConverter.toConnectData(AvroConverter.java:148)
	at org.apache.kafka.connect.runtime.WorkerSinkTask.convertValue(WorkerSinkTask.java:580)
	... 13 more
Caused by: org.apache.kafka.common.errors.SerializationException: Unknown magic byte!
	at io.confluent.kafka.serializers.AbstractKafkaSchemaSerDe.getByteBuffer(AbstractKafkaSchemaSerDe.java:600)
	... 17 more
`
	refused = `org.apache.kafka.connect.errors.ConnectException: Failed to execute bulk request due to 'java.net.ConnectException: Connection refused' after 6 attempt(s)
	at io.confluent.connect.elasticsearch.ElasticsearchClient.callWithRetries(ElasticsearchClient.java:437)
	at io.confluent.connect.elasticsearch.ElasticsearchClient.bulk(ElasticsearchClient.java:309)
	at java.base/java.lang.Thread.run(Thread.java:1583)
Caused by: java.net.ConnectException: Connection refused
	at java.base/sun.nio.ch.Net.pollConnect(Native Method)
	at org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor.processEvent(DefaultConnectingIOReactor.java:174)
	... 1 more
`
)
