<div align="center">

# ⋈ connectwatch

**watch your kafka connect connectors in the terminal.**<br>
see which task failed and why, without digging through worker logs, and restart it.

[![ci](https://img.shields.io/github/actions/workflow/status/Dem1241/connectwatch/ci.yml?branch=main&style=flat-square&label=ci)](https://github.com/Dem1241/connectwatch/actions/workflows/ci.yml)
[![Go](https://img.shields.io/badge/go-1.26-00ADD8?style=flat-square&logo=go&logoColor=white)](https://go.dev)
[![Kafka Connect](https://img.shields.io/badge/kafka%20connect-3.7%20to%204.1-313244?style=flat-square&logo=apachekafka&logoColor=f5c2e7)](#what-it-understands)
[![License](https://img.shields.io/badge/license-MIT-f5c2e7?style=flat-square)](LICENSE)

<img src="assets/connectors.png" alt="connectwatch showing six connectors, a debezium postgres connector with a failed task selected and its RecordTooLargeException explained" width="860">

</div>

## why

kafka connect is happy to tell you a connector is `RUNNING` while every one of
its tasks has failed. finding out why means `curl`ing the status endpoint,
fishing a stack trace out of a json string, scrolling past forty lines of
`at org.apache.kafka.connect.runtime...` to the `Caused by` at the bottom, and
then working out what that actually means.

connectwatch puts every connector on one screen with a dot per task, worst
first. the detail pane pulls the root cause out of the trace, says what it
usually means (a record too large, a converter that doesn't match, a
replication slot something else still holds) and what to change. the whole
trace and the config, with passwords masked, are a `tab` away. `R` restarts
what failed and `p` pauses or resumes, after asking first.

it goes well with [slotwatch](https://github.com/Dem1241/slotwatch), for the
replication slot a stuck debezium connector leaves behind, and
[puddle](https://github.com/Dem1241/puddle), for the change events it writes.

## try it

```sh
go install github.com/Dem1241/connectwatch@latest
connectwatch --demo
```

or grab a binary for linux, macos or windows from the
[releases](https://github.com/Dem1241/connectwatch/releases).

`--demo` watches a pretend cluster: a debezium task that fails again every
time you restart it, a sink that hit a record it can't read, one that keeps
flapping and one somebody paused. restarting and pausing work there too.

## use it

```sh
connectwatch                                   # http://localhost:8083
connectwatch -u http://connect:8083            # or set CONNECT_URL
connectwatch -u https://me:secret@connect.internal
connectwatch --read-only                       # look, never touch
```

| key | does |
|:--|:--|
| `↑` `↓` or `j` `k` | move through the connectors |
| `tab` | overview, stack trace or config |
| `R` | restart the connector's failed tasks |
| `p` | pause or resume the connector |
| `s` | sort by health, name or type |
| `space` | pause or resume refreshing |
| `r` | refresh now |
| `ctrl+d` `ctrl+u` | scroll the detail pane |
| `/` | filter by name, class, type, state or worker |
| `esc` | clear the filter |
| `q` | quit |

### in cron, ci or kubernetes

```sh
connectwatch --check            # a table, and an exit code
connectwatch --check --json     # the same for scripts, without configs
```

`--check` looks once and exits with `0` when everything is fine, `1` when
something needs watching, `2` when something failed and `3` when it couldn't
check at all, like the usual monitoring plugins expect.

```text
LEVEL   CONNECTOR           TYPE    CLASS                       STATE    TASKS
danger  debezium-shop       source  PostgresConnector           RUNNING  0/1 running
danger  s3-sink-orders      sink    S3SinkConnector             RUNNING  2/3 running
ok      debezium-inventory  source  MySqlConnector              RUNNING  1/1 running

danger debezium-shop
  - 1 of 1 task failed
  task 0: RecordTooLargeException: The message is 1338210 bytes when serialized ...
```

## what it flags

- failed connectors, and failed tasks of connectors that still say `RUNNING`
- connectors that keep failing and coming back, `--flaps` times (3) within
  `--flap-window` (15 minutes), which a restart cron job would otherwise hide
- tasks waiting for a worker for more than a minute
- connectors that run with no tasks at all

## what it understands

connectwatch recognises the failures that come up again and again and says
what to do about them: debezium replication slots that are taken, dropped or
invalidated, purged mysql binlogs, records too large for the producer,
converter and schema registry mismatches, `errors.tolerance` giving up, bad
credentials, missing grants and ACLs, unreachable databases and brokers, a
worker out of memory and plugins missing from the plugin path. anything else
still gets its root cause pulled out of the trace.

it works with any kafka connect from 2.3 on (`?expand=status&expand=info`),
including debezium's, strimzi's and confluent's images. 3.7, 3.9 and 4.1 are
tested in ci. restarting only the failed tasks needs 3.0 or newer, older
workers restart just the connector.

## how it works

connectwatch reads `/connectors?expand=status&expand=info` every few seconds
(`--interval`), so a refresh is one request however many connectors there
are. it keeps the last few minutes of states per connector to count failures.
the interface is [bubble tea](https://github.com/charmbracelet/bubbletea).

```sh
go test ./...
CONNECTWATCH_TEST_URL=http://localhost:8083 go test ./internal/connect  # against a real worker
golangci-lint run
```

## license

[MIT](LICENSE)
