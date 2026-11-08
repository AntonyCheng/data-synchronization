# Patched SeaTunnel 2.3.13 connectors

Two connector jars of the engine image carry a patch, each in its own directory, compiled against the
jar it replaces classes in and self-tested by `../build-patched-connector.ps1` before it is published:

| Directory | Jar | Patch |
|---|---|---|
| `connector-cdc-mysql/` | `connector-cdc-mysql-2.3.13.jar` | decode ZTE GoldenDB's non-standard `Table_map` event (below) |
| `connector-kafka/` | `connector-kafka-2.3.13.jar` | a record the Kafka sink cannot deliver fails the job instead of vanishing ([Kafka sink](#kafka-sink-delivery-patch)) |

# GoldenDB `Table_map` compatibility patch

## Why

ZTE GoldenDB (`goldendb_version = V_ALL-DBV6.1.03.12SP1`, DN storage node, MySQL
protocol, `@@version` reports `8.9.99`) writes a **non-standard `Table_map_event`**.

Between the post-header (`table_id` 6 bytes + `flags` 2 bytes) and the standard
payload (`schema_name_length`) it inserts **8 extra bytes** - two little-endian
`uint32`, observed as `(1, 1)`, the first matching the node's `group_id`. It also
sets `flags = 0x1f01` instead of the standard `0x0001`.

Raw bytes captured from a live node (`test.t_test`, columns `int` + `varchar`):

```
88 00 00 00 00 00        table_id = 136                      standard
01 1f                    flags = 0x1f01                      NON-STANDARD (0x0001)
01 00 00 00 01 00 00 00  <- 8 extra bytes                    NON-STANDARD (absent)
04 "test" 00             schema                              standard
06 "t_test" 00           table                               standard
02 / 03 0f               2 columns / LONG, VARCHAR           standard
02 fc 03 / 02            metadata / null bitmap              standard
01 01 00 02 01 2e        optional metadata                   standard
```

Everything after the 8 bytes is byte-for-byte standard MySQL 8.0, and **every other
event type is fully standard** - `Write_rows`, `GTID`, `Query` and `Xid` were all
verified byte-for-byte.

A stock parser reads `01` as `schema_name_length` and `00` as the name, so it yields
an empty schema, then an empty table, then reads `00` as a column type. That is the
root cause of every symptom seen:

- `mysqlbinlog` prints ``Table_map: `.` `` with blank row images and phantom repeated blocks
- Apache Canal throws `IllegalArgumentException: unknow type : 0` at `TableMapLogEvent.java:504`
- Debezium / SeaTunnel MySQL-CDC match nothing against the include list, so CDC yields no events

It is unconditional in this build: setting the only plausible switch,
`enable_binlog_gtmgtid_index = OFF`, produced a byte-for-byte identical event.

## What the patch does

`connector-cdc-mysql/.../TableMapEventDataDeserializer.java` is `com.zendesk:mysql-binlog-connector-java:0.27.2`
(bundled unshaded inside `connector-cdc-mysql-2.3.13.jar`) with one change: instead of
blindly skipping `flags` + `schema_name_length` with `skip(3)`, it reads `flags` and
skips the 8 vendor bytes when the vendor bits are set:

```java
int flags = inputStream.readInteger(2);
if ((flags & 0xFF00) == 0x1F00) {
    inputStream.skip(8);
}
inputStream.skip(1); // length of database name
```

**The MySQL path is unaffected**: a standard server always writes `flags = 0x0001`,
whose high byte is zero, so the branch is never taken and the byte stream is consumed
exactly as before. This is covered by the regression case in the build script's
self-test.

## How it is applied

Which jar to patch matters. SeaTunnel 2.3.13 scans **`/opt/seatunnel/connectors/`** -
that is where `plugin-mapping.properties` lives - not `connectors/seatunnel/`, and the
`apache/seatunnel` base image ships its own connector builds there. Those are different
artifacts from the Maven Central ones in `vendor/connectors/` (for MySQL-CDC: different md5,
same bundled `com.zendesk:mysql-binlog-connector-java:0.27.2`), so patching a Maven jar has no
effect on the running engine.

`build-patched-connector.ps1` therefore, for each directory here:

1. takes the connector jar out of the base image (`docker create` + `docker cp`, cached in
   `vendor/base/`) - or out of the local engine image when the base image is not pulled - and
   accepts it only with the original jar's SHA-1 (an engine image already carries patched jars),
2. compiles the directory against it (`--release 8`, the engine runs Java 8),
3. runs the directory's self-test and refuses to publish unless it passes - for MySQL-CDC two real
   captured GoldenDB events plus a standard MySQL event, for Kafka a send that fails without a broker,
4. writes a copy of the base jar with every recompiled class of a package replaced into
   `vendor/patched/`.

The Dockerfile then copies `vendor/patched/` into `/opt/seatunnel/connectors/`,
overwriting the base image's own jars, so exactly one copy of each is on the scanned path.

`dev.ps1 up -Poc` runs `fetch-vendor.ps1`, then `build-patched-connector.ps1`, then
`docker compose up --detach --build` (the `--build` is what carries a rebuilt patch
into the image).

## Upstream

Report to ZTE: GoldenDB's `Table_map_event` is not wire-compatible with the standard
MySQL binlog format. Ask for a compatibility mode that omits the 8 bytes, which would
let stock Canal / Debezium / SeaTunnel work with no patch at all.

# Kafka sink delivery patch

## Why

The Kafka sink's sender for `AT_LEAST_ONCE` / `NON` semantics,
`org.apache.seatunnel.connectors.seatunnel.kafka.sink.KafkaNoTransactionSender`, calls
`KafkaProducer.send(record)` and discards the returned future; its `snapshotState` only calls
`flush()`, which waits for the sends but reports none of their failures (confirmed with `javap -c` on
the jar in the engine image). A record the producer refuses - larger than `max.request.size`, larger
than the topic's `max.message.bytes`, a topic it may not write - completes its future exceptionally and
is gone: the job stays RUNNING, the checkpoint completes and the source offset moves past the row.

Reproduced on the local stack (2026-11-08): a FULL_CDC MySQL -> Kafka task, three INSERTs, the middle
one a 1.5 MB `LONGTEXT`. The raw topic held the rows before and after it, never the large one; the task
stayed RUNNING with no error, and the engine counted it as read, written and committed
(`SourceReceivedCount = SinkWriteCount = SinkCommittedCount = 5`). Nothing was logged.

## What the patch does

`connector-kafka/.../KafkaNoTransactionSender.java` is the 2.3.13 class with a send callback that keeps
the first failure. The next `send`, the next checkpoint (`snapshotState`, after the flush) or `close`
throws it, so the checkpoint that would have covered the record never completes: the job restores from
the previous checkpoint, meets the same record again and ends FAILED with the producer's error
(`RecordTooLargeException: ...`) as its error message - which the platform shows. Nothing else changes:
a healthy send path makes exactly the producer calls it made before.

The platform also lifts the limit a record hits in the first place: generated Kafka sinks carry
`max.request.size = sync.engine.kafka-max-record-bytes` (16 MiB by default) and the bridge creates the
raw topic with a matching `max.message.bytes`.

## Upstream

Worth reporting to Apache SeaTunnel: `KafkaNoTransactionSender` loses records the producer refuses
under `AT_LEAST_ONCE` semantics.
