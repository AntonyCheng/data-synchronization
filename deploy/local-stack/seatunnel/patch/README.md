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

`TableMapEventDataDeserializer.java` is `com.zendesk:mysql-binlog-connector-java:0.27.2`
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
`apache/seatunnel` base image ships its own connector build there. That build is a
different artifact from the Maven Central one in `vendor/connectors/` (different md5,
same bundled `com.zendesk:mysql-binlog-connector-java:0.27.2`), so patching the Maven
jar has no effect on the running engine.

`build-patched-connector.ps1` therefore:

1. pulls `connector-cdc-mysql-2.3.13.jar` out of the base image (`docker create` +
   `docker cp`, cached in `vendor/base/`),
2. compiles `patch/` against it,
3. runs `GoldenDbTableMapSelfTest` - two real captured GoldenDB events plus a standard
   MySQL event - and refuses to publish unless all three pass,
4. writes a copy of the base jar with the two recompiled classes replaced into
   `vendor/patched/`.

The Dockerfile then copies `vendor/patched/` into `/opt/seatunnel/connectors/`,
overwriting the base image's own jar, so exactly one copy is on the scanned path.

`dev.ps1 up -Poc` runs `fetch-vendor.ps1`, then `build-patched-connector.ps1`, then
`docker compose up --detach --build` (the `--build` is what carries a rebuilt patch
into the image).

## Upstream

Report to ZTE: GoldenDB's `Table_map_event` is not wire-compatible with the standard
MySQL binlog format. Ask for a compatibility mode that omits the 8 bytes, which would
let stock Canal / Debezium / SeaTunnel work with no patch at all.
