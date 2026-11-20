<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied. See the License for the
specific language governing permissions and limitations
under the License.
-->

# Iceberg snapshot source

This is a preview, opt-in JDK 17 connector using Iceberg **1.12.0**. It implements the
GeaFlow readable-table SPI; it does not provide a sink, CDC, or streaming reads.
Use a dedicated JDK 17 compiler/driver/worker distribution. The default JDK 8/11
reactor does not include this module.

## Build and verify

From the repository root, with JDK 17 and Maven:

```bash
mvn -Piceberg-jdk17 \
  -pl geaflow/geaflow-dsl/geaflow-dsl-connector/geaflow-dsl-connector-iceberg -am install \
  '-Dtest=Iceberg*Test,OffsetStoreRecoveryTest,JdbcPoolIsolationTest' \
  -Dsurefire.failIfNoSpecifiedTests=false -Dmaven.javadoc.skip=true
```

Run `IcebergCompatibilityTest`, `OffsetStoreRecoveryTest`, and
`JdbcPoolIsolationTest` first for the P0 baseline. The complete suite additionally
covers the fixed-snapshot source and real SourceFunction/JDBC offset path.
Do not interpret successful unit tests as a REST/object-storage or cluster SLA certification.

`install` also runs the isolated-artifact integration test. After that, run
`bash tools/ci/check-iceberg-isolation.sh` to verify a separate JVM with the existing
file/Paimon runtime dependencies. Set `MVN` to a Maven wrapper path if needed.
For a disposable PostgreSQL database, add `-Piceberg-postgres-it` and
`-Diceberg.test.postgres.url=jdbc:postgresql://localhost:5432/postgres?user=postgres`
to the first command (or combine the profiles with a comma). Supply the test
password through `ICEBERG_TEST_PG_PASSWORD`. The test creates and removes only its
own generated schema; it requires permission to create schemas.

The `isolated` classifier relocates Iceberg, Parquet, Avro, Jackson, HTTP client 5
and their selected Java dependencies. Hadoop **2.7.4**, GeaFlow APIs, SLF4J and
native codec packages remain shared with the host. Hadoop is deliberately not
relocated because its XML-configured implementations cross class boundaries.
Snappy, Gzip and Zstd were exercised against the packaged artifact; HDFS/S3,
Kerberos and credential renewal still require environment-specific verification.
Deploy the isolated artifact, not the unshaded module JAR, and use JDK 17 throughout.

## Supported read contract

- Iceberg v1/v2 tables with a stable UUID; unencrypted Parquet data **and delete** files.
- Full visible rows at one immutable snapshot: latest at planning time, explicit
  snapshot ID, timestamp in milliseconds, or a named tag/branch.
- Position and equality deletes use the Iceberg reader, including delete columns
  omitted from the output projection. Equality delete sequence semantics remain SDK-owned.
- Output types: BOOLEAN, INTEGER, LONG, FLOAT, DOUBLE, STRING, BINARY_STRING,
  DECIMAL, DATE. Output order follows the DSL schema; physical reads follow frozen field IDs.
- ALL streams lazily across every assigned task. SIZE counts rows per logical shard
  and can stop early at its estimated row-buffer byte limit. TIME is rejected.
- A fixed shard set is independent of worker parallelism. No online rescaling claim.
- Restoring an old cursor deterministically replays the original task after deletes.
  Delivery can repeat after a downstream failure; this is **not end-to-end exactly-once**.

Unsupported modes, v3/v4, encrypted/other-format files, incompatible output types,
missing snapshots, changed table UUIDs, mismatched windows and damaged plans fail
explicitly. A historical equality-delete field absent from the snapshot schema is
also rejected until historical schema union is certified. TIMESTAMP is deliberately
rejected because GeaFlow's existing type serializer loses sub-millisecond precision.

## Durable plan registry

Provision the tables in `src/main/resources/iceberg-plan-store.sql` once. The
connector does not create database objects or clean up plans automatically.
A run root and its tasks are committed in one JDBC transaction. Concurrent planners
use the primary key to select one winner and reload that committed plan.

The default limits are 100,000 tasks and 64 MiB of serialized task characters
(conservatively counted at two bytes per character). Planning and one shard are
materialized within these limits; this is a bounded first implementation, not an
unbounded external-sort/paged planner. SDK manifest/delete-index memory is additional.
One task's delete files are limited to 64 MiB (or `plan.max-bytes`) and 10,000 files;
compressed delete bytes are **not** an exact bound on the expanded delete index.

The registry is separate from GeaFlow's **offset backend**. For process recovery,
configure the engine's JDBC offset backend too. A MEMORY offset store cannot
recover after process loss. Plan and offset database connections must reach the
same persistent state from every worker. H2 file databases are local test fixtures;
use an operational database with a separately supplied JDBC driver for a cluster.
PostgreSQL 17.12 has been exercised for concurrent publication, transaction rollback
and reading the winning plan. This is not database failover or lost-commit-ACK certification.

The JDBC offset implementation now owns a pool per store, avoiding cross-job
database reuse and premature pool shutdown. An `OffsetStore` owns two stores;
budget database connections accordingly and configure
`geaflow.store.jdbc.connection.pool.size` (default 10 per pool). Source close also
closes these pools. The plan registry itself uses short-lived JDBC connections.

`run-id` must be unique per logical source run and stable across retries. Reusing
it with different options or schema is rejected. A deliberate new scan uses a new
run ID. Set `resume=true` when rebuilding a lost execution graph: a missing registry
entry then fails instead of creating a new latest scan. Retain the snapshot, data,
delete files, registry, and offsets for the entire recovery horizon.
GeaFlow offset keys also contain the engine job/table identity. Preserve that
identity during recovery; reusing only `run-id` in a new job preserves the snapshot
plan but does not import the previous job's offsets.

## Options

All connector keys below have the prefix `geaflow.dsl.iceberg.`.

| Key | Default / requirement | Meaning |
| --- | --- | --- |
| `mode` | `snapshot` | Only fixed snapshots are supported. |
| `run-id` | required | Durable logical scan token, unique per source run. |
| `table` | required | Iceberg identifier, e.g. `analytics.events`. |
| `catalog.type` | `hadoop` | `hadoop` or `rest`; service certification is separate. |
| `catalog.properties.*` | SDK properties | e.g. `warehouse`, `uri`; no custom `io-impl`/`catalog-impl`. |
| `catalog.secret.*` | environment variable name | Resolve a catalog secret on each open; never serialize its value. |
| `plan.jdbc.url` | required | Registry JDBC URL, without embedded credentials. |
| `plan.jdbc.user-env` | unset | Environment variable holding database username. |
| `plan.jdbc.password-env` | unset | Environment variable holding database password. |
| `resume` | `false` | Require an existing plan when rebuilding the graph. |
| `snapshot-id` | unset | Retained snapshot ID. |
| `as-of-timestamp-ms` | unset | Select from retained main history at or before this epoch time. |
| `ref` | unset | Named branch/tag head frozen during initial planning. |
| `shard-count` | `64` | Fixed K, independent of worker P; cannot change on recovery. |
| `split.target-bytes` | `134217728` | Target split size; does not split inside Parquet row groups. |
| `batch.max-bytes` | `67108864` | Estimated SIZE row-buffer limit; oversized single rows fail. |
| `plan.max-tasks` | `100000` | Planning admission limit. |
| `plan.max-bytes` | `67108864` | Planning and delete-file admission limit. |

Snapshot selectors are mutually exclusive. Unknown connector keys are rejected.
`table` uses the SDK's dot-separated identifier syntax; literal dots inside an
individual namespace or table name are not supported by this configuration.
Credentials belong in environment references, not persisted configuration or logs.
Do not use `io-impl` to bypass the certified FileIO boundary. REST's default FileIO
and remote object stores must be verified in the target distribution before deployment.

Example (database schema and driver must already be provisioned):

```sql
CREATE TABLE events (
  id BIGINT,
  payload VARCHAR
) WITH (
  type = 'iceberg',
  `geaflow.dsl.iceberg.run-id` = 'events-import-20261120-001',
  `geaflow.dsl.iceberg.table` = 'analytics.events',
  `geaflow.dsl.iceberg.catalog.type` = 'hadoop',
  `geaflow.dsl.iceberg.catalog.properties.warehouse` = 'file:///srv/warehouse',
  `geaflow.dsl.iceberg.plan.jdbc.url` = 'jdbc:postgresql://registry:5432/geaflow',
  `geaflow.dsl.iceberg.plan.jdbc.user-env` = 'ICEBERG_PLAN_DB_USER',
  `geaflow.dsl.iceberg.plan.jdbc.password-env` = 'ICEBERG_PLAN_DB_PASSWORD',
  `geaflow.dsl.iceberg.shard-count` = '7'
);
```

The local warehouse example requires shared paths on all participating workers.
Set worker/source parallelism explicitly; K is not a request to start K workers.
SQL predicates continue to execute in GeaFlow; this connector does not advertise
new filter-pushdown capabilities.

## Recovery diagnostics

Cursors carry codec version, plan checksum, shard, next task identity/index,
visible row ordinal, row count, and **target window ID**. A reused offset bucket
from another window is rejected. ALL's immutable END cursor reports `-1` for its
unknown final row count in the console; this numeric display is not recovery state.

A failed fetch discards its reader. A failed collect reuses the previous durable
cursor. If a connection drops during plan commit, the store reconciles by the run
key; it never treats a network exception alone as proof that the commit failed.
Missing plan/task rows and checksum mismatches cannot trigger a new scan.

Do not delete a plan or expire its snapshot to repair a recovery error. Restore the
missing state or submit a deliberate new run with an explicitly chosen snapshot.
Changes to SDK version, cursor format, sharding, or projection require a new run
until a state migration is implemented and tested.


## Multi-process P1 acceptance

The opt-in [`geaflow-dsl-connector-iceberg-tests`](../../geaflow-dsl-connector-iceberg-tests/README.md)
module exercises the isolated artifact with the complete host runtime. It launches
separate master, driver and single-worker container JVMs, uses PostgreSQL for plan
and offset state and Redis for HA, and sends SIGKILL during an uncommitted SIZE
window. The real engine restores its checkpoint and pipeline with flying batches
set to 1 or 3. Tests compare complete row multisets and require the frozen plan to
exclude a snapshot appended during failure.

This harness tests local multi-process execution; target Ray/Kubernetes deployments,
remote storage/authentication and large-scale endurance remain separate acceptance
gates. Its transactional idempotent fixture sink does not extend the connector's
production delivery guarantee.

The acceptance fixes also modify the shared operator, processor and DSL runtime.
Build and deploy those components together with the connector; replacing only the
Iceberg JAR does not apply the source-parallelism and window-context fixes.
