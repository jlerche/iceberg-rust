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

# Mutation integration acceptance

This suite exercises the supported v2/main Parquet path through the public
`RowDelta` and caller-controlled compaction APIs. It uses disposable REST/RustFS
services and an independent Spark Iceberg reader. Run from the repository root
with Docker Compose and the repository Rust toolchain installed:

```sh
docker compose -p iceberg-acceptance -f dev/docker-compose.yaml up -d --wait object-store rest
docker compose -p iceberg-acceptance -f dev/docker-compose.yaml run --rm create-buckets
docker compose -p iceberg-acceptance -f dev/docker-compose.yaml build spark-iceberg
cargo test --locked -p iceberg-integration-tests --test rewrite_compaction_test
cargo test --locked -p iceberg-integration-tests --test conflict_commit_test
cargo test --locked -p iceberg-integration-tests --test rewrite_compaction_test spark_reads_rust_row_delta_and_compaction_snapshots -- --ignored --nocapture
```

The Spark test is ignored in the ordinary suite because it requires the built
Compose image. It creates its own tables through Rust, then starts a one-shot
Spark container to read them. Set `ICEBERG_ACCEPTANCE_COMPOSE_PROJECT` when using
a different Compose project name. The existing fixture expects REST on port 8181
and S3 on port 9000; these ports must be available before starting the stack.

The equality case checks replacements, a tombstone, null keys, duplicate rows,
and untouched partitions. The position case checks a shared delete file,
multiple source files, and a same-commit data-file target. Both check exact rows,
retained snapshots, snapshot operations, sequence boundaries, and retained source
and delete objects after compaction. Spark independently checks the current main
reference and exact row multisets in the baseline, row-delta, compaction, and
current reads for both cases.

The REST tests also check rejection of a prepared row delta after a concurrent
matching append. The unknown-outcome test delegates one real REST commit, then
simulates loss of its success response at the Catalog boundary. It checks a
single submission and recovery by the exact prepared snapshot. It does not
simulate a dropped HTTP connection or a Postgres transaction.

Successful cases drop only their unique `rewrite_<uuid>` tables, table object
prefixes, and namespaces. Failed cases retain fixtures for diagnosis. The Spark
script only reads these fixtures. Preserve logs before removing this disposable
project:

```sh
docker compose -p iceberg-acceptance -f dev/docker-compose.yaml logs > /tmp/iceberg-acceptance-services.log
docker compose -p iceberg-acceptance -f dev/docker-compose.yaml down -v --remove-orphans
```

Do not use this cleanup command for an unrelated Compose project. The supported
contract, retained-object costs, and deferred features are recorded in
`IMPLEMENTATION_PLAN.md`. Passing this suite defines the fork's initial icestream
integration boundary; application coordination and recovery persistence belong
in icestream.
