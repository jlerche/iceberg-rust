<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
-->

Example usage code for `iceberg-rust`. The REST and OSS examples require their
respective services and credentials.

## Caller-controlled compaction

Run the self-contained example with an in-memory catalog and real local Parquet
files; no services, credentials, or feature flags are required:

```bash
cargo run --locked -p iceberg-examples --example caller-controlled-compaction
```

The executable checks exact rows, duplicate and null preservation, historical
reads, and retained staged artifacts. It exits with an error or assertion failure
if an expected outcome does not hold. Its three scenarios demonstrate:

1. Select two live file descriptors, execute without publishing, refresh the
   table, prepare snapshot metadata, and transfer the standard `TableCommit`
   with `try_into_table_commit()`. An unselected file remains live.
2. Append after preparation, observe a definitive catalog requirement conflict,
   reload the table, and explicitly prepare the completed result again. The
   appended rows survive and the data outputs are reused. This example's
   in-memory catalog checks requirements before changing its table pointer;
   do not assume every catalog error proves rejection.
3. Commit successfully and simulate a lost response at the catalog-call boundary.
   A retryable error hint does not establish failure. An intentionally stale
   read leaves the outcome unknown: retain the files and do not resubmit.
   After another append, finding the exact prepared snapshot in the same table's
   retained main ancestry establishes publication even though it is no longer
   the head. Missing evidence remains unknown, including after history expiration.

The example prints a receipt before handoff: table identity and UUID, validated
publication-base metadata location and snapshot, prepared snapshot identity and
manifest-list location, and candidate artifact paths. `compaction/recovery.rs`
keeps the receipt in memory for demonstration. A caller that needs restart
recovery must durably retain this information before submission and retain the
resulting outcome; neither the in-memory receipt nor its console output provides
that guarantee. The in-memory catalog pointer also does not survive restart.

A downstream catalog adapter can consume the same standard commit. It must
atomically enforce requirements and the validated metadata base when coordinating
publication with external state. This example publishes through `Catalog` and
does not implement an external database transaction or establish cross-store
atomicity. Publication protection is conservative; an artifact inventory is
neither an ownership proof nor permission to delete files.

The example prints and retains its unique disposable local warehouse on both
success and failure, including source, staged, and published objects. It does
not clean up unknown outcomes. Remove that demo-only directory manually when
finished inspecting it; this is not a production orphan-cleanup procedure.
