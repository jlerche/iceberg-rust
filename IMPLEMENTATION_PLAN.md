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

# Implementation Plan: Iceberg Mutation and DataFusion Write Support

## 1. Scope

This repository is an Iceberg implementation. The purpose of this plan is to
add missing, generally useful Iceberg features: complete snapshot-producing
transactions, safe file mutation, conflict detection, maintenance operations,
and scalable DataFusion writes.

The features may be useful to downstream systems, but no downstream product
architecture is part of this work. Every API, type, metadata field, module, and
test added under this plan must make sense for an ordinary Iceberg user with no
knowledge of any particular application.

## 2. Hard repository boundary

The following work is explicitly outside this repository and outside this plan:

- OLAP engine or data-warehouse orchestration;
- incremental or materialized-view planning and execution;
- view definitions, dependency graphs, refresh state, or source checkpoints;
- Postgres-specific catalogs, schemas, transactions, or control-plane state;
- proprietary logical CDC or change-sidecar formats;
- engine-specific row IDs or logical-to-physical row locators;
- proprietary arrangements, secondary indexes, or index routing;
- atomic publication protocols that combine an Iceberg commit with
  application-specific state;
- application query fingerprints, provenance, operator state, or audit models.

Those concerns must live in a separate repository that consumes this fork
through its public Iceberg and DataFusion APIs. They must not appear as optional
features, experimental modules, generic-looking extension points, table
properties, snapshot-summary keys, or reserved columns here.

If a proposed change cannot be explained and tested purely in terms of the
Iceberg specification or generic DataFusion integration, it does not belong in
this repository.

## 3. Compatibility invariants

### 3.1 Standard Iceberg semantics

All committed tables must remain ordinary Iceberg tables. Metadata, manifests,
data files, delete files, snapshots, sequence numbers, and operation summaries
must follow the Iceberg specification. External readers must require no
fork-specific metadata to obtain correct table contents.

### 3.2 No proprietary table state

Do not add custom metadata fields, reserved columns, snapshot properties, file
formats, or catalog requirements for downstream consumers. New metadata support
is appropriate only when it implements the Iceberg specification or preserves
standard metadata supplied by another Iceberg implementation.

### 3.3 Catalog neutrality

Core transactions prepare standard `TableUpdate` and `TableRequirement` values
and publish through the existing `Catalog` abstraction. Improvements must work
with any conforming catalog. No transaction behavior may depend on a particular
database, REST service implementation, or external coordinator.

### 3.4 DataFusion remains an integration

The separately maintained `iceberg-datafusion` integration may plan scans,
write files, and expose physical execution operators. `crates/iceberg` must not depend on DataFusion. Transaction semantics
and validation remain usable by non-DataFusion callers.

### 3.5 Physical and logical operation semantics

Use standard Iceberg operation meanings consistently:

- append adds rows;
- overwrite changes logical table contents;
- delete removes logical rows;
- replace rewrites physical representation without changing logical contents.

Compaction and reclustering must never be reported as logical row changes.

## 4. Repository baseline

Baseline reviewed on 2026-10-06 against upstream commit
`f2409e9f5eaff1c643985013d927e17fdf16e6ec` (fast-forwarded from
`db4f6091850814b83989721afe12aa9e4406d6b3`, 215 upstream commits):

- workspace version `0.10.1`, MSRV Rust `1.95`, and selected lint toolchain
  `nightly-2026-04-16`; DataFusion is no longer a workspace dependency;
- `SnapshotProducer`, `SnapshotProduceOperation`, manifest-list writing,
  snapshot summaries, and retrying `Transaction::commit` remain present;
- `FastAppendAction` remains the only data-file-mutating transaction action;
  removed-entry handling in `transaction/snapshot.rs` remains a TODO;
- retries reload metadata and replay ordered actions against staged table state,
  collecting each action's requirements; preparation/publication separation,
  shared-base requirement compilation, and unknown-outcome recovery still need
  implementation;
- `cow_rewrite::CowRewriteBuilder` now plans, reads visible rows, and writes
  replacement data files, returning old/new file sets without committing.
  It preserves source partition values/spec and planned-snapshot schema, and
  does not apply a sort order. Reuse this primitive rather than building a
  second core rewrite executor; its presence does not complete overwrite,
  compaction, delete reconciliation, or conflict validation;
- equality-delete and position-delete writers, including `PositionDeleteInput`,
  already exist. Extend their validation and transaction integration;
- v3 deletion-vector reading and row-lineage scan metadata have advanced.
  Reader support does not establish correct v3 mutation or rewrite support;
- `ExpireSnapshotsAction` remains metadata-only; physical cleanup is future work;
- `docs/rfcs/0003_stateful_transaction.md` is a **draft** stateful-action RFC,
  not an implemented lifecycle. Align A's action-state ownership with it while
  retaining this plan's publication, recovery, and reference guarantees;
- upstream removed `crates/integrations/datafusion`, its playground, and
  `crates/sqllogictest` in `a1de3a10f` after migration. DataFusion milestones
  are companion-repository work, not current workspace implementation targets;
- the disposable S3-compatible integration fixture now uses RustFS and
  `ICEBERG_TEST_OBJECT_STORE_ENDPOINT`, replacing RustFS-specific configuration.

Core entry points are `crates/iceberg/src/transaction/`,
`crates/iceberg/src/cow_rewrite/`, `crates/iceberg/src/catalog/mod.rs`, and
`crates/iceberg/src/writer/base_writer/`. Former DataFusion source paths below
are migration references only; resolve their current equivalents before work.

### Upstream sync decisions

Keep the core roadmap and compatibility invariants. No milestone is marked
complete by this source review. Reuse the upstream COW and delete-writer
primitives, and review their limits with the exact row/sequence fixtures below.
A–F and core H–M remain actionable here. G, K, and DataFusion portions of H–J
are deferred to the migrated integration repository until its location, pinned
Iceberg dependency, and validation commands are recorded. Do not restore the
removed integration crates solely to satisfy this plan. Core milestone exits
must identify companion checks that remain pending; the whole roadmap is not
complete until those checks pass too.

The [Iceberg specification](https://iceberg.apache.org/spec/) governs format
semantics; the [REST catalog protocol](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml)
defines portable commit requirements and outcomes. Record the specification
revision used by interoperability fixtures when implementation begins.

## 5. Process choices

- Use the existing `main` branch; no fork branch taxonomy is required.
- Keep `origin` pointed at this fork and `upstream` pointed at Apache
  `iceberg-rust`.
- Upstream mergeability is not a constraint, but standard Iceberg compatibility
  is.
- Do not add release or crate-publishing ceremony until deployment requires it.
- GitHub Actions remain disabled. Validation is local and manually invoked.
- Prefer vertical, tested operations over speculative extension frameworks.

## 6. Code boundaries

Work should remain in the existing generic crates:

```text
crates/iceberg                         transaction and Iceberg semantics
crates/catalog/*                       standard catalog implementations
crates/test_utils                      reusable Iceberg fixtures
crates/integration_tests               catalog and object-store integration
```

DataFusion scan/write work belongs in the migrated companion repository.

Do not add product-oriented crate families or a generic `extensions` crate.
Operation-independent types belong in `crates/iceberg`; execution-engine glue
belongs in the appropriate integration crate.

## 7. Transaction lifecycle

### 7.1 Reference-aware snapshot targeting

Every snapshot-producing operation targets a named reference instead of
implicitly using `current_snapshot`:

```rust
pub struct SnapshotTarget {
    pub reference: String,
    pub expected_snapshot_id: Option<i64>,
}
```

`main` is the default reference. The expected snapshot is the optimistic base
for that reference; `None` asserts that the reference does not exist, rather
than referring to an existing empty branch. Permit this for the initial `main`
snapshot; creation of any other branch must be explicit. Never recreate a
branch that disappeared during retry implicitly.

Keep the original planning base separate from the refreshed base of a commit
attempt. Rebase only after validating all changes since the planning base;
callers may instead request a strict expected-head check that fails on movement.

Planning, file-level diffing, validation, retry, and ancestry checks must follow
the target reference's history. They must not substitute the table's globally
current snapshot or another branch. Snapshot-producing operations may advance
branches. Tags are immutable and must be rejected as mutation targets.

### 7.2 Two-stage preparation

Keep the reusable logical plan separate from metadata built against one exact
table state:

```text
PlannedTransaction
    -> apply ordered metadata actions and PlannedSnapshotOperation values
    -> build manifests, manifest lists, snapshots, and standard updates
PreparedTableCommit
    -> Catalog::update_table
```

`PlannedTransaction` owns the shared base and ordered action list described in
Section 7.5. Each snapshot-producing entry is a
`PlannedSnapshotOperation`.

`PlannedSnapshotOperation` contains:

- standard Iceberg operation kind;
- file changes whose immutable files already exist;
- `SnapshotTarget`;
- affected file, partition, or predicate scope;
- validation and retry policy;
- snapshot summary properties supplied by the caller;
- format-version capability requirements.

`PreparedTableCommit` represents the complete ordered transaction and contains:

- the exact base table metadata location and target-reference snapshot;
- ordered standard `TableUpdate` and `TableRequirement` values;
- any snapshots and standard summaries produced by the transaction;
- client-written manifests and manifest lists built for that base;
- known client-staged data, delete, manifest, and manifest-list artifacts,
  classified by content and reuse behavior.

It must not require a prewritten table metadata JSON location. Catalogs differ:

- clients write data files, delete files, manifests, and manifest lists;
- some catalogs accept standard updates and construct or persist the resulting
  table metadata during `Catalog::update_table`;
- a catalog that supports client-staged table metadata may use that capability
  internally, but it is not a requirement of `PreparedTableCommit` or the
  generic `Catalog` contract.

The generic transaction tracks artifacts that it wrote. A catalog owns the
lifecycle and failure reporting for metadata artifacts it creates internally.

Retain the one-call `Transaction::commit` API as a convenience wrapper over
plan, prepare, and publish.

### 7.3 Retry and artifact reuse

Data/delete files and prepared metadata have different lifetimes:

- immutable data and delete files may often be reused after a conflict;
- manifests, manifest lists, snapshot metadata, computed summaries, and commit
  requirements are tied to the exact table state against which they were built;
- after a changed table pointer, prepared metadata is normally abandoned and
  rebuilt from `PlannedSnapshotOperation`;
- if file-level changes since the base do not overlap the operation scope, the
  operation may revalidate and rebuild metadata while reusing compatible files;
- if changes overlap the scope, the caller must completely replan and may need
  to rewrite files.

Distinguish `Committed`, `NotCommitted`, and `CommitStateUnknown` outcomes.
A confirmed conflict may revalidate and prepare a new attempt. A timeout,
connection loss, or cancellation after submission is not proof of failure.
Do not rebuild, resubmit, or delete potentially committed artifacts while the
outcome is unknown. An unchanged reload alone does not prove that an in-flight
request cannot still succeed.

Lost-response recovery uses bounded status checks and the full snapshot identity
checks in Section 7.6. Inspect retained snapshots as well as target ancestry: a
subsequent rollback or branch deletion does not undo a successful publication.
If success is proven, return that outcome without republishing or moving the
branch back. If neither success nor definitive failure can be established,
return `CommitStateUnknown` with recovery information and protected artifacts.
Missing or expired history cannot establish failure. Metadata-only transactions
need catalog evidence of their outcome; snapshot-based recovery does not apply.

### 7.4 Format-version capability validation

Every planned operation declares and validates its required Iceberg
capabilities before writing metadata:

- v1 rejects delete files and sequence-number behavior unavailable in v1;
- equality and position deletes require their supported format-version
  semantics;
- data and delete manifests use the structures required by the table's format
  version;
- v3 writes must implement mandatory row-ID assignment/inheritance and preserve
  row lineage during rewrites; deletion-vector support is gated separately;
  reject any operation lacking mandatory version semantics even if an existing
  writer can serialize its metadata;
- a retry re-runs capability checks if table format version or relevant metadata
  changes.

Capability checks should be centralized and shared by transaction actions,
manifest writers, delete writers, and DataFusion integration. Unsupported
combinations must fail before publication, not produce best-effort metadata.

### 7.5 Multi-action transaction composition

The lifecycle refactor must preserve transactions containing multiple actions.
A transaction has one catalog base loaded at the start of an attempt and an
ordered action list. Actions are applied in declaration order to an evolving
in-memory table state:

```text
catalog base
    -> action 1 updates staged table state
    -> action 2 observes action 1
    -> ...
    -> one PreparedTableCommit with ordered updates and requirements
```

This permits combinations such as schema evolution followed by append. The
append validates and builds against the schema produced by the preceding action,
while the catalog commit remains atomic.

Separate action preconditions checked against staged state from requirements
sent to the catalog, which are checked against the pre-transaction state.
Compile one compatible set of catalog requirements from the shared catalog
base; never concatenate assertions about intermediate staged heads or schemas.
For example, two appends based on head S0 produce S1 then S2, but the catalog
asserts head S0 only. Test the emitted request with a catalog that checks all
requirements before applying updates, including schema evolution plus append.

Rules for snapshot-producing actions:

- zero, one, or multiple snapshot-producing actions may appear in a transaction;
- multiple snapshot actions must initially target the same mutable reference;
- they form an ordered parent/child snapshot chain in action order;
- each later action observes snapshots and metadata updates from earlier actions;
- actions are not silently combined into one snapshot;
- coalescing compatible actions, such as adjacent appends, may be added only as
  an explicit optimization with equivalent operation and summary semantics;
- incompatible targets, requirements, scopes, or format capabilities are
  rejected before publication;
- atomic multi-reference mutation is outside the initial implementation and is
  rejected explicitly.

On retry, reload one new shared catalog base and replay the complete ordered
action list. Reuse decisions are made per action, but requirements and prepared
metadata are rebuilt for the whole transaction so action ordering is preserved.

### 7.6 Snapshot identity and collision handling

Generate each snapshot ID before writing metadata that embeds it. Check the ID
against every snapshot still present in table metadata, not only the target
reference's current history. Repeat the check after reloading metadata during a
retry. A collision requires a new ID and rebuilding every artifact containing
the old ID.

Lost-response recovery must not accept a snapshot solely because its ID
matches. Where available from standard metadata, compare:

- snapshot ID and parent snapshot ID;
- sequence number;
- standard operation kind and summary;
- manifest-list location;
- target reference and its history position when that history is retained;
  later branch movement is handled as described in Section 7.3.

Treat the operation as already committed only when the reloaded snapshot is the
same prepared snapshot under these checks. An ID collision with different
content is a conflict, not successful idempotent recovery. This behavior uses
standard snapshot identity and does not require a proprietary idempotency field.
Check generated IDs against earlier staged snapshots in the same transaction
too. For a multi-snapshot commit, verify the prepared chain and its association
with the atomic publication; recovery must never publish only a suffix of the
transaction. Preserve recovery evidence independently of later branch movement.

## 8. Cross-cutting correctness foundations

### 8.1 Sequence numbers

Sequence-number behavior is a dedicated prerequisite, not an implementation
detail of rewrite or row delta. Centralize and test:

- snapshot sequence-number assignment;
- manifest sequence numbers;
- manifest entry data sequence numbers;
- manifest entry file sequence numbers;
- inheritance when serialized values are absent;
- assignment for newly added data files;
- assignment for equality and position-delete files;
- preservation of logical data age during physical rewrite;
- equality-delete applicability;
- position-delete applicability;
- behavior for v1 tables where v2/v3 sequence semantics do not apply.

Provide one spec-driven applicability function used by scans, validation,
rewrite planning, and tests. Do not duplicate comparison rules across actions.

### 8.2 File-level snapshot diff

Add the snapshot/file-diff utility immediately after the file-change model. For
two snapshots on the target reference's valid ancestry path, return exact
standard metadata for:

- added and removed data files;
- added and removed equality/position-delete files;
- snapshot IDs, sequence values, content types, partition spec IDs, and source
  manifest information needed by callers.

Normalize inherited values when reading results. Handle reference-history cases
explicitly:

- a fast-forward from the expected snapshot to a newer snapshot is diffed and
  passed to operation-specific validation;
- divergent or non-fast-forward history returns a structured non-ancestor
  conflict; do not silently diff from a common ancestor;
- deletion of the target reference during retry is a conflict;
- detectable replacement or recreation of a reference is a conflict unless its
  expected history is proven compatible, subject to the observability limits
  below;
- a base snapshot no longer retained returns a history-expired result and
  requires planning from a new base;
- tags remain invalid mutation targets even when their snapshots are retained.

This utility is reused by conflict validation, overwrite planning, rewrite
verification, tests, and diagnostics. It remains strictly file-level and must
not label file changes as logical row inserts, updates, or deletes.

Expose ordered per-snapshot changes separately from an optional net live-set
diff. Conflict validation consumes the ordered changes: adding a matching file
and then removing it must not erase evidence of a concurrent write. Standard
references have no persistent incarnation ID; deletion and recreation with the
same head cannot always be detected. Promise only checks supported by retained
metadata, and return an unresolved-history result when evidence is insufficient.

### 8.3 Manifest reuse and merging

A manifest is reusable only when all of its entries and inherited semantics
remain valid in the new snapshot. Reuse must consider:

- manifest content type and target format version;
- partition spec identity;
- entry status and live-file state;
- snapshot ID inheritance;
- data and file sequence-number inheritance;
- whether the operation changes or invalidates any entry;
- whether reuse preserves correct delete applicability.

Unchanged file paths alone are not proof that a manifest is reusable.

Manifest merging is a separate, optional optimization with its own size and
entry-count policy. Mutation correctness must not require merging, and merging
must not be hidden inside the generic file-removal path.

### 8.4 Schema, spec, and sort evolution during retry

Before reusing staged data/delete files after metadata evolution, classify the
result explicitly:

```text
ReusableAsWritten
ReusableAfterMetadataRebuild
RequiresFileRewrite
RequiresOperationReplan
```

The decision must consider:

- field IDs, types, requiredness, and defaults in the written file schema;
- equality-delete field IDs;
- whether each recorded partition spec still exists and is valid;
- whether the operation requires the current default partition spec;
- transform changes and how they affect operation scope;
- sort-order requirements promised by the operation or writer;
- format-version changes and newly enabled/disabled capabilities.

Do not assume a staged file is reusable merely because it is immutable and can
still be opened.

### 8.5 Overwrite modes and predicates

Model overwrite modes explicitly:

```rust
pub enum OverwriteMode {
    ExplicitFiles,
    FilterOverwrite,
    DynamicPartitionOverwrite,
}
```

Keep two different predicates for filter overwrite:

- the logical row predicate describing the requested overwrite;
- the conservative conflict-detection predicate produced by projecting the
  logical predicate through every relevant partition spec.

Do not reuse either predicate as the other. Dynamic partition overwrite derives
its scope from incoming file partitions and is not an alias for filter
overwrite. Implementation order remains explicit files first, then filter
overwrite after predicate conflict validation, then dynamic partition overwrite
after partition-replacement semantics are stable.

Inclusive pruning only identifies candidate files. Metadata-only filter
overwrite may remove a file only when strict partition/metrics evaluation
proves every row matches. Partial or unknown matches require row evaluation and
copy-on-write that preserves nonmatching rows, or explicit rejection. Validate
replacement rows against the requested predicate. Across partition specs,
whole-partition replacement likewise requires proof of full coverage; an old
coarse partition overlapping a new finer one cannot be removed wholesale.

### 8.6 Separate rewrite operation families

Use distinct types, APIs, and snapshot-planning paths:

- `RewriteDataFiles` for compaction, reclustering, and data-file replacement;
- `RewriteManifests` for manifest-only organization;
- `RewritePositionDeleteFiles` for position-delete consolidation or remapping.

Do not expose a single ambiguous `RewriteFiles` action. Manifest rewriting is an
optimization over metadata and must not be conflated with data-file compaction.

### 8.7 Snapshot summary ownership

The snapshot producer owns every standard computed summary key, including
added/removed/total file, record, byte, and partition metrics. Caller-supplied
properties may add non-reserved descriptive values but must not replace a
producer-owned key.

Validate caller properties before metadata writing. Reject collisions with
standard or producer-computed keys, even when the supplied value happens to
match. Do not resolve collisions through insertion order or silent overwrite.
For multi-snapshot transactions, validate and compute properties independently
for each snapshot-producing action.

### 8.8 File-location comparison

Preserve every Iceberg file location exactly as supplied when serializing
metadata. Do not normalize schemes, authorities, percent escapes, slash runs,
dot segments, query strings, or object keys in a way that could change object
identity.

Use these comparison rules for duplicate detection and file liveness:

- remote/object-store locations use exact Iceberg location-string equality;
- the original location string remains the value stored and returned;
- for an explicitly local filesystem `FileIO`, a separate comparison key may
  make a path absolute against a fixed configured root; do not collapse `..`
  across a potentially symbolic-link component, since this can change identity;
- local comparison must not resolve symlinks, require the path to exist, or
  rewrite the stored Iceberg location;
- URI-like values that cannot be proven to use the local `FileIO` retain exact
  string semantics.

Centralize this policy so transaction validation, manifest resolution, snapshot
diff, and orphan discovery do not invent different notions of path equality.

### 8.9 Cancellation and resource limits

Planning, file writing, metadata preparation, and distributed result collection
must accept cancellation and enforce configurable hard limits:

- bounded task and object-store I/O concurrency;
- target and maximum manifest entry count and serialized byte size;
- maximum manifest count and manifest-list size per commit;
- maximum data/delete file descriptors and staged artifacts per operation;
- maximum serialized `WriteCommitMessage` size and aggregate coordinator input;
- bounded in-memory buffering while reading manifests or collecting results.

Check cancellation before expensive stages and before catalog publication. Once
catalog success is known, cancellation cannot undo the commit and must return or
recover the committed result. A cancelled or limit-exceeded operation returns
all known partial staged artifacts for caller cleanup and never publishes a
partial selected task set.

Returning an artifact inventory is not authorization to delete it. Distinguish
caller-owned inputs, attempt-owned new files, shared reusable files, committed
files, and files protected by an unknown commit outcome. Cleanup must preserve
files still used by another attempt or retained snapshot. Cancellation by
dropping an async future cannot return an inventory: record created paths in an
operation-owned tracker before I/O so the owner can recover partial writes.

After a catalog update request has been submitted, cancellation must not assume
failure. If the response is unknown, reload and apply the same full snapshot
identity checks as lost-response recovery before allowing any retry.

Hard maximums protect correctness and memory use. Softer target sizes may guide
manifest splitting or task planning but must not be confused with the hard
limits.

### 8.10 Worker descriptors and metrics validation

Treat distributed file descriptors and writer metrics as untrusted coordinator
inputs. Apply serialization-size and collection-count limits before allocating
large structures. Validate:

- required descriptors and metrics for the file content and format version;
- maximum metric-map entries and maximum encoded bound size per field;
- nonnegative and internally consistent record, value, null, NaN, byte, and
  split counts where present;
- lower/upper bound shape and field IDs without assuming bounds are complete;
- file size, partition/spec identity, content type, sort promises, and referenced
  data-file fields;
- agreement between aggregate task metrics and the selected descriptors.

Default validation should inspect descriptors and may use inexpensive object
metadata such as existence and size. It must not reread every Parquet footer or
scan every output file. A caller-selectable strict mode may verify file footers,
checksums, and complete metrics when the additional I/O is warranted.

## 9. Delivery milestones

### Dependency and release gates

Deliver A first. Develop B and C together; sequence and capability tests from C
must pass before B is considered complete. D precedes public mutations E/F/H/J;
G requires E/F and scans supporting every delete type accepted by the operation.
I follows H and delete-applicability fixtures from C; L follows the applicable
mutation foundations. K may begin after A and integrate operations as they land.
Artifact ownership and unknown-outcome protection belong in A; destructive
maintenance remains gated on M. Each milestone must name its supported
format/operation matrix and explicitly reject unsupported combinations.

Paths below are relative to the repository root; within core implementation
steps, `transaction/`, `spec/`, `expr/`, and `arrow/` mean subdirectories of
`crates/iceberg/src/`. DataFusion paths identify the former
`crates/integrations/datafusion/src/` layout and must be mapped to the companion
repository before G/K or integration portions of other milestones begin. New module
names are proposed implementation locations, not claims that APIs already exist.
The validation steps are required future work, not results of this plan review.

### Milestone A: lifecycle refactor with unchanged fast append

#### Work

1. Document current ownership, side effects, and retry behavior for
   `Transaction`, `FastAppendAction`, `SnapshotProducer`, `ActionCommit`,
   `TableCommit`, and each standard catalog update path.
2. Introduce `SnapshotTarget`, an ordered planned transaction containing
   `PlannedSnapshotOperation` values, and `PreparedTableCommit` without adding
   removed-file support.
3. Separate preparation from publication and retain one-call commit.
4. Make fast append target a named branch and require its expected snapshot.
5. Preserve current valid fast-append manifests, summaries, table contents, and
   public API behavior.
6. Rebuild prepared metadata after a relevant catalog conflict rather than
   resubmitting it unchanged.
7. Preserve ordered multi-action composition, including schema evolution
   followed by append and multiple compatible snapshot actions.
8. Keep table metadata JSON creation catalog-managed where required; do not make
   a prewritten metadata location part of the generic prepared-commit contract.
9. Reject caller summary-property collisions with producer-owned keys.
10. Check snapshot IDs against all retained snapshot metadata and use the full
    identity checks in Section 7.6 for lost-response recovery.

#### Implementation details

1. Refactor `transaction/mod.rs`, `action.rs`, and `snapshot.rs` in place.
   Review the draft stateful-transaction RFC and reuse its distinction between
   transaction, action-execution, and attempt lifetimes; do not assume its
   proposed stateful interface has already landed.
   Keep existing action builders and `ApplyTransactionAction` as adapters to
   the new lifecycle. Introduce internal `plan.rs`, `prepare.rs`, and
   `validation.rs` modules only as responsibilities outgrow the existing files.
2. Give preparation an explicit context containing the original planning base,
   refreshed catalog base, evolving staged table, target, limits, cancellation,
   and artifact tracker. Resolve parents from the target branch. Allocate IDs
   before output creation and register paths before writing bytes.
3. Compile catalog requirements separately from staged action preconditions.
   Build `TableCommit` only at publication. Store prepared snapshot identities
   and artifact ownership in a recovery handle retained after ambiguous errors;
   keep the existing `Transaction::commit -> Result<Table>` wrapper while
   designing an additive API for detailed outcomes.
4. Replace blanket retry-by-error behavior with outcome classification at the
   catalog boundary. Audit each catalog adapter before classifying a response
   as definitive failure; preserve unknown outcomes through error conversion.

#### Validation steps

1. Extend unit tests beside `transaction/mod.rs` and `append.rs` with a fake
   catalog that checks all requirements against its stored base before applying
   updates. Assert zero catalog updates during prepare, then exactly one atomic
   publication for schema-plus-append and two-append transactions.
2. Use barriers to commit a competing writer between prepare and publish.
   Assert safe rebase preserves its files, emits new metadata, and reuses only
   permitted immutable files. Force a snapshot-ID collision deterministically.
3. Simulate success with a lost response, an in-flight delayed success, and an
   unavailable status check. Assert no second append, no premature deletion,
   and a recoverable unknown outcome where evidence is insufficient.
4. Run the transaction unit-test command in Section 11, then the REST conflict
   integration test. Compare decoded fast-append manifests and summaries with
   baseline fixtures, ignoring only generated IDs, timestamps, and locations.

#### Exit criteria

- Preparation alone does not advance the catalog pointer.
- Valid fast-append output remains equivalent where deterministic values permit;
  existing unsupported format behavior is rejected under Section 7.4.
- Schema evolution followed by append commits atomically in action order.
- Compatible snapshot actions form an explicit parent/child chain; incompatible
  composition is rejected before publication.
- Tags cannot be mutated.
- Retry and lost-response tests never publish duplicate append rows.
- Client-written and catalog-managed staged artifacts have explicit owners after
  failure.

### Milestone B: generic file-change model

#### Work

1. Add operation-neutral collections for added/removed data files and
   added/removed delete files.
2. Validate content type, partition spec, partition value shape, duplicate
   paths, contradictory changes, and file liveness using the centralized
   location-comparison rules in Section 8.8.
3. Resolve live manifest entries from the selected parent snapshot.
4. Emit deleted entries with their original data/file sequence values and the
   correct deletion snapshot.
5. Reuse manifests only under the rules in Section 8.3.
6. Extend standard snapshot-summary collection for added/removed data/delete
   files, records, bytes, totals, and changed partitions.
7. Keep public overwrite, rewrite, and row-delta APIs private until this kernel
   passes its fixture and interoperability tests.

#### Implementation details

1. Add an internal `FileChanges` value with four explicit collections and a
   resolved-file record containing the original manifest, spec ID, snapshot ID,
   and normalized data/file sequence values. Resolve removals from the parent
   snapshot; never trust a caller's replacement descriptor for an existing file.
2. Extend `SnapshotProduceOperation` and manifest processing in `snapshot.rs`.
   Index requested removals by the Section 8.8 comparison key, stream parent
   manifests, and track whether every removal matched exactly one live entry.
   Reject nonexistent removals and duplicate or contradictory additions.
3. Group changed entries by content and spec ID; write bounded manifests using
   `spec/manifest/writer.rs`. Preserve eligible unchanged manifests. Recompute
   summaries through `spec/snapshot_summary.rs` with checked arithmetic.

#### Validation steps

1. Use small fixtures with two data manifests, a delete manifest, and two specs.
   Exercise each change category alone and in combination; decode output with
   the existing readers and compare the exact live-file set and entry fields.
2. Assert an unaffected manifest keeps its location while a changed manifest
   is rewritten; test inherited fields, fully removed manifests, and old deleted
   entries. Confirm retained snapshots still read their original file sets.
3. Reject missing/duplicate paths, content/spec mismatches, and summary overflow
   before publication. Exercise count/byte limits at the boundary and one over.
   Run transaction, manifest, and snapshot-summary unit tests.

#### Exit criteria

- The internal kernel represents all four file-change categories.
- Added and deleted entries round-trip through standard manifest readers.
- Unaffected manifests are reused only when inherited semantics remain valid.
- Correctness does not depend on manifest merging.

### Milestone C: sequence-number and snapshot-diff foundation

#### Work

1. Implement and test every sequence-number rule in Section 8.1 across
   supported format versions.
2. Add shared data/delete applicability evaluation.
3. Add the reference-aware file-level snapshot-diff API from Section 8.2.
4. Normalize inherited snapshot, data-sequence, and file-sequence values at the
   API boundary.
5. Add format-version capability validation shared by all planned operations.
6. Add fixtures for mixed data/delete manifests, inherited values, snapshot
   ancestry, and expired-history failures.
7. Cover fast-forward, divergence, reference deletion/replacement, and a base
   snapshot expiring during retry.

#### Implementation details

1. Extract sequence resolution and applicability from manifest readers,
   `delete_file_index.rs`, and `scan/context.rs` into shared internal helpers.
   Pass spec ID explicitly through delete-index keys and lookups; partition
   values alone are not a sufficient identity. Keep scan and mutation consumers
   on the same applicability implementation.
2. Add an internal snapshot-diff module that walks parent IDs from current head
   to the original base, reverses that path, and streams each snapshot's changes
   in order. Resolve entry inheritance before returning records. Expose a
   separate net-diff adapter without using it for conflict detection.
3. Centralize a capability matrix keyed by format version, operation, and
   encountered content/features. Invoke it before metadata I/O and on retry.
   Distinguish unavailable history from an empty initial table.

#### Validation steps

1. Add table-driven sequence fixtures for older/equal/newer data ages, absent
   inherited values, global equality deletes, multiple specs with equal
   partition tuples, and v1 behavior. Compare scan tasks and mutation validator
   applicability results for the same fixture.
2. Test a three-snapshot history where a file is added then removed: ordered
   diff must retain both changes while net diff may be empty. Cover divergent
   branches, expired intermediate history, and the first snapshot of a table.
3. For each capability-matrix cell, assert valid output or a specific rejection
   with zero metadata writes. Run scan, delete-index, and manifest unit tests.

#### Exit criteria

- Sequence behavior has a centralized, independently tested implementation.
- Diff results are exact at the file level and make no logical-row claims.
- v1/v2/v3 unsupported combinations fail before metadata is written.

### Milestone D: reusable conflict validation

#### Work

1. Validate against the target reference's base and current histories.
2. Use standard catalog requirements for UUID, schema/spec/sort IDs, allocation
   counters where needed, and expected reference snapshot. Metadata-location
   compare-and-swap is catalog-internal; do not invent a portable requirement
   absent from the protocol. Document which dependencies are protected by each
   assertion and which are revalidated locally.
3. Add validators for required-file liveness, new data, new deletes, deletes
   applying to rewritten files, and data added to replaced partitions.
4. Represent affected scope as the entire table, explicit files, partitions, or
   a bound predicate.
5. Use the file-level snapshot diff and Iceberg projection/pruning to detect
   overlaps conservatively, including intermediate changes that cancel in a
   net diff. Bind predicates by field ID; missing metrics mean possible overlap.
6. Return structured conflict diagnostics identifying validator, reference,
   snapshot, file, partition, or predicate.
7. Apply Section 8.4 before reusing staged files after schema/spec/sort
   evolution.

#### Implementation details

1. Implement validators as functions of the original plan, refreshed metadata,
   and ordered diff. Return a structured reason plus the reuse/replan decision
   from Section 8.4; do not mutate the plan's original base during validation.
2. Reuse `expr/visitors/inclusive_projection.rs`, `strict_projection.rs`, and
   metrics evaluators. Bind by schema field ID and cache projections per spec.
   Treat missing bounds as possible overlap; strict proof remains a separate
   decision from inclusive candidate selection.
3. Have each action declare required-file and conflict-scope dependencies.
   Add the resulting base requirements to the compiler from A so a head change
   after validation forces another attempt. Document the isolation policy for
   each operation rather than enabling every validator indiscriminately.

#### Validation steps

1. Drive two writers from one base with barriers, covering disjoint and
   overlapping file/partition/predicate scopes. Assert either the union of
   safe effects or a structured conflict with the catalog left unchanged.
2. Race a commit after validation but before publication; verify requirements
   force revalidation. Include a matching add-then-remove history and missing
   file metrics so net diffs or pruning cannot hide conflicts.
3. Evolve schema, partition spec, sort order, and format version independently;
   assert the documented reuse decision and retained field IDs. Run expression
   evaluator tests, transaction tests, and the REST conflict integration test.

#### Exit criteria

- Concurrent appends succeed when safe.
- File/partition operations conflict only with conservatively overlapping
  changes.
- No retry silently discards another writer's files.
- Metadata evolution causes explicit reuse, rewrite, or replan decisions.

### Milestone E: `DeleteFiles` and explicit-file `OverwriteFiles`

#### Work

1. Add a standalone `DeleteFiles` action that removes complete live data files
   with no replacements and emits standard delete operation semantics.
2. Validate related delete files: retain shared equality deletes, reject unsafe
   applicability changes, and remove position-delete files only when explicitly
   included and proven obsolete.
3. Require callers to use `DeleteFiles` rather than representing complete-file
   deletion as an empty overwrite.
4. Expose `OverwriteMode::ExplicitFiles` with exact removed and replacement data
   files and require replacement output.
5. Emit standard overwrite snapshots and correct manifest entries/summaries.
6. Validate removed-file liveness and concurrent data/delete conflicts.
7. Keep logical overwrite scope separate from any projected conflict scope.
8. Add `FilterOverwrite` only after explicit-file behavior is stable; bind its
   logical predicate and project a separate conservative predicate through all
   relevant specs.
9. Defer `DynamicPartitionOverwrite` to the partition-replacement foundation in
   Milestone F.

#### Implementation details

1. Add distinct `delete_files.rs` and `overwrite.rs` action modules under
   `transaction/`, following `append.rs` builder conventions. Resolve explicit
   removals through B and assemble operation summaries in the producer.
2. Keep explicit-file overwrite independent of a row engine. Require a declared
   logical conflict scope for predicate/isolation guarantees that cannot be
   inferred from the removed paths. Validate associated delete files through C.
3. For filter overwrite, classify candidates as fully matching, disjoint, or
   requiring row evaluation. Return a structured rewrite-required result for
   partial/unknown matches until G can execute them; never silently delete a
   whole candidate file. Define empty-input and no-op behavior in API docs.

#### Validation steps

1. Write real Parquet files containing duplicate values and nulls. Delete one
   whole file, then replace another; compare exact row multisets and untouched
   file identities after each commit, including time travel to the old snapshot.
2. Test a file containing both matching and nonmatching rows: metadata-only
   overwrite must reject it or require G. Test out-of-predicate replacement
   output and missing removal files before any catalog update.
3. Cover shared equality deletes, position deletes spanning retained files,
   and overlapping concurrent writes. Run action tests and add real-file
   mutation cases under `crates/integration_tests/tests/`.

#### Exit criteria

- Complete live data files can be removed with a standard delete snapshot.
- Explicit files are replaced atomically.
- Concurrent changes outside the scope survive.
- Conflicting changes inside the scope fail with actionable diagnostics.
- External readers observe standard overwrite semantics.

### Milestone F: `ReplacePartitions`

#### Work

1. Derive target partitions from replacement files while retaining partition
   spec identity.
2. Find live data and applicable delete files across every relevant spec.
3. Support explicit empty-partition replacement.
4. Reject concurrent data/delete changes only in conservatively overlapping
   partitions.
5. Implement `DynamicPartitionOverwrite` on this foundation rather than as
   filter overwrite.
6. Add dedicated partition-spec-evolution tests for:
   - files written under multiple partition spec IDs;
   - transform evolution;
   - overlapping logical regions represented differently by different specs;
   - empty-partition replacement;
   - concurrent writes under old and new specs.

Partition identity in internal maps and validation keys must always include the
applicable spec ID. Cross-spec overlap is a separate semantic calculation, not
partition-key equality.

#### Implementation details

1. Add `replace_partitions.rs` with explicit `(spec_id, partition)` targets and
   replacement files. Represent removal of an empty-output partition through
   explicit targets, since no incoming file can identify it.
2. Project target regions through each live spec using D. Remove whole files
   only when coverage is proven; route partial coverage to copy-on-write or
   reject it. Do not compare partition tuples from different specs directly.
3. Implement dynamic partition overwrite as target derivation followed by the
   same planner. Preserve data and delete files outside its logical region.

#### Validation steps

1. Replace one of two partitions and explicitly empty the other. Check exact
   output rows, retained snapshots, and untouched file locations.
2. Evolve day partitioning to hour partitioning. Replacing one hour must retain
   the other hours in an old day file through rewriting, or explicitly reject
   the plan. Repeat with bucket/truncate evolution and null partition values.
3. Interleave old-spec and new-spec concurrent writes inside and outside the
   target region; verify conservative conflicts and preservation of disjoint
   writes. Run partition/projection and transaction tests plus real-file cases.

#### Exit criteria

- Complete partitions can be replaced or removed.
- Non-target partitions remain unchanged.
- Spec evolution cannot cause live files to be missed.
- Concurrent old/new-spec writes are conservatively validated.

### Milestone G: DataFusion copy-on-write execution

Companion-repository milestone. First evaluate an adapter over the existing
core `cow_rewrite` primitive. Its source-schema/spec preservation and lack of
sorting do not satisfy every partition-replacement or reclustering use case;
add explicit supported modes before using it for those operations.

#### Work

1. Plan affected files from explicit inputs or a bound filter using Iceberg
   pruning.
2. Build DataFusion scans that apply all applicable standard delete files.
3. Accept a caller-supplied row transformation and produce complete replacement
   file contents.
4. Reuse standard partitioning, Parquet metrics, and sort-order writers.
5. Return a `PlannedSnapshotOperation`; do not publish inside the file writer.
6. Validate written-file reuse after concurrent schema/spec/sort evolution.

#### Implementation details

1. Extend `physical_plan/scan.rs` to consume a pinned snapshot and explicit
   affected-file selection. Reuse Iceberg delete filtering; reject unsupported
   delete types before starting output writers. Carry all fields needed to
   reconstruct full replacement rows, even for narrow caller projections.
2. Add copy-on-write planning beside `physical_plan/write.rs`. Evaluate the row
   predicate and retain unmatched rows from affected files; send resulting full
   rows through existing projection, repartition, sort, and `TaskWriter` paths.
   Treat only SQL TRUE as a match, retaining FALSE and UNKNOWN rows.
3. Return descriptors and tracked artifacts to core mutation planning. Keep
   publication in `physical_plan/commit.rs` or the caller's transaction, and
   preserve existing INSERT behavior and count results.

#### Validation steps

1. Extend `tests/integration_datafusion_test.rs` with partial-file updates,
   null predicates, duplicate rows, nested fields, and applicable deletes.
   Compare complete row multisets with an independently computed expected set.
2. Use multiple input partitions and an empty output task; assert a single
   atomic publication. Cancel during scan/write and verify partial output is
   tracked while catalog state stays unchanged.
3. Run DataFusion unit and integration targets, including existing INSERT
   cases. Inspect the physical plan to confirm bulk rows are not coalesced at
   the commit coordinator, and use I/O counters to check bounded concurrency.

#### Exit criteria

- DataFusion can produce an explicit-file overwrite and full partition
  replacement without owning catalog semantics.
- A metadata-only retry safely reuses files only after capability checks.
- Core Iceberg remains independent of DataFusion.

### Milestone H: `RewriteDataFiles` without applicable deletes

#### Work

1. Add the distinct `RewriteDataFiles` action with standard replace semantics.
2. Initially reject groups with any applicable deletes, including deletion
   vectors, both at planning time and during commit validation.
3. Require caller assertions for record count, partition/spec coverage, optional
   logical checksum, and optional key-range coverage.
4. Add a compaction/reclustering planner based on partition, spec, size, bounds,
   sort order, and age.
5. Execute independent rewrite groups with DataFusion.
6. Verify disjoint removed-file sets before combining groups.

#### Implementation details

1. Add `rewrite_data_files.rs` as a core action and a separate DataFusion group
   planner. Use stable grouping by spec/partition with configured byte/file
   limits, then optional ordering constraints; reject overlapping source sets.
2. Preserve a group's original validation base and source descriptors through
   execution. Check for applicable deletes again during commit, including
   deletes introduced while output files were being written.
3. Validate record totals and declared coverage as cheap checks. Document that
   caller assertions or matching counts cannot prove logical equivalence;
   engine-owned compaction must preserve every input row by construction.

#### Validation steps

1. Compact several small files with repeated values into fewer files and
   recluster them. Compare full row multisets, record totals, declared ordering,
   standard replace summaries, and historical snapshot readability.
2. Submit overlapping groups and inject a new applicable delete during writing;
   both must fail without publishing replacements. A disjoint concurrent append
   must survive successful revalidation.
3. Run core rewrite tests and DataFusion real-file cases; record input/output
   file counts and bytes to demonstrate the configured planner policy.

#### Exit criteria

- Data files can be compacted or reclustered without logical content changes.
- The snapshot operation is standard `replace`.
- Files with applicable deletes are rejected until Milestone I.

### Milestone I: `RewriteDataFiles` with delete reconciliation

#### Work

1. Classify applicable equality deletes, position deletes, and deletion vectors
   only where their format-version support is complete.
2. Apply position deletes while rewriting data and retire obsolete position
   delete files.
3. Define whether equality deletes are materialized or retained for each
   rewrite group, and assign output data sequence numbers accordingly. Do not
   merge files with different delete applicability under one sequence number
   unless equivalence is established; otherwise split the group or reject it.
4. Determine which delete files remain valid, require separate rewrite, or
   become obsolete.
5. Verify logical equivalence with counts/checksums and exact test comparisons.
6. Ensure physical rewrites never report logical row changes.

#### Implementation details

1. Extend the rewrite planner with per-source delete applicability from C.
   Choose materialization or retention of equality deletes explicitly, deriving
   the output sequence policy before writing; split incompatible groups.
2. Reuse `arrow/delete_file_loader.rs`, `arrow/delete_filter.rs`, and positional
   delete readers. Apply old position deletes before changing physical row
   positions. Remove a shared delete file only after checking every remaining
   referenced data file, not merely the rewritten subset.
3. Revalidate concurrent deletes before publication. If they change which rows
   output should contain and cannot be reconciled safely, return a replan result
   and retain the output inventory for controlled cleanup.

#### Validation steps

1. Create data files with different data sequence numbers and equality deletes
   affecting only some of them. Rewrite together and separately; assert exact
   logical contents and no resurrected or newly deleted rows.
2. Cover position deletes spanning rewritten and retained files, duplicate
   delete records, and a new delete committed during the rewrite. Verify shared
   delete files remain while any live target still needs them.
3. Run scan/delete-filter regression tests and real-file rewrite cases. Compare
   before/after rows with an external reader supporting the tested delete type;
   test unsupported deletion-vector combinations as explicit rejections.

#### Exit criteria

- Rewrites do not resurrect deleted rows or remove live rows.
- Equality and position-delete applicability remains correct.
- External readers return identical logical contents before and after rewrite.

### Milestone J: `RowDelta`

#### Work

1. Add standard row-delta actions for added data and equality/position-delete
   files.
2. Validate delete content, equality field IDs, referenced data files, specs,
   metrics, format capabilities, and sequence assignments.
3. Produce correct data and delete manifests.
4. Detect concurrent data that may match equality deletes.
5. Detect concurrent deletes or rewrites affecting referenced files.
6. Document and test supported snapshot and serializable isolation guarantees.
7. Add generic DataFusion equality/position-delete writers when callers supply
   the necessary keys or physical positions.

#### Implementation details

1. Add `row_delta.rs` using B's combined data/delete changes, C's sequence rules,
   and D's operation-specific validation. Define a documented default isolation
   policy and explicit stronger-policy options before exposing the builder.
2. Reuse `writer/base_writer/equality_delete_writer.rs`,
   `position_delete_writer.rs`, and `position_delete_input.rs`. Extend supported
   format checks and transaction integration, checking referenced paths, nonnegative
   physical positions, ordering requirements, and descriptor content.
3. Preserve physical source positions through filtering for position-delete
   generation; post-filter batch offsets are not source file positions. Keep
   the generic writer usable without DataFusion and add integration adapters
   only where callers supply sufficient keys or positions.

#### Validation steps

1. Test data-only, delete-only, and mixed deltas, including same-commit data and
   deletes, composite equality keys, null keys, and physically positioned rows.
   Compare scans with independently computed expected rows.
2. Reject invalid equality IDs, wrong delete content, and invalid positions.
   Race equality deltas with matching appends and position deltas with rewrites;
   assert outcomes separately for each advertised isolation policy.
3. Run equality-writer, delete-index, scan, and row-delta tests, then external
   reader checks for each supported format/delete combination.

#### Exit criteria

- Row deltas produce valid standard snapshots for supported format versions.
- Concurrent changes follow documented isolation semantics.
- Supported external readers return correct contents.

### Milestone K: versioned distributed DataFusion commit messages

Companion-repository milestone; record the migrated writer/commit paths and
pinned dependency before implementing the codec or running integration checks.

#### Protocol

Conceptual in-process payload (the wire DTO is defined separately):

```rust
pub struct WriteCommitMessage {
    pub version: u16,
    pub write_id: Uuid,
    pub table_uuid: Uuid,
    pub task_id: u32,
    pub attempt_id: u32,
    pub data_files: Vec<DataFile>,
    pub delete_files: Vec<DataFile>,
    pub staged_artifacts: Vec<StagedArtifact>,
    pub metrics: WriterMetrics,
}
```

The message must be compact, serializable, versioned independently of the
in-process writer, and reject unknown incompatible versions. Decode and validate
its configured maximum serialized size before accepting descriptor collections.
Use a stable wire DTO with explicit fields and compatibility rules rather than
exposing the incidental serde layout of internal `DataFile` types. Bind each
message to a coordinator-owned write context: table UUID, target branch,
planning base, schema/spec IDs, and the expected logical task set. `write_id`
is transport context and must not become a custom table metadata field.

#### Work

1. Return one commit message per logical task attempt.
2. Make physical filenames unique per attempt.
3. Let the execution coordinator finalize exactly one successful attempt per
   expected logical task; freeze that selection before preparation. An attempt
   number alone does not establish success, and late results cannot change it.
4. Accept an identical redelivery of the same task/attempt idempotently; reject
   different payloads for that identity, mixed fragments, missing tasks, and
   conflicting file ownership. Multiple attempts may be received for a task,
   but only its selected attempt contributes files and metrics. Empty tasks
   send an explicit successful message; an empty write has a defined task set.
5. Treat messages as untrusted and validate schema, partition/spec values, sort
   order, file identity, required metrics, internal metric consistency, and
   staged-artifact classification under Section 8.10.
6. Aggregate selected messages into planned append, complete-file delete,
   overwrite, partition-replacement, data-rewrite, or row-delta operations.
7. Keep bulk row data distributed; collect only compact commit messages at the
   coordinator.
8. Enforce per-message, per-task, and aggregate descriptor/artifact limits with
   bounded coordinator memory.
9. Propagate cancellation to workers, stop accepting new results, and return
   partial task artifacts for cleanup without publishing an incomplete task set.
10. Offer strict footer/checksum validation as an opt-in mode rather than the
    default coordinator path.

#### Implementation details

1. Add a versioned commit-message codec beside DataFusion's existing writer and
   commit operators. Use a bounded envelope decoder followed by bounded
   collections; reject over-limit lengths before allocating their payloads.
   Specify integer widths, optional fields, and unknown-field/version behavior.
2. Adapt `TaskWriter::close` results into messages with write/table/task/attempt
   identity. Build the expected task set from finalized execution planning,
   not from whichever messages happen to arrive.
3. Implement coordinator states for collecting, selected, preparing, submitted,
   and completed/unknown. Freeze winners before preparation, account limits
   across all received attempts, and track loser artifacts separately. Limit
   concurrency and preserve cancellation and outcome rules from A.

#### Validation steps

1. Add checked-in wire fixtures for valid, truncated, incompatible-version, and
   over-limit messages. Test exact-limit acceptance and limit-plus-one rejection
   before allocation using decoder counters.
2. Permute message arrival and redelivery for a fixed scheduler winner set.
   Assert identical selected descriptors, counts, and commit contents. Include
   differing duplicate payloads, absent tasks, empty tasks, and wrong contexts.
3. Inject failures before and after selection and submission; require either
   no publication, one complete publication, or an explicit unknown outcome.
   Measure peak descriptor buffering and I/O counters with many tasks; strict
   mode may read footers, while default mode must not reread every file.

#### Exit criteria

- Task retries cannot duplicate logical output.
- Mixed attempt output is rejected deterministically.
- Protocol compatibility is covered by serialized fixtures.
- Every supported mutation can consume selected commit messages.

### Milestone L: `RewriteManifests` and delete-file maintenance

#### Work

1. Add a distinct `RewriteManifests` action with entry-count and byte-size
   policies.
2. Preserve the live file set, partition specs, adding snapshot IDs, and
   resolved sequence semantics. Rewritten manifests encode retained live files
   as existing entries and may omit historical deleted entries; do not copy
   stale added/deleted statuses or unresolved inheritance blindly.
3. Keep manifest merging optional and independent of mutation correctness.
4. Add a distinct `RewritePositionDeleteFiles` action for consolidation and
   safe remapping.
5. Use `RewriteDataFiles` when delete application requires rewriting data;
   never hide data-file changes inside manifest rewrite.
6. Add delete-file selection policies based on size, count, target files, and
   applicability.

#### Implementation details

1. Add separate manifest and position-delete rewrite action modules. Group
   manifest entries by content/spec, resolve inheritance first, and stream
   bounded output using existing manifest writers. Never read data rows for a
   manifest-only operation.
2. Consolidate position deletes only within compatible applicability groups,
   preserving referenced paths and positions. Deduplicate entries as allowed
   by the format and recalculate metrics. Remapping requires an explicit,
   verified old-to-new physical position mapping; otherwise reject it or use I.
3. Validate liveness of rewritten inputs on retry and preserve unrelated
   concurrent manifests and delete files.

#### Validation steps

1. Compare normalized live descriptors and delete applicability before and
   after manifest rewrite. Include mixed entry statuses and inherited fields;
   assert zero data-file reads/writes using the storage recorder.
2. Consolidate deletes spanning several files and sequence ages; compare exact
   query results and decoded referenced positions. Reject incompatible remaps.
3. Race both rewrite families with append/removal operations. Run manifest,
   delete-reader, and maintenance action tests plus external-reader checks.

#### Exit criteria

- Manifest-only rewrite changes no live file set or delete applicability.
- Position-delete maintenance remains a distinct, inspectable operation.
- Each rewrite family has unambiguous API and tests.

### Milestone M: expiration, orphan discovery, and recovery

#### Referenced-file enumeration

Reachability must include:

- every snapshot still present in current table metadata, including snapshots
  not reachable from the current branch head;
- snapshots protected by any standard reference or retention rule;
- every branch and tag;
- current and retained previous table metadata JSON files;
- manifest lists;
- data and delete manifests;
- data files;
- equality and position-delete files and supported deletion vectors;
- statistics and partition-statistics files;
- caller-provided staged-artifact exclusions for active planned/prepared work.

#### Work

1. Enumerate referenced files without relying only on the current snapshot or
   current branch ancestry.
2. Discover unreferenced artifacts but require a configurable age threshold
   before deletion.
3. Never infer safety solely from a filename prefix or missing current-snapshot
   reference.
4. Integrate prepared-operation exclusions so active files are protected.
5. Preserve standard branch, tag, snapshot-expiration, and metadata-retention
   semantics.
6. Add dry-run output explaining why each candidate is retained or removable.
7. Add failure recovery tests for partially written metadata stacks and expired
   histories.
8. Let snapshot expiration determine when a snapshot is removed from the
   retained set and ceases protecting its referenced files; orphan discovery
   must not independently reinterpret snapshot retention.
9. Commit snapshot expiration atomically through the catalog before deleting
   files. A conflict or unknown outcome blocks deletion. Recompute reachability
   from refreshed metadata before executing a cleanup plan and stop on an
   incomplete inventory or unsupported referenced-file type.
10. Limit deletion to explicit caller-owned storage roots. A file location in
    a descriptor is not proof of ownership; shared files and cross-table reuse
    require exclusions or a complete ownership inventory.
11. Document the concurrency contract: the age threshold must exceed the
    maximum staging/retry window, with active and unknown-outcome artifacts
    excluded. A refresh alone cannot close the race with an uncoordinated
    writer publishing an old file. When these conditions cannot be assured,
    return discovery/dry-run results only. Include files referenced by previous
    metadata that the chosen retention policy promises to keep readable.

#### Implementation details

1. Extend the existing `transaction/expire_snapshots.rs` metadata action rather
   than introducing a second retention algorithm. Keep physical deletion in a
   separate maintenance layer using the reachability inventory above.
2. Build a streaming inventory of protected paths with retention reasons.
   `Storage` currently has no object-listing method: initially accept a
   caller-supplied candidate stream with location and modification time, or add
   a separately reviewed listing capability with bounded backend pagination.
   Missing timestamps are not grounds for deletion.
3. Produce a reviewable cleanup plan containing the observed table identity,
   metadata base, cutoff, exclusions, and candidate reasons. Refresh and
   revalidate before execution; use per-object deletion with bounded concurrency
   and report partial failures. Never implement reachability cleanup through
   `delete_prefix`, which bypasses candidate-level protection.

#### Validation steps

1. Build a fixture with two branches, a tag, retained detached snapshots,
   statistics, previous metadata, active staged files, and unknown-outcome
   files. Assert the exact protected and candidate sets in dry-run output.
2. Inject commit conflicts, inventory read failures, missing modification
   times, and new references between discovery and deletion. Assert zero unsafe
   deletions; test age cutoffs at equality and on either side using a fake clock.
3. Fail a subset of object deletions, rerun cleanup, and verify idempotent
   handling of already absent objects and useful per-path failure reporting.
   Run existing expiration tests and new maintenance tests on local storage
   and RustFS, restricted to isolated test-owned prefixes.

#### Exit criteria

- Under the documented ownership and writer-coordination contract, destructive
  cleanup cannot select retained, active staged, or unknown-outcome files.
- Age thresholds are mandatory for destructive cleanup.
- Snapshot expiration preserves all standard references and retention rules.
- Dry-run and failure recovery are deterministic and diagnosable.

## 10. Failure-injection matrix

Provide explicit test hooks rather than relying only on incidental I/O errors.
Inject failures:

1. after data/delete-file writing;
2. after manifest writing;
3. after manifest-list writing;
4. after metadata JSON writing when that stage is observable in the catalog
   implementation;
5. immediately before catalog update;
6. during catalog update;
7. after catalog success but before the client receives the response;
8. during conflict retry and metadata rebuilding;
9. after an unknown commit outcome, while the original request remains in
   flight, and after a stale reload;
10. between cleanup discovery, expiration publication, and physical deletion.

For each point, assert catalog visibility, target-reference state, reachable and
unreachable artifacts, retry behavior, and whether files or metadata may be
reused. The lost-response-after-success test is mandatory for append and must
prove that retry does not create a second snapshot for the same prepared
append.

Catalog implementations that construct or persist table metadata entirely
inside `Catalog::update_table` inject metadata-write failures within that call.
The generic client test harness must not require a client-visible metadata JSON
stage or assume the client owns catalog-created orphan metadata.

Failure hooks should be deterministic test interfaces around lifecycle stages,
not production behavior controlled by application-specific state.

### Test harness implementation

1. Start with private test helpers beside transaction tests; move helpers into
   `crates/test_utils` only once multiple crates need them. Use deterministic
   clock and snapshot-ID providers, plus barriers at prepare, validation,
   publication, and response delivery. Avoid timing-dependent sleeps.
2. Implement a recording catalog with independently controlled stored metadata
   and responses. Check requirements against the stored base, apply updates
   atomically, and support a stale load, definitive conflict, delayed success,
   lost response, and unavailable status lookup. Record every submitted request.
3. Wrap the existing `Storage`/`StorageFactory` boundary to record created,
   opened, closed, and deleted paths and bytes in flight. Inject write/close/read
   failures by operation count or lifecycle barrier, not by production flags.
   Include failure after object creation but before the caller records success.
4. Compare each run with an independent expected row multiset, normalized live
   descriptor set, catalog snapshot graph, and artifact ownership ledger.
   After recovery, verify catalog publication count and every protected path.
   A test that only checks an error string is insufficient.
5. Run the same lifecycle cases through REST adapter request/response fixtures
   and representative catalog-managed metadata paths. Keep real object-store
   cases separate from pure unit cases so they are runnable independently.

## 11. Validation strategy without hosted CI

The absence of GitHub Actions does not relax correctness requirements. Run the
smallest relevant local suite during development and the full local gate at
milestone exits.

### Per-change checks

Run from the repository root with the toolchain selected by
`rust-toolchain.toml`. Use `--locked` to avoid incidental dependency updates.
These commands are validation instructions for implementation changes; they do
not imply that implementation or tests have been completed by editing this plan.

```bash
cargo fmt --all -- --check
cargo clippy --locked -p iceberg --all-targets -- -D warnings
cargo test --locked -p iceberg --lib
```

Add relevant standard catalog and integration crates when they change.
DataFusion checks must run in the companion repository with its own lockfile
and documented package/test names; the removed workspace targets cannot run
here and must not be recorded as passes.

### Focused development commands

The following filters target existing modules; add the proposed action tests
under their corresponding modules as each milestone lands. Check the reported
test count: a successful run with zero matching tests is not validation.

```bash
cargo test --locked -p iceberg --lib transaction::
cargo test --locked -p iceberg --lib spec::manifest
cargo test --locked -p iceberg --lib spec::snapshot_summary
cargo test --locked -p iceberg --lib delete_file_index
cargo test --locked -p iceberg --lib scan::
cargo test --locked -p iceberg --lib expr::visitors::
cargo test --locked -p iceberg --lib arrow::
cargo test --locked -p iceberg --lib writer::base_writer::equality_delete_writer
cargo test --locked -p iceberg --lib writer::base_writer::position_delete
cargo test --locked -p iceberg --lib cow_rewrite::
```

Use transaction/manifest checks for A–F, scan/delete/writer checks for C/I/J,
and companion-repository DataFusion checks for G–K. L/M run their new
action/maintenance tests plus the affected existing suites. Focused filters accelerate iteration; they do
not replace the unfiltered per-change gate before completing an implementation
slice.

### Catalog and object-store integration

The existing fixture in `crates/integration_tests/src/lib.rs` expects services
to be started externally. `dev/docker-compose.yaml` provides REST, RustFS,
Spark, and provisioning services, plus catalog/storage emulators. Run against
disposable test data; inspect container health before interpreting failures.

```bash
make docker-up
docker compose -f dev/docker-compose.yaml ps
cargo test --locked -p iceberg-integration-tests --test conflict_commit_test
cargo test --locked -p iceberg-integration-tests
cargo test --locked -p iceberg-catalog-rest
```

For nondefault endpoints, use the existing `ICEBERG_TEST_REST_ENDPOINT` and
`ICEBERG_TEST_OBJECT_STORE_ENDPOINT` configuration in `crates/test_utils/src/lib.rs`.
Do not replace shared production endpoint variables or reuse existing tables.
Collect Compose logs for failed integration runs and clean up the disposable
stack with `make docker-down` afterward; that target removes its volumes.
Add SQL, Glue, HMS, S3 Tables, or storage-package checks when their adapters
change, using their existing fixtures and configuration.

### Full milestone gate

Run these repository targets at milestone exits after the focused suites pass:

```bash
make check
make check-msrv
make unit-test
make test
```

`make check` includes workspace/all-feature linting, TOML formatting, unused
dependency checks, crate license/notice validation, and dependency license
checks. `make unit-test` includes documentation tests. `make test`
starts the disposable Compose stack, uses nextest for workspace/all-feature
tests, and tears the stack down. Install the selected nightly and the workspace
MSRV before the gate; Make targets install several auxiliary Cargo tools.
Record any unavailable backend or environment prerequisite as an incomplete
gate rather than treating a skipped run as a pass.

For public API changes, review generated API differences in the affected
crates, update their `public-api.txt` snapshots intentionally, then run
`make check-public-api`. `make generate-public-api` regenerates snapshots for
all eligible crates; inspect its diff and retain only intended API changes.
Keep additive lifecycle APIs documented with executable examples while
preserving existing append call sites.

### External-reader and resource validation procedure

1. For each supported operation/format cell, create a small table with duplicate
   rows, nulls, multiple files/partitions, and applicable deletes. Capture exact
   typed row values with multiplicities, plus decoded metadata, before mutation.
2. Commit with Rust; load through a fresh external-engine session to avoid
   cached state. Compare complete rows and snapshot operation/ancestry, then
   read the retained pre-mutation snapshot. For concurrent cases, compare the
   documented serial result or assert a rejected commit with unchanged state.
3. Reverse the direction: create/evolve data with the external engine, read and
   mutate with Rust, then reread externally. Use Spark fixtures already in
   `dev/spark`; add reproducible scripts for new mutation scenarios. Trino is
   not supplied by the current Compose file: record its separate setup and
   exact queries. Unsupported engine features are explicit matrix exclusions.
4. Run resource fixtures with increasing file/task counts and configurable small
   hard limits. Record peak descriptor buffering, concurrent I/O, metadata bytes,
   file reads, and elapsed time. Assert configured bounds and no per-file footer
   rereads in default commit mode; avoid hardware-specific latency pass limits.
5. Preserve random seeds and minimized failing fixtures. Validate row contents
   independently of the implementation's summary/count calculation; matching
   counts alone cannot establish correctness.

### Evidence recorded at each milestone

Add a short validation record to the implementing change containing the source
commit, toolchain, dependency-lock revision, commands, test counts/results,
fixture seeds, supported format/operation matrix, and external-engine versions
and commands. Include resource measurements where relevant and identify blocked
or excluded checks explicitly. An exit criterion is complete only when its
associated check has passed; a proposed test name is not evidence of a pass.

### Milestone checks

- deterministic metadata, manifest, manifest-list, and snapshot fixtures;
- format-version matrices for every snapshot-producing action;
- sequence-number and delete-applicability fixtures;
- randomized schemas, partition specs, transforms, file changes, and concurrent
  histories;
- reference-history tests covering main, branches, immutable tags, and
  non-ancestor snapshots, reference deletion/replacement, and expired bases;
- multi-action tests covering metadata actions followed by snapshot actions,
  ordered snapshot chains, incompatible targets, and whole-transaction retry;
- catalog requirements checked against the original base, never staged heads;
- snapshot-ID collision and full-identity lost-response tests;
- unknown-outcome protection, stale reloads, delayed success, subsequent branch
  rollback/deletion, and metadata-only commit recovery;
- ordered conflict histories where additions and removals cancel in a net diff;
- caller summary-property collision tests;
- exact remote-location and narrowly normalized local-path comparison tests;
- exact table-content comparison around overwrite and rewrite operations;
- partial-file predicate matches and coarse-to-fine partition evolution;
- rewrite groups with mixed data sequence numbers and delete applicability;
- complete-file `DeleteFiles` tests including related position/equality deletes;
- every failure-injection point in Section 10;
- cancellation and configured hard-limit tests at each preparation/write stage;
- integration tests against local files and an S3-compatible object store;
- catalog tests where metadata JSON is catalog-managed rather than
  client-written;
- manual Spark and Trino interoperability for append, complete-file delete,
  overwrite, partition replacement, data rewrite, manifest rewrite, and row
  delta as support lands;
- DataFusion end-to-end tests using real Parquet data and delete files;
- serialized compatibility fixtures for every `WriteCommitMessage` version;
- task redelivery, conflicting duplicate payloads, speculative attempts, empty
  tasks, wrong write/table context, and late results after selection;
- adversarial commit-message descriptors and metrics, with strict validation
  tested separately from the default no-reread path;
- reachability fixtures containing retained unreferenced snapshots, multiple
  branches/tags, previous metadata, statistics, and staged exclusions;
- cleanup races with concurrent commits, unknown outcomes, shared files, and
  staging windows longer than the configured age threshold.

Record exact external-engine versions and commands so manual compatibility runs
are reproducible without adding hosted CI.

## 12. Acceptance test for repository scope

Before accepting a design or pull request under this plan, answer all of these
questions:

1. Is the capability defined by standard Iceberg behavior or generic
   DataFusion integration?
2. Can it be documented without referring to a particular downstream system?
3. Does it avoid custom table metadata, reserved application columns, and
   proprietary sidecars?
4. Does core behavior work through the existing catalog abstraction?
5. Would an ordinary Iceberg user plausibly use and test this feature?

If any answer is no, move the work to the downstream repository.

## 13. Immediate implementation slices

### Slice 1: lifecycle refactor only

1. Document the current fast-append flow.
2. Introduce reference targeting, ordered planned-transaction/action, and
   prepared-commit types.
3. Separate preparation from publication.
4. Retain the existing one-call commit API.
5. Preserve multi-action ordering and shared staged table state, including
   schema evolution followed by append, while compiling catalog requirements
   against the original shared base.
6. Keep table metadata JSON persistence catalog-owned and optional from the
   client's perspective.
7. Preserve fast-append output exactly where deterministic values permit.
8. Add snapshot-ID collision checks and reject caller collisions with computed
   summary keys.
9. Add cancellation and initial preparation/manifest hard limits.
10. Test conflicts, metadata rebuilding, lost responses, unknown outcomes,
    immutable tags, and staged-artifact ownership/protection. Do not promise
    destructive cleanup before Milestone M.

Do not add removed-file handling in Slice 1.

### Slice 2: generic file mutation kernel

1. Add data/delete file additions and removals.
2. Resolve live manifest entries and emit deleted entries.
3. Preserve and assign sequence numbers using the shared sequence foundation.
4. Add the reference-aware, file-level snapshot-diff API.
5. Add centralized format-version validation.
6. Add inherited-sequence and manifest-reuse tests.
7. Keep overwrite, rewrite, and row-delta public APIs private until the kernel
   is stable.

## 14. Completion definition

This roadmap is complete when the fork provides:

1. reference-aware planned operations and prepared commits;
2. correct sequence assignment, inheritance, preservation, and delete
   applicability;
3. exact standard file-level snapshot diffs;
4. append, complete-file delete, overwrite modes, partition replacement,
   data-file rewrite, manifest rewrite, position-delete rewrite, and row delta;
5. correct manifest reuse, removal entries, summaries, and format-version gates;
6. operation-appropriate conflict validation, metadata rebuilding, and file
   reuse decisions under metadata evolution;
7. versioned distributed DataFusion commit messages with deterministic attempt
   selection, untrusted-input validation, and bounded resource use;
8. explicit failure recovery including lost responses after successful commit;
9. ordered multi-action transactions and catalog-neutral table metadata
   persistence;
10. cancellation and hard limits across planning, preparation, and distributed
    writes;
11. compaction and reclustering that preserve logical contents;
12. safe expiration and orphan discovery across all standard reachable file
    types and caller-provided staged exclusions; and
13. interoperability with ordinary Iceberg catalogs and readers without any
    downstream product state or proprietary metadata.
