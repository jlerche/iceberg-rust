<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Apache Iceberg Rust — Agent Instructions

This file provides repository-specific guidance for automated agents working
in this repository.

## Comments, docs, and diff scope

Default to no comment. Leave existing comments as they are unless you change
the code they describe or they are wrong.

- Comment only what the code cannot say: invariants, non-obvious
  constraints, links to external behavior. Do not restate the code.
- Do not describe how the code changed or explain review history
  ("previously", "instead of", "unlike X"). Put that in the commit message
  or PR description.
- Public items get a one-line doc summary of what they do for the caller,
  not how. Expand only for contract the caller must know, such as the
  errors it returns (`# Errors`) or when it panics (`# Panics`).
- Test names say what they check. Do not narrate asserts.
- Cover the edge cases and error paths the change adds, not only the
  happy path.
- Build the expected value and assert equality with it, rather than
  checking fields one at a time.
- Do not add tests that duplicate existing coverage.
- Do not rename, reformat, refactor, or reorder code unrelated to the
  change. A bug fix does not carry an API redesign.
- Reuse existing helpers, types, and test fixtures (such as
  `TableTestFixture`) before adding new ones. Use arrow-rs kernels
  instead of reimplementing them.
- Use the narrowest visibility that works. Make items `pub` only when they
  are part of the API the change intends to expose. Run
  `make generate-public-api` to update each crate's `public-api.txt`, and
  check its diff for API growth.

## Pull requests

- Follow the PR template. Keep each section short: the problem, the
  approach, and anything a reviewer would not guess from the diff. In
  AI Disclosure, name the areas of uncertainty or assumptions a reviewer
  should focus on.
- Do not restate the diff: no file-by-file walkthroughs, per-function
  summaries, or "changes made" checklists.

## Security Model

When assessing potential vulnerabilities or calibrating automated security
findings, use [`SECURITY-THREAT-MODEL.md`](SECURITY-THREAT-MODEL.md) as the
authoritative detailed description of this repository's security boundaries,
trust assumptions, and non-boundaries.

## Fork development policy

- This is an independent fork. Never open a pull request to Apache upstream.
- Fetch and merge upstream changes into this fork when requested; push work to
  `origin` (`jlerche/iceberg-rust`), never to `upstream`.
- When refreshing from upstream, always resolve code conflicts in favor of
  upstream code. Adapt or rework fork implementations afterward, even when that
  requires substantial readjustment. Do not retain conflicting fork code merely
  to minimize the size of the sync.
- Work on `main` unless the user requests another branch. Upstream mergeability
  is not a design constraint; standard Iceberg compatibility remains required.
- Keep GitHub Actions disabled and run relevant validation locally.
- Prefer the smallest tested vertical slice that makes file rewrites usable.
  Reuse upstream COW, manifest, and transaction primitives rather than building
  speculative frameworks before a working mutation path.

## Fork Rust quality gate

Use pgderive's Clippy policy for new fork modules: deny `all`, `pedantic`, and
`nursery`, plus its restriction lints; use the thresholds in
`dev/fork-lints/clippy.toml`.
Use checked conversions and small functions, and fix findings rather than
silencing them. Narrow exceptions require `#[expect(lint, reason = "...")]`.
Run `make check-fork-lints` for the stricter fork-module gate. It is also part
of `make check`. Opt new
fork modules into the policy with module-level deny attributes, as in
`transaction/rewrite_data_files.rs`; they also cover child test modules.
The script leaves upstream modules at their existing lint policy. It suppresses
three globally enabled threshold lints on the command line; source-level denies
re-enable them inside fork modules. Keep normal workspace lint checks separate,
using their default configuration. Do not add broad upstream lint exemptions.

Apply pgderive's 1,000 physical-line Rust file limit only to the fork-owned
modules listed in `dev/fork-lints/modules.txt`. Register new fork-owned modules
and their test files there; shared/upstream modules remain outside that gate.
The imported checker supports a justified `// pgderive: max-lines=N -- reason`
within the first ten lines. Prefer splitting a fork module over an exception.
The checker defaults to that fork-owned list, including when run directly.

## Independent review before commits

Before every future commit, request an independent sub-agent review of the
proposed changes. Reviewers must not implement the changes they review. Resolve
or explicitly document actionable findings, rerun affected checks, and obtain
review of substantive follow-up fixes before committing. Keep reviews local;
never open an upstream PR. Review the accumulated fork implementation when
requested as well as the current change.
