#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Enforce Rust source file limits, including justified file-local overrides."""

from pathlib import Path
import os
import re
import sys

DEFAULT_LIMIT = 1000
HEADER_LINES = 10
MARKER = re.compile(r"^\s*//\s*pgderive:\s*max-lines\b")
DIRECTIVE = re.compile(r"^\s*//\s*pgderive: max-lines=([1-9][0-9]*) -- (\S.*?)\s*$")


def violations(text: str) -> list[str]:
    """Return policy violations for one file's decoded contents."""
    lines = text.splitlines()
    directives = [(number, line) for number, line in enumerate(lines, 1) if MARKER.match(line)]
    limit = DEFAULT_LIMIT
    errors = []
    if len(directives) > 1:
        errors.append("duplicate max-lines directives")
    for number, line in directives:
        match = DIRECTIVE.fullmatch(line)
        if match is None:
            errors.append(f"line {number}: use '// pgderive: max-lines=N -- reason'")
        elif number > HEADER_LINES:
            errors.append(f"line {number}: directive must be within the first {HEADER_LINES} lines")
        else:
            limit = int(match.group(1))
    if len(lines) > limit:
        errors.append(f"{len(lines)} physical lines exceeds allowed {limit}")
    return errors


def rust_files() -> list[Path]:
    """Read the explicit fork-owned module list; upstream files are excluded."""
    manifest = Path("dev/fork-lints/modules.txt")
    return sorted({Path(name) for name in manifest.read_text(encoding="utf-8").splitlines() if name})


def main() -> int:
    """Check explicit paths, or the registered fork-owned Rust modules."""
    root = Path(__file__).resolve().parent.parent
    os.chdir(root)
    files = [Path(name) for name in sys.argv[1:]] if len(sys.argv) > 1 else rust_files()
    failed = False
    for path in files:
        if not path.exists():
            # Deleted tracked files are not source files in this checkout.
            if len(sys.argv) > 1:
                print(f"{path}: file does not exist", file=sys.stderr)
                failed = True
            continue
        try:
            errors = violations(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError) as error:
            errors = [str(error)]
        for error in errors:
            print(f"{path}: {error}", file=sys.stderr)
            failed = True
    if not failed:
        print(f"Rust file length: {len(files)} files passed (default {DEFAULT_LIMIT} lines).")
    return int(failed)


if __name__ == "__main__":
    sys.exit(main())
