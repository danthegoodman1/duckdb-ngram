#!/usr/bin/env bash
# Assert that the checked-out duckdb and extension-ci-tools submodules are the
# commits HEAD pins as gitlinks, that each submodule tree holds exactly its
# commit's files (no modified or untracked file), and that neither is missing
# or in conflict. The gitlinks are the only place the pins live; the
# Correctness workflow and benchmarks/release_evidence.py both run this script.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

status=0
for name in duckdb extension-ci-tools; do
	expected="$(git ls-tree HEAD -- "$name" | awk '$1 == "160000" {print $3}')"
	if ! [[ "$expected" =~ ^[0-9a-f]{40}$ ]]; then
		echo "verify_pins: HEAD records no submodule gitlink for $name" >&2
		exit 1
	fi
	actual="$(git -C "$name" rev-parse HEAD)"
	if [ "$actual" != "$expected" ]; then
		echo "verify_pins: $name is checked out at $actual but HEAD pins $expected" >&2
		status=1
	fi
	dirt="$(git -C "$name" status --porcelain=v1 --untracked-files=all)"
	if [ -n "$dirt" ]; then
		echo "verify_pins: $name has modified or untracked files:" >&2
		printf '%s\n' "$dirt" >&2
		status=1
	fi
	echo "$name $expected"
done

submodule_status="$(git submodule status --recursive)"
printf '%s\n' "$submodule_status"
if printf '%s\n' "$submodule_status" | grep -Eq '^[-+U]'; then
	echo "verify_pins: a submodule is missing, modified, or conflicted" >&2
	status=1
fi
exit "$status"
