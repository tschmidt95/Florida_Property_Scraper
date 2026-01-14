#!/usr/bin/env bash
set -euo pipefail

# CI-safe: scans tracked files for common GitHub token / credential patterns.
# Does NOT print matching lines (to avoid leaking secrets).

patterns=(
  'x-access-token:'
  'ghp_[A-Za-z0-9_]+'
  'github_pat_[A-Za-z0-9_]+'
  'GITHUB_TOKEN='
  'GH_TOKEN='
)

# Build a single alternation regex
re="$(IFS='|'; echo "${patterns[*]}")"

# Limit scan to tracked files only.
# Print only filenames if any matches are found.
mapfile -t hits < <(git ls-files -z | xargs -0 grep -IlE "$re" || true)

if ((${#hits[@]} > 0)); then
  echo "SECURITY_SCAN_FAILED: Potential secret patterns found in tracked files:" >&2
  for f in "${hits[@]}"; do
    echo "- $f" >&2
  done
  exit 1
fi

echo "SECURITY_SCAN_OK"
