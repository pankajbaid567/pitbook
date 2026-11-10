#!/usr/bin/env bash
# Fails if docs/resume-claims.md references a proof file, command package or result file that does not exist.
# Glob patterns (bench/results/crashtest-*.json) must match at least one file.
set -euo pipefail
cd "$(dirname "$0")/.."
bad=0
while read -r p; do
  if ! compgen -G "$p" >/dev/null; then echo "missing proof path: $p"; bad=1; fi
done < <(grep -oE '`(docs|bench|cmd|internal|scripts|web)/[^` ]+`' docs/resume-claims.md | tr -d '`' | sort -u)
[ "$bad" -eq 0 ] && echo "claims: every referenced proof path exists"
exit $bad
