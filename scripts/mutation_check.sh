#!/usr/bin/env bash
# Injects deliberate bugs into a scratch copy of the matching core and checks
# that the reference-model property test fails for each one. The repository
# itself is never modified.
set -uo pipefail
cd "$(dirname "$0")/.."
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT
rsync -a --exclude web --exclude bin --exclude .git --exclude bench ./ "$TMP/src/"
cd "$TMP/src/internal/book"
cp book.go "$TMP/book.go.orig"
caught=0; total=0
mut() { # name, sed expression
  cp "$TMP/book.go.orig" book.go
  sed -i.bak "$2" book.go && rm -f book.go.bak
  total=$((total+1))
  if cmp -s "$TMP/book.go.orig" book.go; then echo "$1: mutation did not apply"; return; fi
  if go test -count=1 -short -run TestPropertyEngineEqualsReference . >/dev/null 2>&1; then
    echo "$1: SURVIVED (test did not notice)"
  else
    echo "$1: killed"; caught=$((caught+1))
  fi
}
mut "self-trade prevention disabled"            's/if m.Participant == part {/if false \&\& m.Participant == part {/'
mut "crossing test off by one (< instead of <=)" 's/return resting <= limit/return resting < limit/'
mut "FOK ignores self-trade blocking"            's/if o.Participant == c.Participant {/if false \&\& o.Participant == c.Participant {/'
mut "replace keeps priority when size grows"     's/c.Price == o.Price \&\& c.Qty <= o.Rem/c.Price == o.Price/'
mut "market orders rest instead of cancelling"   's/case typ == Market:/case typ == Market \&\& false:/'
echo "mutations killed: $caught of $total"
[ "$caught" -eq "$total" ]
