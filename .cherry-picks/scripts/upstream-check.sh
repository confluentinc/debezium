#!/bin/bash
# Upstream skip detection: checks if source commits are already in target via upstream
# Usage: ./upstream-check.sh <sha-list-file> <start> <end> <upstream-tag>

SHA_FILE="${1:-/tmp/cherry-pick-shas.txt}"
START="${2:-1}"
END="${3:-104}"
UPSTREAM_TAG="${4:-FETCH_HEAD}"
REPO="$(git rev-parse --show-toplevel)"

echo "=== Upstream Skip Detection ==="
echo "Upstream tag: $UPSTREAM_TAG | Range: $START-$END"
echo ""

skip_count=0
pick_count=0
maybe_count=0

for i in $(seq "$START" "$END"); do
  sha=$(sed -n "${i}p" "$SHA_FILE")
  [ -z "$sha" ] && continue

  subject=$(git -C "$REPO" show "$sha" --format="%s" --no-patch)

  # Strategy 1: Extract DBZ ticket from commit message
  dbz_ticket=$(echo "$subject" | grep -oE 'DBZ-[0-9]+' | head -1)

  upstream_sha=""
  upstream_match=""
  match_method=""

  if [ -n "$dbz_ticket" ]; then
    upstream_sha=$(git -C "$REPO" log "$UPSTREAM_TAG" --format="%H" --grep="$dbz_ticket" | head -1)
    if [ -n "$upstream_sha" ]; then
      upstream_match=$(git -C "$REPO" log "$upstream_sha" -1 --oneline)
      match_method="DBZ ticket ($dbz_ticket)"
    fi
  fi

  # Strategy 2: If no DBZ ticket match, strip CC-XXXX and search by message
  if [ -z "$upstream_sha" ]; then
    clean_subject=$(echo "$subject" | sed 's/CC-[0-9]*[: _-]*//' | sed 's/(#[0-9]*)//g' | sed 's/^[[:space:]:-]*//' | sed 's/[[:space:]]*$//')

    if [ -n "$clean_subject" ] && [ ${#clean_subject} -gt 10 ]; then
      upstream_sha=$(git -C "$REPO" log "$UPSTREAM_TAG" --format="%H" --fixed-strings --grep="$clean_subject" | head -1)
      if [ -n "$upstream_sha" ]; then
        upstream_match=$(git -C "$REPO" log "$upstream_sha" -1 --oneline)
        match_method="Message match"
      fi
    fi

    # Strategy 2b: Try with just the first meaningful phrase
    if [ -z "$upstream_sha" ] && [ -n "$clean_subject" ]; then
      short_subject=$(echo "$clean_subject" | sed 's/ [-_(#].*//' | head -c 60)
      if [ ${#short_subject} -ge 15 ]; then
        upstream_sha=$(git -C "$REPO" log "$UPSTREAM_TAG" --format="%H" --fixed-strings --grep="$short_subject" | head -1)
        if [ -n "$upstream_sha" ]; then
          upstream_match=$(git -C "$REPO" log "$upstream_sha" -1 --oneline)
          match_method="Partial message match"
        fi
      fi
    fi
  fi

  # Check ancestry if upstream commit found
  if [ -n "$upstream_sha" ]; then
    if git -C "$REPO" merge-base --is-ancestor "$upstream_sha" "$UPSTREAM_TAG" 2>/dev/null; then
      status="SKIP"
      skip_count=$((skip_count + 1))
    else
      status="PICK (upstream exists but NOT in target tag)"
      maybe_count=$((maybe_count + 1))
    fi
  else
    status="PICK"
    pick_count=$((pick_count + 1))
  fi

  # Output
  if [ "$status" = "SKIP" ]; then
    echo "#${i} | SKIP | ${sha:0:10} | ${subject}"
    echo "     Method: $match_method"
    echo "     Upstream: $upstream_match"
    echo ""
  elif [ "$status" != "PICK" ]; then
    echo "#${i} | $status | ${sha:0:10} | ${subject}"
    echo "     Method: $match_method"
    echo "     Upstream: $upstream_match"
    echo ""
  fi
done

echo "=== Summary ==="
echo "SKIP: $skip_count | MAYBE: $maybe_count | PICK: $pick_count | TOTAL: $((skip_count + maybe_count + pick_count))"
