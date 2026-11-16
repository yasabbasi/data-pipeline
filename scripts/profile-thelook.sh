#!/usr/bin/env bash
# Read-only profile of the live source tables. Run it by hand, outside CI:
#   bash scripts/profile-thelook.sh
# Uses your gcloud Application Default Credentials and default project (the BigQuery sandbox works).
# Runs every query even if one fails and saves all output to data/profile/latest.txt (gitignored).
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
project="${GCP_PROJECT:-$(gcloud config get-value project 2>/dev/null)}"
if [[ -z "$project" ]]; then
  echo "No GCP project: set GCP_PROJECT or run 'gcloud config set project <id>'." >&2
  exit 1
fi

out="$root/data/profile/latest.txt"
mkdir -p "$(dirname "$out")"
: > "$out"

failed=()
for f in "$root"/sql/profile/*.sql; do
  echo "== $(basename "$f"): $(sed -n '1s/^-- *//p' "$f")" | tee -a "$out"
  # Drop comment-only lines: bq parses a query argument that starts with "--" as a flag.
  query="$(grep -v '^[[:space:]]*--' "$f")"
  if ! bq --project_id="$project" query --quiet --use_legacy_sql=false --format=pretty \
    --max_rows=200 "$query" 2>&1 | tee -a "$out"; then
    failed+=("$(basename "$f")")
  fi
done

echo "Saved to data/profile/latest.txt"
if ((${#failed[@]})); then
  echo "Failed: ${failed[*]}" | tee -a "$out" >&2
  exit 1
fi
