#!/usr/bin/env bash
# Demo helper (run while scripts/run_all.sh is running):
#   scripts/demo.sh baseline            quiet history across regions
#   scripts/demo.sh spike [region] [keyword] [count]
#   scripts/demo.sh reset               wipe posts, alerts, raw files and Spark checkpoints
set -euo pipefail
cd "$(dirname "$0")/.."
source venv/bin/activate

case "${1:-}" in
  baseline) python -m ingestion.simulate baseline --minutes 60 --per-minute 4 ;;
  spike)    python -m ingestion.simulate spike --region "${2:-Assam}" --keyword "${3:-flood}" --count "${4:-40}" ;;
  reset)
    read -r -p "Delete ALL posts/alerts in MongoDB, raw_data/ batches and checkpoints? [y/N] " yn
    [[ "$yn" == "y" ]] || exit 0
    python -c "from common.db import get_db; d=get_db(); d.posts.delete_many({}); d.alerts.delete_many({})"
    rm -f raw_data/batch_*.json ingestion/seen_ids.json; rm -rf checkpoints
    echo "Reset done. Restart run_all.sh so Spark starts with a clean checkpoint." ;;
  *) sed -n '2,6p' "$0"; exit 1 ;;
esac
