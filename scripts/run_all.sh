#!/usr/bin/env bash
# Start the whole pipeline: Spark streaming job + anomaly detector + Streamlit dashboard.
# Ctrl+C stops everything. Logs go to logs/.
set -euo pipefail
cd "$(dirname "$0")/.."
source venv/bin/activate
mkdir -p logs raw_data

if ! python -c "from common.db import get_db; get_db().client.admin.command('ping')" 2>/dev/null; then
  echo "MongoDB is not running. Start it with:  brew services start mongodb-community" >&2
  exit 1
fi

pids=()
cleanup() { echo; echo "Stopping..."; kill "${pids[@]}" 2>/dev/null || true; wait 2>/dev/null || true; }
trap cleanup INT TERM EXIT

python -m streaming.stream_job            > logs/stream.log   2>&1 & pids+=($!)
python -m detector.detector               > logs/detector.log 2>&1 & pids+=($!)
streamlit run dashboard/app.py --server.headless true > logs/dashboard.log 2>&1 & pids+=($!)

if [[ "${USE_REDDIT:-0}" == "1" ]]; then
  python -m ingestion.ingest              > logs/ingest.log   2>&1 & pids+=($!)
  echo "Reddit ingestion: ON"
else
  echo "Reddit ingestion: off (inject data with scripts/demo.sh or 'python -m ingestion.simulate')"
fi

echo "Dashboard: http://localhost:8501   (logs in logs/)"
wait
