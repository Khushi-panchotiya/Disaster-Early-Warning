"""End-to-end: simulator files -> Spark streaming job -> MongoDB -> detector."""
import subprocess
import sys

from common.config import ROOT
from detector.detector import detect
from ingestion import simulate


def run_stream(raw, ckpt):
    r = subprocess.run([sys.executable, "-m", "streaming.stream_job", "--once", "--db", "disaster_test",
                        "--raw-dir", str(raw), "--checkpoint", str(ckpt)],
                       cwd=ROOT, capture_output=True, text=True, timeout=240)
    assert r.returncode == 0, r.stderr[-2000:]


def test_pipeline(db, tmp_path):
    raw, ckpt = tmp_path / "raw", tmp_path / "ckpt"
    simulate.main(["--raw-dir", str(raw), "--seed", "1", "baseline", "--minutes", "60", "--per-minute", "4"])
    simulate.main(["--raw-dir", str(raw), "--seed", "2", "spike", "--region", "Kerala",
                   "--keyword", "earthquake", "--count", "30", "--spread-seconds", "100"])
    (raw / "batch_junk.json").write_text('{"not": "valid"\n{"id": null}\n')  # malformed input must not break it
    run_stream(raw, ckpt)

    assert db.posts.count_documents({}) == 270
    doc = db.posts.find_one({"region": "Kerala", "keyword_matched": "earthquake"})
    assert {"region", "timestamp", "sentiment_score", "keyword_matched", "text_snippet", "permalink"} <= set(doc)
    assert len(doc["text_snippet"]) <= 200

    run_stream(raw, ckpt)                                   # replay: checkpoint + upserts -> no duplicates
    assert db.posts.count_documents({}) == 270

    plan = db.posts.find({"region": "Kerala"}).sort("timestamp", -1).explain()["queryPlanner"]["winningPlan"]
    assert "region_timestamp" in str(plan)                  # compound index is actually used

    alerts = detect(db)
    assert [a["region"] for a in alerts] == ["Kerala"]
