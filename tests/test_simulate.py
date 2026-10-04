import json

from ingestion import simulate

REQUIRED = {"id", "type", "subreddit", "created_utc", "title", "text", "permalink", "score", "keywords"}


def read_all(d):
    return [json.loads(l) for f in sorted(d.glob("batch_*.json")) for l in f.read_text().splitlines()]


def test_spike_schema_and_count(tmp_path):
    simulate.main(["--raw-dir", str(tmp_path), "--seed", "1", "spike",
                   "--region", "Assam", "--keyword", "flood", "--count", "25"])
    recs = read_all(tmp_path)
    assert len(recs) == 25
    assert all(REQUIRED == set(r) for r in recs)
    assert all("flood" in r["keywords"] for r in recs)
    assert len({r["id"] for r in recs}) == 25


def test_baseline_spreads_over_time(tmp_path):
    simulate.main(["--raw-dir", str(tmp_path), "baseline", "--minutes", "30", "--per-minute", "2"])
    recs = read_all(tmp_path)
    assert len(recs) == 60
    assert max(r["created_utc"] for r in recs) - min(r["created_utc"] for r in recs) > 25 * 60


def test_one_custom_text(tmp_path):
    simulate.main(["--raw-dir", str(tmp_path), "one", "--region", "Nepal",
                   "--keyword", "earthquake", "--text", "Big quake in Kathmandu"])
    (r,) = read_all(tmp_path)
    assert "Big quake in Kathmandu" in (r["title"] or r["text"])
    assert r["keywords"] == ["earthquake"]
