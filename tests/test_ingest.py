import json
from types import SimpleNamespace as NS
from unittest.mock import MagicMock

import prawcore
import pytest

from ingestion import ingest


def sub(id, title, text="", sr="india"):
    return NS(id=id, title=title, selftext=text, subreddit=sr, created_utc=1700000000.0,
              permalink=f"/r/{sr}/comments/{id}/x/", score=5)


def com(id, body, sr="news"):
    return NS(id=id, body=body, subreddit=sr, created_utc=1700000001.0,
              permalink=f"/r/{sr}/comments/abc/x/{id}/", score=2)


def fake_reddit(posts=(), comments=()):
    r = MagicMock()
    s = r.subreddit.return_value
    s.new.return_value = list(posts)
    s.comments.return_value = list(comments)
    return r


@pytest.mark.parametrize("text,expected", [
    ("Severe flooding in Assam", ["flood"]),
    ("A 6.1 quake hit; aftershocks expected", ["earthquake"]),
    ("Wild fire near the forest fire line", ["wildfire"]),
    ("Earthquake triggered floods", ["flood", "earthquake"]),
    ("Floodlights at the stadium", []),
    ("nothing here", []),
    ("", []),
    (None, []),
])
def test_match_keywords(text, expected):
    assert sorted(ingest.match_keywords(text)) == sorted(expected)


def test_collect_filters_and_dedupes():
    r = fake_reddit([sub("a", "Flood warning"), sub("b", "Cricket")], [com("c", "huge wildfire")])
    seen = {}
    recs = ingest.collect(r, ["india", "news"], seen)
    assert {x["id"] for x in recs} == {"a", "c"}
    assert {x["type"] for x in recs} == {"post", "comment"}
    assert {"t3_a", "t3_b", "t1_c"} <= set(seen)  # non-matches are remembered too
    assert ingest.collect(r, ["india", "news"], seen) == []  # second poll: nothing new


def test_write_batch_atomic_jsonl(tmp_path):
    recs = [{"id": "a", "text": "ünïcode flood"}, {"id": "b", "text": "x"}]
    p = ingest.write_batch(recs, tmp_path)
    assert p.suffix == ".json" and not list(tmp_path.glob(".*"))
    lines = p.read_text(encoding="utf-8").splitlines()
    assert [json.loads(l)["id"] for l in lines] == ["a", "b"]


def test_seen_roundtrip_and_corrupt(tmp_path):
    f = tmp_path / "seen.json"
    assert ingest.load_seen(f) == {}
    ingest.save_seen({"t3_a": None, "t3_b": None}, f)
    assert list(ingest.load_seen(f)) == ["t3_a", "t3_b"]
    f.write_text("{not json")
    assert ingest.load_seen(f) == {}


def test_run_once_writes_file(tmp_path):
    r = fake_reddit([sub("a", "earthquake in Nepal")])
    n = ingest.run(r, ["india"], 1, once=True, raw_dir=tmp_path / "raw", state_file=tmp_path / "s.json")
    assert n == 1 and len(list((tmp_path / "raw").glob("batch_*.json"))) == 1


def test_run_survives_api_error_then_recovers(tmp_path, monkeypatch):
    monkeypatch.setattr(ingest, "sleep_interruptible", lambda s: None)
    monkeypatch.setattr(ingest, "_stop", False)
    r = fake_reddit()
    calls = []

    def flaky_new(limit):
        calls.append(1)
        if len(calls) == 1:
            raise prawcore.exceptions.RequestException(Exception("x"), (), {})
        monkeypatch.setattr(ingest, "_stop", True)  # stop after the successful poll
        return [sub("a", "flood")]

    r.subreddit.return_value.new.side_effect = flaky_new
    n = ingest.run(r, ["india"], 1, raw_dir=tmp_path / "raw", state_file=tmp_path / "s.json")
    assert len(calls) == 2 and n == 1


def test_missing_credentials_exits(monkeypatch):
    monkeypatch.setattr(ingest, "load_dotenv", lambda *a, **k: None)
    monkeypatch.delenv("REDDIT_CLIENT_ID", raising=False)
    monkeypatch.delenv("REDDIT_CLIENT_SECRET", raising=False)
    with pytest.raises(SystemExit):
        ingest.make_reddit()
