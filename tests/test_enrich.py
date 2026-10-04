import pytest

from common.regions import tag_region
from streaming.enrich import enrich_text, to_document, sentiment, snippet


@pytest.mark.parametrize("text,region", [
    ("Severe flooding in Guwahati tonight", "Assam"),
    ("Earthquake felt in Kathmandu and Pokhara", "Nepal"),
    ("wildfire near SAN DIEGO", "California"),
    ("Floods across Assam, Guwahati and Dibrugarh", "Assam"),
    ("New Delhi is flooded", "Delhi"),
    ("a flood somewhere unspecified", "Unknown"),
    ("", "Unknown"),
    (None, "Unknown"),
])
def test_tag_region(text, region):
    assert tag_region(text) == region


def test_sentiment_direction():
    assert sentiment("Terrible disaster, people are dying and stranded!") < -0.3
    assert sentiment("Wonderful news, everyone is safe and happy") > 0.3
    assert sentiment("") == 0.0


def test_snippet_truncates_and_collapses():
    s = snippet("a   b\n\nc " + "x" * 500)
    assert len(s) == 200 and s.endswith("…") and "  " not in s
    assert snippet("short text") == "short text"


def test_enrich_text_fallback_keyword():
    region, score, kw, snip = enrich_text("Quake hits Tokyo", "", [])
    assert (region, kw) == ("Japan", "earthquake") and snip == "Quake hits Tokyo"


def test_to_document():
    row = {"id": "x1", "created_utc": 1700000000, "permalink": "https://reddit.com/x", "subreddit": "india",
           "type": "comment", "region": "Kerala", "sentiment_score": -0.4, "keyword_matched": "flood",
           "text_snippet": "Flood in Kochi"}
    d = to_document(row)
    assert d["_id"] == "x1" and d["region"] == "Kerala" and d["timestamp"].year == 2023
