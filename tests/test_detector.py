from datetime import datetime, timedelta, timezone

import pytest

from detector.detector import detect, evaluate

NOW = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)


def add(db, region, minutes_ago, n, kw="flood"):
    db.posts.insert_many([{"region": region, "timestamp": NOW - timedelta(minutes=minutes_ago, seconds=i),
                           "keyword_matched": kw, "sentiment_score": -0.5, "text_snippet": "x",
                           "permalink": "u"} for i in range(n)])


def test_evaluate_spike_vs_normal():
    base = [1, 0, 2, 1, 0, 1, 1, 0, 2, 1, 0, 1]
    assert evaluate(base, 20)[0] is True
    assert evaluate(base, 2)[0] is False


def test_evaluate_quiet_region_needs_min_count():
    # baseline all zeros: 3 mentions is a jump but below the absolute floor
    assert evaluate([0] * 12, 3, min_count=5)[0] is False
    assert evaluate([0] * 12, 6, min_count=5)[0] is True


def test_evaluate_noisy_baseline_needs_more():
    noisy = [10, 2, 14, 1, 9, 12, 3, 11, 2, 13, 4, 10]
    assert evaluate(noisy, 15)[0] is False
    assert evaluate(noisy, 40)[0] is True


def test_evaluate_empty_baseline():
    assert evaluate([], 10)[0] is True


def test_detect_flags_only_spiking_region(db):
    for m in range(6, 60, 5):          # steady chatter: ~2 per window in both regions
        add(db, "Assam", m, 2)
        add(db, "Kerala", m, 2)
    add(db, "Assam", 1, 40)            # Assam bursts in the current window
    add(db, "Kerala", 1, 2)
    alerts = detect(db, now=NOW)
    assert [a["region"] for a in alerts] == ["Assam"]
    a = alerts[0]
    assert a["count"] >= 40 and a["severity"] == "critical" and a["top_keyword"] == "flood"
    assert db.alerts.count_documents({}) == 1


def test_detect_ongoing_incident_updates_single_alert(db):
    add(db, "Assam", 1, 30)
    detect(db, now=NOW)
    add(db, "Assam", 0, 10)
    detect(db, now=NOW + timedelta(minutes=1))
    assert db.alerts.count_documents({}) == 1
    assert db.alerts.find_one()["count"] >= 30


def test_detect_ignores_unknown_region_and_quiet_data(db):
    add(db, "Unknown", 1, 50)
    add(db, "Kerala", 1, 2)
    assert detect(db, now=NOW) == []


def test_detect_old_spike_not_current(db):
    add(db, "Assam", 30, 50)           # big burst, but 30 minutes ago
    assert detect(db, now=NOW) == []
