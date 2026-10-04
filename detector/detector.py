"""Anomaly detector: per-region mention counts in a sliding window vs. a rolling mean + k*std baseline.

  python -m detector.detector                 # run continuously
  python -m detector.detector --once          # single evaluation and exit
"""
import argparse
import logging
import math
import time
from datetime import datetime, timedelta, timezone

from common import config
from common.db import ensure_indexes, get_db
from common.regions import UNKNOWN

log = logging.getLogger("detector")


def evaluate(baseline, current, k=3.0, min_count=5, min_std=1.0):
    """Decide whether `current` is a spike relative to the `baseline` window counts.

    threshold = mean + k * max(std, min_std); `min_std` stops near-silent regions from alerting on a
    handful of posts and `min_count` is an absolute floor.
    Returns (is_alert, mean, std, threshold, z_score).
    """
    n = len(baseline)
    mean = sum(baseline) / n if n else 0.0
    std = math.sqrt(sum((x - mean) ** 2 for x in baseline) / n) if n else 0.0
    eff_std = max(std, min_std)
    threshold = max(mean + k * eff_std, min_count)
    z = (current - mean) / eff_std
    return current >= threshold, mean, std, threshold, z


def window_counts(db, now, window_minutes, baseline_windows):
    """Mongo aggregation -> {region: {idx: {count, sentiment, keywords}}}.

    Sliding windows ending at `now`: idx 0 is the last `window_minutes`, idx 1..N the windows before it.
    (Sliding rather than clock-aligned, so a burst is never split across a bucket boundary.)
    """
    width_ms = window_minutes * 60_000
    start = now - timedelta(milliseconds=width_ms * (baseline_windows + 1))
    pipeline = [
        {"$match": {"timestamp": {"$gt": start, "$lte": now}, "region": {"$ne": UNKNOWN}}},
        {"$group": {
            "_id": {"region": "$region",
                    "idx": {"$floor": {"$divide": [{"$subtract": [now, "$timestamp"]}, width_ms]}}},
            "count": {"$sum": 1},
            "sentiment": {"$avg": "$sentiment_score"},
            "keywords": {"$push": "$keyword_matched"},
        }},
    ]
    out = {}
    for d in db.posts.aggregate(pipeline):
        kws = {}
        for kw in d["keywords"]:
            kws[kw] = kws.get(kw, 0) + 1
        out.setdefault(d["_id"]["region"], {})[int(d["_id"]["idx"])] = {
            "count": d["count"], "sentiment": d["sentiment"], "keywords": kws}
    return out


def detect(db, now=None, window_minutes=5, baseline_windows=12, k=3.0, min_count=5, min_std=1.0):
    """Evaluate every region once and upsert alerts. Returns the list of alert documents.

    An ongoing incident keeps updating one alert: if the region already alerted within the last
    `window_minutes`, that alert is refreshed instead of a new one being created.
    """
    now = now or datetime.now(timezone.utc)
    data = window_counts(db, now, window_minutes, baseline_windows)
    alerts = []
    for region, windows in data.items():
        cur = windows.get(0)
        if not cur:
            continue
        baseline = [windows.get(i, {}).get("count", 0) for i in range(1, baseline_windows + 1)]  # silence = 0
        is_alert, mean, std, threshold, z = evaluate(baseline, cur["count"], k, min_count, min_std)
        if not is_alert:
            continue
        top_kw = max(cur["keywords"], key=cur["keywords"].get)
        doc = {
            "region": region,
            "window_minutes": window_minutes,
            "window_start": now - timedelta(minutes=window_minutes),
            "count": cur["count"],
            "baseline_mean": round(mean, 3),
            "baseline_std": round(std, 3),
            "threshold": round(threshold, 3),
            "z_score": round(z, 2),
            "top_keyword": top_kw,
            "avg_sentiment": round(cur["sentiment"], 3),
            "severity": "critical" if cur["count"] >= 2 * threshold else "warning",
            "updated_at": now,
        }
        open_alert = db.alerts.find_one(
            {"region": region, "updated_at": {"$gte": now - timedelta(minutes=window_minutes)}},
            sort=[("updated_at", -1)])
        if open_alert:
            doc["count"] = max(doc["count"], open_alert["count"])  # peak of the incident
            db.alerts.update_one({"_id": open_alert["_id"]}, {"$set": doc})
        else:
            db.alerts.insert_one({**doc, "_id": f"{region}|{int(now.timestamp())}", "detected_at": now})
        alerts.append(doc)
    return alerts


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--interval", type=int, default=10, help="seconds between evaluations")
    ap.add_argument("--window-minutes", type=int, default=5)
    ap.add_argument("--baseline-windows", type=int, default=12, help="how many earlier windows form the baseline")
    ap.add_argument("--k", type=float, default=3.0, help="std-devs above the mean")
    ap.add_argument("--min-count", type=int, default=5, help="never alert below this many mentions")
    ap.add_argument("--db", default=config.MONGO_DB)
    a = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    db = ensure_indexes(get_db(a.db))
    log.info("detector: %dm windows, baseline %d windows, k=%.1f, min_count=%d",
             a.window_minutes, a.baseline_windows, a.k, a.min_count)
    while True:
        try:
            for al in detect(db, None, a.window_minutes, a.baseline_windows, a.k, a.min_count):
                log.warning("ALERT %s [%s] %s: %d mentions (baseline %.2f, z=%.1f)", al["severity"].upper(),
                            al["top_keyword"], al["region"], al["count"], al["baseline_mean"], al["z_score"])
        except Exception:  # keep the detector alive through transient Mongo errors
            log.exception("evaluation failed")
        if a.once:
            break
        time.sleep(a.interval)


if __name__ == "__main__":
    main()
