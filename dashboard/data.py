"""Query layer for the dashboard (plain functions returning DataFrames, so they are testable)."""
from datetime import datetime, timedelta, timezone

import pandas as pd

from common.regions import REGIONS, UNKNOWN


def _since(hours, now=None):
    return (now or datetime.now(timezone.utc)) - timedelta(hours=hours)


def _filter(since, regions=None, keywords=None):
    q = {"timestamp": {"$gte": since}}
    if regions:
        q["region"] = {"$in": list(regions)}
    if keywords:
        q["keyword_matched"] = {"$in": list(keywords)}
    return q


def recent_alerts(db, limit=20):
    return list(db.alerts.find().sort("updated_at", -1).limit(limit))


def active_alert_count(db, minutes=15, now=None):
    return db.alerts.count_documents({"updated_at": {"$gte": (now or datetime.now(timezone.utc)) - timedelta(minutes=minutes)}})


def kpis(db, hours, regions=None, keywords=None):
    q = _filter(_since(hours), regions, keywords)
    agg = list(db.posts.aggregate([
        {"$match": q},
        {"$group": {"_id": None, "n": {"$sum": 1}, "sent": {"$avg": "$sentiment_score"},
                    "regions": {"$addToSet": "$region"}}},
    ]))
    if not agg:
        return {"posts": 0, "avg_sentiment": None, "regions": 0}
    a = agg[0]
    return {"posts": a["n"], "avg_sentiment": a["sent"], "regions": len([r for r in a["regions"] if r != UNKNOWN])}


def timeline(db, hours, bucket_minutes, regions=None, keywords=None, top_n=8):
    """Mentions per time bucket for the top_n busiest regions -> wide DataFrame (index=bucket)."""
    q = _filter(_since(hours), regions, keywords)
    q.setdefault("region", {"$ne": UNKNOWN})
    rows = db.posts.aggregate([
        {"$match": q},
        {"$group": {"_id": {"region": "$region", "bucket": {
            "$dateTrunc": {"date": "$timestamp", "unit": "minute", "binSize": bucket_minutes}}},
            "count": {"$sum": 1}}},
    ])
    df = pd.DataFrame([{"bucket": r["_id"]["bucket"], "region": r["_id"]["region"], "count": r["count"]} for r in rows])
    if df.empty:
        return df
    top = df.groupby("region")["count"].sum().nlargest(top_n).index
    wide = df[df.region.isin(top)].pivot_table(index="bucket", columns="region", values="count", fill_value=0)
    wide.index = pd.to_datetime(wide.index, utc=True)
    full = pd.date_range(wide.index.min(), wide.index.max(), freq=f"{bucket_minutes}min")
    return wide.reindex(full, fill_value=0).rename_axis("time")


def region_summary(db, hours, regions=None, keywords=None):
    q = _filter(_since(hours), regions, keywords)
    q.setdefault("region", {"$ne": UNKNOWN})
    rows = db.posts.aggregate([
        {"$match": q},
        {"$group": {"_id": "$region", "mentions": {"$sum": 1}, "avg_sentiment": {"$avg": "$sentiment_score"}}},
        {"$sort": {"mentions": -1}},
    ])
    df = pd.DataFrame([{"region": r["_id"], "mentions": r["mentions"], "avg_sentiment": r["avg_sentiment"]} for r in rows])
    if df.empty:
        return df
    df["latitude"] = df.region.map(lambda r: REGIONS[r][0])
    df["longitude"] = df.region.map(lambda r: REGIONS[r][1])
    return df


def recent_posts(db, limit=15, region=None, hours=24, keywords=None):
    q = _filter(_since(hours), [region] if region else None, keywords)
    return list(db.posts.find(q).sort("timestamp", -1).limit(limit))
