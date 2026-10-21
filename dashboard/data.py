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


# ---- MongoDB storage explorer -------------------------------------------------------------------

def collection_overview(db):
    """One row per collection: documents, data size, storage size and index size."""
    rows = []
    for name in sorted(db.list_collection_names()):
        st = db.command("collstats", name)
        rows.append({"collection": name, "documents": st["count"], "avg doc (bytes)": st.get("avgObjSize", 0),
                     "data (KB)": round(st["size"] / 1024, 1), "storage (KB)": round(st["storageSize"] / 1024, 1),
                     "indexes": st["nindexes"], "index size (KB)": round(st["totalIndexSize"] / 1024, 1)})
    return pd.DataFrame(rows)


def index_table(db, collection="posts"):
    sizes = db.command("collstats", collection).get("indexSizes", {})
    rows = [{"name": i["name"], "keys": ", ".join(f"{k} ({'asc' if v == 1 else 'desc'})" for k, v in i["key"].items()),
             "size (KB)": round(sizes.get(i["name"], 0) / 1024, 1)} for i in db.get_collection(collection).list_indexes()]
    return pd.DataFrame(rows)


def browse_posts(db, region=None, keyword=None, limit=50):
    q = {}
    if region:
        q["region"] = region
    if keyword:
        q["keyword_matched"] = keyword
    docs = list(db.posts.find(q).sort("timestamp", -1).limit(limit))
    return q, docs


def explain_query(db, query):
    """Summarise the winning plan for the posts query: which index (if any) served it."""
    ex = db.command({"explain": {"find": "posts", "filter": query, "sort": {"timestamp": -1}, "limit": 50},
                     "verbosity": "executionStats"})
    plan = ex["queryPlanner"]["winningPlan"]
    stages, index = [], None
    while plan:
        stages.append(plan["stage"])
        index = plan.get("indexName", index)
        plan = plan.get("inputStage")
    es = ex["executionStats"]
    return {"stages": " → ".join(reversed(stages)), "index": index, "docs_examined": es["totalDocsExamined"],
            "keys_examined": es["totalKeysExamined"], "returned": es["nReturned"],
            "millis": es["executionTimeMillis"]}


# ---- Spark pipeline monitor ---------------------------------------------------------------------

def spark_status(db):
    return db.pipeline_status.find_one({"_id": "spark"})


def pipeline_batches(db, limit=200):
    rows = list(db.pipeline_batches.find().sort("processed_at", -1).limit(limit))
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["processed_at"] = pd.to_datetime(df["processed_at"], utc=True)
    return df.sort_values("processed_at").reset_index(drop=True)
