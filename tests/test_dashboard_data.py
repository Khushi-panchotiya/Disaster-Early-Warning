from datetime import datetime, timezone

from dashboard import data


def seed(db):
    now = datetime.now(timezone.utc)
    db.posts.insert_many([{"_id": f"p{i}", "region": "Assam", "timestamp": now, "keyword_matched": "flood",
                           "sentiment_score": -0.4, "text_snippet": "x", "permalink": "u"} for i in range(30)])


def test_collection_overview_and_indexes(db):
    seed(db)
    ov = data.collection_overview(db).set_index("collection")
    assert ov.loc["posts", "documents"] == 30
    names = set(data.index_table(db, "posts")["name"])
    assert {"_id_", "region_timestamp", "timestamp"} <= names


def test_explain_uses_compound_index(db):
    seed(db)
    assert data.explain_query(db, {"region": "Assam"})["index"] == "region_timestamp"


def test_browse_and_pipeline_tables(db):
    seed(db)
    q, docs = data.browse_posts(db, region="Assam", limit=5)
    assert q == {"region": "Assam"} and len(docs) == 5
    assert data.pipeline_batches(db).empty and data.spark_status(db) is None
    db.pipeline_batches.insert_one({"batch_id": 0, "processed_at": datetime.now(timezone.utc), "rows": 30, "duration_ms": 120})
    assert data.pipeline_batches(db).rows.sum() == 30
