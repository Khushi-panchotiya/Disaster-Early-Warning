"""MongoDB access: one storage layer for enriched posts and alerts."""
from pymongo import ASCENDING, DESCENDING, MongoClient

from common import config


def get_db(name=None, uri=None):
    client = MongoClient(uri or config.MONGO_URI, serverSelectionTimeoutMS=5000, tz_aware=True)
    return client[name or config.MONGO_DB]


def ensure_indexes(db):
    """Compound index on region + timestamp serves the 'mentions in the last N minutes' queries."""
    db.posts.create_index([("region", ASCENDING), ("timestamp", DESCENDING)], name="region_timestamp")
    db.posts.create_index([("timestamp", DESCENDING)], name="timestamp")
    db.alerts.create_index([("detected_at", DESCENDING)], name="detected_at")
    db.alerts.create_index([("region", ASCENDING), ("updated_at", DESCENDING)], name="region_updated")
    return db
