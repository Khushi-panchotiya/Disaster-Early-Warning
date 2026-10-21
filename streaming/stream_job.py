"""Spark Structured Streaming job: raw_data/*.json -> enrich (region + VADER) -> MongoDB.

  python -m streaming.stream_job              # run continuously (5s micro-batches)
  python -m streaming.stream_job --once       # process everything currently in raw_data/ and exit
"""
import argparse
import logging
import os
import sys
import time
from datetime import datetime, timezone

from pymongo import ReplaceOne
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from common import config
from common.db import ensure_indexes, get_db
from streaming.enrich import enrich_text, to_document

log = logging.getLogger("stream")

RAW_SCHEMA = T.StructType([
    T.StructField("id", T.StringType()),
    T.StructField("type", T.StringType()),
    T.StructField("subreddit", T.StringType()),
    T.StructField("created_utc", T.LongType()),
    T.StructField("title", T.StringType()),
    T.StructField("text", T.StringType()),
    T.StructField("permalink", T.StringType()),
    T.StructField("score", T.LongType()),
    T.StructField("keywords", T.ArrayType(T.StringType())),
])
ENRICH_TYPE = T.StructType([
    T.StructField("region", T.StringType()),
    T.StructField("sentiment_score", T.DoubleType()),
    T.StructField("keyword_matched", T.StringType()),
    T.StructField("text_snippet", T.StringType()),
])


@F.udf(returnType=ENRICH_TYPE)
def enrich_udf(title, text, keywords):
    return enrich_text(title, text, keywords)


def build_spark():
    # Python workers must use this venv and be able to import the project's modules.
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYTHONPATH"] = os.pathsep.join(filter(None, [str(config.ROOT), os.environ.get("PYTHONPATH")]))
    return (SparkSession.builder.master("local[*]").appName("DisasterEarlyWarning")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.ui.enabled", "true")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate())


def enriched_stream(spark, raw_dir):
    raw = spark.readStream.schema(RAW_SCHEMA).option("maxFilesPerTrigger", 20).json(str(raw_dir))
    return (raw.filter(F.col("id").isNotNull() & F.col("created_utc").isNotNull())
            .withColumn("e", enrich_udf("title", "text", "keywords"))
            .select("id", "type", "subreddit", "created_utc", "permalink", "e.*"))


def make_writer(db_name):
    def write_batch(batch_df, batch_id):
        t0 = time.time()
        rows = batch_df.collect()
        if not rows:
            return
        db = get_db(db_name)
        # upsert by Reddit id -> replaying a file never creates duplicates
        ops = [ReplaceOne({"_id": r["id"]}, {k: v for k, v in to_document(r).items() if k != "_id"}, upsert=True)
               for r in rows]
        db.posts.bulk_write(ops, ordered=False)
        ms = int((time.time() - t0) * 1000)
        db.pipeline_batches.insert_one({"batch_id": int(batch_id), "processed_at": datetime.now(timezone.utc),
                                        "rows": len(ops), "duration_ms": ms})  # feeds the dashboard's Spark tab
        log.info("batch %s: upserted %d enriched posts in %d ms", batch_id, len(ops), ms)
    return write_batch


def publish_status(db, spark):
    """Record where the Spark UI lives so the dashboard can link to it."""
    sc = spark.sparkContext
    db.pipeline_status.replace_one({"_id": "spark"}, {
        "ui_url": sc.uiWebUrl, "app_id": sc.applicationId, "app_name": sc.appName, "master": sc.master,
        "spark_version": spark.version, "started_at": datetime.now(timezone.utc)}, upsert=True)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--once", action="store_true", help="process existing files then exit")
    ap.add_argument("--raw-dir", default=config.RAW_DIR)
    ap.add_argument("--checkpoint", default=config.CHECKPOINT_DIR / "stream")
    ap.add_argument("--db", default=config.MONGO_DB)
    ap.add_argument("--interval", default="5 seconds")
    a = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    db = ensure_indexes(get_db(a.db))
    spark = build_spark()
    spark.sparkContext.setLogLevel("ERROR")
    publish_status(db, spark)
    writer = (enriched_stream(spark, a.raw_dir).writeStream
              .foreachBatch(make_writer(a.db))
              .option("checkpointLocation", str(a.checkpoint)))
    writer = writer.trigger(availableNow=True) if a.once else writer.trigger(processingTime=a.interval)
    log.info("streaming from %s -> MongoDB %s", a.raw_dir, a.db)
    writer.start().awaitTermination()


if __name__ == "__main__":
    main()
