"""Inject fake Reddit-style disaster posts into raw_data/ (same schema as ingest.py).

Examples:
  python -m ingestion.simulate baseline --minutes 60 --per-minute 1      # quiet history across regions
  python -m ingestion.simulate spike --region Assam --keyword flood --count 40 --spread-seconds 120
  python -m ingestion.simulate one --region Kerala --keyword earthquake --text "Strong tremors felt in Kochi"
"""
import argparse
import random
import time
import uuid

from common.config import RAW_DIR
from common.keywords import match_keywords
from common.regions import REGIONS as _REGIONS
from ingestion.ingest import write_batch

REGIONS = {name: places for name, (_la, _lo, places) in _REGIONS.items()}

TEMPLATES = {
    "flood": [
        "Severe flooding in {place}, {region}. Water is entering homes, please help!",
        "Flood warning for {place}. Rivers rising fast, roads are underwater",
        "Streets in {place} completely flooded, people stranded on rooftops",
        "Is anyone else seeing the flood situation in {region}? {place} looks terrible",
        "Heavy rain caused floods near {place}, evacuations started",
    ],
    "earthquake": [
        "Just felt a strong earthquake in {place}, {region}! Buildings shaking",
        "Earthquake in {place} - anyone safe? Aftershocks continuing",
        "Tremors felt across {region}, {place} reporting cracks in buildings",
        "Major quake near {place}, power is out and people are on the streets",
        "Did anyone feel that earthquake in {place}? Scary",
    ],
    "wildfire": [
        "Wildfire spreading fast near {place}, {region}. Smoke everywhere",
        "Forest fire out of control close to {place}, evacuation ordered",
        "Huge wildfire visible from {place}, sky is orange and ash is falling",
        "Is the {region} wildfire reaching {place}? Roads closed",
        "Wildfire near {place} destroyed several homes, please stay safe",
    ],
}
MILD = [  # low-urgency chatter for baseline traffic
    "Old photos of the {keyword} in {place} years ago, such a memory",
    "Documentary about {keyword} preparedness in {region} is worth a watch",
    "Anyone studying {keyword} risk in {place}? Looking for resources",
]
SUBREDDITS = ["india", "news", "worldnews", "weather", "NaturalDisasters"]


def make_record(region, keyword, now, text=None, mild=False, rng=random):
    place = rng.choice(REGIONS.get(region, [region]))
    pool = MILD if mild else TEMPLATES[keyword]
    body = text or rng.choice(pool).format(place=place, region=region, keyword=keyword)
    rid = "sim" + uuid.uuid4().hex[:8]
    sr = rng.choice(SUBREDDITS)
    is_post = rng.random() < 0.5
    return {
        "id": rid,
        "type": "post" if is_post else "comment",
        "subreddit": sr,
        "created_utc": int(now),
        "title": body if is_post else "",
        "text": "" if is_post else body,
        "permalink": f"https://reddit.com/r/{sr}/comments/{rid}/simulated/",
        "score": rng.randint(1, 500),
        "keywords": match_keywords(body) or [keyword],
    }


def write(records, raw_dir):
    path = write_batch(records, raw_dir)
    print(f"wrote {len(records)} records -> {path}")
    return path


def cmd_baseline(a, rng):
    now = time.time()
    records = []
    for m in range(a.minutes, 0, -1):
        for _ in range(a.per_minute):
            region = rng.choice(a.regions or list(REGIONS))
            kw = rng.choice(list(TEMPLATES))
            ts = now - m * 60 + rng.uniform(0, 59)
            records.append(make_record(region, kw, ts, mild=rng.random() < 0.4, rng=rng))
    return write(records, a.raw_dir)


def cmd_spike(a, rng):
    now = time.time()
    records = [make_record(a.region, a.keyword, now - rng.uniform(0, a.spread_seconds), rng=rng)
               for _ in range(a.count)]
    return write(records, a.raw_dir)


def cmd_one(a, rng):
    return write([make_record(a.region, a.keyword, time.time(), text=a.text, rng=rng)], a.raw_dir)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--raw-dir", default=RAW_DIR)
    ap.add_argument("--seed", type=int, help="make output reproducible")
    sp = ap.add_subparsers(dest="cmd", required=True)

    b = sp.add_parser("baseline", help="quiet background traffic spread over the past N minutes")
    b.add_argument("--minutes", type=int, default=60)
    b.add_argument("--per-minute", type=int, default=1, help="records per minute across all regions")
    b.add_argument("--regions", nargs="+", choices=list(REGIONS))
    b.set_defaults(fn=cmd_baseline)

    s = sp.add_parser("spike", help="burst of posts for one region/keyword")
    s.add_argument("--region", required=True, choices=list(REGIONS))
    s.add_argument("--keyword", required=True, choices=list(TEMPLATES))
    s.add_argument("--count", type=int, default=40)
    s.add_argument("--spread-seconds", type=int, default=120, help="spread timestamps over the last N seconds")
    s.set_defaults(fn=cmd_spike)

    o = sp.add_parser("one", help="a single hand-written record")
    o.add_argument("--region", required=True)
    o.add_argument("--keyword", required=True, choices=list(TEMPLATES))
    o.add_argument("--text", help="custom text (default: random template)")
    o.set_defaults(fn=cmd_one)

    a = ap.parse_args(argv)
    a.fn(a, random.Random(a.seed))


if __name__ == "__main__":
    main()
