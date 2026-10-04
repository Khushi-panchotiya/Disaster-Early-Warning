"""Reddit ingestion: poll subreddits for disaster keywords, write JSON-lines batches to raw_data/."""
import argparse
import json
import logging
import os
import re
import signal
import time
from datetime import datetime, timezone
from pathlib import Path

import praw
import prawcore
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = ROOT / "raw_data"
STATE_FILE = ROOT / "ingestion" / "seen_ids.json"

SUBREDDITS = ["india", "news", "worldnews", "weather", "NaturalDisasters"]

# keyword reported in the record -> regex variants that count as a match
KEYWORDS = {
    "flood": r"flood(?:s|ed|ing|waters?)?",
    "earthquake": r"earthquakes?|quakes?|tremors?|aftershocks?",
    "wildfire": r"wild\s?fires?|forest\s?fires?|bush\s?fires?",
}
KEYWORD_PATTERNS = {k: re.compile(rf"\b(?:{v})\b", re.IGNORECASE) for k, v in KEYWORDS.items()}

MAX_SEEN = 50_000
FETCH_LIMIT = 100

log = logging.getLogger("ingest")
_stop = False


def match_keywords(text):
    """Return the list of disaster keywords found in text."""
    return [k for k, p in KEYWORD_PATTERNS.items() if p.search(text or "")]


def load_seen(path=STATE_FILE):
    try:
        return dict.fromkeys(json.loads(Path(path).read_text()))
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_seen(seen, path=STATE_FILE):
    ids = list(seen)[-MAX_SEEN:]  # dicts keep insertion order -> drop oldest
    tmp = Path(path).with_suffix(".tmp")
    tmp.write_text(json.dumps(ids))
    os.replace(tmp, path)


def submission_record(s, keywords):
    return {
        "id": s.id,
        "type": "post",
        "subreddit": str(s.subreddit),
        "created_utc": int(s.created_utc),
        "title": s.title or "",
        "text": s.selftext or "",
        "permalink": f"https://reddit.com{s.permalink}",
        "score": s.score,
        "keywords": keywords,
    }


def comment_record(c, keywords):
    return {
        "id": c.id,
        "type": "comment",
        "subreddit": str(c.subreddit),
        "created_utc": int(c.created_utc),
        "title": "",
        "text": c.body or "",
        "permalink": f"https://reddit.com{c.permalink}",
        "score": c.score,
        "keywords": keywords,
    }


def collect(reddit, subreddits, seen):
    """One poll: return new matching records and mark every item (match or not) as seen."""
    records = []
    sub = reddit.subreddit("+".join(subreddits))
    for s in sub.new(limit=FETCH_LIMIT):
        key = f"t3_{s.id}"
        if key in seen:
            continue
        seen[key] = None
        kws = match_keywords(f"{s.title} {s.selftext}")
        if kws:
            records.append(submission_record(s, kws))
    for c in sub.comments(limit=FETCH_LIMIT):
        key = f"t1_{c.id}"
        if key in seen:
            continue
        seen[key] = None
        kws = match_keywords(c.body)
        if kws:
            records.append(comment_record(c, kws))
    return records


def write_batch(records, raw_dir=RAW_DIR):
    """Write one JSON-lines file atomically (hidden temp file, then rename) so Spark never reads a partial file."""
    raw_dir = Path(raw_dir)
    raw_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    final = raw_dir / f"batch_{stamp}.json"
    tmp = raw_dir / f".batch_{stamp}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    os.replace(tmp, final)
    return final


def make_reddit():
    load_dotenv(ROOT / ".env")
    cid, secret = os.getenv("REDDIT_CLIENT_ID"), os.getenv("REDDIT_CLIENT_SECRET")
    if not cid or not secret:
        raise SystemExit("Missing REDDIT_CLIENT_ID / REDDIT_CLIENT_SECRET in .env (see .env.example)")
    return praw.Reddit(
        client_id=cid,
        client_secret=secret,
        user_agent=os.getenv("REDDIT_USER_AGENT", "disaster-early-warning by u/unknown"),
        check_for_async=False,
    )


def _handle_signal(signum, _frame):
    global _stop
    log.info("signal %s received, finishing current poll then exiting", signum)
    _stop = True


def sleep_interruptible(seconds):
    end = time.monotonic() + seconds
    while not _stop and time.monotonic() < end:
        time.sleep(min(1, end - time.monotonic()))


def run(reddit, subreddits, interval, max_minutes=None, once=False, raw_dir=RAW_DIR, state_file=STATE_FILE):
    seen = load_seen(state_file)
    deadline = time.monotonic() + max_minutes * 60 if max_minutes else None
    backoff = 0
    total = 0
    log.info("polling r/%s every %ss (%d ids already seen)", "+".join(subreddits), interval, len(seen))
    while not _stop:
        try:
            records = collect(reddit, subreddits, seen)
            if records:
                path = write_batch(records, raw_dir)
                total += len(records)
                log.info("wrote %d records -> %s (total %d)", len(records), path.name, total)
            else:
                log.info("no new matches")
            save_seen(seen, state_file)
            backoff = 0
            wait = interval
        except prawcore.exceptions.TooManyRequests as e:
            retry = getattr(getattr(e, "response", None), "headers", {}).get("retry-after")
            wait = max(int(retry) if retry and str(retry).isdigit() else 0, 60)
            log.warning("rate limited, sleeping %ss", wait)
        except (prawcore.exceptions.RequestException, prawcore.exceptions.ServerError,
                prawcore.exceptions.ResponseException) as e:
            backoff = min(max(backoff * 2, 15), 600)
            wait = backoff
            log.warning("API error (%s: %s), retrying in %ss", type(e).__name__, e, wait)
        except (prawcore.exceptions.OAuthException, prawcore.exceptions.InvalidToken) as e:
            raise SystemExit(f"Reddit authentication failed: {e}. Check credentials in .env")
        if once or (deadline and time.monotonic() + wait >= deadline):
            break
        sleep_interruptible(wait)
    save_seen(seen, state_file)
    log.info("stopped; %d records written this run", total)
    return total


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--interval", type=int, default=60, help="seconds between polls (default 60)")
    ap.add_argument("--subreddits", nargs="+", default=SUBREDDITS)
    ap.add_argument("--max-minutes", type=float, help="stop after this many minutes")
    ap.add_argument("--once", action="store_true", help="poll a single time and exit")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)
    run(make_reddit(), args.subreddits, args.interval, args.max_minutes, args.once)


if __name__ == "__main__":
    main()
