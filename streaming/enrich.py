"""Pure enrichment logic (region tagging, VADER sentiment, snippet) - unit-testable without Spark."""
import re
from datetime import datetime, timezone

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

from common.keywords import match_keywords
from common.regions import tag_region

SNIPPET_LEN = 200
_analyzer = None


def sentiment(text):
    """VADER compound score in [-1, 1]; strongly negative = urgent/distressed."""
    global _analyzer
    if _analyzer is None:
        _analyzer = SentimentIntensityAnalyzer()
    return round(_analyzer.polarity_scores(text or "")["compound"], 4)


def snippet(text, n=SNIPPET_LEN):
    text = re.sub(r"\s+", " ", text or "").strip()
    return text if len(text) <= n else text[: n - 1].rstrip() + "…"


def enrich_text(title, text, keywords):
    """Return (region, sentiment_score, keyword_matched, text_snippet) for one post/comment."""
    full = f"{title or ''} {text or ''}".strip()
    kws = list(keywords or []) or match_keywords(full)
    return tag_region(full), sentiment(full), (kws[0] if kws else None), snippet(full)


def to_document(r):
    """Map an enriched row (dict-like, from Spark) to the MongoDB document stored in `posts`."""
    return {
        "_id": r["id"],
        "region": r["region"],
        "timestamp": datetime.fromtimestamp(r["created_utc"], tz=timezone.utc),
        "sentiment_score": r["sentiment_score"],
        "keyword_matched": r["keyword_matched"],
        "text_snippet": r["text_snippet"],
        "permalink": r["permalink"],
        "subreddit": r["subreddit"],
        "type": r["type"],
    }
