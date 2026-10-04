"""Disaster keyword matching shared by ingestion and enrichment."""
import re

# keyword reported in the record -> regex variants that count as a match
KEYWORDS = {
    "flood": r"flood(?:s|ed|ing|waters?)?",
    "earthquake": r"earthquakes?|quakes?|tremors?|aftershocks?",
    "wildfire": r"wild\s?fires?|forest\s?fires?|bush\s?fires?",
}
KEYWORD_PATTERNS = {k: re.compile(rf"\b(?:{v})\b", re.IGNORECASE) for k, v in KEYWORDS.items()}


def match_keywords(text):
    """Return the list of disaster keywords found in text."""
    return [k for k, p in KEYWORD_PATTERNS.items() if p.search(text or "")]
