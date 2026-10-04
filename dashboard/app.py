"""Streamlit dashboard: live alert feed, mention timeline, region map and evidence snippets.

  streamlit run dashboard/app.py
"""
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import streamlit as st  # noqa: E402

from common import config  # noqa: E402
from common.db import ensure_indexes, get_db  # noqa: E402
from common.keywords import KEYWORDS  # noqa: E402
from common.regions import REGIONS  # noqa: E402
from dashboard import data  # noqa: E402

st.set_page_config(page_title="Disaster Early-Warning", page_icon="🚨", layout="wide")


@st.cache_resource
def db_handle(name):
    return ensure_indexes(get_db(name))


st.title("🚨 Disaster Early-Warning System")
st.caption("Reddit chatter → Spark Structured Streaming → MongoDB → spike detector")

with st.sidebar:
    st.header("Controls")
    db_name = st.text_input("MongoDB database", config.MONGO_DB)
    hours = st.select_slider("Look-back", options=[1, 3, 6, 12, 24, 72], value=3, format_func=lambda h: f"{h} h")
    bucket = st.select_slider("Timeline bucket", options=[1, 5, 10, 15, 30], value=5, format_func=lambda m: f"{m} min")
    regions = st.multiselect("Regions", sorted(REGIONS), placeholder="All regions")
    keywords = st.multiselect("Disaster type", list(KEYWORDS), placeholder="All types")
    refresh = st.select_slider("Auto-refresh", options=[0, 5, 10, 30, 60], value=10,
                               format_func=lambda s: "off" if s == 0 else f"{s} s")

try:
    db = db_handle(db_name)
    db.command("ping")
except Exception as e:  # noqa: BLE001
    st.error(f"Cannot reach MongoDB: {e}")
    st.stop()


def ago(ts):
    secs = int((datetime.now(timezone.utc) - ts.replace(tzinfo=timezone.utc)).total_seconds())
    return f"{secs}s ago" if secs < 90 else f"{secs // 60} min ago" if secs < 5400 else f"{secs // 3600} h ago"


@st.fragment(run_every=refresh or None)
def live():
    k = data.kpis(db, hours, regions, keywords)
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Mentions", f"{k['posts']:,}")
    c2.metric("Active alerts (15 min)", data.active_alert_count(db))
    c3.metric("Regions with chatter", k["regions"])
    c4.metric("Avg sentiment", "–" if k["avg_sentiment"] is None else f"{k['avg_sentiment']:+.2f}",
              help="VADER compound score: -1 very negative/urgent, +1 very positive")

    st.subheader("Alert feed")
    alerts = data.recent_alerts(db, 10)
    if not alerts:
        st.success("No alerts so far. Mention volume looks normal.")
    for a in alerts:
        box = st.error if a["severity"] == "critical" else st.warning
        box(f"**{a['severity'].upper()} · {a['region']}** — {a['count']} '{a['top_keyword']}' mentions in "
            f"{a['window_minutes']} min (normal ≈ {a['baseline_mean']:.1f}, z = {a['z_score']:.1f}, "
            f"sentiment {a['avg_sentiment']:+.2f}) · {ago(a['updated_at'])}",
            icon="🚨" if a["severity"] == "critical" else "⚠️")

    left, right = st.columns([3, 2])
    with left:
        st.subheader("Mention timeline")
        tl = data.timeline(db, hours, bucket, regions, keywords)
        if tl.empty:
            st.info("No data in this window yet.")
        else:
            st.line_chart(tl, y_label="mentions per bucket", x_label="time (UTC)")
    with right:
        st.subheader("Region map")
        rs = data.region_summary(db, hours, regions, keywords)
        if rs.empty:
            st.info("No regional data yet.")
        else:
            alerting = {a["region"] for a in alerts if (datetime.now(timezone.utc) - a["updated_at"].replace(
                tzinfo=timezone.utc)).total_seconds() < 900}
            rs["size"] = 150_000 + 400_000 * rs.mentions / rs.mentions.max()
            rs["color"] = rs.region.map(lambda r: "#e5383b" if r in alerting else "#4c78a8aa")
            st.map(rs, latitude="latitude", longitude="longitude", size="size", color="color")
            st.caption("🔴 region with an active alert · 🔵 other regions · size = mentions")
            st.dataframe(rs[["region", "mentions", "avg_sentiment"]].round(2), hide_index=True, height=200)

    st.subheader("Evidence: latest matching posts")
    pick = st.selectbox("Region", ["All"] + sorted(REGIONS), key="evidence_region")
    for p in data.recent_posts(db, 12, None if pick == "All" else pick, hours, keywords):
        st.markdown(f"`{p['timestamp']:%H:%M:%S}` **{p['region']}** · {p['keyword_matched']} · "
                    f"sentiment {p['sentiment_score']:+.2f} — {p['text_snippet']} [source]({p['permalink']})")


live()
