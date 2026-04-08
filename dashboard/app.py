"""
dashboard/app.py — Belgrade Parking Analytics Dashboard (Step 9).

Pages
-----
  1. Live Overview      — all 27 locations right now, auto-refreshes every 60s
  2. Historical         — occupancy trend + hour-of-day heatmap per location
  3. Events             — upcoming city events with nearby parking impact
  4. Data Quality       — scrape health, missing data, per-location row counts
"""

import os
from datetime import datetime, timedelta, timezone

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2
import psycopg2.extras
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────────────────────────────────────
# Page config
# ─────────────────────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Belgrade Parking",
    page_icon="🅿️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────────────────────────────────────
# Database connection
# ─────────────────────────────────────────────────────────────────────────────

@st.cache_resource
def get_conn():
    """Persistent psycopg2 connection, cached across reruns."""
    raw = os.environ.get("DATABASE_URL", "")
    dsn = raw.replace("postgresql+asyncpg://", "postgresql://")
    return psycopg2.connect(dsn, cursor_factory=psycopg2.extras.RealDictCursor)


def query(sql: str, params=None) -> pd.DataFrame:
    """Run a SQL query and return a DataFrame. Reconnects on dropped connection."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            rows = cur.fetchall()
            if not rows:
                return pd.DataFrame()
            return pd.DataFrame([dict(r) for r in rows])
    except psycopg2.OperationalError:
        # connection dropped — clear cache and retry once
        get_conn.clear()
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            rows = cur.fetchall()
            return pd.DataFrame([dict(r) for r in rows]) if rows else pd.DataFrame()


# ─────────────────────────────────────────────────────────────────────────────
# Shared helpers
# ─────────────────────────────────────────────────────────────────────────────

BELGRADE_TZ = timezone(timedelta(hours=2))   # CEST; adjust to +1 in winter

def occ_color(pct):
    """Return a hex color for an occupancy percentage."""
    if pd.isna(pct):
        return "#9ca3af"
    if pct < 50:
        return "#22c55e"
    if pct < 80:
        return "#f59e0b"
    return "#ef4444"


def pct_bar_html(pct, width=120):
    """Render a small inline HTML progress bar."""
    if pd.isna(pct):
        return "<span style='color:#9ca3af'>No data</span>"
    color = occ_color(pct)
    filled = int(width * float(pct) / 100)
    return (
        f"<div style='display:flex;align-items:center;gap:6px'>"
        f"<div style='width:{width}px;height:12px;background:#e5e7eb;border-radius:6px;overflow:hidden'>"
        f"<div style='width:{filled}px;height:100%;background:{color};border-radius:6px'></div>"
        f"</div>"
        f"<span style='font-size:13px;color:{color};font-weight:600'>{pct:.0f}%</span>"
        f"</div>"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Sidebar navigation
# ─────────────────────────────────────────────────────────────────────────────

st.sidebar.image(
    "https://upload.wikimedia.org/wikipedia/commons/thumb/f/ff/"
    "Coat_of_arms_of_Belgrade.svg/120px-Coat_of_arms_of_Belgrade.svg.png",
    width=60,
)
st.sidebar.title("Belgrade Parking")
st.sidebar.caption("Analytics Dashboard")

page = st.sidebar.radio(
    "Navigate",
    ["🟢 Live Overview", "📈 Historical", "🎭 Events", "🔍 Data Quality"],
    label_visibility="collapsed",
)

st.sidebar.divider()
st.sidebar.caption(
    f"Local time: {datetime.now(BELGRADE_TZ).strftime('%d %b %Y  %H:%M')}"
)


# ═════════════════════════════════════════════════════════════════════════════
# PAGE 1 — Live Overview
# ═════════════════════════════════════════════════════════════════════════════

if page == "🟢 Live Overview":

    st.title("🟢 Live Overview")
    st.caption("Current free-spot counts for all Belgrade parking locations. Refreshes every 60 s.")

    # Auto-refresh every 60 seconds
    st.markdown(
        "<meta http-equiv='refresh' content='60'>",
        unsafe_allow_html=True,
    )

    df = query("""
        SELECT
            l.name,
            l.location_type,
            l.neighborhood,
            l.address,
            l.total_spots,
            s.free_spots,
            s.occupancy_pct,
            s.scraped_at AT TIME ZONE 'Europe/Belgrade' AS scraped_at
        FROM parking_locations l
        LEFT JOIN LATERAL (
            SELECT free_spots, occupancy_pct, scraped_at
            FROM parking_snapshots
            WHERE location_id = l.id
            ORDER BY scraped_at DESC
            LIMIT 1
        ) s ON TRUE
        ORDER BY
            CASE WHEN s.occupancy_pct IS NULL THEN 1 ELSE 0 END,
            s.occupancy_pct DESC NULLS LAST
    """)

    if df.empty:
        st.warning("No data yet — waiting for the first scrape cycle.")
        st.stop()

    # ── Summary metric strip ──
    total_free  = df["free_spots"].sum(skipna=True)
    total_spots = df["total_spots"].sum(skipna=True)
    live_count  = df["free_spots"].notna().sum()
    overall_occ = (
        round((total_spots - total_free) / total_spots * 100, 1)
        if total_spots > 0 else None
    )

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Free spots (total)", f"{int(total_free):,}")
    c2.metric("Capacity (known)", f"{int(total_spots):,}")
    c3.metric("Overall occupancy", f"{overall_occ}%" if overall_occ else "—")
    c4.metric("Locations reporting", f"{live_count} / {len(df)}")

    st.divider()

    # ── Split into garages vs parking lots ──
    for loc_type, label in [("garage", "🏢 Garages"), ("parking_lot", "🅿️ Parking Lots")]:
        subset = df[df["location_type"] == loc_type].copy()
        if subset.empty:
            continue

        st.subheader(label)

        # Build display DataFrame
        rows = []
        for _, r in subset.iterrows():
            name = r["name"].replace('Garaža "', "").replace('Parkiralište "', "").rstrip('"')
            scraped = (
                r["scraped_at"].strftime("%H:%M:%S")
                if pd.notna(r["scraped_at"]) else "—"
            )
            rows.append({
                "Name":        name,
                "Neighborhood": r["neighborhood"] or "—",
                "Address":     r["address"] or "—",
                "Free":        int(r["free_spots"]) if pd.notna(r["free_spots"]) else "—",
                "Total":       int(r["total_spots"]) if pd.notna(r["total_spots"]) else "—",
                "Occupancy":   pct_bar_html(r["occupancy_pct"]),
                "Updated":     scraped,
            })

        display_df = pd.DataFrame(rows)
        st.markdown(
            display_df.to_html(escape=False, index=False),
            unsafe_allow_html=True,
        )
        st.write("")

    # ── Occupancy bar chart ──
    st.divider()
    st.subheader("Occupancy at a glance")

    chart_df = df.dropna(subset=["occupancy_pct"]).copy()
    chart_df["short_name"] = (
        chart_df["name"]
        .str.replace(r'Garaža "', "", regex=True)
        .str.replace(r'Parkiralište "', "", regex=True)
        .str.rstrip('"')
    )
    chart_df["color"] = chart_df["occupancy_pct"].apply(occ_color)
    chart_df = chart_df.sort_values("occupancy_pct", ascending=True)

    fig = px.bar(
        chart_df,
        x="occupancy_pct",
        y="short_name",
        orientation="h",
        color="color",
        color_discrete_map="identity",
        text=chart_df["occupancy_pct"].apply(lambda v: f"{v:.0f}%"),
        labels={"occupancy_pct": "Occupancy (%)", "short_name": ""},
        range_x=[0, 105],
        height=max(340, len(chart_df) * 30),
    )
    fig.update_traces(
        textposition="outside",
        textfont=dict(size=12, color="#111827"),
        cliponaxis=False,
    )
    fig.update_layout(
        showlegend=False,
        margin=dict(l=0, r=60, t=10, b=10),
        xaxis=dict(
            ticksuffix="%",
            tickfont=dict(size=13, color="#374151"),
            title_font=dict(size=14, color="#111827"),
            gridcolor="#f3f4f6",
        ),
        yaxis=dict(
            tickfont=dict(size=13, color="#111827"),
            title="",
        ),
        plot_bgcolor="white",
        paper_bgcolor="white",
        font=dict(family="sans-serif", size=13, color="#111827"),
    )
    st.plotly_chart(fig, use_container_width=True)


# ═════════════════════════════════════════════════════════════════════════════
# PAGE 2 — Historical Analysis
# ═════════════════════════════════════════════════════════════════════════════

elif page == "📈 Historical":

    st.title("📈 Historical Analysis")

    # ── Controls ──
    locations_df = query(
        "SELECT id, name FROM parking_locations ORDER BY name"
    )
    if locations_df.empty:
        st.warning("No locations found.")
        st.stop()

    name_to_id = dict(zip(locations_df["name"], locations_df["id"]))

    col1, col2 = st.columns([3, 1])
    with col1:
        selected_name = st.selectbox(
            "Location",
            options=list(name_to_id.keys()),
            format_func=lambda n: (
                n.replace('Garaža "', "🏢 ").replace('Parkiralište "', "🅿️ ").rstrip('"')
            ),
        )
    with col2:
        period = st.selectbox("Period", ["24h", "7d", "30d"], index=0)

    loc_id = name_to_id[selected_name]

    interval_map = {"24h": "24 hours", "7d": "7 days", "30d": "30 days"}
    interval = interval_map[period]

    # ── Hourly averages ──
    hist_df = query("""
        SELECT
            date_trunc('hour', scraped_at) AT TIME ZONE 'Europe/Belgrade' AS hour,
            ROUND(AVG(free_spots))::int          AS free_spots,
            MAX(total_spots)                     AS total_spots,
            ROUND(AVG(occupancy_pct)::numeric,1) AS occupancy_pct
        FROM parking_snapshots
        WHERE location_id = %s
          AND scraped_at >= NOW() - INTERVAL %s
        GROUP BY date_trunc('hour', scraped_at)
        ORDER BY hour ASC
    """, (loc_id, interval))

    short_name = (
        selected_name
        .replace('Garaža "', "")
        .replace('Parkiralište "', "")
        .rstrip('"')
    )

    if hist_df.empty:
        st.info(f"No data for {short_name} in the last {period}.")
        st.stop()

    # ── Trend chart ──
    st.subheader(f"{short_name} — occupancy trend ({period})")

    hist_df["color"] = hist_df["occupancy_pct"].apply(occ_color)

    fig_trend = go.Figure()
    fig_trend.add_trace(go.Scatter(
        x=hist_df["hour"],
        y=hist_df["occupancy_pct"],
        mode="lines+markers",
        line=dict(color="#1d4ed8", width=2.5),
        marker=dict(
            color=hist_df["color"],
            size=8,
            line=dict(color="white", width=1.5),
        ),
        hovertemplate="%{x|%a %d %b %H:%M}<br><b>Occupancy: %{y:.0f}%</b><extra></extra>",
        name="Occupancy %",
    ))

    # Free spots on secondary axis
    fig_trend.add_trace(go.Scatter(
        x=hist_df["hour"],
        y=hist_df["free_spots"],
        mode="lines+markers",
        line=dict(color="#0369a1", width=1.5, dash="dot"),
        marker=dict(size=4, color="#0369a1"),
        yaxis="y2",
        hovertemplate="%{x|%a %d %b %H:%M}<br><b>Free spots: %{y}</b><extra></extra>",
        name="Free spots",
    ))

    fig_trend.update_layout(
        yaxis=dict(
            title="Occupancy %",
            title_font=dict(size=13, color="#111827"),
            tickfont=dict(size=12, color="#374151"),
            range=[0, 105],
            ticksuffix="%",
            dtick=20,
            gridcolor="#e5e7eb",
            gridwidth=1,
            zeroline=False,
        ),
        yaxis2=dict(
            title="Free spots",
            title_font=dict(size=13, color="#0369a1"),
            tickfont=dict(size=12, color="#0369a1"),
            overlaying="y",
            side="right",
            showgrid=False,
            zeroline=False,
        ),
        xaxis=dict(
            tickfont=dict(size=12, color="#374151"),
            showgrid=False,
        ),
        hovermode="x unified",
        legend=dict(
            orientation="h", yanchor="bottom", y=1.02,
            font=dict(size=13, color="#111827"),
        ),
        plot_bgcolor="white",
        paper_bgcolor="white",
        margin=dict(l=0, r=0, t=30, b=0),
        height=340,
        font=dict(family="sans-serif", size=13, color="#111827"),
    )
    st.plotly_chart(fig_trend, use_container_width=True)

    # ── Hour-of-day heatmap ──
    st.subheader(f"{short_name} — average occupancy by day & hour")
    st.caption("Shows patterns across the full selected period — when is this location busiest?")

    heatmap_df = query("""
        SELECT
            EXTRACT(DOW  FROM scraped_at AT TIME ZONE 'Europe/Belgrade')::int AS dow,
            EXTRACT(HOUR FROM scraped_at AT TIME ZONE 'Europe/Belgrade')::int AS hour,
            ROUND(AVG(occupancy_pct)::numeric, 1) AS avg_occ
        FROM parking_snapshots
        WHERE location_id = %s
          AND scraped_at >= NOW() - INTERVAL %s
          AND occupancy_pct IS NOT NULL
        GROUP BY dow, hour
        ORDER BY dow, hour
    """, (loc_id, interval))

    if not heatmap_df.empty:
        # Pivot: rows = days, cols = hours 0-23
        pivot = heatmap_df.pivot(index="dow", columns="hour", values="avg_occ")
        pivot = pivot.reindex(index=range(7), columns=range(24))  # ensure full grid

        day_labels = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
        pivot.index = [day_labels[i] for i in pivot.index]

        fig_heat = px.imshow(
            pivot,
            labels=dict(x="Hour of day", y="", color="Avg occupancy %"),
            color_continuous_scale=[
                [0.0,  "#16a34a"],   # dark green  — empty
                [0.5,  "#fbbf24"],   # amber        — half full
                [1.0,  "#dc2626"],   # dark red     — full
            ],
            zmin=0, zmax=100,
            aspect="auto",
            text_auto=".0f",
        )
        fig_heat.update_traces(
            textfont=dict(size=12, color="white"),
        )
        fig_heat.update_layout(
            margin=dict(l=0, r=0, t=10, b=0),
            height=300,
            font=dict(family="sans-serif", size=13, color="#111827"),
            coloraxis_colorbar=dict(
                ticksuffix="%",
                tickfont=dict(size=12, color="#374151"),
                title_font=dict(size=12),
                thickness=14,
                len=0.9,
            ),
        )
        fig_heat.update_xaxes(
            tickvals=list(range(0, 24, 2)),
            ticktext=[f"{h}:00" for h in range(0, 24, 2)],
            tickfont=dict(size=12, color="#374151"),
            title_font=dict(size=13, color="#111827"),
        )
        fig_heat.update_yaxes(
            tickfont=dict(size=13, color="#111827"),
        )
        st.plotly_chart(fig_heat, use_container_width=True)
    else:
        st.info("Not enough data yet to show a heatmap — needs at least a few days of scraping.")

    # ── Summary stats ──
    st.divider()
    s1, s2, s3, s4 = st.columns(4)
    s1.metric("Avg occupancy", f"{hist_df['occupancy_pct'].mean():.1f}%" if not hist_df.empty else "—")
    s2.metric("Peak occupancy", f"{hist_df['occupancy_pct'].max():.1f}%" if not hist_df.empty else "—")
    s3.metric("Lowest occupancy", f"{hist_df['occupancy_pct'].min():.1f}%" if not hist_df.empty else "—")
    s4.metric("Hours of data", str(len(hist_df)))


# ═════════════════════════════════════════════════════════════════════════════
# PAGE 3 — Events
# ═════════════════════════════════════════════════════════════════════════════

elif page == "🎭 Events":

    st.title("🎭 Upcoming City Events")
    st.caption("Events scraped daily from Belgrade venues. Updated every day at 03:00 UTC.")

    days = st.slider("Show events for the next N days", 1, 30, 14)

    events_df = query("""
        SELECT
            event_name,
            event_type,
            venue_name,
            event_date,
            event_time,
            expected_attendance
        FROM city_events
        WHERE event_date >= CURRENT_DATE
          AND event_date <= CURRENT_DATE + (%s * INTERVAL '1 day')
        ORDER BY event_date, event_time NULLS LAST
    """, (days,))

    if events_df.empty:
        st.info("No upcoming events found.")
        st.stop()

    # ── Metric strip ──
    e1, e2, e3 = st.columns(3)
    e1.metric("Upcoming events", len(events_df))
    e2.metric(
        "Total expected attendance",
        f"{int(events_df['expected_attendance'].sum(skipna=True)):,}"
        if events_df["expected_attendance"].notna().any() else "—",
    )
    e3.metric("Venues", events_df["venue_name"].nunique())

    st.divider()

    # ── Event type color map ──
    TYPE_COLORS = {
        "concert":   "#8b5cf6",
        "theatre":   "#3b82f6",
        "sports":    "#22c55e",
        "religious": "#f59e0b",
        "festival":  "#ec4899",
        "other":     "#9ca3af",
    }
    TYPE_ICONS = {
        "concert":   "🎵",
        "theatre":   "🎭",
        "sports":    "⚽",
        "religious": "⛪",
        "festival":  "🎉",
        "other":     "📅",
    }

    # ── Group by date ──
    events_df["event_date"] = pd.to_datetime(events_df["event_date"])
    for date, group in events_df.groupby("event_date"):
        date_label = date.strftime("%A, %d %B %Y")
        st.subheader(date_label)

        for _, ev in group.iterrows():
            etype = ev["event_type"] or "other"
            icon  = TYPE_ICONS.get(etype, "📅")
            color = TYPE_COLORS.get(etype, "#9ca3af")
            time_str = (
                ev["event_time"].strftime("%H:%M")
                if pd.notna(ev["event_time"]) else "Time TBD"
            )
            att = (
                f"{int(ev['expected_attendance']):,} expected"
                if pd.notna(ev["expected_attendance"]) else ""
            )

            st.markdown(
                f"<div style='border-left:4px solid {color};padding:8px 12px;"
                f"margin-bottom:8px;border-radius:0 6px 6px 0;background:#f9fafb'>"
                f"<strong style='color:#111827'>{icon} {ev['event_name']}</strong><br>"
                f"<span style='color:#6b7280;font-size:13px'>"
                f"{ev['venue_name']} &nbsp;·&nbsp; {time_str}"
                f"{'&nbsp;·&nbsp;' + att if att else ''}"
                f"</span></div>",
                unsafe_allow_html=True,
            )

        st.write("")

    # ── Events by venue chart ──
    st.divider()
    st.subheader("Events by venue")
    venue_counts = events_df["venue_name"].value_counts().reset_index()
    venue_counts.columns = ["Venue", "Count"]
    fig_v = px.bar(
        venue_counts, x="Count", y="Venue", orientation="h",
        color_discrete_sequence=["#1d4ed8"],
        text="Count",
        height=max(220, len(venue_counts) * 48),
    )
    fig_v.update_traces(
        textposition="outside",
        textfont=dict(size=13, color="#111827"),
        cliponaxis=False,
    )
    fig_v.update_layout(
        margin=dict(l=0, r=40, t=10, b=0),
        plot_bgcolor="white",
        paper_bgcolor="white",
        font=dict(family="sans-serif", size=13, color="#111827"),
        xaxis=dict(
            title="Number of events",
            tickfont=dict(size=12, color="#374151"),
            title_font=dict(size=13, color="#111827"),
            gridcolor="#f3f4f6",
        ),
        yaxis=dict(
            title="",
            tickfont=dict(size=13, color="#111827"),
        ),
    )
    st.plotly_chart(fig_v, use_container_width=True)


# ═════════════════════════════════════════════════════════════════════════════
# PAGE 4 — Data Quality
# ═════════════════════════════════════════════════════════════════════════════

elif page == "🔍 Data Quality":

    st.title("🔍 Data Quality & Health")

    # ── Scrape cycle health ──
    st.subheader("Scrape cycle health (last 2 hours)")

    cycles_df = query("""
        SELECT
            date_trunc('minute', scraped_at) AT TIME ZONE 'Europe/Belgrade' AS minute,
            COUNT(DISTINCT location_id) AS locations_scraped
        FROM parking_snapshots
        WHERE scraped_at >= NOW() - INTERVAL '2 hours'
        GROUP BY date_trunc('minute', scraped_at)
        ORDER BY minute DESC
        LIMIT 30
    """)

    if not cycles_df.empty:
        c1, c2, c3 = st.columns(3)
        c1.metric("Cycles in last 2h", len(cycles_df))
        c2.metric("Avg locations/cycle", f"{cycles_df['locations_scraped'].mean():.1f}")
        c3.metric(
            "Last cycle",
            cycles_df["minute"].iloc[0].strftime("%H:%M:%S")
            if len(cycles_df) > 0 else "—",
        )

        fig_cycles = px.bar(
            cycles_df.sort_values("minute"),
            x="minute", y="locations_scraped",
            labels={"minute": "Time", "locations_scraped": "Locations scraped"},
            color_discrete_sequence=["#22c55e"],
            height=240,
        )
        fig_cycles.add_hline(
            y=26, line_dash="dash", line_color="#ef4444", line_width=2,
            annotation_text="Expected: 26", annotation_position="top right",
            annotation_font=dict(size=12, color="#ef4444"),
        )
        fig_cycles.update_layout(
            margin=dict(l=0, r=0, t=10, b=0),
            plot_bgcolor="white",
            paper_bgcolor="white",
            font=dict(family="sans-serif", size=13, color="#111827"),
            xaxis=dict(
                tickfont=dict(size=12, color="#374151"),
                title_font=dict(size=13, color="#111827"),
                gridcolor="#f3f4f6",
            ),
            yaxis=dict(
                tickfont=dict(size=12, color="#374151"),
                title_font=dict(size=13, color="#111827"),
                gridcolor="#f3f4f6",
                range=[0, 30],
            ),
        )
        st.plotly_chart(fig_cycles, use_container_width=True)
    else:
        st.warning("No scrape data in the last 2 hours — is the scraper container running?")

    st.divider()

    # ── Snapshot counts per location ──
    st.subheader("Snapshot counts per location (all time)")

    counts_df = query("""
        SELECT
            l.name,
            l.location_type,
            l.total_spots,
            COUNT(s.id)                               AS total_snapshots,
            MAX(s.scraped_at) AT TIME ZONE 'Europe/Belgrade' AS last_seen,
            ROUND(AVG(s.occupancy_pct)::numeric, 1)   AS avg_occupancy
        FROM parking_locations l
        LEFT JOIN parking_snapshots s ON s.location_id = l.id
        GROUP BY l.id, l.name, l.location_type, l.total_spots
        ORDER BY total_snapshots DESC
    """)

    if not counts_df.empty:
        counts_df["short_name"] = (
            counts_df["name"]
            .str.replace(r'Garaža "', "🏢 ", regex=True)
            .str.replace(r'Parkiralište "', "🅿️ ", regex=True)
            .str.rstrip('"')
        )
        counts_df["last_seen_str"] = counts_df["last_seen"].apply(
            lambda x: x.strftime("%H:%M  %d %b") if pd.notna(x) else "Never"
        )
        counts_df["total_spots_str"] = counts_df["total_spots"].apply(
            lambda x: str(int(x)) if pd.notna(x) else "❌ Missing"
        )
        counts_df["avg_occ_str"] = counts_df["avg_occupancy"].apply(
            lambda x: f"{x:.1f}%" if pd.notna(x) else "—"
        )

        st.dataframe(
            counts_df[[
                "short_name", "total_spots_str",
                "total_snapshots", "avg_occ_str", "last_seen_str",
            ]].rename(columns={
                "short_name":       "Location",
                "total_spots_str":  "Total spots",
                "total_snapshots":  "Snapshots",
                "avg_occ_str":      "Avg occupancy",
                "last_seen_str":    "Last scraped",
            }),
            use_container_width=True,
            hide_index=True,
        )

    st.divider()

    # ── Missing total_spots ──
    missing_df = query("""
        SELECT name, location_type, address, neighborhood
        FROM parking_locations
        WHERE total_spots IS NULL
        ORDER BY location_type, name
    """)

    st.subheader(f"Locations missing total_spots ({len(missing_df)})")
    if missing_df.empty:
        st.success("All locations have total_spots set!")
    else:
        st.caption(
            "These locations can report free spots but not occupancy %. "
            "Add total_spots to db/init.sql and run the UPDATE query to fix."
        )
        missing_df["short_name"] = (
            missing_df["name"]
            .str.replace(r'Garaža "', "🏢 ", regex=True)
            .str.replace(r'Parkiralište "', "🅿️ ", regex=True)
            .str.rstrip('"')
        )
        st.dataframe(
            missing_df[["short_name", "location_type", "address", "neighborhood"]]
            .rename(columns={
                "short_name":    "Location",
                "location_type": "Type",
                "address":       "Address",
                "neighborhood":  "Neighborhood",
            }),
            use_container_width=True,
            hide_index=True,
        )

    st.divider()

    # ── Total DB size ──
    st.subheader("Database summary")
    totals_df = query("""
        SELECT
            COUNT(*)                                       AS total_snapshots,
            COUNT(DISTINCT location_id)                    AS locations,
            MIN(scraped_at) AT TIME ZONE 'Europe/Belgrade' AS oldest,
            MAX(scraped_at) AT TIME ZONE 'Europe/Belgrade' AS newest,
            ROUND(
                (pg_total_relation_size('parking_snapshots') / 1024.0 / 1024.0)::numeric, 2
            ) AS size_mb
        FROM parking_snapshots
    """)

    if not totals_df.empty:
        r = totals_df.iloc[0]
        d1, d2, d3, d4 = st.columns(4)
        d1.metric("Total snapshots", f"{int(r['total_snapshots']):,}")
        d2.metric("Table size", f"{r['size_mb']} MB")
        d3.metric(
            "Data since",
            r["oldest"].strftime("%d %b %H:%M") if pd.notna(r["oldest"]) else "—",
        )
        d4.metric(
            "Latest snapshot",
            r["newest"].strftime("%d %b %H:%M") if pd.notna(r["newest"]) else "—",
        )
