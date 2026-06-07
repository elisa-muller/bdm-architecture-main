from __future__ import annotations

import os
from collections import Counter
from dataclasses import dataclass
from typing import Any

import altair as alt
import pandas as pd
import streamlit as st
from deltalake import DeltaTable


st.set_page_config(
    page_title="Music Intelligence Dashboard",
    page_icon="",
    layout="wide",
    initial_sidebar_state="expanded",
)


@dataclass(frozen=True)
class DeltaSource:
    label: str
    uri: str
    description: str


def env(name: str, default: str) -> str:
    return os.getenv(name, default)


def storage_options() -> dict[str, str]:
    endpoint = env("MINIO_ENDPOINT", "http://minio:9000").rstrip("/")
    secure = endpoint.startswith("https://")
    return {
        "AWS_ACCESS_KEY_ID": env("MINIO_ACCESS_KEY", env("MINIO_ROOT_USER", "minioadmin")),
        "AWS_SECRET_ACCESS_KEY": env("MINIO_SECRET_KEY", env("MINIO_ROOT_PASSWORD", "minioadmin")),
        "AWS_REGION": env("AWS_REGION", "us-east-1"),
        "AWS_ENDPOINT_URL": endpoint,
        "AWS_ALLOW_HTTP": "false" if secure else "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }


EXPLOITATION_BUCKET = env("EXPLOITATION_BUCKET", "exploitation")

SOURCES = {
    "trends": DeltaSource(
        label="Song trend aggregates",
        uri=env(
            "EXPLOITATION_TRENDS_AGG_DELTA_URI",
            f"s3://{EXPLOITATION_BUCKET}/semi_structured/trends/delta/song_trend_aggregates_delta",
        ),
        description="Aggregated social-media trend signals by song.",
    ),
    "audio": DeltaSource(
        label="Song audio features",
        uri=env(
            "EXPLOITATION_SONG_AUDIO_FEATURES_DELTA_URI",
            f"s3://{EXPLOITATION_BUCKET}/structured/song_audio_features/delta/song_audio_features_delta",
        ),
        description="Joined Last.fm, MusicBrainz and ReccoBeats attributes.",
    ),
    "recommender": DeltaSource(
        label="Recommender song features",
        uri=env(
            "EXPLOITATION_RECOMMENDER_FEATURES_DELTA_URI",
            f"s3://{EXPLOITATION_BUCKET}/recommender/song_features/delta/song_recommender_features_delta",
        ),
        description="Audio features enriched with trend signals.",
    ),
    "feedback_summary": DeltaSource(
        label="Recommendation feedback summary",
        uri=env(
            "EXPLOITATION_RECOMMENDATION_FEEDBACK_SUMMARY_DELTA_URI",
            f"s3://{EXPLOITATION_BUCKET}/consumption/recommendations/feedback_summary_delta",
        ),
        description="Daily recommendation acceptance and satisfaction KPIs.",
    ),
    "outcomes": DeltaSource(
        label="Recommendation outcomes",
        uri=env(
            "EXPLOITATION_RECOMMENDATION_OUTCOMES_DELTA_URI",
            f"s3://{EXPLOITATION_BUCKET}/consumption/recommendations/recommendation_outcomes_delta",
        ),
        description="Recommendation requests joined with latest feedback outcome.",
    ),
}


@st.cache_data(ttl=60, show_spinner=False)
def read_delta(uri: str) -> tuple[pd.DataFrame, str | None]:
    try:
        table = DeltaTable(uri, storage_options=storage_options())
        return table.to_pandas(), None
    except Exception as exc:
        return pd.DataFrame(), str(exc)


def format_number(value: Any) -> str:
    if value is None or pd.isna(value):
        return "0"
    value = float(value)
    if abs(value) >= 1_000_000:
        return f"{value / 1_000_000:.1f}M"
    if abs(value) >= 1_000:
        return f"{value / 1_000:.1f}K"
    if value.is_integer():
        return f"{int(value):,}"
    return f"{value:,.2f}"


def as_rate(value: Any) -> str:
    if value is None or pd.isna(value):
        return "0.0%"
    return f"{float(value) * 100:.1f}%"


def safe_sum(df: pd.DataFrame, column: str) -> float:
    if column not in df.columns or df.empty:
        return 0
    return pd.to_numeric(df[column], errors="coerce").fillna(0).sum()


def safe_mean(df: pd.DataFrame, column: str) -> float:
    if column not in df.columns or df.empty:
        return 0
    series = pd.to_numeric(df[column], errors="coerce").dropna()
    return float(series.mean()) if not series.empty else 0


def has_real_values(df: pd.DataFrame, column: str) -> bool:
    if column not in df.columns or df.empty:
        return False
    series = df[column]
    if pd.api.types.is_numeric_dtype(series):
        return pd.to_numeric(series, errors="coerce").fillna(0).ne(0).any()
    return series.map(is_real_value).any()


def is_real_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, (list, tuple, set)):
        return len(value) > 0
    if hasattr(value, "tolist"):
        converted = value.tolist()
        if isinstance(converted, list):
            return len(converted) > 0
        value = converted
    try:
        if pd.isna(value):
            return False
    except (TypeError, ValueError):
        pass
    return str(value).strip().lower() not in {"", "none", "nan", "[]"}


def list_value(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        raw_values = value
    elif hasattr(value, "tolist"):
        raw_values = value.tolist()
    else:
        try:
            if pd.isna(value):
                return []
        except (TypeError, ValueError):
            pass
        raw_values = [value]

    values = []
    for item in raw_values:
        text = str(item).strip()
        if text and text.lower() not in {"none", "nan", "[]"}:
            values.append(text)
    return values


def chart_title(title: str) -> alt.TitleParams:
    return alt.TitleParams(text=title, anchor="middle")


def date_filter(df: pd.DataFrame, key: str, default_latest: bool = True) -> pd.DataFrame:
    date_columns = [
        column
        for column in ["event_date", "snapshot_date", "last_event_ts_utc", "recommendation_event_ts"]
        if column in df.columns
    ]
    if not date_columns or df.empty:
        return df

    column = date_columns[0]
    parsed = pd.to_datetime(df[column], errors="coerce", utc=True)
    valid = parsed.dropna()
    if valid.empty:
        return df

    min_date = valid.min().date()
    max_date = valid.max().date()
    default_value = (max_date, max_date) if default_latest else (min_date, max_date)
    selected = st.sidebar.date_input(
        "Date range",
        value=default_value,
        min_value=min_date,
        max_value=max_date,
        key=key,
    )
    if isinstance(selected, tuple) and len(selected) == 2:
        start, end = selected
        mask = parsed.dt.date.between(start, end)
        return df[mask.fillna(False)]
    return df


def bar_chart(
    df: pd.DataFrame,
    x: str,
    y: str,
    title: str,
    x_title: str,
    y_title: str,
    tooltip: list[str] | None = None,
) -> alt.Chart:
    return (
        alt.Chart(df)
        .mark_bar(color="#0f6cbd")
        .encode(
            x=alt.X(f"{x}:N", title=x_title, sort="-y"),
            y=alt.Y(f"{y}:Q", title=y_title),
            tooltip=tooltip or [x, y],
        )
        .properties(title=chart_title(title), height=360)
    )


def line_chart(
    df: pd.DataFrame,
    x: str,
    y: str,
    color: str,
    title: str,
    x_title: str,
    y_title: str,
) -> alt.Chart:
    return (
        alt.Chart(df)
        .mark_line(point=True)
        .encode(
            x=alt.X(f"{x}:T", title=x_title),
            y=alt.Y(f"{y}:Q", title=y_title),
            color=alt.Color(f"{color}:N", title="Metric"),
            tooltip=[x, color, y],
        )
        .properties(title=chart_title(title), height=360)
    )


def pie_chart(df: pd.DataFrame, title: str) -> alt.Chart:
    df = df.copy()
    total = pd.to_numeric(df["count"], errors="coerce").fillna(0).sum()
    df["percentage"] = 0.0 if total == 0 else df["count"] / total
    df["legend_label"] = df.apply(
        lambda row: f"{row['hashtag']} {row['percentage']:.0%}",
        axis=1,
    )

    donut = (
        alt.Chart(df)
        .mark_arc(innerRadius=72, outerRadius=150, stroke="#ffffff", strokeWidth=2)
        .encode(
            theta=alt.Theta("count:Q", title="Mentions"),
            color=alt.Color(
                "legend_label:N",
                title="Hashtag share",
                scale=alt.Scale(scheme="tableau20"),
                legend=alt.Legend(orient="right", labelLimit=260),
            ),
            order=alt.Order("count:Q", sort="descending"),
            tooltip=[
                alt.Tooltip("hashtag:N", title="Hashtag"),
                alt.Tooltip("count:Q", title="Mentions"),
                alt.Tooltip("percentage:Q", title="Share", format=".1%"),
            ],
        )
    )
    center_label = (
        alt.Chart(pd.DataFrame({"label": ["Top hashtags"]}))
        .mark_text(size=15, fontWeight="bold", color="#5f667a")
        .encode(text="label:N")
    )

    return (
        (donut + center_label)
        .properties(title=chart_title(title), height=330)
    )


def show_source_status(results: dict[str, tuple[pd.DataFrame, str | None]]) -> None:
    with st.sidebar.expander("Data sources", expanded=False):
        for key, source in SOURCES.items():
            df, error = results[key]
            if error:
                st.caption(f"{source.label}: not available yet")
            else:
                st.caption(f"{source.label}: {len(df):,} rows")


def show_empty_state(name: str, source: DeltaSource, error: str | None) -> None:
    st.info(
        f"{name} is waiting for data from `{source.uri}`. "
        "Run the upstream Airflow DAGs and refresh this page."
    )
    if error:
        with st.expander("Technical detail"):
            st.code(error)


def trend_tab(trends: pd.DataFrame, error: str | None) -> None:
    st.subheader("Trend Intelligence")
    if trends.empty:
        show_empty_state("Trend Intelligence", SOURCES["trends"], error)
        return

    trends = date_filter(trends, key="trend_date_range", default_latest=True)
    current_window = has_real_values(trends, "recent_post_count_24h")
    posts_col = "recent_post_count_24h" if current_window else "post_count"
    views_col = "views_24h" if current_window else "views_sum"
    engagement_col = "engagement_24h" if current_window else "engagement_total"
    avg_engagement_col = "avg_engagement_rate_24h" if current_window else "avg_engagement_rate"

    metric_cols = st.columns(5)
    metric_cols[0].metric("Songs", format_number(trends["isrc"].nunique() if "isrc" in trends else len(trends)))
    metric_cols[1].metric("Posts", format_number(safe_sum(trends, posts_col)))
    metric_cols[2].metric("Views", format_number(safe_sum(trends, views_col)))
    metric_cols[3].metric("Engagement", format_number(safe_sum(trends, engagement_col)))
    metric_cols[4].metric("Avg engagement", as_rate(safe_mean(trends, avg_engagement_col)))

    ranking_column = "engagement_total"
    if has_real_values(trends, "trend_score"):
        ranking_column = "trend_score"
    top = trends.sort_values(ranking_column, ascending=False).head(15)
    left, right = st.columns((1.4, 1))
    with left:
        chart_df = top.copy()
        if "track" not in chart_df:
            chart_df["track"] = chart_df.index.astype(str)
        chart_title = (
            "Top Trending Songs by Current Trend Score"
            if ranking_column == "trend_score"
            else "Top Trending Songs by Engagement"
        )
        y_title = "Current Trend Score" if ranking_column == "trend_score" else "Engagement"
        st.altair_chart(
            bar_chart(
                chart_df,
                x="track",
                y=ranking_column,
                title=chart_title,
                x_title="Track",
                y_title=y_title,
                tooltip=[col for col in ["track", "artist", ranking_column, posts_col, views_col] if col in chart_df],
            ),
            width="stretch",
        )
    with right:
        table_cols = [
            col
            for col in [
                "track",
                "artist",
                posts_col,
                views_col,
                engagement_col,
                avg_engagement_col,
                "historical_popularity_score",
                "dominant_region",
                "top_hashtags",
            ]
            if col in top.columns
        ]
        if has_real_values(top, "trend_score"):
            table_cols.insert(2, "trend_score")
        st.dataframe(top[table_cols], width="stretch", hide_index=True)

    if "top_hashtags" in top.columns:
        hashtag_counter: Counter[str] = Counter()
        for tags in top["top_hashtags"]:
            hashtag_counter.update(list_value(tags))

        if hashtag_counter:
            hashtag_df = pd.DataFrame(
                hashtag_counter.most_common(8),
                columns=["hashtag", "count"],
            )
            st.altair_chart(
                pie_chart(
                    hashtag_df,
                    title="Most Used Hashtags Among Top Trending Songs",
                ),
                width="stretch",
            )


def audio_tab(audio: pd.DataFrame, error: str | None) -> None:
    st.subheader("Music Feature Explorer")
    if audio.empty:
        show_empty_state("Music Feature Explorer", SOURCES["audio"], error)
        return

    artist_options = sorted(audio["artist_name"].dropna().unique()) if "artist_name" in audio else []
    selected_artists = st.multiselect("Artists", artist_options, max_selections=8)
    if selected_artists:
        audio = audio[audio["artist_name"].isin(selected_artists)]

    metric_cols = st.columns(4)
    metric_cols[0].metric("Tracks", format_number(len(audio)))
    metric_cols[1].metric("Avg energy", f"{safe_mean(audio, 'energy'):.2f}")
    metric_cols[2].metric("Avg valence", f"{safe_mean(audio, 'valence'):.2f}")
    metric_cols[3].metric("Avg danceability", f"{safe_mean(audio, 'danceability'):.2f}")

    left, right = st.columns((1, 1))
    with left:
        scatter_cols = [col for col in ["track_name", "artist_name", "energy", "valence", "danceability", "tempo"] if col in audio]
        scatter_df = audio[scatter_cols].copy()
        st.altair_chart(
            alt.Chart(scatter_df)
            .mark_circle(size=80, opacity=0.75)
            .encode(
                x=alt.X("energy:Q", title="Energy"),
                y=alt.Y("valence:Q", title="Valence"),
                color=alt.Color("danceability:Q", title="Danceability", scale=alt.Scale(scheme="blues")),
                tooltip=[col for col in ["track_name", "artist_name", "energy", "valence", "danceability", "tempo"] if col in scatter_df],
            )
            .properties(title=chart_title("Song Energy vs Valence"), height=360),
            width="stretch",
        )
    with right:
        top_energy = audio.sort_values("energy", ascending=False).head(12) if "energy" in audio else audio.head(12)
        table_cols = [col for col in ["track_name", "artist_name", "energy", "valence", "danceability", "tempo"] if col in top_energy]
        st.dataframe(top_energy[table_cols], width="stretch", hide_index=True)


def recommender_tab(features: pd.DataFrame, feedback: pd.DataFrame, outcomes: pd.DataFrame, errors: dict[str, str | None]) -> None:
    st.subheader("Recommendation Performance")
    if feedback.empty and outcomes.empty:
        if features.empty:
            show_empty_state("Recommendation Performance", SOURCES["recommender"], errors.get("recommender"))
            return

        metric_cols = st.columns(5)
        metric_cols[0].metric("Ready songs", format_number(len(features)))
        metric_cols[1].metric("With trend data", format_number(safe_sum(features, "has_trend_data")))
        metric_cols[2].metric("Avg energy", f"{safe_mean(features, 'energy'):.2f}")
        metric_cols[3].metric("Avg valence", f"{safe_mean(features, 'valence'):.2f}")
        metric_cols[4].metric("Avg danceability", f"{safe_mean(features, 'danceability'):.2f}")

        st.info(
            "Recommendation feature data is ready, but no recommendation interaction events "
            "or feedback events have been generated yet."
        )

        score_column = "trend_score"
        if not has_real_values(features, score_column):
            score_column = "historical_popularity_score"
        if not has_real_values(features, score_column):
            score_column = "energy"

        ranked = features.sort_values(score_column, ascending=False).head(15)
        left, right = st.columns((1.2, 1))
        with left:
            if "track_name" in ranked and score_column in ranked:
                st.altair_chart(
                    bar_chart(
                        ranked,
                        x="track_name",
                        y=score_column,
                        title=f"Top Recommendation-Ready Songs by {score_column.replace('_', ' ').title()}",
                        x_title="Track",
                        y_title=score_column.replace("_", " ").title(),
                        tooltip=[col for col in ["track_name", "artist_name", score_column, "energy", "valence"] if col in ranked],
                    ),
                    width="stretch",
                )
        with right:
            table_cols = [
                col
                for col in [
                    "track_name",
                    "artist_name",
                    "energy",
                    "valence",
                    "danceability",
                ]
                if col in ranked
            ]
            for optional_col in ["trend_score", "historical_popularity_score", "top_hashtags", "dominant_region"]:
                if has_real_values(ranked, optional_col):
                    table_cols.append(optional_col)
            st.dataframe(ranked[table_cols], width="stretch", hide_index=True)
        return

    filtered_feedback = (
        date_filter(feedback, key="recommendation_feedback_date_range", default_latest=False)
        if not feedback.empty
        else feedback
    )
    metric_cols = st.columns(5)
    metric_cols[0].metric("Recommendation rows", format_number(len(outcomes)))
    metric_cols[1].metric("Feedback events", format_number(safe_sum(filtered_feedback, "feedback_events")))
    metric_cols[2].metric("Acceptance rate", as_rate(safe_mean(filtered_feedback, "acceptance_rate")))
    metric_cols[3].metric("Skip rate", as_rate(safe_mean(filtered_feedback, "skip_rate")))
    metric_cols[4].metric("Avg satisfaction", f"{safe_mean(filtered_feedback, 'avg_satisfaction_score'):.2f}")

    left, right = st.columns((1.2, 1))
    with left:
        if not filtered_feedback.empty and "event_date" in filtered_feedback:
            daily = (
                filtered_feedback.groupby("event_date", dropna=False)[["feedback_events", "accepted_count", "skipped_count"]]
                .sum()
                .sort_index()
                .reset_index()
            )
            daily_long = daily.melt(
                id_vars=["event_date"],
                value_vars=["feedback_events", "accepted_count", "skipped_count"],
                var_name="metric",
                value_name="count",
            )
            daily_long["event_date"] = pd.to_datetime(daily_long["event_date"], errors="coerce")
            st.altair_chart(
                line_chart(
                    daily_long,
                    x="event_date",
                    y="count",
                    color="metric",
                    title="Recommendation Feedback Events Over Time",
                    x_title="Event Date",
                    y_title="Number of Events",
                ),
                width="stretch",
            )
        else:
            st.info("Feedback summary exists, but there is no date column to plot yet.")
    with right:
        if not filtered_feedback.empty:
            score = "acceptance_rate" if "acceptance_rate" in filtered_feedback else "feedback_events"
            top = filtered_feedback.sort_values(score, ascending=False).head(12)
            table_cols = [
                col
                for col in [
                    "system_selected_track_name",
                    "system_selected_artist_name",
                    "feedback_events",
                    "acceptance_rate",
                    "skip_rate",
                    "avg_satisfaction_score",
                ]
                if col in top
            ]
            st.dataframe(top[table_cols], width="stretch", hide_index=True)

    if not outcomes.empty:
        with st.expander("Recommendation outcome details", expanded=True):
            outcome_cols = [
                col
                for col in [
                    "image_name",
                    "system_selected_track_name",
                    "system_selected_artist_name",
                    "system_selected_similarity_score",
                    "outcome_status",
                    "selected_track_name",
                    "selected_rank",
                    "satisfaction_score",
                ]
                if col in outcomes
            ]
            st.dataframe(outcomes[outcome_cols].head(50), width="stretch", hide_index=True)

    if not features.empty:
        with st.expander("Recommendation feature table preview", expanded=False):
            preview_cols = [
                col
                for col in [
                    "track_name",
                    "artist_name",
                    "energy",
                    "valence",
                    "danceability",
                    "tempo",
                    "has_trend_data",
                ]
                if col in features
            ]
            for optional_col in [
                "trend_score",
                "historical_popularity_score",
                "top_hashtags",
                "dominant_region",
            ]:
                if has_real_values(features, optional_col):
                    preview_cols.append(optional_col)
            preview = features.sort_values(
                "historical_popularity_score" if has_real_values(features, "historical_popularity_score") else "energy",
                ascending=False,
            )
            st.dataframe(preview[preview_cols].head(50), width="stretch", hide_index=True)

            if not has_real_values(features, "has_trend_data"):
                st.caption(
                    "Trend enrichment is still missing from the recommender feature table. "
                    "Rerun the recommender feature DAG after the trend aggregate DAG."
                )


def monitoring_tab(results: dict[str, tuple[pd.DataFrame, str | None]]) -> None:
    st.subheader("Pipeline Data Status")
    rows = []
    for key, source in SOURCES.items():
        df, error = results[key]
        processed_columns = [column for column in df.columns if "processed_at" in column] if not df.empty else []
        latest = ""
        if processed_columns:
            latest = str(pd.to_datetime(df[processed_columns[0]], errors="coerce", utc=True).max())
        rows.append(
            {
                "dataset": source.label,
                "rows": len(df),
                "available": not bool(error),
                "latest_processed_at": latest,
                "uri": source.uri,
            }
        )
    st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True)


def main() -> None:
    st.title("Music Intelligence Dashboard")
    st.caption("Trends, audio features and recommendation feedback from the Exploitation Zone.")

    results = {key: read_delta(source.uri) for key, source in SOURCES.items()}
    show_source_status(results)

    st.sidebar.header("Controls")
    if st.sidebar.button("Refresh data"):
        st.cache_data.clear()
        st.rerun()

    tab_trends, tab_audio, tab_rec, tab_monitoring = st.tabs(
        ["Trend Intelligence", "Music Features", "Recommendations", "Data Status"]
    )

    with tab_trends:
        trend_tab(*results["trends"])
    with tab_audio:
        audio_tab(*results["audio"])
    with tab_rec:
        errors = {key: error for key, (_, error) in results.items()}
        recommender_tab(
            features=results["recommender"][0],
            feedback=results["feedback_summary"][0],
            outcomes=results["outcomes"][0],
            errors=errors,
        )
    with tab_monitoring:
        monitoring_tab(results)


if __name__ == "__main__":
    main()
