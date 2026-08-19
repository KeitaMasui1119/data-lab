"""Streamlit dashboard over the JEPX gold layer.

Three views, one per finding the gold tables were built to carry:

1. a price heatmap over delivery date x time code, from the interval fact
2. the market splitting rate per fiscal year and area, from the daily table
3. the intraday price curve, from the period profile

Run it with ``streamlit run src/dashboard/app.py`` (or ``docker compose up
dashboard``). It reads the gold tables straight off RustFS, so the same AWS_*
environment the pipelines use is all it needs -- no Iceberg catalog, because
``iceberg_scan`` resolves a table from its location.

This module is only glue: fetching lives in ``queries.py`` and figures in
``charts.py``, both free of Streamlit so they can be tested and screenshotted
without a server.
"""

from __future__ import annotations

import duckdb
import polars as pl
import streamlit as st

from common.duckdb_utils import create_duckdb_connection
from dashboard import charts, queries
from dashboard.theme import MAX_SERIES

st.set_page_config(page_title="JEPX スポット価格", layout="wide")

DAY_TYPES = {"平日": "weekday", "休日 (土日祝)": "holiday"}


@st.cache_resource
def get_connection() -> duckdb.DuckDBPyConnection:
    """One DuckDB connection for the app; S3 settings come from the env."""
    return create_duckdb_connection()


@st.cache_data(ttl=3600)
def load_areas() -> list[str]:
    return queries.fetch_areas(
        get_connection(), daily_relation=queries.iceberg(queries.GOLD_DAILY_LOCATION)
    )


@st.cache_data(ttl=3600)
def load_fiscal_years() -> list[int]:
    return queries.fetch_fiscal_years(
        get_connection(), daily_relation=queries.iceberg(queries.GOLD_DAILY_LOCATION)
    )


@st.cache_data(ttl=3600)
def load_price_heatmap(fiscal_year: int, area_name: str) -> pl.DataFrame:
    return queries.fetch_price_heatmap(
        get_connection(),
        area_spread_relation=queries.iceberg(queries.GOLD_AREA_SPREAD_LOCATION),
        fiscal_year=fiscal_year,
        area_name=area_name,
    )


@st.cache_data(ttl=3600)
def load_split_rate_matrix() -> pl.DataFrame:
    return queries.fetch_split_rate_matrix(
        get_connection(), daily_relation=queries.iceberg(queries.GOLD_DAILY_LOCATION)
    )


@st.cache_data(ttl=3600)
def load_intraday_profile(
    area_name: str, day_type: str, fiscal_years: tuple[int, ...]
) -> pl.DataFrame:
    return queries.fetch_intraday_profile(
        get_connection(),
        profile_relation=queries.iceberg(queries.GOLD_PROFILE_LOCATION),
        area_name=area_name,
        day_type=day_type,
        fiscal_years=list(fiscal_years),
    )


def _table_view(frame: pl.DataFrame, label: str) -> None:
    """Ship the table alongside every chart.

    The palette validator flags one series colour as sub-3:1 on the light
    surface, and its documented relief is visible labels plus a table view.
    """
    with st.expander(f"{label}（表で見る）"):
        st.dataframe(frame, width="stretch")


def render_price_heatmap(fiscal_year: int, area_name: str) -> None:
    frame = load_price_heatmap(fiscal_year, area_name)
    if frame.is_empty():
        st.info("この年度・エリアの組み合わせにはデータがありません。")
        return

    upper, peak = charts.price_heatmap_bounds(frame)
    st.plotly_chart(charts.build_price_heatmap(frame), width="stretch")
    st.caption(
        f"色は 0〜{upper:.1f} 円/kWh（99パーセンタイル）でクリップ。"
        f"この年度の最高値は {peak:.1f} 円/kWh。"
    )
    _table_view(frame, "価格ヒートマップ")


def render_split_rate() -> None:
    frame = load_split_rate_matrix()
    if frame.is_empty():
        st.info("データがありません。")
        return

    st.plotly_chart(charts.build_split_rate_heatmap(frame), width="stretch")
    st.caption(
        "エリア価格がシステムプライスと1ティック(0.01円)以上離れたコマの割合。"
        "分断が常態化していく様子が縦方向に読める。"
    )
    _table_view(frame, "市場分断率")


def render_intraday_profile(
    area_name: str, day_type_label: str, fiscal_years: list[int]
) -> None:
    frame = load_intraday_profile(
        area_name, DAY_TYPES[day_type_label], tuple(sorted(fiscal_years))
    )
    if frame.is_empty():
        st.info("この条件にはデータがありません。")
        return

    slots = charts.assign_series_slots(
        sorted(fiscal_years), st.session_state.get("profile_series_slots")
    )
    st.session_state["profile_series_slots"] = slots
    st.plotly_chart(
        charts.build_intraday_profile(frame, series_slots=slots), width="stretch"
    )
    st.caption(
        "月別セルを観測日数で重み付けして年度に畳んだ値。"
        "昼間が沈み夕方が立つ形（ダックカーブ）が年々はっきりしていく。"
    )
    _table_view(frame, "日内カーブ")


def main() -> None:
    st.title("JEPX スポット価格ダッシュボード")
    st.caption(
        "gold レイヤー（`jepx_spot_price_daily` / `_period_profile` / "
        "`_area_spread`）のみを参照しています。"
    )

    try:
        areas = load_areas()
        fiscal_years = load_fiscal_years()
    except Exception as error:
        st.error(
            "gold テーブルを読めませんでした。RustFS が起動していること、"
            f"AWS_* 環境変数が設定されていることを確認してください。\n\n{error}"
        )
        return

    if not areas or not fiscal_years:
        st.warning(
            "gold テーブルが空です。`ingest-jepx-silver-to-gold` を実行してください。"
        )
        return

    heatmap_tab, split_tab, profile_tab = st.tabs(
        ["価格ヒートマップ", "市場分断", "日内カーブ"]
    )

    with heatmap_tab:
        left, right = st.columns(2)
        selected_year = left.selectbox("年度", fiscal_years, key="heatmap_year")
        selected_area = right.selectbox(
            "エリア", areas, index=areas.index("tokyo") if "tokyo" in areas else 0
        )
        render_price_heatmap(int(selected_year), str(selected_area))

    with split_tab:
        render_split_rate()

    with profile_tab:
        left, middle, right = st.columns([1, 1, 2])
        profile_area = left.selectbox(
            "エリア",
            areas,
            index=areas.index("kyushu") if "kyushu" in areas else 0,
            key="profile_area",
        )
        profile_day_type = middle.selectbox("曜日区分", list(DAY_TYPES))
        spread = max(1, len(fiscal_years) // 2)
        defaults = [fiscal_years[0], fiscal_years[spread], fiscal_years[-1]]
        selected_years = right.multiselect(
            f"比較する年度（最大{MAX_SERIES}件）",
            fiscal_years,
            default=sorted(set(defaults))[:MAX_SERIES],
        )
        if len(selected_years) > MAX_SERIES:
            st.warning(
                f"{MAX_SERIES}件までにしてください。"
                "色の識別性を保証できるのがこの本数までのためです。"
            )
            selected_years = selected_years[:MAX_SERIES]
        render_intraday_profile(
            str(profile_area), str(profile_day_type), [int(y) for y in selected_years]
        )


main()
