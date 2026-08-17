"""Plotly figures for the JEPX dashboard.

Kept free of Streamlit so each figure can be built, tested and screenshotted
without a running server. ``app.py`` is only the glue that fetches a frame,
hands it here, and renders the result.

Colour choices and the reasoning behind them live in ``theme.py``.
"""

from __future__ import annotations

from typing import cast

import plotly.graph_objects as go
import polars as pl

from dashboard.queries import slot_label
from dashboard.theme import (
    SEQUENTIAL_BLUE,
    SERIES_COLORS,
    TEXT_SECONDARY,
    plotly_layout,
)

# Prices span two orders of magnitude once the 2021 crisis is in range, so a
# linear ramp to the true maximum flattens every ordinary day into the
# lightest step. Clip the ramp here and state the real peak in the caption.
HEATMAP_CLIP_QUANTILE = 0.99


def _sequential_colorscale() -> list[list[object]]:
    """Spread the one-hue ramp evenly across 0..1."""
    last = len(SEQUENTIAL_BLUE) - 1
    return [[index / last, color] for index, color in enumerate(SEQUENTIAL_BLUE)]


def price_heatmap_bounds(frame: pl.DataFrame) -> tuple[float, float]:
    """Return the colour ceiling and the true peak for the caption."""
    if frame.is_empty():
        return 0.0, 0.0
    quantile = frame["area_price"].quantile(HEATMAP_CLIP_QUANTILE)
    peak = frame["area_price"].max()
    return (
        float(cast(float, quantile) if quantile is not None else 0.0),
        float(cast(float, peak) if peak is not None else 0.0),
    )


def build_price_heatmap(frame: pl.DataFrame) -> go.Figure:
    """Delivery date x time code, coloured by area price."""
    pivot = frame.pivot(on="time_code", index="delivery_date", values="area_price")
    time_codes = sorted(
        int(column) for column in pivot.columns if column != "delivery_date"
    )
    upper, _ = price_heatmap_bounds(frame)

    figure = go.Figure(
        go.Heatmap(
            z=pivot.select([str(code) for code in time_codes]).to_numpy(),
            x=[slot_label(code) for code in time_codes],
            y=pivot["delivery_date"].to_list(),
            colorscale=_sequential_colorscale(),
            zmin=0,
            zmax=upper or None,
            colorbar={"title": "円/kWh", "outlinewidth": 0},
            hovertemplate="%{y} %{x}<br>%{z:.2f} 円/kWh<extra></extra>",
        )
    )
    figure.update_layout(
        plotly_layout(
            height=560,
            xaxis={"title": "時刻 (JST)", "showgrid": False},
            yaxis={"title": "受渡日", "showgrid": False},
        )
    )
    return figure


def build_split_rate_heatmap(frame: pl.DataFrame) -> go.Figure:
    """Fiscal year x area, coloured by the share of slots that split."""
    pivot = frame.pivot(on="area_name", index="fiscal_year", values="split_pct")
    areas = [column for column in pivot.columns if column != "fiscal_year"]

    figure = go.Figure(
        go.Heatmap(
            z=pivot.select(areas).to_numpy(),
            x=areas,
            y=pivot["fiscal_year"].to_list(),
            colorscale=_sequential_colorscale(),
            zmin=0,
            zmax=100,
            colorbar={"title": "%", "outlinewidth": 0},
            hovertemplate="FY%{y} %{x}<br>分断率 %{z:.1f}%<extra></extra>",
        )
    )
    figure.update_layout(
        plotly_layout(
            height=560,
            xaxis={"title": "エリア", "showgrid": False},
            yaxis={"title": "年度", "showgrid": False, "dtick": 1},
        )
    )
    return figure


def assign_series_slots(
    selected: list[int], previous: dict[int, int] | None = None
) -> dict[int, int]:
    """Map each fiscal year to a colour slot, keeping survivors stable.

    Colour follows the entity, never its rank: deselecting one year must not
    repaint the others. Assigning by position in the current selection would
    do exactly that, so a year keeps the slot it was first given and only
    releases it when it leaves the selection.
    """
    kept = {year: slot for year, slot in (previous or {}).items() if year in selected}
    used = set(kept.values())
    for year in selected:
        if year in kept:
            continue
        free = next(
            (slot for slot in range(len(SERIES_COLORS)) if slot not in used), None
        )
        if free is None:  # more years than slots; the caller caps the selection
            break
        kept[year] = free
        used.add(free)
    return kept


def build_intraday_profile(
    frame: pl.DataFrame, *, series_slots: dict[int, int] | None = None
) -> go.Figure:
    """One line per fiscal year, each labelled at its right end.

    The direct labels are not decoration: the validator flags one series
    colour as sub-3:1 on the light surface, and visible labels are its
    documented relief, so identity never rests on hue alone.
    """
    figure = go.Figure()
    fiscal_years = sorted(frame["fiscal_year"].unique().to_list())
    slots = series_slots or assign_series_slots(fiscal_years)

    for fiscal_year in fiscal_years:
        series = frame.filter(pl.col("fiscal_year") == fiscal_year).sort("time_code")
        if series.is_empty():
            continue
        color = SERIES_COLORS[slots.get(fiscal_year, 0) % len(SERIES_COLORS)]
        figure.add_trace(
            go.Scatter(
                x=[slot_label(code) for code in series["time_code"].to_list()],
                y=series["avg_price"].to_list(),
                mode="lines",
                name=f"FY{fiscal_year}",
                line={"color": color, "width": 2},
                hovertemplate=(
                    f"FY{fiscal_year} %{{x}}<br>%{{y:.2f}} 円/kWh<extra></extra>"
                ),
            )
        )
        figure.add_annotation(
            x=slot_label(int(series["time_code"][-1])),
            y=float(series["avg_price"][-1]),
            text=f"FY{fiscal_year}",
            showarrow=False,
            xanchor="left",
            xshift=6,
            font={"color": TEXT_SECONDARY, "size": 12},
        )

    figure.update_layout(
        plotly_layout(
            height=460,
            xaxis={"title": "時刻 (JST)", "showgrid": False},
            yaxis={"title": "平均エリア価格 (円/kWh)"},
            showlegend=True,
            legend={"orientation": "h", "y": 1.12, "x": 0},
        )
    )
    return figure
