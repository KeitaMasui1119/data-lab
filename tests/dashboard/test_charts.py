"""Unit tests for the dashboard's Plotly figures.

The figures are built without Streamlit, so their structure can be asserted
directly. What matters here is the handful of rules a chart is easy to break:
colour follows the entity rather than its rank, magnitude gets one sequential
hue, and every line carries a direct label because one series colour sits
below 3:1 on the light surface.
"""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from dashboard.charts import (
    assign_series_slots,
    build_intraday_profile,
    build_price_heatmap,
    build_split_rate_heatmap,
    price_heatmap_bounds,
)
from dashboard.theme import MAX_SERIES, SEQUENTIAL_BLUE, SERIES_COLORS


def _profile_frame(years: list[int], *, last_price: float = 10.0) -> pl.DataFrame:
    rows = [
        {
            "fiscal_year": year,
            "time_code": time_code,
            "avg_price": last_price + index,
            "observation_count": 20,
        }
        for index, year in enumerate(years)
        for time_code in (1, 48)
    ]
    return pl.DataFrame(rows)


def test_assign_series_slots_gives_each_year_its_own_slot() -> None:
    assert assign_series_slots([2024, 2025, 2026]) == {2024: 0, 2025: 1, 2026: 2}


def test_assign_series_slots_keeps_survivors_when_one_is_removed() -> None:
    """Colour follows the entity: deselecting a year must not repaint the rest.

    Assigning by position would move FY2026 from slot 2 to slot 1 here, which
    is the anti-pattern this function exists to avoid.
    """
    # Arrange
    previous = assign_series_slots([2024, 2025, 2026])

    # Act
    after = assign_series_slots([2024, 2026], previous)

    # Assert
    assert after == {2024: 0, 2026: 2}


def test_assign_series_slots_reuses_a_freed_slot_for_a_newcomer() -> None:
    # Arrange
    previous = assign_series_slots([2024, 2025, 2026])

    # Act
    after = assign_series_slots([2024, 2026, 2020], previous)

    # Assert
    assert after[2024] == 0  # unchanged
    assert after[2026] == 2  # unchanged
    assert after[2020] == 1  # took the slot FY2025 released


def test_assign_series_slots_stops_at_the_palette_size() -> None:
    """The palette validates three slots; a fourth is not a generated hue."""
    assigned = assign_series_slots([2021, 2022, 2023, 2024])

    assert len(assigned) == MAX_SERIES


def test_intraday_profile_draws_one_line_and_one_label_per_year() -> None:
    """Every line is directly labelled, so identity never rests on hue alone."""
    # Arrange
    frame = _profile_frame([2015, 2025])

    # Act
    figure = build_intraday_profile(frame)

    # Assert
    assert len(figure.data) == 2  # pyright: ignore[reportArgumentType]
    assert len(figure.layout.annotations) == 2  # pyright: ignore[reportArgumentType]
    assert {a.text for a in figure.layout.annotations} == {  # pyright: ignore[reportOptionalIterable]
        "FY2015",
        "FY2025",
    }


def test_intraday_profile_honours_the_slot_assignment() -> None:
    # Arrange
    frame = _profile_frame([2015, 2025])

    # Act
    figure = build_intraday_profile(frame, series_slots={2015: 2, 2025: 0})

    # Assert
    colors = {trace.name: trace.line.color for trace in figure.data}  # pyright: ignore[reportAttributeAccessIssue]
    assert colors == {"FY2015": SERIES_COLORS[2], "FY2025": SERIES_COLORS[0]}


def test_intraday_profile_keeps_a_legend_for_multiple_series() -> None:
    figure = build_intraday_profile(_profile_frame([2015, 2025]))

    assert figure.layout.showlegend is True


def test_price_heatmap_clips_the_ramp_at_the_99th_percentile() -> None:
    """A single 2021-style spike must not flatten every ordinary day."""
    # Arrange
    prices = [10.0] * 99 + [250.0]
    frame = pl.DataFrame(
        {
            "delivery_date": [date(2026, 4, 1)] * 100,
            "time_code": list(range(1, 101)),
            "area_price": prices,
        }
    )

    # Act
    upper, peak = price_heatmap_bounds(frame)

    # Assert
    assert peak == 250.0
    assert upper < peak  # the ramp stops well below the outlier


def test_price_heatmap_bounds_are_zero_for_an_empty_frame() -> None:
    frame = pl.DataFrame(
        schema={
            "delivery_date": pl.Date,
            "time_code": pl.Int32,
            "area_price": pl.Float64,
        }
    )

    assert price_heatmap_bounds(frame) == (0.0, 0.0)


def test_price_heatmap_uses_one_sequential_hue() -> None:
    """Magnitude gets a single hue, light to dark -- never a rainbow."""
    # Arrange
    frame = pl.DataFrame(
        {
            "delivery_date": [date(2026, 4, 1), date(2026, 4, 1)],
            "time_code": [1, 2],
            "area_price": [10.0, 20.0],
        }
    )

    # Act
    figure = build_price_heatmap(frame)

    # Assert
    scale_colors = [str(step[1]) for step in figure.data[0].colorscale]  # pyright: ignore[reportAttributeAccessIssue,reportIndexIssue]
    assert scale_colors == list(SEQUENTIAL_BLUE)


def test_split_rate_heatmap_pins_the_scale_to_a_full_percentage() -> None:
    # Arrange
    frame = pl.DataFrame(
        {
            "fiscal_year": [2025, 2025, 2026, 2026],
            "area_name": ["tokyo", "kyushu"] * 2,
            "split_pct": [10.0, 20.0, 90.0, 95.0],
        }
    )

    # Act
    figure = build_split_rate_heatmap(frame)

    # Assert: a rate chart whose scale floated with the data would misread
    # across filters.
    assert figure.data[0].zmin == 0  # pyright: ignore[reportAttributeAccessIssue]
    assert figure.data[0].zmax == 100  # pyright: ignore[reportAttributeAccessIssue]


@pytest.mark.parametrize(
    "builder",
    [build_price_heatmap, build_split_rate_heatmap],
    ids=["price", "split"],
)
def test_heatmaps_drop_the_gridlines_behind_the_cells(builder) -> None:
    """Gridlines under a filled grid are noise."""
    # Arrange
    frame = (
        pl.DataFrame(
            {
                "delivery_date": [date(2026, 4, 1)],
                "time_code": [1],
                "area_price": [10.0],
            }
        )
        if builder is build_price_heatmap
        else pl.DataFrame(
            {"fiscal_year": [2026], "area_name": ["tokyo"], "split_pct": [50.0]}
        )
    )

    # Act
    figure = builder(frame)

    # Assert
    assert figure.layout.xaxis.showgrid is False
    assert figure.layout.yaxis.showgrid is False
