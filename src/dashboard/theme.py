"""Chart palette for the JEPX dashboard.

The app commits to light mode rather than shipping a half-working dark one:
`.streamlit/config.toml` pins the theme, so every colour here is chosen and
validated against the light chart surface.

Values come from the data-visualisation reference palette and were checked
with its validator before use::

    validate_palette.js "#2a78d6,#eb6834,#1baf7a" --mode light --pairs all
    → lightness band PASS, chroma floor PASS,
      CVD separation PASS (worst all-pairs dE 9.2),
      normal-vision floor PASS (worst 24.0),
      contrast WARN: aqua sits at 2.74:1 on the light surface.

The contrast warning is why every line carries a direct label at its right
end and every chart ships a table view: identity never rests on the hue
alone. Three slots is also the documented cap for all-pairs safety, which is
why the year comparison allows at most three series.
"""

from __future__ import annotations

# Categorical slots, in fixed order. Never cycled: a fourth series is not a
# generated hue, it is a reason to fold or facet.
SERIES_COLORS = ("#2a78d6", "#eb6834", "#1baf7a")
MAX_SERIES = len(SERIES_COLORS)

# Sequential ramp, light to dark, for magnitude (heatmaps).
SEQUENTIAL_BLUE = (
    "#cde2fb",
    "#b7d3f6",
    "#9ec5f4",
    "#86b6ef",
    "#6da7ec",
    "#5598e7",
    "#3987e5",
    "#2a78d6",
    "#256abf",
    "#1c5cab",
    "#184f95",
    "#104281",
    "#0d366b",
)

SURFACE = "#fcfcfb"
TEXT_PRIMARY = "#0b0b0b"
TEXT_SECONDARY = "#52514e"
TEXT_MUTED = "#898781"
GRIDLINE = "#e1e0d9"
AXIS = "#c3c2b7"

FONT_FAMILY = 'system-ui, -apple-system, "Segoe UI", sans-serif'


def plotly_layout(**overrides: object) -> dict[str, object]:
    """Build the shared Plotly layout: recessive chrome, text in ink tokens."""
    layout: dict[str, object] = {
        "paper_bgcolor": SURFACE,
        "plot_bgcolor": SURFACE,
        "font": {"family": FONT_FAMILY, "color": TEXT_SECONDARY, "size": 13},
        "title": {"font": {"color": TEXT_PRIMARY, "size": 16}},
        "xaxis": {
            "gridcolor": GRIDLINE,
            "linecolor": AXIS,
            "zeroline": False,
            "tickfont": {"color": TEXT_MUTED},
        },
        "yaxis": {
            "gridcolor": GRIDLINE,
            "linecolor": AXIS,
            "zeroline": False,
            "tickfont": {"color": TEXT_MUTED},
        },
        "margin": {"l": 60, "r": 90, "t": 50, "b": 50},
        "hoverlabel": {"font": {"family": FONT_FAMILY}},
    }
    layout.update(overrides)
    return layout
