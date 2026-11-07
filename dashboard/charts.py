"""
charts.py - gráfiku Altair ho kór fixu ba kada tipu anomalia.

Naran tipu anomalia hatudu iha Tetun (Voltajen Tun, Na'ok Eletrisidade, ...);
termu tékniku iha Inglés (Precision, Recall, Feature Importance, ...).
Palette: 6 slot kategóriku validadu (CVD + normal-vision) ba modu light no dark;
Normal = cinzentu neutru. Kór tuir tipu, nunka tuir rank.
"""
from __future__ import annotations

import altair as alt
import pandas as pd
import streamlit as st

from common import domain as D

LIGHT = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300"]
DARK = ["#3987e5", "#d95926", "#199e70", "#c98500", "#d55181", "#008300"]
NEUTRAL = {"light": "#a3a39e", "dark": "#6b6a64"}
TIME_AXIS = alt.Axis(format="%H:%M", tickCount=8, labelOverlap=True)

TET = D.ANOMALY_TET
TYPES = [TET[a] for a in D.ANOMALIES]          # 6 tipu anomalia (Tetun)
ALL = [TET[a] for a in D.ANOMALY_TYPES]        # Normal + 6 tipu (Tetun)


def tet(x):
    """English type name(s) -> Tetun (Series, list or str). Unknown values pass through."""
    if isinstance(x, pd.Series):
        return x.map(lambda v: TET.get(v, v))
    if isinstance(x, (list, tuple)):
        return [TET.get(v, v) for v in x]
    return TET.get(x, x)


def theme() -> str:
    try:
        return "dark" if st.context.theme.type == "dark" else "light"
    except Exception:
        return "light"


def colors(include_normal: bool = False) -> dict[str, str]:
    """Tetun type name -> colour."""
    t = theme()
    pal = dict(zip(TYPES, DARK if t == "dark" else LIGHT))
    if include_normal:
        pal = {TET[D.NORMAL]: NEUTRAL[t], **pal}
    return pal


def color_of(name: str) -> str:
    """Colour of a type given in English or Tetun."""
    return colors(True)[tet(name)]


def scale(include_normal: bool = False, domain: list[str] | None = None) -> alt.Scale:
    pal = colors(include_normal)
    dom = domain or list(pal)
    return alt.Scale(domain=dom, range=[pal[d] for d in dom])


def legend(title: str = "Tipu anomalia"):
    return alt.Legend(title=title, orient="bottom", direction="horizontal", columns=4, symbolType="square")


def show(chart: alt.Chart) -> None:
    st.altair_chart(chart, width="stretch")


def stacked_time(df: pd.DataFrame, x: str, y: str, color: str, height: int = 260, title_y: str = "Readings") -> alt.Chart:
    """Bar empilhadu iha tempu (anomalia kada minutu). `color` column must hold Tetun names."""
    dom = [c for c in TYPES if c in set(df[color])] or None
    return (alt.Chart(df).mark_bar(cornerRadiusTopLeft=2, cornerRadiusTopRight=2)
            .encode(x=alt.X(f"{x}:T", title=None, axis=TIME_AXIS),
                    y=alt.Y(f"{y}:Q", title=title_y, stack=True),
                    color=alt.Color(f"{color}:N", scale=scale(domain=dom), legend=legend()),
                    order=alt.Order(f"{color}_order:Q"),
                    tooltip=[alt.Tooltip(f"{x}:T", title="Minutu", format="%H:%M"),
                             alt.Tooltip(f"{color}:N", title="Tipu"), alt.Tooltip(f"{y}:Q", title=title_y)])
            .properties(height=height))


def hbar(df: pd.DataFrame, cat: str, val: str, color: str | None = None, height: int | None = None,
         title_x: str = "", sort: str = "-x", fmt: str = ",.0f") -> alt.Chart:
    enc = dict(y=alt.Y(f"{cat}:N", title=None, sort=sort, axis=alt.Axis(labelLimit=240, labelOverlap=False)),
               x=alt.X(f"{val}:Q", title=title_x),
               tooltip=[alt.Tooltip(f"{cat}:N"), alt.Tooltip(f"{val}:Q", format=fmt)])
    if color:
        enc["color"] = alt.Color(f"{color}:N", scale=scale(include_normal=True), legend=None)
    else:
        enc["color"] = alt.value(LIGHT[0] if theme() == "light" else DARK[0])
    n = df[cat].nunique()
    return (alt.Chart(df).mark_bar(cornerRadiusEnd=4, height={"band": 0.7})
            .encode(**enc).properties(height=height or max(180, 40 * n)))


def stacked_hbar(df: pd.DataFrame, cat: str, val: str, color: str, title_x: str = "Readings") -> alt.Chart:
    n = df[cat].nunique()
    return (alt.Chart(df).mark_bar(height={"band": 0.7})
            .encode(y=alt.Y(f"{cat}:N", title=None, sort="-x", axis=alt.Axis(labelLimit=240, labelOverlap=False)),
                    x=alt.X(f"sum({val}):Q", title=title_x),
                    color=alt.Color(f"{color}:N", scale=scale(), legend=legend()),
                    tooltip=[alt.Tooltip(f"{cat}:N"), alt.Tooltip(f"{color}:N", title="Tipu"),
                             alt.Tooltip(f"{val}:Q", format=",.0f", title=title_x)])
            .properties(height=max(160, 30 * n)))


def heatmap(cm: list[list[int]], labels: list[str], title: str = "") -> alt.Chart:
    """Confusion matrix: liña = atuál, kolumna = prediction. Kór = % husi liña (recall)."""
    labels = tet(list(labels))
    rows = []
    for i, a in enumerate(labels):
        tot = sum(cm[i]) or 1
        for j, p in enumerate(labels):
            rows.append({"Atuál": a, "Prediction": p, "n": cm[i][j], "pct": cm[i][j] / tot})
    df = pd.DataFrame(rows)
    hue = LIGHT[0] if theme() == "light" else DARK[0]
    base = alt.Chart(df).encode(
        x=alt.X("Prediction:N", sort=labels, title="Prediction",
                axis=alt.Axis(labelAngle=-35, labelLimit=200, labelOverlap=False)),
        y=alt.Y("Atuál:N", sort=labels, title="Atuál (ground truth)", axis=alt.Axis(labelLimit=200, labelOverlap=False)))
    cells = base.mark_rect(stroke="white" if theme() == "light" else "#1a1a19", strokeWidth=2).encode(
        color=alt.Color("pct:Q", scale=alt.Scale(domain=[0, 1], range=["#f4f8fd" if theme() == "light" else "#20262e", hue]),
                        legend=None),
        tooltip=[alt.Tooltip("Atuál:N"), alt.Tooltip("Prediction:N"), alt.Tooltip("n:Q", format=",", title="Readings"),
                 alt.Tooltip("pct:Q", format=".1%", title="% husi liña")])
    txt = base.mark_text(fontSize=11).encode(
        text=alt.Text("n:Q", format=","),
        color=alt.condition("datum.pct > 0.5", alt.value("white"), alt.value("#52514e" if theme() == "light" else "#c3c2b7")))
    return (cells + txt).properties(height=34 * len(labels) + 40, title=title)


def type_long(df: pd.DataFrame, time_col: str) -> pd.DataFrame:
    """Kolumna n_sag/n_theft... -> formatu naruk (time, tipu Tetun, n)."""
    from common.db import AGG_TYPE_COLS
    long = df.melt(id_vars=[time_col], value_vars=list(AGG_TYPE_COLS.values()), var_name="col", value_name="n")
    inv = {v: TET[k] for k, v in AGG_TYPE_COLS.items()}
    long["tipu"] = long["col"].map(inv)
    long["tipu_order"] = long["tipu"].map({a: i for i, a in enumerate(TYPES)})
    return long[long["n"] > 0]
