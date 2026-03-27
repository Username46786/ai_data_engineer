from __future__ import annotations

import streamlit as st


def render_chart(result_pdf, chart_spec: dict[str, str]) -> None:
    kind = chart_spec["kind"]
    x_col = chart_spec["x"]
    y_col = chart_spec["y"]

    if kind == "metric":
        value = result_pdf.iloc[0][y_col]
        if isinstance(value, (int, float)):
            display_value = f"{value:,.2f}"
        else:
            display_value = value
        st.metric(label=y_col.replace("_", " ").title(), value=display_value)
        return

    chart_df = result_pdf.set_index(x_col)
    if kind == "line":
        st.line_chart(chart_df[y_col], width="stretch")
        return

    st.bar_chart(chart_df[y_col], width="stretch")
