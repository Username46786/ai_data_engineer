from __future__ import annotations

from pathlib import Path
from typing import Any

import streamlit as st

from executor import QueryExecutionError, execute_query
from nl_to_intent import SUPPORTED_INTENTS, build_example_questions
from spark_session import load_csv_to_spark
from visualizer import render_chart


APP_TITLE = "AI Data Engineer"
BASE_DIR = Path(__file__).resolve().parent
SAMPLE_PATH = BASE_DIR / "sample_data" / "sales.csv"


def _init_page() -> None:
    st.set_page_config(page_title=APP_TITLE, page_icon="AI", layout="wide", initial_sidebar_state="expanded")
    st.markdown(
        """
        <style>
        .block-container {padding-top: 2rem; padding-bottom: 2rem;}
        .hero {
            background: linear-gradient(135deg, #0f172a 0%, #1d4ed8 55%, #38bdf8 100%);
            color: white; padding: 1.6rem 1.8rem; border-radius: 20px;
            box-shadow: 0 18px 45px rgba(15, 23, 42, 0.20); margin-bottom: 1.2rem;
        }
        .hero h1 {margin: 0; font-size: 2.1rem;}
        .hero p {margin: 0.5rem 0 0 0; font-size: 1rem; opacity: 0.92;}
        .pill {
            display: inline-block; padding: 0.35rem 0.7rem; border-radius: 999px;
            background: rgba(255,255,255,0.16); margin-right: 0.4rem; margin-top: 0.7rem; font-size: 0.85rem;
        }
        .section-card {
            background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 18px;
            padding: 1rem 1rem 0.9rem 1rem; margin-bottom: 1rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _render_header() -> None:
    st.markdown(
        """
        <div class="hero">
            <h1>AI Data Engineer</h1>
            <p>Upload a CSV, ask a business question, and get a Spark-powered answer with logic you can trust.</p>
            <span class="pill">PySpark Engine</span>
            <span class="pill">Intent-Based Querying</span>
            <span class="pill">Chart + Table + Explanation</span>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _sidebar_controls() -> tuple[Any, str]:
    with st.sidebar:
        st.header("Data Source")
        uploaded_file = st.file_uploader("Upload CSV", type=["csv"])
        use_sample = st.toggle("Use included sample dataset", value=uploaded_file is None)
        active_source = str(SAMPLE_PATH) if use_sample or uploaded_file is None else "uploaded"

        st.divider()
        st.subheader("Supported Intent Types")
        for intent in SUPPORTED_INTENTS:
            st.caption(f"- {intent}")

        st.divider()
        st.subheader("Demo Questions")
        for question in build_example_questions():
            st.caption(f"- {question}")

    return uploaded_file, active_source


def _load_dataset(uploaded_file: Any, active_source: str) -> tuple[Any, dict[str, Any]]:
    if active_source == "uploaded" and uploaded_file is not None:
        return load_csv_to_spark(uploaded_file)
    return load_csv_to_spark(SAMPLE_PATH)


def _render_dataset_overview(dataset_info: dict[str, Any]) -> None:
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.subheader("Dataset Overview")
    metric_a, metric_b, metric_c = st.columns(3)
    metric_a.metric("Rows", f"{dataset_info['row_count']:,}")
    metric_b.metric("Columns", len(dataset_info["column_names"]))
    metric_c.metric("Source", dataset_info["source_label"])

    st.markdown("**Columns**")
    st.write(", ".join(dataset_info["column_names"]))

    preview_col, schema_col = st.columns([1.7, 1.1])
    with preview_col:
        st.markdown("**Preview**")
        st.dataframe(dataset_info["preview_df"], width="stretch", hide_index=True)
    with schema_col:
        st.markdown("**Schema**")
        st.dataframe(dataset_info["schema_df"], width="stretch", hide_index=True)
    st.markdown("</div>", unsafe_allow_html=True)


def _render_query_area() -> str:
    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.subheader("Ask a Business Question")
    question = st.text_input(
        "Question",
        value=st.session_state.get("question", "Which region drives the most revenue?"),
        placeholder="Try: Monthly sales trend",
    )
    st.caption("The app uses safe analytical templates instead of executing arbitrary generated code.")
    st.markdown("</div>", unsafe_allow_html=True)
    return question


def _render_results(result: dict[str, Any]) -> None:
    chart_col, details_col = st.columns([1.35, 1])
    with chart_col:
        st.markdown('<div class="section-card">', unsafe_allow_html=True)
        st.subheader("Chart")
        render_chart(result["result_pdf"], result["chart_spec"])
        st.markdown("</div>", unsafe_allow_html=True)

    with details_col:
        st.markdown('<div class="section-card">', unsafe_allow_html=True)
        st.subheader("Explanation")
        st.write(result["explanation"])
        st.markdown("**Recognized intent**")
        st.code(result["intent"], language="text")
        st.markdown("**Spark logic**")
        st.code(result["spark_logic"], language="python")
        st.markdown("</div>", unsafe_allow_html=True)

    st.markdown('<div class="section-card">', unsafe_allow_html=True)
    st.subheader("Result Table")
    st.dataframe(result["result_pdf"], width="stretch", hide_index=True)
    st.markdown("</div>", unsafe_allow_html=True)


def main() -> None:
    _init_page()
    _render_header()
    uploaded_file, active_source = _sidebar_controls()

    try:
        spark_df, dataset_info = _load_dataset(uploaded_file, active_source)
    except Exception as exc:
        st.error(f"Unable to load the CSV into Spark. Details: {exc}")
        st.stop()

    _render_dataset_overview(dataset_info)
    question = _render_query_area()

    if not st.button("Run Spark Analysis", type="primary", use_container_width=True):
        st.info("Upload a CSV or use the sample dataset, then ask a question like `Top 5 categories by sales`.")
        return

    if not question.strip():
        st.warning("Enter a question before running the analysis.")
        return

    with st.spinner("Running structured Spark analysis..."):
        try:
            result = execute_query(spark_df, question)
            _render_results(result)
        except QueryExecutionError as exc:
            st.error(str(exc))
            st.info("Try a supported pattern: aggregation, group by, sorting, top N, filtering, or time trend.")
        except Exception as exc:
            st.error(f"Unexpected error while executing the Spark workflow: {exc}")


if __name__ == "__main__":
    main()
