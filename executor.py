from __future__ import annotations

from dataclasses import asdict
from typing import Any

from pyspark.sql import DataFrame

from nl_to_intent import IntentSpec, classify_intent
from spark_templates import apply_template


class QueryExecutionError(RuntimeError):
    pass


def execute_query(spark_df: DataFrame, question: str) -> dict[str, Any]:
    try:
        intent_spec: IntentSpec = classify_intent(question, spark_df)
    except Exception as exc:
        raise QueryExecutionError(f"Could not classify the question. Details: {exc}") from exc

    try:
        result_df, spark_logic, explanation = apply_template(spark_df, intent_spec)
    except Exception as exc:
        raise QueryExecutionError(f"Spark template execution failed. Details: {exc}") from exc

    if result_df is None:
        raise QueryExecutionError("The Spark execution path did not produce `result_df`.")

    try:
        result_pdf = result_df.toPandas()
    except Exception as exc:
        raise QueryExecutionError(f"Spark produced a result, but display conversion failed. Details: {exc}") from exc

    if result_pdf.empty:
        raise QueryExecutionError("The query ran successfully but returned no rows. Try a broader question or different grouping.")

    return {
        "intent": intent_spec.intent,
        "intent_details": asdict(intent_spec),
        "result_df": result_df,
        "result_pdf": result_pdf,
        "spark_logic": spark_logic,
        "explanation": explanation,
        "chart_spec": _build_chart_spec(result_pdf, intent_spec),
    }


def _build_chart_spec(result_pdf, intent_spec: IntentSpec) -> dict[str, str]:
    columns = list(result_pdf.columns)
    if len(columns) == 1:
        return {"kind": "metric", "x": columns[0], "y": columns[0]}
    if intent_spec.intent == "time trend":
        return {"kind": "line", "x": columns[0], "y": columns[1]}
    return {"kind": intent_spec.chart_preference, "x": columns[0], "y": columns[1]}
