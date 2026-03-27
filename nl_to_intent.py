from __future__ import annotations

import re
from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql.types import DateType, NumericType, TimestampType


SUPPORTED_INTENTS = [
    "aggregation",
    "group by",
    "sorting",
    "top N",
    "filtering",
    "time trend",
]


@dataclass
class IntentSpec:
    intent: str
    metric_column: str
    group_column: str | None = None
    filter_column: str | None = None
    filter_value: str | None = None
    top_n: int = 10
    sort_direction: str = "desc"
    time_column: str | None = None
    time_grain: str = "month"
    aggregation: str = "sum"
    chart_preference: str = "bar"


def build_example_questions() -> list[str]:
    return [
        "Which region drives the most revenue?",
        "Monthly sales trend",
        "Average order value by category",
        "Count customers by state",
        "Top 5 categories by sales",
    ]


def classify_intent(question: str, spark_df: DataFrame) -> IntentSpec:
    normalized = question.strip().lower()
    if not normalized:
        raise ValueError("Question is empty.")

    columns = spark_df.columns
    lowered_map = {column.lower(): column for column in columns}
    _validate_question_scope(normalized, columns)
    numeric_columns = [field.name for field in spark_df.schema.fields if isinstance(field.dataType, NumericType)]
    date_columns = [
        field.name
        for field in spark_df.schema.fields
        if isinstance(field.dataType, (DateType, TimestampType)) or "date" in field.name.lower()
    ]

    metric_column = _pick_metric_column(normalized, columns, numeric_columns)
    group_column = _pick_group_column(normalized, columns, exclude={metric_column})
    time_column = _pick_time_column(normalized, date_columns)
    top_n = _extract_top_n(normalized)

    if any(keyword in normalized for keyword in ["trend", "monthly", "weekly", "daily", "over time"]):
        return IntentSpec(
            intent="time trend",
            metric_column=metric_column,
            time_column=time_column or _require(date_columns, "a date column for time trends"),
            time_grain=_detect_time_grain(normalized),
            aggregation="sum",
            chart_preference="line",
        )

    if "average" in normalized or "avg" in normalized:
        return IntentSpec(
            intent="group by" if group_column else "aggregation",
            metric_column=metric_column,
            group_column=group_column,
            aggregation="avg",
            chart_preference="bar",
        )

    if "count" in normalized:
        count_group = _pick_group_column(normalized, columns)
        if count_group:
            count_metric = _find_column_by_keyword("customer", columns) or _find_column_by_keyword("order", columns) or count_group
            return IntentSpec(
                intent="group by",
                metric_column=count_metric,
                group_column=count_group,
                aggregation="count",
                chart_preference="bar",
            )
        return IntentSpec(intent="aggregation", metric_column=metric_column, aggregation="count", chart_preference="metric")

    filter_match = re.search(r"(?:for|where|in)\s+([a-zA-Z_][\w\s]+?)\s+(?:is|=)\s+([a-zA-Z0-9_\-\s]+)", normalized)
    if filter_match:
        filter_column = _fuzzy_match_column(filter_match.group(1).strip(), lowered_map)
        if filter_column:
            return IntentSpec(
                intent="filtering",
                metric_column=metric_column,
                filter_column=filter_column,
                filter_value=filter_match.group(2).strip(),
                aggregation="sum",
                chart_preference="bar",
            )

    if "top" in normalized or "highest" in normalized or "most" in normalized:
        return IntentSpec(
            intent="top N",
            metric_column=metric_column,
            group_column=group_column or _pick_best_dimension(columns, numeric_columns),
            aggregation="sum",
            top_n=top_n if "top" in normalized else 1,
            chart_preference="bar",
        )

    if "sort" in normalized or "lowest" in normalized or "smallest" in normalized:
        return IntentSpec(
            intent="sorting",
            metric_column=metric_column,
            group_column=group_column or _pick_best_dimension(columns, numeric_columns),
            aggregation="sum",
            sort_direction="asc" if any(word in normalized for word in ["lowest", "smallest", "asc"]) else "desc",
            chart_preference="bar",
        )

    if "by" in normalized or group_column:
        return IntentSpec(
            intent="group by",
            metric_column=metric_column,
            group_column=group_column or _pick_best_dimension(columns, numeric_columns),
            aggregation="sum",
            chart_preference="bar",
        )

    return IntentSpec(intent="aggregation", metric_column=metric_column, aggregation="sum", chart_preference="metric")


def _validate_question_scope(question: str, columns: list[str]) -> None:
    analytic_keywords = {
        "average",
        "avg",
        "sum",
        "total",
        "count",
        "top",
        "highest",
        "lowest",
        "smallest",
        "most",
        "least",
        "trend",
        "monthly",
        "weekly",
        "daily",
        "over time",
        "by",
        "revenue",
        "sales",
        "category",
        "region",
        "state",
        "customer",
        "order",
        "filter",
        "where",
    }
    column_keywords = {column.lower().replace("_", " ") for column in columns}
    has_analytic_signal = any(keyword in question for keyword in analytic_keywords)
    has_column_signal = any(keyword in question for keyword in column_keywords)
    if not has_analytic_signal and not has_column_signal:
        raise ValueError(
            "This question does not match the supported analytics patterns. Try a question about trends, counts, top values, filters, or groupings."
        )


def _pick_metric_column(question: str, columns: list[str], numeric_columns: list[str]) -> str:
    aliases = {
        "revenue": ["revenue", "sales", "amount", "total", "price"],
        "sales": ["sales", "revenue", "amount", "total"],
        "order value": ["order_value", "order value", "amount", "sales"],
        "count": ["id", "customer", "order"],
    }
    for phrase, candidates in aliases.items():
        if phrase in question:
            for candidate in candidates:
                column = _find_column_by_keyword(candidate, columns)
                if column:
                    return column

    direct_match = _pick_group_column(question, numeric_columns)
    if direct_match:
        return direct_match

    if numeric_columns:
        for keyword in ["sales", "revenue", "amount", "total", "order_value", "quantity"]:
            column = _find_column_by_keyword(keyword, numeric_columns)
            if column:
                return column
        return numeric_columns[0]

    raise ValueError("No numeric metric column was found. Upload a dataset with at least one numeric field.")


def _pick_group_column(question: str, columns: list[str], exclude: set[str] | None = None) -> str | None:
    exclude = exclude or set()
    dimension_aliases = [
        "region",
        "regions",
        "category",
        "categories",
        "state",
        "states",
        "customer",
        "customers",
        "segment",
        "segments",
        "product",
        "products",
        "month",
        "months",
    ]
    for keyword in dimension_aliases:
        if keyword in question:
            column = _find_column_by_keyword(keyword, columns)
            if column and column not in exclude:
                return column

    by_match = re.search(r"\bby\s+([a-zA-Z_][\w\s]+)", question)
    if by_match:
        phrase = by_match.group(1).strip()
        for candidate in re.split(r"\s+", phrase):
            column = _find_column_by_keyword(candidate, columns)
            if column and column not in exclude:
                return column
    return None


def _pick_time_column(question: str, date_columns: list[str]) -> str | None:
    if not date_columns:
        return None
    for keyword in ["date", "month", "day", "time"]:
        if keyword in question:
            column = _find_column_by_keyword(keyword, date_columns)
            if column:
                return column
    return date_columns[0]


def _pick_best_dimension(columns: list[str], numeric_columns: list[str]) -> str:
    non_numeric = [column for column in columns if column not in numeric_columns]
    for keyword in ["region", "category", "state", "customer", "segment", "product"]:
        column = _find_column_by_keyword(keyword, non_numeric)
        if column:
            return column
    return _require(non_numeric, "a categorical column for grouping")


def _extract_top_n(question: str) -> int:
    match = re.search(r"top\s+(\d+)", question)
    return int(match.group(1)) if match else 5


def _detect_time_grain(question: str) -> str:
    if "daily" in question or "day" in question:
        return "day"
    if "weekly" in question or "week" in question:
        return "week"
    return "month"


def _find_column_by_keyword(keyword: str, columns: list[str]) -> str | None:
    normalized_keyword = keyword.lower().replace(" ", "_")
    if normalized_keyword.endswith("ies"):
        candidates = [normalized_keyword, normalized_keyword[:-3] + "y"]
    elif normalized_keyword.endswith("s"):
        candidates = [normalized_keyword, normalized_keyword[:-1]]
    else:
        candidates = [normalized_keyword]
    for column in columns:
        normalized_column = column.lower().replace(" ", "_")
        for candidate in candidates:
            if candidate == normalized_column or candidate in normalized_column:
                return column
    return None


def _fuzzy_match_column(text: str, lowered_map: dict[str, str]) -> str | None:
    normalized = text.lower().replace(" ", "_")
    for lowered, actual in lowered_map.items():
        candidate = lowered.replace(" ", "_")
        if normalized == candidate or normalized in candidate:
            return actual
    return None


def _require(values: list[str], description: str) -> str:
    if not values:
        raise ValueError(f"The dataset does not contain {description}.")
    return values[0]
