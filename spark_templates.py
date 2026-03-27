from __future__ import annotations

from pyspark.sql import DataFrame, functions as F

from nl_to_intent import IntentSpec


def apply_template(spark_df: DataFrame, intent_spec: IntentSpec) -> tuple[DataFrame, str, str]:
    if intent_spec.intent == "aggregation":
        return _run_aggregation(spark_df, intent_spec)
    if intent_spec.intent == "group by":
        return _run_group_by(spark_df, intent_spec)
    if intent_spec.intent == "sorting":
        return _run_sorting(spark_df, intent_spec)
    if intent_spec.intent == "top N":
        return _run_top_n(spark_df, intent_spec)
    if intent_spec.intent == "filtering":
        return _run_filtering(spark_df, intent_spec)
    if intent_spec.intent == "time trend":
        return _run_time_trend(spark_df, intent_spec)
    raise ValueError(f"Unsupported intent: {intent_spec.intent}")


def _aggregate_expression(intent_spec: IntentSpec):
    metric = F.col(intent_spec.metric_column)
    alias = f"{intent_spec.aggregation}_{intent_spec.metric_column}"
    if intent_spec.aggregation == "avg":
        return F.avg(metric).alias(alias), alias
    if intent_spec.aggregation == "count":
        return F.count(metric).alias(alias), alias
    return F.sum(metric).alias(alias), alias


def _run_aggregation(spark_df: DataFrame, intent_spec: IntentSpec) -> tuple[DataFrame, str, str]:
    agg_expr, alias = _aggregate_expression(intent_spec)
    result_df = spark_df.agg(agg_expr)
    logic = f"result_df = spark_df.agg(F.{intent_spec.aggregation}(F.col('{intent_spec.metric_column}')).alias('{alias}'))"
    explanation = f"This computes the {intent_spec.aggregation} of `{intent_spec.metric_column}` across the full dataset."
    return result_df, logic, explanation


def _run_group_by(spark_df: DataFrame, intent_spec: IntentSpec) -> tuple[DataFrame, str, str]:
    if not intent_spec.group_column:
        raise ValueError("Group-by intent requires a grouping column.")
    agg_expr, alias = _aggregate_expression(intent_spec)
    result_df = spark_df.groupBy(intent_spec.group_column).agg(agg_expr).orderBy(F.col(alias).desc())
    logic = (
        f"result_df = spark_df.groupBy('{intent_spec.group_column}')"
        f".agg(F.{intent_spec.aggregation}(F.col('{intent_spec.metric_column}')).alias('{alias}'))"
        f".orderBy(F.col('{alias}').desc())"
    )
    explanation = (
        f"This groups the dataset by `{intent_spec.group_column}` and computes the "
        f"{intent_spec.aggregation} of `{intent_spec.metric_column}` for each group."
    )
    return result_df, logic, explanation


def _run_sorting(spark_df: DataFrame, intent_spec: IntentSpec) -> tuple[DataFrame, str, str]:
    grouped_df, _, _ = _run_group_by(spark_df, intent_spec)
    metric_alias = grouped_df.columns[-1]
    sort_expr = F.col(metric_alias).asc() if intent_spec.sort_direction == "asc" else F.col(metric_alias).desc()
    result_df = grouped_df.orderBy(sort_expr)
    logic = (
        f"grouped_df = spark_df.groupBy('{intent_spec.group_column}')"
        f".agg(F.{intent_spec.aggregation}(F.col('{intent_spec.metric_column}')).alias('{metric_alias}'))\n"
        f"result_df = grouped_df.orderBy(F.col('{metric_alias}').{intent_spec.sort_direction}())"
    )
    explanation = (
        f"This summarizes `{intent_spec.metric_column}` by `{intent_spec.group_column}` and sorts the results "
        f"in {intent_spec.sort_direction} order."
    )
    return result_df, logic, explanation


def _run_top_n(spark_df: DataFrame, intent_spec: IntentSpec) -> tuple[DataFrame, str, str]:
    grouped_df, _, _ = _run_group_by(spark_df, intent_spec)
    metric_alias = grouped_df.columns[-1]
    result_df = grouped_df.orderBy(F.col(metric_alias).desc()).limit(intent_spec.top_n)
    logic = (
        f"grouped_df = spark_df.groupBy('{intent_spec.group_column}')"
        f".agg(F.{intent_spec.aggregation}(F.col('{intent_spec.metric_column}')).alias('{metric_alias}'))\n"
        f"result_df = grouped_df.orderBy(F.col('{metric_alias}').desc()).limit({intent_spec.top_n})"
    )
    explanation = (
        f"This ranks `{intent_spec.group_column}` by {intent_spec.aggregation} `{intent_spec.metric_column}` "
        f"and returns the top {intent_spec.top_n} rows."
    )
    return result_df, logic, explanation


def _run_filtering(spark_df: DataFrame, intent_spec: IntentSpec) -> tuple[DataFrame, str, str]:
    if not intent_spec.filter_column or not intent_spec.filter_value:
        raise ValueError("Filtering intent requires a filter column and value.")
    agg_expr, alias = _aggregate_expression(intent_spec)
    result_df = spark_df.filter(F.lower(F.col(intent_spec.filter_column)) == intent_spec.filter_value.lower()).agg(agg_expr)
    logic = (
        f"filtered_df = spark_df.filter(F.lower(F.col('{intent_spec.filter_column}')) == '{intent_spec.filter_value.lower()}')\n"
        f"result_df = filtered_df.agg(F.{intent_spec.aggregation}(F.col('{intent_spec.metric_column}')).alias('{alias}'))"
    )
    explanation = (
        f"This filters the dataset where `{intent_spec.filter_column}` equals `{intent_spec.filter_value}` "
        f"and computes the {intent_spec.aggregation} of `{intent_spec.metric_column}`."
    )
    return result_df, logic, explanation


def _run_time_trend(spark_df: DataFrame, intent_spec: IntentSpec) -> tuple[DataFrame, str, str]:
    if not intent_spec.time_column:
        raise ValueError("Time-trend intent requires a date or timestamp column.")
    agg_expr, alias = _aggregate_expression(intent_spec)
    format_map = {"day": "yyyy-MM-dd", "week": "yyyy-'W'ww", "month": "yyyy-MM"}
    result_df = (
        spark_df.withColumn(
            "time_period",
            F.date_format(F.to_timestamp(F.col(intent_spec.time_column)), format_map[intent_spec.time_grain]),
        )
        .groupBy("time_period")
        .agg(agg_expr)
        .orderBy("time_period")
    )
    logic = (
        f"bucketed_df = spark_df.withColumn('time_period', "
        f"F.date_format(F.to_timestamp(F.col('{intent_spec.time_column}')), '{format_map[intent_spec.time_grain]}'))\n"
        f"result_df = bucketed_df.groupBy('time_period')"
        f".agg(F.{intent_spec.aggregation}(F.col('{intent_spec.metric_column}')).alias('{alias}'))"
        f".orderBy('time_period')"
    )
    explanation = (
        f"This buckets `{intent_spec.time_column}` by {intent_spec.time_grain} and calculates the "
        f"{intent_spec.aggregation} of `{intent_spec.metric_column}` over time."
    )
    return result_df, logic, explanation
