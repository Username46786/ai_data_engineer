from __future__ import annotations

import os
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any

import pandas as pd
from pyspark.sql import DataFrame, SparkSession


_SPARK_SESSION: SparkSession | None = None


def _spark_temp_dir() -> str:
    return os.getenv("SPARK_LOCAL_DIR", "/tmp/spark-temp")


def get_spark_session() -> SparkSession:
    global _SPARK_SESSION
    if _SPARK_SESSION is None:
        temp_dir = _spark_temp_dir()
        os.makedirs(temp_dir, exist_ok=True)
        _SPARK_SESSION = (
            SparkSession.builder.appName("AI Data Engineer")
            .master(os.getenv("SPARK_MASTER", "local[2]"))
            .config("spark.ui.enabled", "false")
            .config("spark.ui.showConsoleProgress", "false")
            .config("spark.sql.repl.eagerEval.enabled", "false")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "4"))
            .config("spark.default.parallelism", os.getenv("SPARK_DEFAULT_PARALLELISM", "4"))
            .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "1g"))
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.local.dir", temp_dir)
            .config("spark.sql.warehouse.dir", os.path.join(temp_dir, "warehouse"))
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.python.worker.reuse", "true")
            .getOrCreate()
        )
        _SPARK_SESSION.sparkContext.setLogLevel("ERROR")
    return _SPARK_SESSION


def stop_spark() -> None:
    global _SPARK_SESSION
    if _SPARK_SESSION is not None:
        _SPARK_SESSION.stop()
        _SPARK_SESSION = None


def _materialize_upload(uploaded_file: Any) -> tuple[str, str]:
    suffix = Path(uploaded_file.name).suffix or ".csv"
    with NamedTemporaryFile(delete=False, suffix=suffix) as tmp_file:
        tmp_file.write(uploaded_file.getbuffer())
        return tmp_file.name, uploaded_file.name


def load_csv_to_spark(file_source: Any) -> tuple[DataFrame, dict[str, Any]]:
    spark = get_spark_session()
    if hasattr(file_source, "getbuffer"):
        csv_path, source_label = _materialize_upload(file_source)
    else:
        csv_path = str(file_source)
        source_label = Path(csv_path).name

    spark_df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .option("dateFormat", "yyyy-MM-dd")
        .csv(csv_path)
    )

    dataset_info = preview_dataset(spark_df)
    dataset_info["source_label"] = source_label
    return spark_df, dataset_info


def preview_dataset(spark_df: DataFrame, limit: int = 10) -> dict[str, Any]:
    return {
        "preview_df": spark_df.limit(limit).toPandas(),
        "schema_df": pd.DataFrame(
            [{"column": field.name, "type": field.dataType.simpleString()} for field in spark_df.schema.fields]
        ),
        "row_count": spark_df.count(),
        "column_names": spark_df.columns,
    }
