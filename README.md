# AI Data Engineer

AI Data Engineer is a Streamlit demo that lets users upload a CSV, load it into a PySpark DataFrame, ask a natural-language business question, and get a Spark-powered answer as a chart, table, explanation, and visible Spark logic.

## Prerequisites

- Python 3.10 or newer
- Java 11 or newer available on your `PATH` because PySpark needs a local JVM

## Features

- CSV upload with Spark schema inference
- Dataset preview, schema, row count, and column list
- Safe intent-based query execution
- Structured support for `aggregation`, `group by`, `sorting`, `top N`, `filtering`, and `time trend`
- `result_df` is produced in Spark first, then converted to pandas only for display
- Clear output with chart, result table, explanation, and visible PySpark logic

## Files

- `app.py`
- `spark_session.py`
- `nl_to_intent.py`
- `spark_templates.py`
- `executor.py`
- `visualizer.py`
- `sample_data/sales.csv`
- `README.md`
- `requirements.txt`

## Example questions

- Which region drives the most revenue?
- Monthly sales trend
- Average order value by category
- Count customers by state
- Top 5 categories by sales

## Setup

1. Create a virtual environment.
2. Activate the virtual environment.
3. Install dependencies.

```bash
pip install -r requirements.txt
```

4. Start the app.

```bash
streamlit run app.py
```

If you want a single project command on Windows, you can also run:

```bat
run_app.bat
```

The batch script also helps on machines where `streamlit`, `python`, or `java` are not already available on `PATH`, as long as the local project package folder or the ArcGIS Python runtime is present.

## Entrypoint

- Streamlit entrypoint: `app.py`
- Main local launch command: `streamlit run app.py`
- The bundled demo dataset is loaded from `sample_data/sales.csv` using an absolute path derived from `app.py`, so it works even if you launch Streamlit from another directory.
- Spark runs in lightweight local mode with cloud-friendly defaults: `local[2]`, low shuffle partitions, localhost binding, and `/tmp` storage.

## Render Deployment

Deploy this repo as a Render Python Web Service.

- Build command: `pip install -r requirements.txt`
- Start command: `streamlit run app.py --server.port=$PORT --server.address=0.0.0.0`
- Python version: use Python 3.11
- Environment: make sure Java 11+ is available because PySpark needs a JVM

Render note:

- Add a repo-root `.python-version` file with `3.11.11` so Render uses Python 3.11 instead of its newer default, which can break `pyarrow` installs.
- If PySpark fails on Render with `JAVA_GATEWAY_EXITED`, deploy with Docker so Java is installed in the runtime image.

## Docker Deployment

For Render, you can deploy this repo as a Docker web service instead of a plain Python service.

- Render will build from the repo `Dockerfile`
- The image uses Python 3.11 and installs OpenJDK 17
- The container starts with:
  `streamlit run app.py --server.port=$PORT --server.address=0.0.0.0`

This keeps the existing app architecture unchanged while ensuring PySpark has a working JVM at runtime.

Recommended environment variables:

- `SPARK_DRIVER_MEMORY=1g`
- `SPARK_SQL_SHUFFLE_PARTITIONS=4`
- `SPARK_DEFAULT_PARALLELISM=4`
- `SPARK_LOCAL_DIR=/tmp/spark-temp`
- `PANDAS_USE_NUMEXPR=0`

Deployment notes:

- This app keeps Streamlit as the UI and uses PySpark in local mode, which is appropriate for demos and smaller uploaded CSVs.
- On Render, Spark temp data should stay in `/tmp`, which is what the app uses by default.
- Keep uploads reasonably small because the app converts the final Spark result to pandas for display and charting.
- If your Render environment does not already provide Java, use a base image or setup step that includes OpenJDK 11+.

## Demo flow

1. Start with the sample dataset.
2. Show the preview and schema.
3. Ask `Which region drives the most revenue?`
4. Ask `Monthly sales trend`
5. Highlight the visible Spark logic and safe template mapping.

## Reliability

- No arbitrary free-form code generation
- Deterministic intent classification and Spark templates
- Graceful errors when a question cannot be mapped safely
- Imports are local module imports, so running from the project root works without package restructuring
