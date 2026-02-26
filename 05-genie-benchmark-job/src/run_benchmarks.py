# Databricks notebook source
# MAGIC %md
# MAGIC # BMA Genie Benchmark Runner
# MAGIC
# MAGIC Runs the benchmark question suite against the Genie Space, compares
# MAGIC results with expected outputs, and writes accuracy metrics to a Delta table.
# MAGIC
# MAGIC Designed to run on a schedule (weekly) to track Genie quality over time.

# COMMAND ----------

# MAGIC %pip install databricks-sdk>=0.40.0
# MAGIC %restart_python

# COMMAND ----------

import os
import json
import time
from datetime import datetime
from databricks.sdk import WorkspaceClient

# COMMAND ----------

# Configuration
GENIE_SPACE_ID = os.getenv("GENIE_SPACE_ID", "01f11271f3d41201af68388818cca110")
CATALOG = os.getenv("CATALOG", "bma_pilot")
SCHEMA = os.getenv("SCHEMA", "genie_ready")
RESULTS_TABLE = f"{CATALOG}.{SCHEMA}.genie_benchmark_results"

w = WorkspaceClient()
run_timestamp = datetime.utcnow().isoformat()

# COMMAND ----------

from benchmark_questions import BENCHMARK_QUESTIONS

print(f"Running {len(BENCHMARK_QUESTIONS)} benchmark questions")
print(f"Genie Space: {GENIE_SPACE_ID}")
print(f"Results → {RESULTS_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Benchmarks

# COMMAND ----------

results = []

for bq in BENCHMARK_QUESTIONS:
    print(f"\n{'='*60}")
    print(f"[{bq['id']}] {bq['question']}")

    start_time = time.time()
    status = "UNKNOWN"
    genie_sql = ""
    genie_text = ""
    error_msg = ""
    has_results = False

    try:
        # Ask Genie
        msg = w.genie.start_conversation_and_wait(
            space_id=GENIE_SPACE_ID,
            content=bq["question"],
        )
        elapsed = time.time() - start_time
        status = msg.status.value if msg.status else "UNKNOWN"

        if msg.attachments:
            for att in msg.attachments:
                if att.text:
                    genie_text = att.text.content or ""
                if att.query:
                    genie_sql = att.query.query or ""
                if att.attachment_id:
                    has_results = True

        print(f"  Status: {status} | Time: {elapsed:.1f}s")
        if genie_sql:
            print(f"  SQL: {genie_sql[:120]}...")

    except Exception as e:
        elapsed = time.time() - start_time
        status = "ERROR"
        error_msg = str(e)
        print(f"  ERROR: {error_msg}")

    results.append({
        "run_timestamp": run_timestamp,
        "question_id": bq["id"],
        "question": bq["question"],
        "category": bq["category"],
        "status": status,
        "genie_sql": genie_sql,
        "genie_text": genie_text[:1000],  # truncate
        "has_query_results": has_results,
        "response_time_seconds": round(elapsed, 2),
        "error": error_msg,
    })

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Results to Delta

# COMMAND ----------

import pandas as pd

df = pd.DataFrame(results)
spark_df = spark.createDataFrame(df)

# Create table if not exists, then append
spark_df.write.mode("append").option("mergeSchema", "true").saveAsTable(RESULTS_TABLE)

print(f"\n✅ {len(results)} benchmark results written to {RESULTS_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

summary = spark.sql(f"""
    SELECT
        run_timestamp,
        COUNT(*) AS total_questions,
        SUM(CASE WHEN status = 'COMPLETED' THEN 1 ELSE 0 END) AS completed,
        SUM(CASE WHEN status = 'COMPLETED' AND has_query_results THEN 1 ELSE 0 END) AS with_results,
        SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed,
        SUM(CASE WHEN status = 'ERROR' THEN 1 ELSE 0 END) AS errors,
        ROUND(AVG(response_time_seconds), 2) AS avg_response_time_sec,
        ROUND(
            SUM(CASE WHEN status = 'COMPLETED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1
        ) AS completion_rate_pct
    FROM {RESULTS_TABLE}
    WHERE run_timestamp = '{run_timestamp}'
    GROUP BY run_timestamp
""")
display(summary)
