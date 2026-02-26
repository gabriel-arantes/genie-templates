# Databricks notebook source
# MAGIC %md
# MAGIC # BMA CPI Dashboard — Setup & Configuration
# MAGIC
# MAGIC This notebook creates an AI/BI Lakeview Dashboard with:
# MAGIC - CPI index levels, YoY inflation trends, and regional comparisons
# MAGIC - **Ask Genie** enabled so users can ask follow-up questions in natural language
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Genie Space configured with `bma_pilot.genie_ready.cpi_world_country_aggregates`
# MAGIC - SQL Warehouse (Starter or Pro)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Configuration

# COMMAND ----------

# Configurable parameters — update these for your environment
CATALOG = "bma_pilot"
SCHEMA = "genie_ready"
TABLE = "cpi_world_country_aggregates"
WAREHOUSE_ID = "5eb73ca40f08c607"                # SQL Warehouse for the dashboard
GENIE_SPACE_ID = "01f11271f3d41201af68388818cca110"  # Genie Space ID

DASHBOARD_NAME = "BMA — CPI World Regional Aggregates"
DASHBOARD_PARENT_PATH = "/Shared/bma-dashboards"
FQN = f"{CATALOG}.{SCHEMA}.{TABLE}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Create Dashboard via Lakeview API
# MAGIC
# MAGIC The dashboard is defined as a serialized JSON payload. We use the Databricks
# MAGIC SDK to create it programmatically so it can be version-controlled and
# MAGIC replicated across workspaces.
# MAGIC
# MAGIC ### Table schema
# MAGIC | Column | Type | Description |
# MAGIC |---|---|---|
# MAGIC | `series_code` | string | IMF series identifier |
# MAGIC | `country_code` | string | Regional aggregate (World, G7, Africa, etc.) |
# MAGIC | `transformation_type` | string | Index, MoM % change, YoY % change |
# MAGIC | `transformation_label` | string | Human-readable transformation description |
# MAGIC | `period` | string | YYYY-MM format |
# MAGIC | `year` | bigint | Calendar year |
# MAGIC | `month` | bigint | Calendar month (1-12) |
# MAGIC | `cpi_value` | double | CPI value |

# COMMAND ----------

import json
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.dashboards import Dashboard

w = WorkspaceClient()

# Ensure parent folder exists
try:
    w.workspace.mkdirs(DASHBOARD_PARENT_PATH)
except Exception:
    pass  # already exists

# COMMAND ----------

# Dashboard definition — Lakeview serialized format
dashboard_spec = {
    "pages": [
        {
            "name": "cpi_overview",
            "displayName": "CPI Overview",
            "layout": []
        }
    ],
    "datasets": [
        {
            "name": "latest_index_by_region",
            "displayName": "Latest CPI Index by Region",
            "query": f"""
                SELECT country_code AS region,
                       period,
                       cpi_value AS cpi_index
                FROM {FQN}
                WHERE transformation_type = 'Index'
                  AND period = (
                      SELECT MAX(period) FROM {FQN}
                      WHERE transformation_type = 'Index'
                        AND cpi_value IS NOT NULL
                  )
                ORDER BY cpi_value DESC
            """
        },
        {
            "name": "world_index_trend",
            "displayName": "World CPI Index Trend (Monthly)",
            "query": f"""
                SELECT period,
                       year,
                       month,
                       cpi_value AS cpi_index
                FROM {FQN}
                WHERE country_code = 'World'
                  AND transformation_type = 'Index'
                  AND cpi_value IS NOT NULL
                ORDER BY period
            """
        },
        {
            "name": "yoy_inflation_by_region",
            "displayName": "Year-over-Year Inflation by Region",
            "query": f"""
                SELECT country_code AS region,
                       period,
                       year,
                       month,
                       cpi_value AS yoy_pct_change
                FROM {FQN}
                WHERE transformation_type = 'Period average, Year-over-year (YOY) percent change'
                  AND cpi_value IS NOT NULL
                  AND year >= 2018
                ORDER BY period, country_code
            """
        },
        {
            "name": "top_regions_yoy",
            "displayName": "Top Regions by YoY Inflation (Latest)",
            "query": f"""
                SELECT country_code AS region,
                       cpi_value AS yoy_pct_change,
                       period
                FROM {FQN}
                WHERE transformation_type = 'Period average, Year-over-year (YOY) percent change'
                  AND period = (
                      SELECT MAX(period) FROM {FQN}
                      WHERE transformation_type = 'Period average, Year-over-year (YOY) percent change'
                        AND cpi_value IS NOT NULL
                  )
                ORDER BY cpi_value DESC
            """
        },
        {
            "name": "g7_vs_emerging_index",
            "displayName": "G7 vs Emerging Markets — CPI Index",
            "query": f"""
                SELECT country_code AS region,
                       period,
                       cpi_value AS cpi_index
                FROM {FQN}
                WHERE transformation_type = 'Index'
                  AND country_code IN ('G7', 'Emerging Market and Developing Economies', 'World')
                  AND cpi_value IS NOT NULL
                ORDER BY period, country_code
            """
        },
        {
            "name": "mom_change_world",
            "displayName": "World Month-over-Month CPI Change",
            "query": f"""
                SELECT period,
                       year,
                       month,
                       cpi_value AS mom_pct_change
                FROM {FQN}
                WHERE country_code = 'World'
                  AND transformation_type = 'Period average, Period-over-period percent change'
                  AND cpi_value IS NOT NULL
                  AND year >= 2020
                ORDER BY period
            """
        },
        {
            "name": "data_coverage",
            "displayName": "Data Coverage Summary",
            "query": f"""
                SELECT COUNT(DISTINCT country_code) AS num_regions,
                       COUNT(DISTINCT transformation_type) AS num_metrics,
                       MIN(period) AS earliest_period,
                       MAX(period) AS latest_period,
                       MIN(year) AS earliest_year,
                       MAX(year) AS latest_year,
                       COUNT(*) AS total_records
                FROM {FQN}
                WHERE cpi_value IS NOT NULL
            """
        }
    ]
}

print(f"Dashboard specification created with {len(dashboard_spec['datasets'])} datasets")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Create the Dashboard
# MAGIC
# MAGIC Using the Lakeview API. After creation, navigate to the dashboard in the UI
# MAGIC to arrange widgets visually and publish.

# COMMAND ----------

dashboard = w.lakeview.create(
    dashboard=Dashboard(
        display_name=DASHBOARD_NAME,
        parent_path=DASHBOARD_PARENT_PATH,
        warehouse_id=WAREHOUSE_ID,
        serialized_dashboard=json.dumps(dashboard_spec),
    )
)

dashboard_url = f"{w.config.host}sql/dashboardsv3/{dashboard.dashboard_id}"
print(f"✅ Dashboard created: {dashboard_url}")
print(f"   Dashboard ID: {dashboard.dashboard_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Enable Ask Genie on the Dashboard
# MAGIC
# MAGIC To enable Ask Genie:
# MAGIC 1. Open the dashboard in the UI at the URL above
# MAGIC 2. Click **Edit** → **Settings** (gear icon)
# MAGIC 3. Under **AI/BI Genie**, toggle **Enable Genie** and select your Genie Space
# MAGIC 4. **Publish** the dashboard
# MAGIC
# MAGIC Once enabled, users will see a "Ask Genie" button on the published dashboard
# MAGIC that lets them ask natural-language follow-up questions about the data.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Publish the Dashboard
# MAGIC
# MAGIC Publish with embedded credentials so consumers don't need warehouse access.

# COMMAND ----------

w.lakeview.publish(
    dashboard_id=dashboard.dashboard_id,
    warehouse_id=WAREHOUSE_ID,
    embed_credentials=True,
)

print(f"✅ Dashboard published with embedded credentials")
print(f"   Published URL: {dashboard_url}")
