# Databricks notebook source
# MAGIC %md
# MAGIC # BMA CPI Weekly Report via Genie
# MAGIC
# MAGIC This job asks Genie a set of predefined business questions,
# MAGIC collects the responses (text + data), and generates an HTML report
# MAGIC saved to a Unity Catalog Volume for distribution.
# MAGIC
# MAGIC This demonstrates how Genie can be used for **automated reporting**
# MAGIC without any human writing SQL.

# COMMAND ----------

# MAGIC %pip install databricks-sdk>=0.40.0 jinja2
# MAGIC %restart_python

# COMMAND ----------

import os
import time
import json
from datetime import datetime
from databricks.sdk import WorkspaceClient

# COMMAND ----------

# Configuration
GENIE_SPACE_ID = os.getenv("GENIE_SPACE_ID", "01f11271f3d41201af68388818cca110")
CATALOG = os.getenv("CATALOG", "bma_pilot")
SCHEMA = os.getenv("SCHEMA", "genie_ready")
VOLUME = "reports"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

w = WorkspaceClient()
report_date = datetime.utcnow().strftime("%Y-%m-%d")

# COMMAND ----------

# Ensure volume exists
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Report Questions
# MAGIC
# MAGIC These are the business questions that the weekly report answers.
# MAGIC Modify this list to change the report content.

# COMMAND ----------

REPORT_QUESTIONS = [
    {
        "section": "Bermuda CPI Snapshot",
        "question": "What is the most recent CPI value for Bermuda and how does it compare to the previous year?",
    },
    {
        "section": "Global Rankings",
        "question": "List the top 10 countries with the highest CPI in the most recent year available",
    },
    {
        "section": "Peer Comparison",
        "question": "Compare the CPI of Bermuda, United States, United Kingdom, and Canada for the last 5 available years",
    },
    {
        "section": "Trends",
        "question": "What is the average CPI across all countries for each of the last 5 years?",
    },
    {
        "section": "Data Coverage",
        "question": "How many countries and years are covered in the dataset?",
    },
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query Genie for Each Section

# COMMAND ----------

sections = []

for rq in REPORT_QUESTIONS:
    print(f"\n📊 Section: {rq['section']}")
    print(f"   Question: {rq['question']}")

    try:
        msg = w.genie.start_conversation_and_wait(
            space_id=GENIE_SPACE_ID,
            content=rq["question"],
        )

        text_response = ""
        sql_response = ""
        table_html = ""

        if msg.attachments:
            for att in msg.attachments:
                if att.text:
                    text_response = att.text.content or ""
                if att.query:
                    sql_response = att.query.query or ""

                # Fetch query results for table rendering
                if att.attachment_id:
                    try:
                        qr = w.genie.get_message_attachment_query_result(
                            space_id=GENIE_SPACE_ID,
                            conversation_id=msg.conversation_id,
                            message_id=msg.id,
                            attachment_id=att.attachment_id,
                        )
                        if qr.columns and qr.data_array:
                            col_names = [c.name for c in qr.columns]
                            rows = qr.data_array[:50]  # limit rows
                            # Build HTML table
                            header = "".join(f"<th>{c}</th>" for c in col_names)
                            body = ""
                            for row in rows:
                                body += "<tr>" + "".join(f"<td>{v}</td>" for v in row) + "</tr>"
                            table_html = f"<table><thead><tr>{header}</tr></thead><tbody>{body}</tbody></table>"
                    except Exception as e:
                        print(f"   ⚠️ Could not fetch results: {e}")

        sections.append({
            "title": rq["section"],
            "question": rq["question"],
            "text": text_response,
            "sql": sql_response,
            "table_html": table_html,
            "status": "OK",
        })
        print(f"   ✅ Done")

    except Exception as e:
        sections.append({
            "title": rq["section"],
            "question": rq["question"],
            "text": "",
            "sql": "",
            "table_html": "",
            "status": f"Error: {e}",
        })
        print(f"   ❌ Error: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate HTML Report

# COMMAND ----------

from jinja2 import Template

REPORT_TEMPLATE = Template("""
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>BMA CPI Weekly Report — {{ report_date }}</title>
<style>
  body { font-family: 'Segoe UI', Arial, sans-serif; max-width: 900px; margin: 40px auto; color: #333; }
  h1 { color: #1a365d; border-bottom: 3px solid #2b6cb0; padding-bottom: 10px; }
  h2 { color: #2b6cb0; margin-top: 30px; }
  .question { color: #666; font-style: italic; margin-bottom: 8px; }
  .response { background: #f7fafc; padding: 15px; border-radius: 8px; margin: 10px 0; }
  .sql { background: #1a202c; color: #a0d2db; padding: 12px; border-radius: 6px;
         font-family: monospace; font-size: 13px; overflow-x: auto; white-space: pre-wrap; }
  table { border-collapse: collapse; width: 100%; margin: 10px 0; }
  th { background: #2b6cb0; color: white; padding: 8px 12px; text-align: left; }
  td { padding: 6px 12px; border-bottom: 1px solid #e2e8f0; }
  tr:nth-child(even) { background: #f7fafc; }
  .footer { margin-top: 40px; padding-top: 20px; border-top: 1px solid #e2e8f0;
            color: #999; font-size: 12px; }
  .error { color: #e53e3e; }
</style>
</head>
<body>
<h1>🏛️ BMA CPI Weekly Report</h1>
<p><strong>Date:</strong> {{ report_date }} | <strong>Source:</strong> Databricks Genie Space</p>

{% for section in sections %}
<h2>{{ section.title }}</h2>
<p class="question">📝 {{ section.question }}</p>

{% if section.status == 'OK' %}
  {% if section.text %}
  <div class="response">{{ section.text }}</div>
  {% endif %}
  {% if section.table_html %}
  {{ section.table_html }}
  {% endif %}
  {% if section.sql %}
  <details><summary>View SQL</summary><div class="sql">{{ section.sql }}</div></details>
  {% endif %}
{% else %}
  <p class="error">{{ section.status }}</p>
{% endif %}
{% endfor %}

<div class="footer">
  Generated automatically by the BMA Genie Scheduled Reports job.<br>
  Powered by Databricks AI/BI Genie Space.
</div>
</body>
</html>
""")

html_report = REPORT_TEMPLATE.render(
    report_date=report_date,
    sections=sections,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Report

# COMMAND ----------

report_filename = f"bma_cpi_report_{report_date}.html"
report_path = f"{VOLUME_PATH}/{report_filename}"

dbutils.fs.put(report_path.replace("/Volumes/", "dbfs:/Volumes/"), html_report, overwrite=True)

print(f"✅ Report saved to: {report_path}")
print(f"   Sections: {len(sections)}")
print(f"   Successful: {sum(1 for s in sections if s['status'] == 'OK')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display Report Preview

# COMMAND ----------

displayHTML(html_report)
