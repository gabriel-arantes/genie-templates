# 02 — AI/BI Dashboard with Ask Genie

Lakeview Dashboard that visualizes CPI World Country Aggregates data with
6 widgets (overview, trends, rankings, YoY analysis), plus **Ask Genie**
enabled for ad-hoc natural-language follow-up questions.

## What Users Get

- Pre-built KPI dashboard accessible via browser (no SQL knowledge needed)
- "Ask Genie" button on the published dashboard for follow-up questions
- Embedded credentials so viewers don't need warehouse access

## Deploy

```bash
databricks bundle validate
databricks bundle deploy
databricks bundle run bma_dashboard_setup
```

After the job runs, open the dashboard URL printed in the output to:
1. Arrange widgets visually in the editor
2. Enable Ask Genie in **Settings → AI/BI Genie**
3. Re-publish

## SQL Queries

All dashboard queries are in `src/dashboard_queries.sql` for reference
and can be used independently or modified for other dashboards.
