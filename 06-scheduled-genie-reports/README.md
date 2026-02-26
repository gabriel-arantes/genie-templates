# 06 — Scheduled Genie Reports

Automated weekly report job that asks Genie predefined business questions
and generates a polished HTML report saved to a Unity Catalog Volume.

## What It Does

1. Asks 5 business questions to the Genie Space (configurable)
2. Collects text responses, SQL, and query results
3. Renders an HTML report with tables and expandable SQL
4. Saves to `/Volumes/my_catalog/genie_ready/reports/`
5. Sends email notification on completion

## Report Sections

- Bermuda CPI Snapshot
- Global Rankings (Top 10)
- Peer Comparison (Bermuda vs US, UK, Canada)
- Trends (5-year average)
- Data Coverage summary

## Deploy

```bash
databricks bundle deploy
databricks bundle run my_genie_weekly_report  # manual trigger
```

## Customization

Edit `REPORT_QUESTIONS` in `src/generate_report.py` to add, remove,
or modify report sections. Each section is simply a natural-language
question — no SQL authoring needed.
