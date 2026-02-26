# 05 — Genie Benchmark Job

Scheduled Lakeflow Job that runs 10 benchmark questions against the Genie Space
every Monday, measures accuracy and response times, and writes results to a
Delta table for longitudinal quality tracking.

## Why This Matters

- Track Genie accuracy over time as knowledge store evolves
- Catch regressions early (e.g., after table schema changes)
- Provide quantitative evidence for stakeholder reviews

## Output Table

`my_catalog.genie_ready.genie_benchmark_results`

| Column                | Type     |
|-----------------------|----------|
| run_timestamp         | STRING   |
| question_id           | STRING   |
| question              | STRING   |
| category              | STRING   |
| status                | STRING   |
| genie_sql             | STRING   |
| has_query_results     | BOOLEAN  |
| response_time_seconds | DOUBLE   |
| completion_rate_pct   | DOUBLE   |

## Deploy

```bash
databricks bundle deploy
databricks bundle run my_genie_benchmark  # manual trigger
```

The job also runs automatically every Monday at 8 AM (Atlantic/Bermuda timezone).
