# 03 — Databricks App with Genie (Gradio)

Full-stack web application deployed as a Databricks App with Genie Space
as a managed resource. Users get a chat interface, generated SQL preview,
and tabular results — all inside Databricks.

## Features

- 💬 Chat interface with conversation context (follow-up questions)
- 📊 Query results displayed as interactive tables
- 🔍 Generated SQL visible for transparency
- 🔒 Runs on the app's service principal — no user tokens needed

## Deploy

```bash
databricks bundle validate
databricks bundle deploy
```

The app URL will be printed after deployment. Share it with stakeholders.

## Architecture

```
User Browser → Databricks App (Gradio) → SDK w.genie → Genie Space → SQL Warehouse
```

The Genie Space is added as an **app resource** in `databricks.yml`, so the
app's service principal automatically gets the right permissions.

## References

- [Add Genie Space resource to Databricks App](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/genie)
- [Manage Apps with DABs](https://docs.databricks.com/aws/en/dev-tools/bundles/apps-tutorial)
