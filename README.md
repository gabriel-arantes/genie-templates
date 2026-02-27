# Acme Genie Space — Consumption Templates

Databricks Asset Bundle (DAB) templates for consuming the Acme Genie Space
(`my_catalog.genie_ready.cpi_world_country_aggregates`) across multiple surfaces.

## Templates

| #  | Template                        | Surface         | Description                                                                                  |
|----|---------------------------------|-----------------|----------------------------------------------------------------------------------------------|
| 01 | **Teams Bot**                   | Microsoft Teams | Azure Bot Service + Azure Web App that proxies natural-language questions to Genie            |
| 02 | **AI/BI Dashboard + Ask Genie** | Databricks UI   | Lakeview Dashboard with CPI KPIs + Genie enabled for ad-hoc follow-up questions              |
| 03 | **Databricks App (Gradio)**     | Databricks Apps | Full-stack Gradio app with Genie Space as a resource — chat + table + chart in one UI        |
| 04 | **LangGraph Agent**             | Agent Framework | Multi-tool LangGraph agent using `GenieAgent` as a tool, logged & served via MLflow          |
| 05 | **Genie Benchmark Job**         | Lakeflow Jobs   | Scheduled job that runs benchmark questions against Genie and writes accuracy metrics to UC   |
| 06 | **Scheduled Genie Reports**     | Lakeflow Jobs   | Cron job that asks Genie predefined questions, formats answers, and emails stakeholders       |

## Prerequisites

- Databricks CLI ≥ 0.250.0
- A curated Genie Space with `my_catalog.genie_ready.cpi_world_country_aggregates`
- SQL Pro or Serverless Warehouse
- Unity Catalog enabled workspace
- (Templates 01) Azure subscription for Bot Service + Web App

## Quick Start

```bash
# 1. Clone / copy any template folder
cd 03-databricks-app-genie

# 2. Edit variables in databricks.yml (workspace host, genie_space_id, warehouse_id)
#    Placeholders are marked as ${var.xxx}

# 3. Validate
databricks bundle validate

# 4. Deploy (defaults to "dev" target)
databricks bundle deploy

# 5. Run (if the bundle defines a job)
databricks bundle run <job_name>
```

## Configuration Variables

Every template uses the same set of variables so you configure once:

| Variable            | Description                                   | Example                                  |
|---------------------|-----------------------------------------------|------------------------------------------|
| `genie_space_id`    | ID of the Genie Space (from URL)              | `01f04abcde1234567890abcd`               |
| `warehouse_id`                | SQL Warehouse ID (Starter Warehouse)          | `5eb73ca40f08c607`                       |
| `catalog`           | Unity Catalog catalog                         | `my_catalog`                              |
| `schema`            | Unity Catalog schema                          | `genie_ready`                            |
| `workspace_host`    | Databricks workspace URL                      | `https://dbc-7749e812-1ece.cloud.databricks.com/` |

## Replication to Client Environment

These templates are designed for near-100% reuse. To migrate:

1. Update `databricks.yml` variables to point to the client workspace + Genie Space.
2. `databricks bundle deploy -t prod`
3. For the Teams Bot (01): re-deploy the Azure infra via the included Terraform or manual steps.
