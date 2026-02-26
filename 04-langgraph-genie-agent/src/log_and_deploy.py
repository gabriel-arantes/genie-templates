# Databricks notebook source
# MAGIC %md
# MAGIC # BMA CPI LangGraph Agent — Log, Register & Deploy
# MAGIC
# MAGIC This notebook:
# MAGIC 1. Logs the LangGraph agent to MLflow
# MAGIC 2. Registers it in Unity Catalog
# MAGIC 3. Deploys it to a Model Serving endpoint
# MAGIC
# MAGIC The served agent can then be consumed by the Teams bot, Databricks App,
# MAGIC or any other client via REST API.

# COMMAND ----------

# MAGIC %pip install mlflow>=2.18 langchain langgraph databricks-langchain pydantic
# MAGIC %restart_python

# COMMAND ----------

import os
import mlflow

# Configuration — update these
GENIE_SPACE_ID = "01f11271f3d41201af68388818cca110"
LLM_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"
CATALOG = "bma_pilot"
SCHEMA = "genie_ready"
MODEL_NAME = f"{CATALOG}.{SCHEMA}.bma_cpi_genie_agent"

os.environ["GENIE_SPACE_ID"] = GENIE_SPACE_ID
os.environ["LLM_ENDPOINT"] = LLM_ENDPOINT

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Test the agent locally

# COMMAND ----------

from agent import agent

# Quick test
test_result = agent.invoke(
    {"messages": [{"role": "user", "content": "What is the CPI for Bermuda in the most recent year?"}]}
)
for msg in test_result["messages"]:
    role = msg.get("role", msg.get("type", "unknown"))
    content = msg.get("content", "")
    if content:
        print(f"[{role}]: {content[:200]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Log the agent with MLflow

# COMMAND ----------

from mlflow.models.resources import DatabricksGenieSpace, DatabricksServingEndpoint

mlflow.set_registry_uri("databricks-uc")

# Declare the resources the agent needs — this is critical for serving
resources = [
    DatabricksServingEndpoint(endpoint_name=LLM_ENDPOINT),
    DatabricksGenieSpace(genie_space_id=GENIE_SPACE_ID),
]

with mlflow.start_run(run_name="bma_cpi_genie_agent") as run:
    logged_agent = mlflow.pyfunc.log_model(
        artifact_path="agent",
        python_model="agent.py",
        pip_requirements=[
            "mlflow>=2.18",
            "langchain",
            "langgraph",
            "databricks-langchain",
            "pydantic",
        ],
        resources=resources,
    )
    print(f"✅ Agent logged: {logged_agent.model_uri}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Register in Unity Catalog

# COMMAND ----------

registered = mlflow.register_model(
    model_uri=logged_agent.model_uri,
    name=MODEL_NAME,
)
print(f"✅ Registered: {MODEL_NAME} version {registered.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Deploy to Model Serving
# MAGIC
# MAGIC You can deploy via the UI (Model Serving page) or programmatically:

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import (
    EndpointCoreConfigInput,
    ServedEntityInput,
)

w = WorkspaceClient()
ENDPOINT_NAME = "bma-cpi-genie-agent"

try:
    w.serving_endpoints.create_and_wait(
        name=ENDPOINT_NAME,
        config=EndpointCoreConfigInput(
            served_entities=[
                ServedEntityInput(
                    entity_name=MODEL_NAME,
                    entity_version=str(registered.version),
                    scale_to_zero_enabled=True,
                )
            ]
        ),
    )
    print(f"✅ Endpoint '{ENDPOINT_NAME}' deployed and ready")
except Exception as e:
    if "already exists" in str(e).lower():
        print(f"ℹ️ Endpoint '{ENDPOINT_NAME}' already exists — update it via UI or SDK")
    else:
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Test the served endpoint

# COMMAND ----------

import json

response = w.serving_endpoints.query(
    name=ENDPOINT_NAME,
    messages=[
        {"role": "user", "content": "Compare the CPI of Bermuda vs United States in the last 5 years"}
    ],
)
print(json.dumps(response.as_dict(), indent=2))
