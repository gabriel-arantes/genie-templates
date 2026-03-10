# Databricks notebook source
# MAGIC %md
# MAGIC # Acme CPI LangGraph Agent — Log, Register & Deploy
# MAGIC
# MAGIC This notebook:
# MAGIC 1. Logs the LangGraph agent to MLflow
# MAGIC 2. Registers it in Unity Catalog
# MAGIC 3. Deploys it to a Model Serving endpoint using `databricks.agents.deploy()`
# MAGIC
# MAGIC The served agent can then be consumed by the Teams bot, Databricks App,
# MAGIC or any other client via REST API.

# COMMAND ----------

# MAGIC %pip install mlflow>=3.10.1 langchain langchain-core langgraph langgraph-prebuilt databricks-langchain databricks-agents pydantic
# MAGIC %restart_python

# COMMAND ----------

import os
import mlflow

# Configuration — update these
GENIE_SPACE_ID = "01f11271f3d41201af68388818cca110"
LLM_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"
WAREHOUSE_ID = "5eb73ca40f08c607"
CATALOG = "my_catalog"
SCHEMA = "genie_ready"
MODEL_NAME = f"{CATALOG}.{SCHEMA}.my_cpi_genie_agent"

os.environ["GENIE_SPACE_ID"] = GENIE_SPACE_ID
os.environ["LLM_ENDPOINT"] = LLM_ENDPOINT

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Test the agent locally

# COMMAND ----------

from agent import langgraph_agent as agent

# Quick test
test_result = agent.invoke(
    {"messages": [{"role": "user", "content": "What is the latest CPI index value for Advanced Economies?"}]}
)
for msg in test_result["messages"]:
    role = msg.get("role", msg.get("type", "unknown"))
    content = msg.get("content", "")
    if content:
        print(f"[{role}]: {content[:200]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Log the agent with MLflow
# MAGIC
# MAGIC Uses **Automatic Authentication Passthrough**: all dependent resources
# MAGIC are declared in the `resources` parameter so Databricks auto-provisions
# MAGIC a service principal with the necessary permissions.

# COMMAND ----------

from mlflow.models.resources import (
    DatabricksGenieSpace,
    DatabricksServingEndpoint,
    DatabricksSQLWarehouse,
    DatabricksTable,
)

mlflow.set_registry_uri("databricks-uc")

with mlflow.start_run(run_name="my_cpi_genie_agent") as run:
    logged_agent = mlflow.pyfunc.log_model(
        python_model="agent.py",
        pip_requirements=[
            "mlflow>=3.10.1",
            "langchain",
            "langchain-core",
            "langgraph",
            "langgraph-prebuilt",
            "databricks-langchain",
            "pydantic",
        ],
        # Automatic Authentication Passthrough — Databricks auto-provisions
        # a service principal with least-privilege access to these resources.
        # Per docs: "if you log a Genie Space, you must also log its tables."
        resources=[
            DatabricksServingEndpoint(endpoint_name=LLM_ENDPOINT),
            DatabricksGenieSpace(genie_space_id=GENIE_SPACE_ID),
            DatabricksSQLWarehouse(warehouse_id=WAREHOUSE_ID),
            DatabricksTable(table_name=f"{CATALOG}.{SCHEMA}.cpi_world_country_aggregates"),
        ],
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
# MAGIC Uses `databricks.agents.deploy()` — the recommended deployment method.
# MAGIC It auto-creates the serving endpoint, provisions authentication, enables
# MAGIC MLflow tracing, and sets up the Review App.

# COMMAND ----------

from databricks import agents

deployment = agents.deploy(
    model_name=MODEL_NAME,
    model_version=registered.version,
    scale_to_zero=True,
)
print(f"✅ Agent deployed!")
print(f"   Query endpoint: {deployment.query_endpoint}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Test the served endpoint

# COMMAND ----------

import json
import time
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

w = WorkspaceClient()

# Wait for the endpoint to be ready (agents.deploy() returns before it's serving)
endpoint_name = deployment.endpoint_name
print(f"⏳ Waiting for endpoint '{endpoint_name}' to be ready...")

while True:
    try:
        ep = w.serving_endpoints.get(endpoint_name)
        state = ep.state
        if state and state.ready == "READY":
            print(f"✅ Endpoint '{endpoint_name}' is READY!")
            break
        print(f"   State: {state.ready if state else 'UNKNOWN'} — waiting 30s...")
    except Exception:
        print(f"   Endpoint not found yet — waiting 30s...")
    time.sleep(30)

response = w.serving_endpoints.query(
    name=endpoint_name,
    messages=[
        ChatMessage(
            role=ChatMessageRole.USER,
            content="What was the CPI trend for Advanced Economies from 2020 to 2023?",
        )
    ],
)
print(json.dumps(response.as_dict(), indent=2))

