# 04 — LangGraph Agent with Genie as a Tool

Multi-tool LangGraph agent that uses `databricks_langchain.genie.GenieAgent`
to query CPI data via the Genie Space. Logged with MLflow, registered in
Unity Catalog, and deployed to Model Serving.

## Architecture

```
User (any client) → Model Serving Endpoint → LangGraph Agent
                                                ├─ LLM (Llama 3.3 70B)
                                                └─ GenieAgent Tool → Genie Space → SQL Warehouse
```

## Why This Matters

- **Composable**: Add more tools (VectorSearch for docs, UC Functions, etc.)
- **Observable**: Full MLflow tracing on every request
- **Governed**: Registered in Unity Catalog with version control
- **Servable**: REST API endpoint for any downstream consumer

## Deploy

Uses **Automatic Authentication Passthrough** — all dependent resources
(Genie Space, LLM endpoint, SQL Warehouse, tables) are declared during
`mlflow.pyfunc.log_model()`, and Databricks auto-provisions a service
principal with the necessary permissions.

```bash
# Validate and deploy the bundle
databricks bundle validate -p <profile>
databricks bundle deploy -p <profile>

# Run the log & deploy job
databricks bundle run my_agent_deploy -p <profile>
```

The `log_and_deploy.py` notebook will:
1. Test the agent locally
2. Log it to MLflow with resource declarations
3. Register it in Unity Catalog
4. Deploy it via `databricks.agents.deploy()`
5. Wait for the endpoint and run a test query

## Extending the Agent

To add more tools (e.g., a RAG retriever for unstructured documents):

```python
from databricks_langchain import VectorSearchRetrieverTool

rag_tool = VectorSearchRetrieverTool(
    index_name="my_catalog.genie_ready.doc_index",
    description="Search Acme regulatory documents"
)

tools = [cpi_data_tool, rag_tool]  # agent now routes between both
```

## References

- [databricks_langchain.genie.GenieAgent](https://api-docs.databricks.com/python/databricks-ai-bridge/latest/databricks_langchain.html)
- [Authentication for AI agents (Model Serving)](https://docs.databricks.com/aws/en/generative-ai/agent-framework/agent-authentication-model-serving)
- [Multi-agent with Genie (Databricks Blog)](https://www.databricks.com/blog/genie-conversation-apis-public-preview)
