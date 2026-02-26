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

```bash
databricks bundle validate
databricks bundle deploy
databricks bundle run bma_agent_deploy
```

## Extending the Agent

To add more tools (e.g., a RAG retriever for unstructured documents):

```python
from databricks_langchain import VectorSearchRetrieverTool

rag_tool = VectorSearchRetrieverTool(
    index_name="bma_pilot.genie_ready.doc_index",
    description="Search BMA regulatory documents"
)

tools = [genie_agent, rag_tool]  # agent now routes between both
```

## References

- [databricks_langchain.genie.GenieAgent](https://api-docs.databricks.com/python/databricks-ai-bridge/latest/databricks_langchain.html)
- [Tutorial: Build a retrieval agent](https://docs.databricks.com/aws/en/generative-ai/tutorials/agent-framework-notebook)
- [Multi-agent with Genie (Databricks Blog)](https://www.databricks.com/blog/genie-conversation-apis-public-preview)
