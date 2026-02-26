"""
agent.py — LangGraph Agent with Genie as a Tool.

This agent uses:
  - databricks_langchain.ChatDatabricks as the LLM backbone
  - databricks_langchain.genie.GenieAgent as a tool for structured data queries
  - LangGraph for orchestration (tool-calling loop)
  - MLflow for logging, tracing, and serving

The agent is designed to be logged with mlflow.pyfunc.log_model() and served
on Databricks Model Serving, where it can be consumed by any downstream app
(Databricks App, Teams bot, Slack bot, etc.).
"""

import os
from typing import Optional, Sequence, Union

from langchain_core.language_models import LanguageModelLike
from langchain_core.runnables import RunnableConfig, RunnableLambda
from langchain_core.tools import BaseTool
from langgraph.graph import END, StateGraph
from langgraph.graph.graph import CompiledGraph
from langgraph.prebuilt.tool_executor import ToolExecutor
from mlflow.langchain.chat_agent_langgraph import ChatAgentState, ChatAgentToolNode

from databricks_langchain import ChatDatabricks
from databricks_langchain.genie import GenieAgent

import mlflow

# ---------------------------------------------------------------------------
# Configuration (from environment or defaults)
# ---------------------------------------------------------------------------
GENIE_SPACE_ID = os.getenv("GENIE_SPACE_ID", "01f11271f3d41201af68388818cca110")
LLM_ENDPOINT = os.getenv("LLM_ENDPOINT", "databricks-meta-llama-3-3-70b-instruct")

# ---------------------------------------------------------------------------
# LLM
# ---------------------------------------------------------------------------
llm = ChatDatabricks(
    endpoint=LLM_ENDPOINT,
    temperature=0.0,
    max_tokens=2048,
)

# ---------------------------------------------------------------------------
# Genie Agent as a Tool
# ---------------------------------------------------------------------------
genie_agent = GenieAgent(
    genie_space_id=GENIE_SPACE_ID,
    genie_agent_name="CPI_Data_Analyst",
    description=(
        "Specializes in analyzing Consumer Price Index (CPI) data across world "
        "countries. Use this tool for questions about CPI values, trends, "
        "comparisons between countries, year-over-year changes, rankings, "
        "and any structured data query about inflation indicators. "
        "The underlying data covers CPI World Country Aggregates with columns: "
        "country_name, country_code, indicator_name, year, value."
    ),
)

# Additional tools can be added here (e.g., VectorSearchRetrieverTool for unstructured data)
tools = [genie_agent]

# ---------------------------------------------------------------------------
# LangGraph Agent Builder
# ---------------------------------------------------------------------------

def create_tool_calling_agent(
    model: LanguageModelLike,
    tools: Union[ToolExecutor, Sequence[BaseTool]],
    agent_prompt: Optional[str] = None,
) -> CompiledGraph:
    """Create a LangGraph agent that uses tools via function-calling."""
    model = model.bind_tools(tools)

    def routing_logic(state: ChatAgentState):
        last_message = state["messages"][-1]
        if last_message.get("tool_calls"):
            return "continue"
        return "end"

    if agent_prompt:
        system_message = {"role": "system", "content": agent_prompt}
        preprocessor = RunnableLambda(
            lambda state: [system_message] + state["messages"]
        )
    else:
        preprocessor = RunnableLambda(lambda state: state["messages"])

    model_runnable = preprocessor | model

    def call_model(state: ChatAgentState, config: RunnableConfig):
        response = model_runnable.invoke(state, config)
        return {"messages": [response]}

    workflow = StateGraph(ChatAgentState)
    workflow.add_node("agent", call_model)
    workflow.add_node("tools", ChatAgentToolNode(tools))
    workflow.set_entry_point("agent")
    workflow.add_conditional_edges("agent", routing_logic, {"continue": "tools", "end": END})
    workflow.add_edge("tools", "agent")

    return workflow.compile()


# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------
SYSTEM_PROMPT = """You are a data analyst assistant for the Bermuda Monetary Authority (BMA).
You help users explore Consumer Price Index (CPI) data from countries around the world.

When users ask about CPI data, trends, comparisons, or rankings, use the CPI_Data_Analyst
tool to query the Genie Space. Always present the results clearly and provide insights.

For questions outside of CPI data, politely explain that you specialize in CPI analysis
and suggest the user rephrase their question in terms of CPI or inflation data.

Key data characteristics:
- Table: bma_pilot.genie_ready.cpi_world_country_aggregates
- Columns: country_name, country_code, indicator_name, year, value
- The 'value' column represents the CPI index value
- Data covers multiple countries and years
"""

# ---------------------------------------------------------------------------
# Build the agent
# ---------------------------------------------------------------------------
agent = create_tool_calling_agent(
    model=llm,
    tools=tools,
    agent_prompt=SYSTEM_PROMPT,
)

# Enable MLflow tracing
mlflow.langchain.autolog()
