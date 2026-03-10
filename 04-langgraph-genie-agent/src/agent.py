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

from langchain_core.tools import tool

@tool("CPI_Data_Analyst", return_direct=False)
def cpi_data_tool(query: str) -> str:
    """
    Specializes in analyzing Consumer Price Index (CPI) data across world
    regions and economies. Use this tool for questions about CPI index values,
    inflation trends, comparisons between regions/economies, year-over-year
    changes, rankings, and any structured data query about CPI indicators.
    The underlying data is monthly CPI World Country Aggregates with columns:
    series_code, series_name, country_code, index_type, coicop_category,
    transformation_type, transformation_label, frequency, unit, period,
    year, month, cpi_value.
    """
    # GenieAgent is instantiated at query time (not at module load) so that
    # the auto-provisioned service principal credentials are available.
    try:
        genie_agent = GenieAgent(
            genie_space_id=GENIE_SPACE_ID,
            genie_agent_name="CPI_Data_Analyst",
            description=(
                "Analyzes Consumer Price Index (CPI) data across world regions. "
                "Monthly data with columns: series_code, series_name, country_code, "
                "index_type, coicop_category, transformation_type, transformation_label, "
                "frequency, unit, period, year, month, cpi_value."
            ),
        )
    except Exception as e:
        return f"Error initializing Genie space: {e}"

    response = genie_agent.invoke({"messages": [{"role": "user", "content": query}]})
    return response["messages"][0].content

# Additional tools can be added here (e.g., VectorSearchRetrieverTool for unstructured data)
tools = [cpi_data_tool]

# ---------------------------------------------------------------------------
# LangGraph Agent Builder
# ---------------------------------------------------------------------------

def create_tool_calling_agent(
    model: LanguageModelLike,
    tools: Sequence[BaseTool],
    agent_prompt: Optional[str] = None,
):
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
SYSTEM_PROMPT = """You are a data analyst assistant for the Acme Corp.
You help users explore Consumer Price Index (CPI) data from economies and regions around the world.

When users ask about CPI data, trends, comparisons, or rankings, use the CPI_Data_Analyst
tool to query the Genie Space. Always present the results clearly and provide insights.

For questions outside of CPI data, politely explain that you specialize in CPI analysis
and suggest the user rephrase their question in terms of CPI or inflation data.

Key data characteristics:
- Table: my_catalog.genie_ready.cpi_world_country_aggregates
- Columns: series_code, series_name, country_code, index_type, coicop_category,
  transformation_type, transformation_label, frequency, unit, period, year, month, cpi_value
- The 'cpi_value' column is the CPI index value (base index ~100)
- 'series_name' identifies the economy/region (e.g. "Advanced Economies", "United States")
- Data is monthly, spanning from 2011 onwards
- 'coicop_category' indicates the item category (e.g. "All Items", "Food", "Energy")
"""

# ---------------------------------------------------------------------------
# Build the agent
# ---------------------------------------------------------------------------
langgraph_agent = create_tool_calling_agent(
    model=llm,
    tools=tools,
    agent_prompt=SYSTEM_PROMPT,
)

from typing import Any, Generator
from mlflow.pyfunc import ChatAgent
from mlflow.types.agent import ChatAgentChunk, ChatAgentMessage, ChatAgentResponse, ChatContext

class LangGraphChatAgent(ChatAgent):
    def __init__(self, agent):
        self.agent = agent

    def predict(
        self,
        messages: list[ChatAgentMessage],
        context: Optional[ChatContext] = None,
        custom_inputs: Optional[dict[str, Any]] = None,
    ) -> ChatAgentResponse:
        request = {"messages": self._convert_messages_to_dict(messages)}
        out_msgs = []
        for event in self.agent.stream(request, stream_mode="updates"):
            for node_data in event.values():
                out_msgs.extend(
                    ChatAgentMessage(**msg) for msg in node_data.get("messages", [])
                )
        return ChatAgentResponse(messages=out_msgs)

    def predict_stream(
        self,
        messages: list[ChatAgentMessage],
        context: Optional[ChatContext] = None,
        custom_inputs: Optional[dict[str, Any]] = None,
    ) -> Generator[ChatAgentChunk, None, None]:
        request = {"messages": self._convert_messages_to_dict(messages)}
        for event in self.agent.stream(request, stream_mode="updates"):
            for node_data in event.values():
                yield from (
                    ChatAgentChunk(**{"delta": msg}) for msg in node_data.get("messages", [])
                )

chat_agent = LangGraphChatAgent(langgraph_agent)

# Enable MLflow tracing
mlflow.langchain.autolog()
mlflow.models.set_model(chat_agent)
