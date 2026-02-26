"""
app.py — Microsoft Teams Bot that proxies questions to Databricks Genie.

Architecture:
  Teams → Azure Bot Service → Azure Web App (this code) → Genie Conversation API

The bot maintains conversation_id per Teams thread so users can ask follow-up
questions within the same Genie conversation context.
"""

import os
import sys
import logging
import traceback
from aiohttp import web
from botbuilder.core import (
    BotFrameworkAdapter,
    BotFrameworkAdapterSettings,
    TurnContext,
)
from botbuilder.schema import Activity, ActivityTypes

from genie_client import GenieClient, GenieResult

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("my-genie-bot")

# ---------------------------------------------------------------------------
# Bot Framework configuration (from Azure Bot Service)
# ---------------------------------------------------------------------------
SETTINGS = BotFrameworkAdapterSettings(
    app_id=os.getenv("MICROSOFT_APP_ID", ""),
    app_password=os.getenv("MICROSOFT_APP_PASSWORD", ""),
)
ADAPTER = BotFrameworkAdapter(SETTINGS)

# ---------------------------------------------------------------------------
# Genie client (singleton)
# ---------------------------------------------------------------------------
genie = GenieClient()

# In-memory map: Teams conversation ID → Genie conversation_id
# In production, replace with Redis or Azure Table Storage.
_conv_map: dict[str, str] = {}


# ---------------------------------------------------------------------------
# Error handler
# ---------------------------------------------------------------------------
async def on_error(context: TurnContext, error: Exception):
    logger.error(f"Unhandled error: {error}")
    traceback.print_exc()
    await context.send_activity(
        "Sorry, something went wrong while querying Genie. Please try again."
    )


ADAPTER.on_turn_error = on_error


# ---------------------------------------------------------------------------
# Message handler
# ---------------------------------------------------------------------------
async def on_message(turn_context: TurnContext):
    if turn_context.activity.type != ActivityTypes.message:
        return

    user_text = (turn_context.activity.text or "").strip()
    if not user_text:
        return

    teams_conv_id = turn_context.activity.conversation.id

    # Handle reset command
    if user_text.lower() in ("/reset", "/new", "new conversation"):
        _conv_map.pop(teams_conv_id, None)
        await turn_context.send_activity("🔄 Started a new Genie conversation.")
        return

    # Send typing indicator
    await turn_context.send_activity(Activity(type=ActivityTypes.typing))

    # Resolve Genie conversation_id (if exists)
    genie_conv_id = _conv_map.get(teams_conv_id)

    # Query Genie
    try:
        result: GenieResult = genie.ask(user_text, conversation_id=genie_conv_id)
    except Exception as e:
        logger.error(f"Genie API error: {e}")
        await turn_context.send_activity(
            f"⚠️ Genie API error: `{e}`"
        )
        return

    # Store conversation mapping for follow-ups
    if result.conversation_id:
        _conv_map[teams_conv_id] = result.conversation_id

    # Format response
    response = _format_response(result)
    await turn_context.send_activity(response)


def _format_response(result: GenieResult) -> str:
    """Format a GenieResult into a Teams-friendly markdown message."""
    if result.status == "FAILED":
        return f"❌ Genie could not answer: {result.error or 'Unknown error'}"

    if result.status == "TIMEOUT":
        return "⏳ The query is taking too long. Please try a simpler question."

    parts = []

    # Text response
    if result.text:
        parts.append(result.text)

    # SQL (collapsed for readability)
    if result.sql:
        parts.append(f"\n**Generated SQL:**\n```sql\n{result.sql}\n```")

    # Table (first 20 rows)
    if result.columns and result.rows:
        col_names = [c.get("name", f"col_{i}") for i, c in enumerate(result.columns)]
        header = "| " + " | ".join(col_names) + " |"
        sep = "| " + " | ".join(["---"] * len(col_names)) + " |"
        rows_md = []
        for row in result.rows[:20]:
            values = row if isinstance(row, list) else list(row.values())
            rows_md.append("| " + " | ".join(str(v) for v in values) + " |")
        table = "\n".join([header, sep] + rows_md)
        if len(result.rows) > 20:
            table += f"\n\n*Showing 20 of {len(result.rows)} rows.*"
        parts.append(f"\n{table}")

    return "\n\n".join(parts) if parts else "Genie returned an empty response."


# ---------------------------------------------------------------------------
# HTTP server
# ---------------------------------------------------------------------------
async def messages(req: web.Request) -> web.Response:
    """Endpoint for Azure Bot Service → this app."""
    if req.content_type == "application/json":
        body = await req.json()
    else:
        return web.Response(status=415)

    activity = Activity().deserialize(body)
    auth_header = req.headers.get("Authorization", "")

    response = await ADAPTER.process_activity(activity, auth_header, on_message)
    if response:
        return web.json_response(data=response.body, status=response.status)
    return web.Response(status=201)


async def health(req: web.Request) -> web.Response:
    return web.json_response({"status": "healthy", "service": "my-genie-teams-bot"})


app = web.Application()
app.router.add_post("/api/messages", messages)
app.router.add_get("/health", health)

if __name__ == "__main__":
    port = int(os.getenv("PORT", "3978"))
    logger.info(f"Starting Acme Genie Teams Bot on port {port}")
    web.run_app(app, host="0.0.0.0", port=port)
