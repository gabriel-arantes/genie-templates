"""
app.py — Databricks App: Acme CPI Explorer powered by Genie.

A Gradio-based web application deployed as a Databricks App that uses
the Genie Space as a resource to let users:
  1. Ask natural-language questions about CPI data
  2. View generated SQL and query results in a table
  3. See automatic chart visualizations via Plotly

Authentication: Uses the app's service principal automatically via
environment variables injected by the Databricks Apps runtime.
"""

import os
import json
import time
import logging
import gradio as gr
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from databricks.sdk import WorkspaceClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("my-genie-app")

# ---------------------------------------------------------------------------
# Databricks SDK — initialized with the app's service principal
# ---------------------------------------------------------------------------
w = WorkspaceClient()
GENIE_SPACE_ID = os.getenv("GENIE_SPACE_ID", "")

# ---------------------------------------------------------------------------
# Custom CSS — Premium dark theme
# ---------------------------------------------------------------------------
CUSTOM_CSS = """
/* ── Global ── */
.gradio-container {
    max-width: 1200px !important;
    margin: 0 auto !important;
    font-family: 'Inter', 'Segoe UI', system-ui, -apple-system, sans-serif !important;
}

/* ── Header ── */
.app-header {
    background: linear-gradient(135deg, #0f172a 0%, #1e293b 50%, #0f172a 100%);
    border: 1px solid rgba(56, 189, 248, 0.15);
    border-radius: 16px;
    padding: 28px 32px;
    margin-bottom: 20px;
    position: relative;
    overflow: hidden;
}
.app-header::before {
    content: '';
    position: absolute;
    top: -50%;
    left: -50%;
    width: 200%;
    height: 200%;
    background: radial-gradient(ellipse at 30% 50%, rgba(56, 189, 248, 0.06) 0%, transparent 60%);
    pointer-events: none;
}
.app-header h1 {
    color: #f8fafc !important;
    font-size: 1.75rem !important;
    font-weight: 700 !important;
    margin: 0 0 6px 0 !important;
    letter-spacing: -0.02em;
}
.app-header p {
    color: #94a3b8 !important;
    font-size: 0.95rem !important;
    margin: 0 !important;
}
.app-header .accent {
    color: #38bdf8;
    font-weight: 600;
}

/* ── Suggestion chips ── */
.suggestion-row {
    display: flex;
    gap: 8px;
    flex-wrap: wrap;
    margin-bottom: 12px;
}
.suggestion-row button {
    background: linear-gradient(135deg, #1e293b, #0f172a) !important;
    border: 1px solid rgba(56, 189, 248, 0.2) !important;
    color: #cbd5e1 !important;
    border-radius: 20px !important;
    padding: 8px 16px !important;
    font-size: 0.82rem !important;
    transition: all 0.2s ease !important;
    cursor: pointer !important;
}
.suggestion-row button:hover {
    border-color: #38bdf8 !important;
    color: #f8fafc !important;
    background: linear-gradient(135deg, #1e3a5f, #0f172a) !important;
    transform: translateY(-1px) !important;
    box-shadow: 0 4px 12px rgba(56, 189, 248, 0.15) !important;
}

/* ── Chat area ── */
.chatbot-container {
    border: 1px solid #1e293b !important;
    border-radius: 12px !important;
    background: #0f172a !important;
}

/* ── SQL accordion ── */
.sql-accordion {
    border: 1px solid #1e293b !important;
    border-radius: 12px !important;
    background: #0f172a !important;
}

/* ── Status badge ── */
.status-badge {
    display: inline-block;
    padding: 4px 12px;
    border-radius: 12px;
    font-size: 0.78rem;
    font-weight: 500;
}
.status-badge.success {
    background: rgba(34, 197, 94, 0.15);
    color: #4ade80;
    border: 1px solid rgba(34, 197, 94, 0.3);
}
.status-badge.info {
    background: rgba(56, 189, 248, 0.15);
    color: #38bdf8;
    border: 1px solid rgba(56, 189, 248, 0.3);
}

/* ── Buttons ── */
.primary-btn {
    background: linear-gradient(135deg, #0284c7, #0369a1) !important;
    border: none !important;
    color: white !important;
    font-weight: 600 !important;
    border-radius: 10px !important;
    transition: all 0.2s ease !important;
}
.primary-btn:hover {
    background: linear-gradient(135deg, #0ea5e9, #0284c7) !important;
    transform: translateY(-1px) !important;
    box-shadow: 0 4px 16px rgba(14, 165, 233, 0.3) !important;
}
.reset-btn {
    background: transparent !important;
    border: 1px solid #334155 !important;
    color: #94a3b8 !important;
    border-radius: 10px !important;
}
.reset-btn:hover {
    border-color: #ef4444 !important;
    color: #fca5a5 !important;
}
"""

# ---------------------------------------------------------------------------
# Genie interaction helpers
# ---------------------------------------------------------------------------

def ask_genie(question: str, conversation_id: str = None) -> dict:
    """
    Send a question to the Genie Space and wait for a response.
    Returns a dict with: text, sql, dataframe, conversation_id, error.
    """
    try:
        if conversation_id:
            msg = w.genie.create_message_and_wait(
                space_id=GENIE_SPACE_ID,
                conversation_id=conversation_id,
                content=question,
            )
        else:
            msg = w.genie.start_conversation_and_wait(
                space_id=GENIE_SPACE_ID,
                content=question,
            )

        result = {
            "text": "",
            "sql": "",
            "dataframe": pd.DataFrame(),
            "conversation_id": msg.conversation_id,
            "error": None,
        }

        if msg.attachments:
            for att in msg.attachments:
                if att.text:
                    result["text"] = att.text.content or ""
                if att.query:
                    result["sql"] = att.query.query or ""

                # Fetch query results if available
                if getattr(att, 'query', None) and getattr(att, 'attachment_id', None) and msg.status.value == "COMPLETED":
                    try:
                        qr = w.genie.get_message_attachment_query_result(
                            space_id=GENIE_SPACE_ID,
                            conversation_id=msg.conversation_id,
                            message_id=msg.id,
                            attachment_id=att.attachment_id,
                        )
                        
                        # SDK >= 0.40.0 returns a nested StatementResponse under `statement_response`
                        sr = getattr(qr, 'statement_response', None) or qr
                        
                        has_schema = hasattr(sr, 'manifest') and sr.manifest and getattr(sr.manifest, 'schema', None) and getattr(sr.manifest.schema, 'columns', None)
                        has_data = hasattr(sr, 'result') and sr.result and getattr(sr.result, 'data_array', None) is not None
                        
                        if has_schema and has_data:
                            col_names = [c.name for c in sr.manifest.schema.columns]
                            df = pd.DataFrame(sr.result.data_array, columns=col_names)
                            # Try to convert numeric-looking columns
                            for col in df.columns:
                                try:
                                    df[col] = pd.to_numeric(df[col])
                                except (ValueError, TypeError):
                                    pass
                            result["dataframe"] = df
                    except Exception as e:
                        logger.warning(f"Could not fetch query result for attachment {att.attachment_id}: {e}")

        return result

    except Exception as e:
        logger.error(f"Genie error: {e}")
        return {
            "text": "",
            "sql": "",
            "dataframe": pd.DataFrame(),
            "conversation_id": conversation_id,
            "error": str(e),
        }


def auto_chart(df: pd.DataFrame) -> go.Figure | None:
    """Try to create a reasonable Plotly chart from the query results."""
    if df.empty or len(df.columns) < 2:
        return None

    try:
        numeric_cols = df.select_dtypes(include="number").columns.tolist()
        non_numeric_cols = [c for c in df.columns if c not in numeric_cols]

        if not numeric_cols:
            return None

        # If we have a time-like column + numeric → line chart
        time_cols = [c for c in non_numeric_cols
                     if any(kw in c.lower() for kw in ["period", "year", "month", "date", "time"])]

        if time_cols and numeric_cols:
            x_col = time_cols[0]
            y_col = numeric_cols[0]
            # If there's a category column, use color
            cat_cols = [c for c in non_numeric_cols if c != x_col]
            if cat_cols and df[cat_cols[0]].nunique() <= 12:
                fig = px.line(df, x=x_col, y=y_col, color=cat_cols[0],
                              markers=True)
            else:
                fig = px.line(df, x=x_col, y=y_col, markers=True)
        elif non_numeric_cols and numeric_cols and len(df) <= 20:
            # Categorical + numeric → bar chart
            fig = px.bar(df, x=non_numeric_cols[0], y=numeric_cols[0],
                         color=non_numeric_cols[0] if df[non_numeric_cols[0]].nunique() <= 12 else None)
        else:
            return None

        # Dark theme styling
        fig.update_layout(
            template="plotly_dark",
            paper_bgcolor="rgba(15, 23, 42, 0.8)",
            plot_bgcolor="rgba(15, 23, 42, 0.5)",
            font=dict(family="Inter, system-ui, sans-serif", color="#cbd5e1"),
            margin=dict(l=40, r=20, t=40, b=40),
            legend=dict(
                bgcolor="rgba(30, 41, 59, 0.8)",
                bordercolor="rgba(56, 189, 248, 0.2)",
                borderwidth=1,
            ),
            xaxis=dict(gridcolor="rgba(51, 65, 85, 0.5)"),
            yaxis=dict(gridcolor="rgba(51, 65, 85, 0.5)"),
        )
        fig.update_traces(
            line=dict(width=2.5) if hasattr(fig.data[0], 'line') else {},
        )
        return fig

    except Exception as e:
        logger.warning(f"Auto-chart failed: {e}")
        return None


# ---------------------------------------------------------------------------
# Suggested questions
# ---------------------------------------------------------------------------
SUGGESTIONS = [
    "What is the latest CPI index for all regions?",
    "Show year-over-year inflation trend for World since 2020",
    "Compare G7 vs Emerging Markets CPI index",
    "Which region had the highest inflation last month?",
    "Show monthly CPI change for World in 2024",
]


# ---------------------------------------------------------------------------
# Gradio UI
# ---------------------------------------------------------------------------

def handle_question(question: str, history: list, conv_id: str):
    """Process user question through Genie and return formatted outputs."""
    if not question.strip():
        return history, "", pd.DataFrame(), None, conv_id

    history = history or []
    history.append({"role": "user", "content": question})

    result = ask_genie(question, conversation_id=conv_id)

    if result["error"]:
        bot_msg = f"⚠️ **Error:** {result['error']}"
    else:
        parts = []
        if result["text"]:
            parts.append(result["text"])
        if not result["dataframe"].empty:
            rows = len(result["dataframe"])
            parts.append(f"\n📊 **{rows} row{'s' if rows != 1 else ''}** returned")
        bot_msg = "\n\n".join(parts) if parts else "Genie returned an empty response."

    history.append({"role": "assistant", "content": bot_msg})
    new_conv_id = result.get("conversation_id") or conv_id

    chart = auto_chart(result.get("dataframe", pd.DataFrame()))

    return (
        history,
        result.get("sql", ""),
        result.get("dataframe", pd.DataFrame()),
        chart,
        new_conv_id,
    )


def use_suggestion(suggestion: str, history: list, conv_id: str):
    """Handle suggestion chip click."""
    return handle_question(suggestion, history, conv_id)


def reset_conversation():
    return [], "", pd.DataFrame(), None, None


# Build the Gradio interface
with gr.Blocks(
    title="Acme CPI Explorer — Genie",
    theme=gr.themes.Default(
        primary_hue=gr.themes.colors.sky,
        neutral_hue=gr.themes.colors.slate,
        font=gr.themes.GoogleFont("Inter"),
    ),
    css=CUSTOM_CSS,
) as app:

    # State
    conv_state = gr.State(value=None)

    # ── Header ──
    gr.HTML("""
    <div class="app-header">
        <h1>🧞 Acme CPI Explorer</h1>
        <p>
            Ask questions about <span class="accent">Consumer Price Index</span> data
            in natural language — powered by Databricks AI/BI Genie
        </p>
    </div>
    """)

    # ── Suggestion chips ──
    with gr.Row(elem_classes="suggestion-row"):
        suggestion_btns = []
        for s in SUGGESTIONS:
            btn = gr.Button(s, size="sm", variant="secondary")
            suggestion_btns.append(btn)

    # ── Main layout ──
    # Chat Area
    chatbot = gr.Chatbot(
        label="Conversation",
        type="messages",
        height=550,
        show_copy_button=True,
        elem_classes="chatbot-container",
    )
    with gr.Row():
        question_input = gr.Textbox(
            placeholder="Ask a question about CPI data...",
            label="Your question",
            scale=5,
            container=False,
        )
        send_btn = gr.Button(
            "Ask Genie ✨", variant="primary", scale=1,
            elem_classes="primary-btn",
        )
        reset_btn = gr.Button(
            "🗑️ Clear", scale=1,
            elem_classes="reset-btn",
        )

    # Technical Details Area
    with gr.Accordion("🔍 View Analysis Details (SQL, Data & Charts)", open=False, elem_classes="details-accordion"):
        with gr.Tabs():
            with gr.TabItem("📈 Visualization"):
                chart_output = gr.Plot(label="Visualization", elem_classes="chart-plot")
            with gr.TabItem("📋 Query Results"):
                data_table = gr.Dataframe(
                    label="Query Results",
                    interactive=False,
                    wrap=True,
                )
            with gr.TabItem("📝 Generated SQL"):
                sql_output = gr.Code(
                    label="SQL",
                    language="sql",
                    lines=12,
                )

    # ── Event handlers ──
    outputs = [chatbot, sql_output, data_table, chart_output, conv_state]

    send_btn.click(
        fn=handle_question,
        inputs=[question_input, chatbot, conv_state],
        outputs=outputs,
        api_name=False,
    ).then(fn=lambda: "", outputs=question_input)

    question_input.submit(
        fn=handle_question,
        inputs=[question_input, chatbot, conv_state],
        outputs=outputs,
        api_name=False,
    ).then(fn=lambda: "", outputs=question_input)

    reset_btn.click(
        fn=reset_conversation,
        outputs=outputs,
        api_name=False,
    )

    # Wire suggestion buttons
    for btn in suggestion_btns:
        btn.click(
            fn=use_suggestion,
            inputs=[btn, chatbot, conv_state],
            outputs=outputs,
            api_name=False,
        )

    # ── Footer ──
    gr.HTML("""
    <div style="text-align: center; padding: 16px 0 4px; color: #475569; font-size: 0.78rem;">
        Acme Genie Templates · Powered by Databricks AI/BI Genie
    </div>
    """)


if __name__ == "__main__":
    app.launch(
        server_name="0.0.0.0",
        server_port=int(os.getenv("PORT", "8080")),
    )
