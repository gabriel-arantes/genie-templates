"""
genie_client.py — Wrapper for Databricks Genie Conversation API.

Handles: start conversation, send follow-up messages, poll for completion,
and retrieve query results. Designed for use inside a Teams bot or any
external application that needs to proxy questions to a Genie Space.

Authentication: Uses a Databricks Personal Access Token (PAT) or
Service Principal OAuth token via environment variables.
"""

import os
import time
import logging
import requests
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "").rstrip("/")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN", "")
GENIE_SPACE_ID = os.getenv("GENIE_SPACE_ID", "")

POLL_INTERVAL_INITIAL = 1.0     # seconds
POLL_INTERVAL_MAX = 10.0        # exponential backoff cap
POLL_TIMEOUT = 300              # 5 minutes max wait


@dataclass
class GenieResult:
    """Structured result from a Genie query."""
    status: str                              # COMPLETED | FAILED | CANCELLED | TIMEOUT
    text: Optional[str] = None               # natural-language response
    sql: Optional[str] = None                # generated SQL (if any)
    columns: list = field(default_factory=list)
    rows: list = field(default_factory=list)
    error: Optional[str] = None
    conversation_id: Optional[str] = None
    message_id: Optional[str] = None


class GenieClient:
    """Thin client around the Genie Conversation API (v2.0)."""

    def __init__(
        self,
        host: str = DATABRICKS_HOST,
        token: str = DATABRICKS_TOKEN,
        space_id: str = GENIE_SPACE_ID,
    ):
        self.host = host
        self.space_id = space_id
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        })

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def ask(self, question: str, conversation_id: Optional[str] = None) -> GenieResult:
        """
        Send a question to the Genie Space. If conversation_id is provided,
        sends a follow-up message; otherwise starts a new conversation.
        Polls until completion and returns the result.
        """
        if conversation_id:
            return self._follow_up(conversation_id, question)
        return self._start_conversation(question)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _start_conversation(self, question: str) -> GenieResult:
        url = f"{self.host}/api/2.0/genie/spaces/{self.space_id}/start-conversation"
        resp = self.session.post(url, json={"content": question})
        resp.raise_for_status()
        data = resp.json()
        conv_id = data.get("conversation_id", "")
        msg_id = data.get("message_id") or data.get("id", "")
        return self._poll(conv_id, msg_id)

    def _follow_up(self, conversation_id: str, question: str) -> GenieResult:
        url = (
            f"{self.host}/api/2.0/genie/spaces/{self.space_id}"
            f"/conversations/{conversation_id}/messages"
        )
        resp = self.session.post(url, json={"content": question})
        resp.raise_for_status()
        data = resp.json()
        msg_id = data.get("id", "")
        return self._poll(conversation_id, msg_id)

    def _poll(self, conversation_id: str, message_id: str) -> GenieResult:
        """Poll GET message endpoint with exponential backoff."""
        url = (
            f"{self.host}/api/2.0/genie/spaces/{self.space_id}"
            f"/conversations/{conversation_id}/messages/{message_id}"
        )
        interval = POLL_INTERVAL_INITIAL
        elapsed = 0.0

        while elapsed < POLL_TIMEOUT:
            time.sleep(interval)
            elapsed += interval
            resp = self.session.get(url)
            resp.raise_for_status()
            data = resp.json()
            status = data.get("status", "")

            if status in ("COMPLETED", "FAILED", "CANCELLED"):
                return self._parse_response(data, conversation_id, message_id)

            # exponential backoff
            interval = min(interval * 1.5, POLL_INTERVAL_MAX)

        return GenieResult(
            status="TIMEOUT",
            error="Genie did not respond within the timeout period.",
            conversation_id=conversation_id,
            message_id=message_id,
        )

    def _parse_response(
        self, data: dict, conversation_id: str, message_id: str
    ) -> GenieResult:
        status = data.get("status", "UNKNOWN")
        error = data.get("error")

        text_content = None
        sql_content = None
        columns = []
        rows = []

        attachments = data.get("attachments") or []
        for att in attachments:
            if att.get("text"):
                text_content = att["text"].get("content", "")
            if att.get("query"):
                sql_content = att["query"].get("query", "")

            # If there is a query result attachment, fetch it
            att_id = att.get("attachment_id")
            if att_id and status == "COMPLETED":
                try:
                    qr = self._get_query_result(
                        conversation_id, message_id, att_id
                    )
                    columns = qr.get("columns", [])
                    rows = qr.get("rows", [])
                except Exception as e:
                    logger.warning(f"Could not fetch query result: {e}")

        return GenieResult(
            status=status,
            text=text_content,
            sql=sql_content,
            columns=columns,
            rows=rows,
            error=str(error) if error else None,
            conversation_id=conversation_id,
            message_id=message_id,
        )

    def _get_query_result(
        self, conversation_id: str, message_id: str, attachment_id: str
    ) -> dict:
        url = (
            f"{self.host}/api/2.0/genie/spaces/{self.space_id}"
            f"/conversations/{conversation_id}/messages/{message_id}"
            f"/attachments/{attachment_id}/query-result"
        )
        resp = self.session.get(url)
        resp.raise_for_status()
        return resp.json()
