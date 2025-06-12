"""
title: n8n Pipe Function (refactored)
original author: Cole Medin
author:  comwise-nl
revision: v0.0.1
date: 2025-04-26 21:35:00

This module defines a Pipe class that connects Open WebUI to an n8n workflow.
"""

from typing import Optional, Callable, Awaitable
from pydantic import BaseModel, Field
import os
import time
import requests
import json
from requests.exceptions import (
    RequestException,
    HTTPError,
    Timeout,
    ConnectionError,
)  # Import specific exceptions
from json import JSONDecodeError


def extract_event_info(event_emitter) -> tuple[Optional[str], Optional[str]]:
    """
    Extracts chat_id and message_id from the event_emitter's closure.
    Added a check to ensure cell_contents is a dictionary for robustness.
    """
    if not event_emitter or not event_emitter.__closure__:
        return None, None
    for cell in event_emitter.__closure__:
        # Ensure the cell content is a dictionary before trying to access keys
        if isinstance(cell.cell_contents, dict):
            request_info = cell.cell_contents
            chat_id = request_info.get("chat_id")
            message_id = request_info.get("message_id")
            return chat_id, message_id
    return None, None


class Pipe:
    class Valves(BaseModel):
        n8n_url: str = Field(
            default="https://[your domain]/webhook/[your webhook URL]",
            description="URL of the N8N webhook endpoint.",
        )
        n8n_bearer_token: str = Field(
            default="...",
            description="Bearer token for authenticating with the N8N webhook.",
        )
        input_field: str = Field(
            default="chatInput",
            description="The key in the payload dictionary that will contain the user's input for N8N.",
        )
        response_field: str = Field(
            default="output",
            description="The key in the successful N8N JSON response dictionary that contains the desired output text.",
        )
        emit_interval: float = Field(
            default=2.0,
            description="Minimum interval in seconds between emitting status updates to the UI.",
        )
        enable_status_indicator: bool = Field(
            default=True,
            description="Enable or disable status indicator emissions to the UI.",
        )
        request_timeout_seconds: float = Field(
            default=300.0,
            description="Total timeout for the HTTP request to N8N (in seconds). Includes connection and read time.",
        )

    def __init__(self):
        self.type = "pipe"
        self.id = "n8n"
        self.name = "n8n"
        self.valves = self.Valves()
        self.last_emit_time = 0
        pass

    async def emit_status(
        self,
        __event_emitter__: Optional[Callable[[dict], Awaitable[None]]],
        level: str,
        message: str,
        done: bool,
    ):
        """
        Emits status updates to the UI via the event emitter.
        Only emits if enabled, if sufficient time has passed since the last non-done emission,
        or if the status is 'done'.
        """
        current_time = time.time()
        if (
            __event_emitter__  # Ensure emitter is available
            and self.valves.enable_status_indicator
            and (
                current_time - self.last_emit_time >= self.valves.emit_interval or done
            )
        ):
            status_message = {
                "type": "status",
                "data": {
                    "status": "complete" if done else "in_progress",
                    "level": level,  # e.g., "info", "warning", "error"
                    "description": message,
                    "done": done,
                },
            }
            await __event_emitter__(status_message)
            # Only update the last emit time for in_progress statuses to respect the interval
            if not done:
                self.last_emit_time = current_time

    async def pipe(
        self,
        body: dict,
        __event_emitter__: Optional[Callable[[dict], Awaitable[None]]] = None,
        __event_call__: Optional[Callable[[dict], Awaitable[dict]]] = None,
        **user,  # Corrected: **user must be the last parameter
    ) -> Optional[dict | str]:  # Changed return type hint to allow dict or str
        """
        Main pipe execution method. Calls the N8N webhook without retry logic
        and handles responses or errors, updating the chat body.
        Returns the N8N response content string on success, or an error dict on failure.
        """
        if isinstance(body, str):
            try:
                body = json.loads(body)
                if not isinstance(body, dict):
                    error_msg = f"Expected 'body' to be a dictionary but got {type(body).__name__}."
                    await self.emit_status(__event_emitter__, "error", error_msg, True)
                    return {"error": error_msg}
            except json.JSONDecodeError:
                error_msg = "Invalid JSON string received in 'body'."
                await self.emit_status(__event_emitter__, "error", error_msg, True)
                return {"error": error_msg}

        if not isinstance(body, dict):
            error_msg = "Request body is not a valid dictionary."
            await self.emit_status(__event_emitter__, "error", error_msg, True)
            return {"error": error_msg}

        # Ensure body has a 'messages' key to prevent errors later
        messages = body.get("messages", [])

        # --- Handle case with no messages ---
        if not messages:
            error_msg = "Error: No messages found in the request body to process."
            await self.emit_status(
                __event_emitter__,
                "error",
                error_msg,
                True,
            )
            # Append error message to the body's messages list for history
            body.setdefault("messages", []).append(
                {"role": "assistant", "content": error_msg}
            )
            # Return an error dictionary, similar to original failure handling
            return {"error": error_msg}

        # --- Extract the latest message ---
        question = messages[-1]["content"]

        await self.emit_status(
            __event_emitter__, "info", f"Calling {self.name} workflow...", False
        )

        # --- Prepare the N8N request ---
        headers = {
            "Authorization": f"Bearer {self.valves.n8n_bearer_token}",
            "Content-Type": "application/json",
        }
        # Get chat_id safely; use a default if not available
        chat_id, _ = extract_event_info(__event_emitter__)

        # Extract system prompt from Open WebUI
        system_prompt = next(
            (
                msg["content"]
                for msg in body.get("messages", [])
                if msg.get("role") == "system"
            ),
            None,
        )

        payload = {"sessionId": chat_id if chat_id is not None else "unknown_session"}
        payload[self.valves.input_field] = (
            question  # Add user's question to the payload
        )

        if system_prompt:
            payload["systemPrompt"] = system_prompt

        n8n_response_content = None
        last_error_message = None

        # --- Make the HTTP request to N8N ---
        try:
            await self.emit_status(
                __event_emitter__,
                "info",
                f"Sending request to N8N...",
                False,
            )

            response = requests.post(
                self.valves.n8n_url,
                json=payload,
                headers=headers,
                timeout=self.valves.request_timeout_seconds,  # Apply timeout
            )

            # --- Process HTTP Response ---
            if 200 <= response.status_code < 300:
                try:
                    n8n_response_data = response.json()

                    # If the response is a list, use the first item
                    if isinstance(n8n_response_data, list):
                        if not n8n_response_data:
                            raise ValueError(
                                "Received an empty list from N8N response."
                            )
                        n8n_response_data = n8n_response_data[0]

                    if (
                        isinstance(n8n_response_data, dict)
                        and self.valves.response_field in n8n_response_data
                    ):
                        n8n_response_content = n8n_response_data[
                            self.valves.response_field
                        ]
                    else:
                        available_keys = (
                            list(n8n_response_data.keys())
                            if isinstance(n8n_response_data, dict)
                            else "N/A"
                        )
                        last_error_message = (
                            f"N8N workflow responded successfully (status {response.status_code}) "
                            f"but missing expected response field '{self.valves.response_field}'. "
                            f"Available keys: {available_keys}. "
                            f"Response snippet: {response.text[:150]}..."
                        )
                        await self.emit_status(
                            __event_emitter__, "error", last_error_message, True
                        )

                except JSONDecodeError:
                    last_error_message = (
                        f"N8N workflow responded successfully (status {response.status_code}) "
                        f"but the response body is not valid JSON. "
                        f"Response snippet: {response.text[:200]}..."
                    )
                    await self.emit_status(
                        __event_emitter__, "error", last_error_message, True
                    )

                except ValueError as e:
                    last_error_message = f"Error processing N8N response: {str(e)}"
                    await self.emit_status(
                        __event_emitter__, "error", last_error_message, True
                    )

            elif 400 <= response.status_code < 500:
                last_error_message = (
                    f"Client error: Received status code {response.status_code} from N8N. "
                    f"This error ({response.status_code}) is typically not transient. Response: {response.text}"
                )
                await self.emit_status(
                    __event_emitter__, "error", last_error_message, True
                )

            else:
                last_error_message = (
                    f"Unexpected HTTP error: Received status code {response.status_code} from N8N. "
                    f"Response: {response.text}"
                )
                await self.emit_status(
                    __event_emitter__, "error", last_error_message, True
                )

        except (RequestException, Exception) as e:
            last_error_message = (
                f"Request to N8N failed: {type(e).__name__} - {str(e)}."
            )
            await self.emit_status(
                __event_emitter__, "warning", last_error_message, False
            )

        # --- After the Request Finishes ---

        if n8n_response_content is not None:
            # --- Success Case: Content was obtained ---
            # Append to messages for conversation history (Open WebUI might use this too)
            body.setdefault("messages", []).append(
                {"role": "assistant", "content": n8n_response_content}
            )

            await self.emit_status(
                __event_emitter__, "info", "N8N workflow completed successfully.", True
            )
            # Return the content directly, matching original working code's return type
            return n8n_response_content
        else:
            # --- Failure Case: Content was NOT obtained ---
            final_error_message = (
                last_error_message
                if last_error_message
                else "An unknown error occurred during the N8N request."
            )
            await self.emit_status(
                __event_emitter__,
                "error",
                f"Final Error calling N8N: {final_error_message}",
                True,
            )
            # Append error message to messages for conversation history
            body.setdefault("messages", []).append(
                {
                    "role": "assistant",
                    "content": f"Error calling N8N workflow: {final_error_message}",
                }
            )
            # Return an error dictionary, similar to original failure handling
            return {"error": final_error_message}
