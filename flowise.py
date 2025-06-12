"""
title: Flowise Pipe Function
author:  comwise-nl
revision: v0.0.1
date: 2025-06-12

This module defines a Pipe class that connects Open WebUI to an n8n workflow.

Streaming does not work because Open WebUI seems to buffer everything embedded in a Markdown code block.
If you want streaming check out the Flowise pipeline
"""

import json
import requests
import traceback
import asyncio

from typing import Optional, Callable, Awaitable
from pydantic import BaseModel, Field
from requests.exceptions import (
    RequestException,
    HTTPError,
    Timeout,
    ConnectionError,
)

class Pipe:
    class Valves(BaseModel):
        flowise_api_url: str = Field(
            default="https://[your_flowise_instance]/api/v1/prediction/[your_chatbot_id]",
            description="The full URL to your Flowise chatbot's prediction endpoint.",
        )
        test_direct_connection: bool = Field(
            default=False,
            description="Test with direct IP/port instead of domain to bypass proxies",
        )
        flowise_api_key: Optional[str] = Field(
            default=None,
            description="Optional: Bearer token for authenticating with the Flowise API.",
        )
        request_timeout_seconds: float = Field(
            default=300.0,
            description="Total timeout for the HTTP request to Flowise (in seconds). Includes connection and read time.",
        )
        enable_status_indicator: bool = Field(
            default=True,
            description="Enable or disable status indicator emissions to the UI.",
        )
        enable_streaming: bool = Field(
            default=True,
            description="Enable streaming output from Flowise. Flowise must support streaming for the selected chatflow.",
        )
        stream_event_name: str = Field(
            default="text",
            description="Fallback key for non-'token' streaming content. For 'token' events, content is in 'data' field.",
        )
        debug_mode: bool = Field(
            default=True,
            description="Enable detailed debug logging to help troubleshoot issues.",
        )
        streaming_delay_seconds: float = Field(
            default=0.05,  # Adjusted default for better streaming
            description="Delay (in seconds) between sending each streaming chunk to Open WebUI. Increase if text appears all at once.",
        )

    def __init__(self):
        self.type = "pipe"
        self.id = "flowise_connector"
        self.name = "Flowise Chatbot"
        self.valves = self.Valves()

    async def emit_status(
        self,
        __event_emitter__: Optional[Callable[[dict], Awaitable[None]]],
        level: str,
        message: str,
        done: bool,
        is_stream_chunk: bool = False,
        stream_content: Optional[str] = None,
    ):
        if self.valves.debug_mode:
            print(f"[FLOWISE DEBUG] {level.upper()}: {message}")
            if stream_content:
                print(f"[FLOWISE DEBUG] Stream content: {stream_content}")
            print(
                f"[FLOWISE DEBUG] Emit Status: done={done}, is_stream_chunk={is_stream_chunk}"
            )

        if __event_emitter__ and self.valves.enable_status_indicator:
            if is_stream_chunk:
                status_message = {
                    "type": "chunk",
                    "data": {
                        "status": "in_progress",
                        "level": level,
                        "description": message,
                        "done": False,
                        "content": stream_content,
                    },
                }
            else:
                status_message = {
                    "type": "status",
                    "data": {
                        "status": "complete" if done else "in_progress",
                        "level": level,
                        "description": message,
                        "done": done,
                    },
                }

            try:
                await __event_emitter__(status_message)
            except Exception as e:
                print(f"ERROR: Failed to emit status: {e}. Message: {status_message}")
            finally:
                await asyncio.sleep(self.valves.streaming_delay_seconds)

    async def pipe(
        self,
        body: dict,
        __event_emitter__: Optional[Callable[[dict], Awaitable[None]]] = None,
        **kwargs,
    ) -> Optional[dict | str]:
        if self.valves.debug_mode:
            print(
                f"[FLOWISE DEBUG] Pipe called with body: {json.dumps(body, indent=2)}"
            )
            print(f"[FLOWISE DEBUG] Flowise URL: {self.valves.flowise_api_url}")
            print(
                f"[FLOWISE DEBUG] enable_streaming valve value: {self.valves.enable_streaming}"
            )
            print(
                f"[FLOWISE DEBUG] streaming_delay_seconds valve value: {self.valves.streaming_delay_seconds}"
            )

        await self.emit_status(
            __event_emitter__, "info", "Flowise Pipe: Starting execution...", False
        )

        full_response_content = ""
        last_error_message = None
        response = None

        try:
            if not isinstance(body, dict):
                error_msg = "Request body is not a valid dictionary."
                await self.emit_status(__event_emitter__, "error", error_msg, True)
                return {"error": error_msg}

            messages = body.get("messages", [])

            if not messages:
                error_msg = "Error: No messages found in the request body to process."
                await self.emit_status(__event_emitter__, "error", error_msg, True)
                return {"error": error_msg}

            user_message = messages[-1]["content"]
            await self.emit_status(
                __event_emitter__,
                "info",
                f"User message: '{user_message[:50]}...' (truncated)",
                False,
            )

            headers = {}

            if self.valves.flowise_api_key:
                headers["Authorization"] = f"Bearer {self.valves.flowise_api_key}"
                await self.emit_status(
                    __event_emitter__, "info", "API Key configured for Flowise.", False
                )
            else:
                await self.emit_status(
                    __event_emitter__,
                    "info",
                    "No API Key configured for Flowise.",
                    False,
                )

            payload = {"question": user_message}

            if self.valves.enable_streaming:
                headers["Cache-Control"] = "no-cache"
                headers["Connection"] = "keep-alive"
                headers["Content-Type"] = "application/json"
                payload["streaming"] = True
            else:
                headers["Content-Type"] = "application/json"

            if self.valves.debug_mode:
                print(
                    f"[FLOWISE DEBUG] Request payload: {json.dumps(payload, indent=2)}"
                )
                print(f"[FLOWISE DEBUG] Request headers: {headers}")

            await self.emit_status(
                __event_emitter__,
                "info",
                f"Sending POST request to {self.valves.flowise_api_url}...",
                False,
            )

            response = requests.post(
                self.valves.flowise_api_url,
                json=payload,
                headers=headers,
                timeout=self.valves.request_timeout_seconds,
                stream=True,
                allow_redirects=False,
            )

            if self.valves.debug_mode:
                print(f"[FLOWISE DEBUG] Response status: {response.status_code}")
                print(f"[FLOWISE DEBUG] Response headers: {dict(response.headers)}")
                if not self.valves.enable_streaming:
                    print(
                        f"[FLOWISE DEBUG] Raw Response Text (first 500 chars): {response.text[:500]}..."
                    )
                else:
                    print(
                        "[FLOWISE DEBUG] Response text not read directly (streaming enabled)."
                    )

            await self.emit_status(
                __event_emitter__,
                "info",
                f"Request complete. HTTP Status: {response.status_code}.",
                False,
            )

            try:
                response.raise_for_status()
                await self.emit_status(
                    __event_emitter__, "info", "HTTP Status OK (2xx).", False
                )
            except HTTPError as e:
                error_detail = (
                    f"HTTP Error {e.response.status_code}: {e.response.text[:200]}..."
                )
                last_error_message = f"Flowise HTTP error: {error_detail}"
                if self.valves.debug_mode:
                    print(f"[FLOWISE DEBUG] HTTP Error: {error_detail}")
                    print(f"[FLOWISE DEBUG] Full response: {e.response.text}")
                await self.emit_status(
                    __event_emitter__, "error", last_error_message, True
                )
                return {"error": last_error_message}

            if self.valves.enable_streaming:
                await self.emit_status(
                    __event_emitter__,
                    "info",
                    "Processing streaming response (manual SSE parsing)...",
                    False,
                )
                try:
                    actual_content_type = response.headers.get("Content-Type", "")
                    if self.valves.debug_mode:
                        print(
                            f"[FLOWISE DEBUG] Pipe sees Content-Type header: '{actual_content_type}'"
                        )
                        print(
                            f"[FLOWISE DEBUG] Is 'text/event-stream' in actual_content_type?: {'text/event-stream' in actual_content_type}"
                        )

                    if "text/event-stream" not in actual_content_type:
                        if self.valves.debug_mode:
                            print(
                                f"[FLOWISE DEBUG] Warning: Expected streaming but got content-type: {actual_content_type}"
                            )
                        try:
                            flowise_response_data = response.json()
                            if isinstance(flowise_response_data, dict):
                                full_response_content = (
                                    flowise_response_data.get("text")
                                    or flowise_response_data.get("answer")
                                    or flowise_response_data.get("response")
                                    or str(flowise_response_data)
                                )
                                await self.emit_status(
                                    __event_emitter__,
                                    "info",
                                    "Received non-streaming response (fallback).",
                                    True,
                                )
                            else:
                                full_response_content = str(flowise_response_data)
                        except json.JSONDecodeError:
                            full_response_content = response.text
                    else:
                        chunk_count = 0
                        with response:
                            for line in response.iter_lines():
                                if line:
                                    decoded_line = line.decode("utf-8")
                                    if self.valves.debug_mode:
                                        print(
                                            f"[FLOWISE DEBUG] Raw SSE line: {decoded_line}"
                                        )

                                    if decoded_line.startswith("data:"):
                                        try:
                                            event_data = decoded_line[
                                                len("data:") :
                                            ].strip()
                                            event_data_json = json.loads(event_data)
                                            inner_event_type = event_data_json.get(
                                                "event"
                                            )
                                            inner_data_content = event_data_json.get(
                                                "data"
                                            )

                                            if (
                                                inner_event_type == "token"
                                                and isinstance(inner_data_content, str)
                                            ):
                                                full_response_content += (
                                                    inner_data_content
                                                )
                                                await self.emit_status(
                                                    __event_emitter__,
                                                    "info",
                                                    "",
                                                    False,
                                                    is_stream_chunk=True,
                                                    stream_content=inner_data_content,  # Emit the whole token chunk
                                                )

                                            elif inner_event_type == "end":
                                                if self.valves.debug_mode:
                                                    print(
                                                        f"[FLOWISE DEBUG] INFO: Flowise stream ended via 'end' event."
                                                    )
                                                break
                                            else:
                                                if self.valves.debug_mode:
                                                    print(
                                                        f"[FLOWISE DEBUG] Info/Meta Event: {inner_event_type}, Data: {str(inner_data_content)[:100]}..."
                                                    )
                                            chunk_count += 1
                                        except json.JSONDecodeError:
                                            if self.valves.debug_mode:
                                                print(
                                                    f"[FLOWISE DEBUG] WARNING: JSONDecodeError for event data. Raw: {event_data[:100]}..."
                                                )
                                            pass
                                    elif decoded_line == "":
                                        pass
                                    else:
                                        if self.valves.debug_mode:
                                            print(
                                                f"[FLOWISE DEBUG] Non-data SSE line (ignored): {decoded_line}"
                                            )

                        await self.emit_status(
                            __event_emitter__,
                            "info",
                            f"Streaming complete. Processed {chunk_count} data events.",
                            True,
                            is_stream_chunk=False,
                        )

                except Exception as e:
                    if self.valves.debug_mode:
                        print(
                            f"[FLOWISE DEBUG] Streaming Exception: {traceback.format_exc()}"
                        )
                    await self.emit_status(
                        __event_emitter__,
                        "error",
                        f"SSE streaming failed: {type(e).__name__} - {str(e)}",
                        True,
                    )
                finally:
                    if response is not None:
                        response.close()

            else:
                await self.emit_status(
                    __event_emitter__,
                    "info",
                    "Processing as NON-STREAMING response (streaming disabled).",
                    False,
                )
                try:
                    with response:
                        flowise_response_data = response.json()
                        if self.valves.debug_mode:
                            print(
                                f"[FLOWISE DEBUG] Non-streaming response data: {json.dumps(flowise_response_data, indent=2)}"
                            )

                        if isinstance(flowise_response_data, dict):
                            full_response_content = (
                                flowise_response_data.get("text")
                                or flowise_response_data.get("answer")
                                or flowise_response_data.get("response")
                                or flowise_response_data.get("output")
                                or str(flowise_response_data)
                            )
                            await self.emit_status(
                                __event_emitter__,
                                "info",
                                "Flowise response received (non-streaming).",
                                True,
                            )
                        else:
                            full_response_content = str(flowise_response_data)
                except json.JSONDecodeError as json_err:
                    if self.valves.debug_mode:
                        print(
                            f"[FLOWISE DEBUG] JSON decode error, using raw text: {response.text}"
                        )
                    full_response_content = response.text
                    await self.emit_status(
                        __event_emitter__,
                        "info",
                        "Received raw text response (JSON decode failed).",
                        True,
                    )
                except Exception as e:
                    if self.valves.debug_mode:
                        print(
                            f"[FLOWISE DEBUG] Non-streaming processing Exception: {traceback.format_exc()}"
                        )
                    last_error_message = f"Error in non-streaming processing: {type(e).__name__} - {str(e)}"
                    await self.emit_status(
                        __event_emitter__, "error", last_error_message, True
                    )
                finally:
                    if response is not None:
                        response.close()

        except RequestException as e:
            last_error_message = f"Network or Request error to Flowise: {type(e).__name__} - {str(e)}. Check URL/connectivity."
            if self.valves.debug_mode:
                print(f"[FLOWISE DEBUG] Request Exception: {traceback.format_exc()}")
            await self.emit_status(__event_emitter__, "error", last_error_message, True)
            if response is not None:
                response.close()
        except json.JSONDecodeError as e:
            error_body = response.text if response else "N/A"
            last_error_message = f"JSON decoding error from Flowise: {e}. Raw response: {error_body[:200]}..."
            if self.valves.debug_mode:
                print(
                    f"[FLOWISE DEBUG] JSON Decode Exception: {traceback.format_exc()}"
                )
            await self.emit_status(__event_emitter__, "error", last_error_message, True)
            if response is not None:
                response.close()
        except Exception as e:
            last_error_message = f"An unexpected error occurred: {type(e).__name__} - {str(e)}. Check pipe code/Flowise."
            if self.valves.debug_mode:
                print(f"[FLOWISE DEBUG] Unexpected Exception: {traceback.format_exc()}")
            await self.emit_status(__event_emitter__, "error", last_error_message, True)
            if response is not None:
                response.close()

        if full_response_content:
            body.setdefault("messages", []).append(
                {"role": "assistant", "content": full_response_content}
            )
            return full_response_content
        else:
            final_error_message = (
                last_error_message
                if last_error_message
                else "Unknown error: No content received and no specific error reported. Check Flowise logs."
            )
            await self.emit_status(
                __event_emitter__,
                "error",
                f"Flowise Pipe Failed: {final_error_message}",
                True,
            )
            return {"error": final_error_message}
