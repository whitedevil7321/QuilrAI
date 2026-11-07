"""
LLM gateway with PII redaction.

Forwards chat requests to the model router and removes emails, SSNs and card
numbers from the answer while it streams back to the client.

Run from the Task4 folder:
    uvicorn LLM_Gateway.LLMGateway:app --port 8080
"""
import json
import logging
import os
import re
import uuid

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse

load_dotenv()

log = logging.getLogger("llm_gateway")

UPSTREAM_URL = os.getenv("UPSTREAM_URL", "http://localhost:8090/v1/chat/completions")


# --- redaction ---------------------------------------------------------------

REPLACEMENT = "[REDACTED]"
MAX_WAITING = 320   # most characters we ever hold back

EMAIL = (
    r"(?<![\w.+-])"
    r"[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]{1,64}"
    r"@"
    r"(?:[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?\.)+"
    r"[A-Za-z]{2,63}"
    r"(?![\w-])(?!\.\w)"        # a full stop right after the address is fine
)
SSN = r"\b(?!000|666|9\d{2})\d{3}-(?!00)\d{2}-(?!0000)\d{4}\b"
CARD = r"(?<!\d)(?:\d[ -]?){12,18}\d(?!\d)"

SECRET_PATTERN = re.compile(f"(?P<EMAIL>{EMAIL})|(?P<SSN>{SSN})|(?P<CARD>{CARD})")

# Text at the end of the buffer that could still turn into one of the above,
# e.g. "jo" (start of an email) or "4111 1111 " (start of a card number)
UNFINISHED_END = re.compile(r"(?:\d+[ -])*[\w.!#$%&'*+/=?^`{|}~@-]*$")


def passes_luhn(number):
    digits = [int(ch) for ch in number if ch.isdigit()]
    if not 13 <= len(digits) <= 19:
        return False
    total = 0
    for position, digit in enumerate(reversed(digits)):
        if position % 2 == 1:
            digit *= 2
            if digit > 9:
                digit -= 9
        total += digit
    return total % 10 == 0


def is_secret(match):
    kind = match.lastgroup
    if kind == "EMAIL":
        return len(match.group()) <= 254
    if kind == "SSN":
        return True
    if kind == "CARD":
        return passes_luhn(match.group())
    return False


def unfinished_start(text):
    look_from = max(0, len(text) - MAX_WAITING)
    match = UNFINISHED_END.search(text, look_from)
    return match.start() if match else look_from


def hide_secrets(text, stream_is_over=False):
    """Split text into (cleaned part to send now, part to keep for the next chunk)."""
    cut = len(text) if stream_is_over else unfinished_start(text)

    pieces = []
    copied_up_to = 0
    for found in SECRET_PATTERN.finditer(text):
        if found.start() >= cut:
            break
        if found.end() > cut:
            # Don't split a value in half; hold all of it back
            cut = found.start()
            break
        if not is_secret(found):
            continue
        pieces.append(text[copied_up_to:found.start()])
        pieces.append(REPLACEMENT)
        copied_up_to = found.end()
    pieces.append(text[copied_up_to:cut])

    return "".join(pieces), text[cut:]


# --- errors ------------------------------------------------------------------

def gateway_error(status, kind, message, request_id=None):
    request_id = request_id or "req_" + uuid.uuid4().hex[:16]
    return JSONResponse({"error": {"type": kind, "message": message, "request_id": request_id}},
                        status_code=status, headers={"x-request-id": request_id})


def upstream_error(status, content, headers):
    """
    The router already sends clean errors, so those are passed on as they are.
    Anything else gets replaced, so raw provider output never reaches the client.
    """
    try:
        error = json.loads(content)["error"]
        clean = {key: error[key] for key in ("type", "message", "request_id")}
    except (ValueError, KeyError, TypeError):
        log.error("unexpected upstream error %s: %s", status, content[:500])
        return gateway_error(502, "upstream_error", "The model service returned an error.")

    extra = {}
    if "retry-after" in headers:
        extra["retry-after"] = headers["retry-after"]
    return JSONResponse({"error": clean}, status_code=status, headers=extra)


# --- server ------------------------------------------------------------------

app = FastAPI()


def sse(data):
    return f"data: {json.dumps(data)}\n\n"


async def clean_stream(upstream, client):
    waiting = {}       # held-back text for each choice index
    last_info = {}     # id/model of the last chunk, reused for the final piece

    try:
        async for line in upstream.aiter_lines():
            if not line.startswith("data:"):
                continue
            data = line[5:].strip()

            if data == "[DONE]":
                for index, text in waiting.items():
                    rest, _ = hide_secrets(text, stream_is_over=True)
                    if rest:
                        yield sse({**last_info, "choices": [
                            {"index": index, "delta": {"content": rest}, "finish_reason": None}]})
                yield "data: [DONE]\n\n"
                return

            try:
                chunk = json.loads(data)
            except ValueError:
                continue

            last_info = {k: chunk[k] for k in ("id", "object", "created", "model") if k in chunk}

            for choice in chunk.get("choices", []):
                index = choice.get("index", 0)
                delta = choice.get("delta") or {}
                new_text = delta.get("content")

                if new_text:
                    ready, waiting[index] = hide_secrets(waiting.get(index, "") + new_text)
                    delta["content"] = ready

                if choice.get("finish_reason"):
                    rest, _ = hide_secrets(waiting.pop(index, ""), stream_is_over=True)
                    delta["content"] = (delta.get("content") or "") + rest
                    choice["delta"] = delta

            # Tool calls and error events from the router go through untouched
            yield sse(chunk)
    except httpx.HTTPError:
        yield sse({"error": {"type": "upstream_error",
                             "message": "The response was interrupted.",
                             "request_id": "req_" + uuid.uuid4().hex[:16]}})
    finally:
        await upstream.aclose()
        await client.aclose()


@app.post("/v1/chat/completions")
@app.post("/openai/v1/chat/completions")    # path used by ChatGroq(base_url=...)
async def chat(request: Request):
    try:
        body = await request.json()
    except ValueError:
        return gateway_error(400, "invalid_request", "Request body must be valid JSON.")

    headers = {"content-type": "application/json"}
    if "authorization" in request.headers:      # the tenant API key, checked by the router
        headers["authorization"] = request.headers["authorization"]

    client = httpx.AsyncClient(timeout=httpx.Timeout(60, connect=10))

    if not body.get("stream"):
        try:
            resp = await client.post(UPSTREAM_URL, json=body, headers=headers)
        except httpx.HTTPError:
            await client.aclose()
            return gateway_error(502, "upstream_error", "The model service is unavailable.")
        await client.aclose()

        if resp.status_code != 200:
            return upstream_error(resp.status_code, resp.text, resp.headers)

        data = resp.json()
        for choice in data.get("choices", []):
            message = choice.get("message") or {}
            if isinstance(message.get("content"), str):
                message["content"], _ = hide_secrets(message["content"], stream_is_over=True)
        return JSONResponse(data)

    try:
        req = client.build_request("POST", UPSTREAM_URL, json=body, headers=headers)
        upstream = await client.send(req, stream=True)
    except httpx.HTTPError:
        await client.aclose()
        return gateway_error(502, "upstream_error", "The model service is unavailable.")

    if upstream.status_code != 200:
        content = (await upstream.aread()).decode(errors="replace")
        await upstream.aclose()
        await client.aclose()
        return upstream_error(upstream.status_code, content, upstream.headers)

    return StreamingResponse(clean_stream(upstream, client), media_type="text/event-stream")