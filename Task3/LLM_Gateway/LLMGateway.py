"""

Run :
    uvicorn LLM_Gateway.LLMGateway:app --port 8080
"""
import json
import os
import re

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse, StreamingResponse

load_dotenv()  


UPSTREAM_URL = os.getenv("UPSTREAM_URL", "https://api.groq.com/openai/v1/chat/completions")


# #############################################################################
#  PART A - THE GUARDRAIL  (find and hide secrets)
# #############################################################################

REPLACEMENT = "[REDACTED]"
MAX_WAITING = 320  

# ---- 1. PATTERNS
EMAIL = (
    r"(?<![\w.+-])"                                         
    r"[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]{1,64}"                
    r"@"                                                     
    r"(?:[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?\.)+"
    r"[A-Za-z]{2,63}"                                       
    r"(?![\w-])(?!\.\w)"                                  
)
SSN = r"\b(?!000|666|9\d{2})\d{3}-(?!00)\d{2}-(?!0000)\d{4}\b"  
CARD = r"(?<!\d)(?:\d[ -]?){12,18}\d(?!\d)"                      

SECRET_PATTERN = re.compile(f"(?P<EMAIL>{EMAIL})|(?P<SSN>{SSN})|(?P<CARD>{CARD})")


UNFINISHED_END = re.compile(r"(?:\d+[ -])*[\w.!#$%&'*+/=?^`{|}~@-]*$")


# ---- 2. CHECKS: is a match really a secret?
def is_real_card_number(number: str) -> bool:
    """Luhn check: real card numbers pass this math test."""
    digits = [int(ch) for ch in number if ch.isdigit()]
    if not 13 <= len(digits) <= 19:
        return False
    total = 0
    for position, digit in enumerate(reversed(digits)):
        if position % 2 == 1:
            digit = digit * 2
            if digit > 9:
                digit = digit - 9
        total = total + digit
    return total % 10 == 0


def is_secret(match: re.Match) -> bool:
    kind = match.lastgroup
    value = match.group()
    if kind == "EMAIL":
        return len(value) <= 254
    if kind == "SSN":
        return True
    if kind == "CARD":
        return is_real_card_number(value)
    return False


# ---- 3. SPLIT: what can we send now, what must wait?
def find_finished_part_end(text: str) -> int:
    look_from = max(0, len(text) - MAX_WAITING)

    match = UNFINISHED_END.search(text, look_from)

    if match is None:
        return look_from

    return match.start()


def hide_secrets(text: str, stream_is_over: bool = False):
    """Return (ready, waiting): ready = cleaned text to send now, waiting = keep for later."""
    cut = len(text) if stream_is_over else find_finished_part_end(text)

    pieces = []
    copied_up_to = 0
    for found in SECRET_PATTERN.finditer(text):
        if found.start() >= cut:
            break                      
        if found.end() > cut:
            cut = found.start()         
            break
        if not is_secret(found):
            continue
        pieces.append(text[copied_up_to:found.start()])
        pieces.append(REPLACEMENT)
        copied_up_to = found.end()
    pieces.append(text[copied_up_to:cut])

    return "".join(pieces), text[cut:]


# #############################################################################
#  PART B - THE GATEWAY SERVER  (forward to Groq, clean the answer)
# #############################################################################

app = FastAPI()


def to_sse(data: dict) -> str:
    """Format one chunk the way OpenAI/Groq streams it."""
    return f"data: {json.dumps(data)}\n\n"


async def clean_stream(upstream: httpx.Response, client: httpx.AsyncClient):
    """
    Read Groq's stream chunk by chunk, hide secrets, send each piece on immediately.
    """
    waiting = {}        
    last_info = {}     

    try:
        async for line in upstream.aiter_lines():
            if not line.startswith("data:"):
                continue                              
            data = line[len("data:"):].strip()

            # ---- end of the stream: release whatever is still waiting
            if data == "[DONE]":
                for index, text in waiting.items():
                    ready, _ = hide_secrets(text, stream_is_over=True)
                    if ready:
                        yield to_sse({**last_info, "choices": [
                            {"index": index, "delta": {"content": ready}, "finish_reason": None}]})
                yield "data: [DONE]\n\n"
                return

            chunk = json.loads(data)
            last_info = {k: chunk[k] for k in ("id", "object", "created", "model") if k in chunk}

            # ---- clean the text in this chunk
            for choice in chunk.get("choices", []):
                index = choice.get("index", 0)
                delta = choice.get("delta") or {}
                new_text = delta.get("content")

                if new_text:                                    # only touch the text part
                    text = waiting.get(index, "") + new_text    # glue to what was waiting
                    ready, waiting[index] = hide_secrets(text)  # split
                    delta["content"] = ready                    # send only the safe part

                if choice.get("finish_reason"):                 # this answer is finished
                    rest, _ = hide_secrets(waiting.pop(index, ""), stream_is_over=True)
                    delta["content"] = (delta.get("content") or "") + rest
                    choice["delta"] = delta

            # tool calls (delta.tool_calls) pass through untouched
            yield to_sse(chunk)
    finally:
        await upstream.aclose()
        await client.aclose()


@app.post("/v1/chat/completions")
@app.post("/openai/v1/chat/completions")    # the path ChatGroq(base_url=...) uses
async def chat(request: Request):
    body = await request.json()

    headers = {"content-type": "application/json"}
    if "authorization" in request.headers:            # the Groq key sent by Client.py
        headers["authorization"] = request.headers["authorization"]

    client = httpx.AsyncClient(timeout=httpx.Timeout(60, connect=10))

    # ---------------- answer NOT streamed: clean the whole message at once
    if not body.get("stream"):
        try:
            r = await client.post(UPSTREAM_URL, json=body, headers=headers)
        except httpx.HTTPError:
            await client.aclose()
            return JSONResponse({"error": {"message": "LLM unavailable"}}, status_code=502)
        await client.aclose()
        if r.status_code != 200:
            return Response(r.content, status_code=r.status_code,
                            media_type=r.headers.get("content-type"))
        data = r.json()
        for choice in data.get("choices", []):
            message = choice.get("message") or {}
            if isinstance(message.get("content"), str):
                message["content"], _ = hide_secrets(message["content"], stream_is_over=True)
        return JSONResponse(data)

    # ---------------- answer streamed: clean it piece by piece
    try:
        req = client.build_request("POST", UPSTREAM_URL, json=body, headers=headers)
        upstream = await client.send(req, stream=True)
    except httpx.HTTPError:
        await client.aclose()
        return JSONResponse({"error": {"message": "LLM unavailable"}}, status_code=502)

    if upstream.status_code != 200:                    # e.g. wrong API key -> show Groq's error
        content = await upstream.aread()
        await upstream.aclose()
        await client.aclose()
        return Response(content, status_code=upstream.status_code,
                        media_type=upstream.headers.get("content-type"))

    return StreamingResponse(clean_stream(upstream, client), media_type="text/event-stream")