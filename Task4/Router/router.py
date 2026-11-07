"""
Model router for the LLM gateway.

Sits in front of the model providers and takes care of three things:
  - a token limit per tenant API key (sliding 60 second window, kept in SQLite)
  - switching to the backup provider when the primary returns 429 or is too slow
  - error responses in one fixed format that never expose upstream details

Run from the Task4 folder:
    uvicorn Router.router:app --port 8090
"""
import asyncio
import json
import logging
import os
import uuid
from contextlib import asynccontextmanager

import anyio
import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse

from Rate_Limiting.Rate_limiter import RateLimiter

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("router")


GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"

PROVIDERS = [
    {
        "name": "primary",
        "url": os.getenv("PRIMARY_URL", GROQ_URL),
        "key": os.getenv("PRIMARY_KEY") or os.getenv("GROQ_ENV", ""),
        "model": os.getenv("PRIMARY_MODEL"),    # None means: use the model the client asked for
        "timeout": float(os.getenv("PRIMARY_TIMEOUT", "3.0")),
    },
    {
        "name": "backup",
        "url": os.getenv("BACKUP_URL", GROQ_URL),
        "key": os.getenv("BACKUP_KEY") or os.getenv("GROQ_ENV", ""),
        "model": os.getenv("BACKUP_MODEL", "llama-3.1-8b-instant"),
        "timeout": float(os.getenv("BACKUP_TIMEOUT", "10.0")),
    },
]

TENANT_KEYS = set(os.getenv("TENANT_KEYS", "tenant-a-key,tenant-b-key").split(","))
TOKEN_LIMIT = int(os.getenv("TOKEN_LIMIT", "50000"))
WINDOW_SECONDS = 60
DEFAULT_MAX_TOKENS = 1024

# Provider answers where trying the backup makes sense
RETRY_ON = {429, 500, 502, 503, 504}

limiter = RateLimiter(os.getenv("RATE_DB", "gateway.db"), TOKEN_LIMIT, WINDOW_SECONDS)


class GatewayError(Exception):
    def __init__(self, status, kind, message, retry_after=None):
        super().__init__(message)
        self.status = status
        self.kind = kind
        self.message = message
        self.retry_after = retry_after


def error_body(kind, message, request_id):
    return {"error": {"type": kind, "message": message, "request_id": request_id}}


def error_response(err, request_id):
    headers = {"x-request-id": request_id}
    if err.retry_after:
        headers["retry-after"] = str(err.retry_after)
    return JSONResponse(error_body(err.kind, err.message, request_id),
                        status_code=err.status, headers=headers)


def all_providers_down():
    return GatewayError(503, "service_unavailable",
                        "All model providers are currently unavailable. Please retry shortly.",
                        retry_after=5)


def provider_rejected(status):
    if status in (401, 403):
        # Our own provider key is wrong. Not the caller's fault.
        return GatewayError(502, "upstream_error", "The model provider could not process the request.")
    return GatewayError(400, "invalid_request", "The model provider rejected the request.")


async def evict_expired():
    while True:
        await asyncio.sleep(WINDOW_SECONDS)
        await limiter.evict_all()


@asynccontextmanager
async def lifespan(app):
    if app.state.http is None:
        app.state.http = httpx.AsyncClient(timeout=httpx.Timeout(60, connect=5))
    cleanup = asyncio.create_task(evict_expired())
    yield
    cleanup.cancel()
    await app.state.http.aclose()


app = FastAPI(lifespan=lifespan)
app.state.http = None


# --- request checks ----------------------------------------------------------

def get_tenant(request):
    scheme, _, key = request.headers.get("authorization", "").partition(" ")
    key = key.strip()
    if scheme.lower() != "bearer" or key not in TENANT_KEYS:
        raise GatewayError(401, "authentication_error", "Missing or invalid API key.")
    return key


async def read_body(request):
    try:
        body = await request.json()
    except ValueError:
        raise GatewayError(400, "invalid_request", "Request body must be valid JSON.")
    if not isinstance(body, dict) or not isinstance(body.get("messages"), list):
        raise GatewayError(400, "invalid_request", "Field 'messages' must be a list.")
    return body


def estimate_tokens(body):
    # Roughly 4 characters per token for everything we send (messages, tools, ...)
    # plus room for the answer. Replaced by the real number once the model replies.
    prompt = len(json.dumps(body.get("messages", []))) + len(json.dumps(body.get("tools", [])))
    answer = body.get("max_tokens") or body.get("max_completion_tokens") or DEFAULT_MAX_TOKENS
    return prompt // 4 + int(answer)


# --- talking to providers ----------------------------------------------------

def build_request(provider, body):
    payload = dict(body)
    if provider["model"]:
        payload["model"] = provider["model"]
    headers = {"content-type": "application/json"}
    if provider["key"]:
        headers["authorization"] = f"Bearer {provider['key']}"
    return app.state.http.build_request("POST", provider["url"], json=payload, headers=headers)


async def complete(body, request_id):
    """Non-streaming call. Returns (answer, provider name)."""
    for provider in PROVIDERS:
        name = provider["name"]
        try:
            async with asyncio.timeout(provider["timeout"]):
                resp = await app.state.http.send(build_request(provider, body))
        except TimeoutError:
            log.warning("[%s] %s timed out after %ss", request_id, name, provider["timeout"])
            continue
        except httpx.HTTPError as e:
            log.warning("[%s] %s unreachable (%s)", request_id, name, type(e).__name__)
            continue

        if resp.status_code == 200:
            try:
                return resp.json(), name
            except ValueError:
                log.warning("[%s] %s sent a body that is not JSON", request_id, name)
                continue

        if resp.status_code in RETRY_ON:
            log.warning("[%s] %s returned %s", request_id, name, resp.status_code)
            continue

        log.error("[%s] %s returned %s: %s", request_id, name, resp.status_code, resp.text[:500])
        raise provider_rejected(resp.status_code)

    raise all_providers_down()


async def open_stream(body, request_id):
    """
    Start a streaming call. The timeout covers everything up to the first line
    of the answer, so a provider that accepts the request but never starts
    talking is treated the same as one that never answers.

    Returns (response, remaining lines, first line, provider name).
    """
    for provider in PROVIDERS:
        name = provider["name"]
        resp = None
        try:
            async with asyncio.timeout(provider["timeout"]):
                resp = await app.state.http.send(build_request(provider, body), stream=True)
                if resp.status_code == 200:
                    lines = resp.aiter_lines()
                    first_line = await anext(lines)
                    return resp, lines, first_line, name
                details = (await resp.aread())[:500]
        except TimeoutError:
            log.warning("[%s] %s timed out after %ss", request_id, name, provider["timeout"])
            if resp is not None:
                await resp.aclose()
            continue
        except (httpx.HTTPError, StopAsyncIteration) as e:
            log.warning("[%s] %s failed to start streaming (%s)", request_id, name, type(e).__name__)
            if resp is not None:
                await resp.aclose()
            continue

        await resp.aclose()
        if resp.status_code in RETRY_ON:
            log.warning("[%s] %s returned %s", request_id, name, resp.status_code)
            continue

        log.error("[%s] %s returned %s: %s", request_id, name, resp.status_code, details)
        raise provider_rejected(resp.status_code)

    raise all_providers_down()


def check_line(line, request_id):
    """
    Look at one line of the provider's stream.
    Returns (line to send on, total tokens if this line reports usage).
    """
    if not line.startswith("data:"):
        return line, None

    data = line[5:].strip()
    if data == "[DONE]":
        return line, None

    try:
        chunk = json.loads(data)
    except ValueError:
        log.warning("[%s] dropped a stream line that is not JSON", request_id)
        return None, None

    if "error" in chunk and not chunk.get("choices"):
        # Error raised by the provider halfway through the stream
        log.error("[%s] provider error in stream: %s", request_id, str(chunk["error"])[:500])
        clean = error_body("upstream_error", "The model provider could not complete the request.",
                           request_id)
        return "data: " + json.dumps(clean), None

    usage = chunk.get("usage") or (chunk.get("x_groq") or {}).get("usage") or {}
    return line, usage.get("total_tokens")


async def relay(resp, lines, first_line, row_id, estimate, request_id):
    used = None

    async def all_lines():
        yield first_line
        async for line in lines:
            yield line

    try:
        async for line in all_lines():
            line, tokens = check_line(line, request_id)
            if tokens:
                used = tokens
            if line is not None:
                yield line + "\n"
    except httpx.HTTPError:
        log.warning("[%s] provider stream broke off", request_id)
        clean = error_body("upstream_error", "The response was interrupted.", request_id)
        yield f"data: {json.dumps(clean)}\n\n"
    finally:
        # Shielded so the bookkeeping still happens if the client disconnects
        with anyio.CancelScope(shield=True):
            await resp.aclose()
            await limiter.set_tokens(row_id, used or estimate)


# --- endpoint ----------------------------------------------------------------

@app.post("/v1/chat/completions")
@app.post("/openai/v1/chat/completions")    # path used by the Groq SDK
async def chat_completions(request: Request):
    request_id = "req_" + uuid.uuid4().hex[:16]
    row_id = None

    try:
        tenant = get_tenant(request)
        body = await read_body(request)

        estimate = estimate_tokens(body)
        if estimate > TOKEN_LIMIT:
            raise GatewayError(413, "request_too_large",
                               f"This request needs about {estimate} tokens, "
                               f"more than the limit of {TOKEN_LIMIT} per minute.")

        allowed, row_id, retry_after = await limiter.try_reserve(tenant, estimate)
        if not allowed:
            raise GatewayError(429, "rate_limit_exceeded",
                               f"Token limit of {TOKEN_LIMIT} per minute reached.",
                               retry_after=retry_after)

        if body.get("stream"):
            resp, lines, first_line, provider = await open_stream(body, request_id)
            reserved, row_id = row_id, None     # relay() owns the reservation from here
            return StreamingResponse(
                relay(resp, lines, first_line, reserved, estimate, request_id),
                media_type="text/event-stream",
                headers={"x-request-id": request_id, "x-served-by": provider},
            )

        answer, provider = await complete(body, request_id)
        used = (answer.get("usage") or {}).get("total_tokens") or estimate
        await limiter.set_tokens(row_id, used)
        return JSONResponse(answer, headers={"x-request-id": request_id, "x-served-by": provider})

    except GatewayError as err:
        if row_id is not None:
            await limiter.remove(row_id)
        return error_response(err, request_id)

    except asyncio.CancelledError:
        # Client went away before we answered: nothing was delivered, so refund
        if row_id is not None:
            with anyio.CancelScope(shield=True):
                await limiter.remove(row_id)
        raise

    except Exception:
        log.exception("[%s] unexpected error", request_id)
        if row_id is not None:
            await limiter.remove(row_id)
        return error_response(GatewayError(500, "internal_error", "An internal error occurred."),
                              request_id)