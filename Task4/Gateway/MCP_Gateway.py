import json
import os

import httpx
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse

app = FastAPI()
DOWNSTREAM_URL = os.getenv("DOWNSTREAM_URL", "http://localhost:8000/mcp")


# Headers copied FROM the agent's request TO the MCP server (Agent → Gateway → MCP)
PASSTHROUGH_REQUEST_HEADERS = ("accept", "mcp-session-id", "mcp-protocol-version", "last-event-id")
# Headers copied FROM the MCP server's reply BACK TO the agent (MCP → Gateway → Agent)
PASSTHROUGH_RESPONSE_HEADERS = ("mcp-session-id",)


TOKENS = {"admin-token": "admin", "viewer-token": "viewer"}


def rpc_error(msg_id, code: int, message: str, status: int = 200):
    return JSONResponse(
        {"jsonrpc": "2.0", "id": msg_id, "error": {"code": code, "message": message}},
        status_code=status,
    )


def get_role(request: Request):
    auth = request.headers.get("Authorization", "")
    scheme, _, token = auth.partition(" ")
    if scheme.lower() != "bearer" or not token.strip():
        return None
    return TOKENS.get(token.strip())


def authorize(msg: dict, role: str):
    if msg["method"] != "tools/call":
        return None
    params = msg.get("params")
    name = params.get("name") if isinstance(params, dict) else None
    if not isinstance(name, str):
        return rpc_error(msg.get("id"), code=-32602, message="Invalid params: name is required")
    if name.startswith("admin_") and role != "admin":
        return rpc_error(msg_id=msg.get("id"), code=-32001, message="Unauthorized Tool Call")
    return None


async def forward(raw: bytes, incoming_header=None) -> httpx.Response:
    header = {"content-type": "application/json"}
    for name in PASSTHROUGH_REQUEST_HEADERS:
        if incoming_header and name in incoming_header:
            header[name] = incoming_header[name]

    async with httpx.AsyncClient() as client:
        return await client.post(DOWNSTREAM_URL, content=raw, headers=header, timeout=10)


HOOK_URL = os.getenv("HOOK_URL", "http://127.0.0.1:6000/inspect")


def parse_upstream(upstream: httpx.Response):
    """Turn the MCP server's reply into a JSON-RPC dict (or None if empty)."""
    if upstream.status_code == 202 or not upstream.content:
        return None
    ctype = upstream.headers.get("content-type", "")
    if ctype.startswith("application/json"):
        return upstream.json()
    if ctype.startswith("text/event-stream"):
        for line in upstream.text.splitlines():
            if line.startswith("data:"):
                return json.loads(line[len("data:"):].strip())
    return None


async def call_hook(role: str, request_msg: dict, reply: dict) -> dict:
    """Send the reply to the hook service on another port; return what it gives back."""
    try:
        async with httpx.AsyncClient() as client:
            r = await client.post(
                HOOK_URL,
                json={"role": role, "request": request_msg, "response": reply},
                timeout=5,
            )
            r.raise_for_status()
            return r.json()
    except httpx.HTTPError as e:
        print(f"[gateway] hook unavailable ({type(e).__name__}), sending original reply")
        return reply


@app.post("/mcp")
async def proxy(request: Request):
    """
    This works as a Gateway that sits between agent and MCP
    """

    raw = await request.body()

    # try parsing to json
    try:
        msg = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return rpc_error(None, -32700, "Parse Error")
    if not isinstance(msg, dict) or msg.get("jsonrpc") != "2.0" or not isinstance(msg.get("method"), str):
        return rpc_error(msg.get("id") if isinstance(msg, dict) else None, -32600, "Invalid Request")

    # Authenticate
    role = get_role(request)
    if role is None:
        return rpc_error(msg.get("id"), -32000, "Missing or invalid token", status=401)

    # authorize
    denied = authorize(msg, role)
    if denied is not None:
        print(f"[gateway] DENIED method={msg['method']} role={role}")
        return denied

    # try sending data upstream
    try:
        upstream = await forward(raw=raw, incoming_header=request.headers)
    except httpx.HTTPError:
        return rpc_error(msg.get("id"), code=-32603, message="Downstream server error", status=502)

    headers = {}
    for h in PASSTHROUGH_RESPONSE_HEADERS:      # go through the allowed list
        if h in upstream.headers:               # did the MCP server send this header?
            headers[h] = upstream.headers[h]

    # Let the hook service see the reply before it goes back to the user
    reply = parse_upstream(upstream)            # bytes (JSON or SSE) -> dict, or None
    if reply is not None:
        reply = await call_hook(role, msg, reply)
        return JSONResponse(reply, status_code=upstream.status_code, headers=headers)

    # nothing to inspect (e.g. 202 empty ack): pass through as before
    return Response(
        content=upstream.content,
        status_code=upstream.status_code,
        media_type=upstream.headers.get("content-type", "application/json"),
        headers=headers,
    )