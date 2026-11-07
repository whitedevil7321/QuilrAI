# AI Field Dev - MCP and LLM Gateway Tasks

Four tasks. Each one builds on the one before it:

| Task | What it adds | Main pieces |
|---|---|---|
| Task1 | MCP server with strict input validation | `MCP/Customer.py`, `Client.py` |
| Task2 | MCP security gateway (bearer token roles, blocks `admin_*` tools for viewers) | `Gateway/MCP_Gateway.py` |
| Task3 | LLM gateway that redacts emails, SSNs and card numbers from streamed answers | `LLM_Gateway/LLMGateway.py` |
| Task4 | Model router with a per-tenant token limit (SQLite) and primary to backup failover | `Router/router.py`, `Rate_Limiting/Rate_limiter.py` |

Every task folder has its own copy of `data.csv` (1000 customers, IDs `CUST-00001` to `CUST-01000`).


## Setup

Python 3.12, from the `AI_field_dev` folder:

```powershell
uv venv
.venv\Scripts\activate
uv pip install -r requirements.txt
uv pip install uvicorn httpx langgraph
```

`requirements.txt` includes a package called `redactor`. None of the tasks use it, and it can
shadow imports, so it's safer to remove it: `uv pip uninstall redactor`.

`.env` sits in the `AI_field_dev` root and is picked up by every task. Keys used:

| Key | Used by | Notes |
|---|---|---|
| `GROQ_ENV` | all clients (Task1-3), router (Task4) | Groq API key |
| `MCP_TOKEN` | clients (Task2-4) | `viewer-token` or `admin-token` |
| `DOWNSTREAM_URL` | MCP gateway | `http://127.0.0.1:8000/mcp` |
| `TENANT_API_KEY` | Task4 client | optional, defaults to `tenant-a-key` |

Every command below is run from inside the task folder (`cd Task1`, `cd Task2`, ...), with each
server in its own terminal.

Ports used:

| Port | Service |
|---|---|
| 8000 | MCP server (`Customer.py`) |
| 3000 | MCP gateway |
| 8080 | LLM gateway |
| 8090 | Model router |
| 6000 | optional hook service (Task3/Task4 MCP gateway) |


## Task1 - MCP server with schema validation

The client starts the MCP server itself over stdio, so only one terminal is needed:

```powershell
cd Task1
python Client.py
```

Tools: `get_customer_record(customer_id)` and `trigger_refund(customer_id, amount, reason)`.
Every `tools/call` is validated with Pydantic before it runs. Bad arguments come back as a
JSON-RPC `-32602` error.

Things to try:

| Ask | Expected |
|---|---|
| `show customer CUST-00001` | record returned |
| `show customer CUST-1` | rejected, ID must be `CUST-` + 5 digits |
| `refund 50 to CUST-00002 because the item was damaged` | refund accepted |
| `refund 50000 to CUST-00002 because the item was damaged` | rejected, max amount is 10000 |
| `refund 50 to CUST-00002 because bad` | rejected, reason must be at least 11 characters |


## Task2 - MCP security gateway

```powershell
cd Task2
python MCP\Customer.py                               # terminal 1, port 8000
uvicorn Gateway.MCP_Gateway:app --port 3000          # terminal 2
python Client.py                                     # terminal 3
```

The gateway reads `Authorization: Bearer <token>`, maps it to a role and forwards `tools/list`
untouched. For `tools/call` it blocks any tool starting with `admin_` unless the role is admin,
returning `-32001 Unauthorized Tool Call` without contacting the MCP server.

Tokens: `admin-token` gives admin, `viewer-token` gives viewer. Anything else gets 401.

Things to try (change `MCP_TOKEN` in `.env`, then restart `Client.py`):

| Ask | viewer-token | admin-token |
|---|---|---|
| `send 50 from user 1 to user 2` | allowed | allowed |
| `reset the API key for customer 5 to 12345` | blocked, gateway prints `DENIED` | allowed |
| `send 100 from admin account 9 to user 2` | blocked | allowed |

When a call is blocked, the `Customer.py` terminal shows no request for it.

Direct check without the client:

```powershell
curl.exe -s localhost:3000/mcp -H "Authorization: Bearer viewer-token" -H "Content-Type: application/json" `
  -d '{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\",\"params\":{\"name\":\"admin_reset_key\"}}'
```


## Task3 - Streaming PII redaction

```powershell
cd Task3
python MCP\Customer.py                               # terminal 1, port 8000
uvicorn Gateway.MCP_Gateway:app --port 3000          # terminal 2
uvicorn LLM_Gateway.LLMGateway:app --port 8080       # terminal 3, forwards to Groq
python Client.py                                     # terminal 4
```

`Client.py` sends its LLM calls to the gateway (`base_url=http://localhost:8080`) instead of
straight to Groq. The gateway reads Groq's stream chunk by chunk and replaces emails, SSNs and
card numbers (Luhn-checked) with `[REDACTED]`. It holds back only the last unfinished word, so a
value split across two chunks is still caught, and the rest of the text streams straight through.

Things to try:

| Ask | Expected |
|---|---|
| `repeat exactly: my email is john@example.com and my SSN is 123-45-6789` | both shown as `[REDACTED]` |
| `repeat exactly: card 4111 1111 1111 1111, order 12345` | card redacted, order number kept |
| `give me the details for CUST-00001` | record shown, email column redacted |

The MCP gateway in this folder also posts every reply to a hook service on port 6000. That service
isn't included, so the gateway prints `hook unavailable ... sending original reply`. This is
harmless.


## Task4 - Rate limiting and model fallback

```
Client.py -> LLM gateway :8080 -> router :8090 -> primary model (Groq)
                                               -> backup model  (on 429, timeout or outage)
Client.py -> MCP gateway :3000 -> Customer.py :8000
```

```powershell
cd Task4
python MCP\Customer.py                               # terminal 1, port 8000
uvicorn Gateway.MCP_Gateway:app --port 3000          # terminal 2
uvicorn Router.router:app --port 8090                # terminal 3
uvicorn LLM_Gateway.LLMGateway:app --port 8080       # terminal 4, forwards to the router
python Client.py                                     # terminal 5
```

`Client.py` first asks you to sign up or log in (accounts are stored with salted PBKDF2 hashes in
`Username_pass.db`). It then sends the tenant key as its API key. The router checks the key,
reserves an estimate of the tokens, calls the primary and swaps the estimate for the real
`total_tokens` once the answer arrives. Usage is stored in `gateway.db`.

How the router behaves:

- 50,000 tokens per tenant key in any 60-second window. Expired rows are deleted on each check and
  by a background task every minute.
- The primary gets 3 seconds to start answering (the first line of the stream). After that, or on
  429, 5xx or a connection error, the backup is used. The backup defaults to Groq's
  `llama-3.1-8b-instant`.
- Every error is `{"error": {"type", "message", "request_id"}}`. Upstream details only go to the
  router log.

Router settings (environment variables, set before starting the router):

| Variable | Default |
|---|---|
| `PRIMARY_URL` / `BACKUP_URL` | Groq chat completions URL |
| `PRIMARY_MODEL` / `BACKUP_MODEL` | client's model / `llama-3.1-8b-instant` |
| `PRIMARY_TIMEOUT` / `BACKUP_TIMEOUT` | `3.0` / `10.0` seconds |
| `TENANT_KEYS` | `tenant-a-key,tenant-b-key` |
| `TOKEN_LIMIT` | `50000` |
| `RATE_DB` | `gateway.db` |

### Experiments

Each one only needs the router restarted with a different setting. Open a fresh terminal for each,
because `$env:` values stay set in a terminal until it's closed.

**Failover on timeout**: give the primary almost no time:
```powershell
$env:PRIMARY_TIMEOUT = "0.01"
uvicorn Router.router:app --port 8090
```
Ask anything. The router log shows `primary timed out after 0.01s` and the answer comes from the
backup.

**Failover when the primary is down**: point the primary at a port nobody listens on:
```powershell
$env:PRIMARY_URL = "http://127.0.0.1:1/v1/chat/completions"
uvicorn Router.router:app --port 8090
```
The log shows `primary failed to start streaming (ConnectError)`. The backup answers.

**Both providers down**: clean 503, nothing internal leaked:
```powershell
$env:PRIMARY_URL = "http://127.0.0.1:1/v1/chat/completions"
$env:BACKUP_URL  = "http://127.0.0.1:2/v1/chat/completions"
uvicorn Router.router:app --port 8090
```
The client says it couldn't process the request. The router returned `service_unavailable`.

**Rate limit**: set a small limit:
```powershell
$env:TOKEN_LIMIT = "6000"
uvicorn Router.router:app --port 8090
```
Ask two or three questions quickly. The client then shows
`You have reached the token limit for this minute. Please try again in N seconds.`
Wait for that many seconds and it works again.

**Usage on disk**: see what the limiter recorded (run in the Task4 folder):
```powershell
python -c "import sqlite3; print(sqlite3.connect('gateway.db').execute('select tenant, tokens, time from usage').fetchall())"
```
Restart the router and run it again. The rows are still there, so limits survive a restart.

**Wrong tenant key**: set `TENANT_API_KEY=wrong-key` in `.env` and restart the client. Every question
is rejected with `authentication_error`.

**Direct calls to the router**, without the client:
```powershell
# normal request (x-served-by header says primary or backup)
curl.exe -i -s localhost:8090/v1/chat/completions -H "Authorization: Bearer tenant-a-key" `
  -H "Content-Type: application/json" `
  -d '{\"model\":\"openai/gpt-oss-120b\",\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}]}'

# bad key -> 401 authentication_error
curl.exe -s localhost:8090/v1/chat/completions -H "Authorization: Bearer nope" `
  -H "Content-Type: application/json" -d '{\"messages\":[]}'

# request bigger than the limit -> 413 request_too_large
curl.exe -s localhost:8090/v1/chat/completions -H "Authorization: Bearer tenant-a-key" `
  -H "Content-Type: application/json" `
  -d '{\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}],\"max_tokens\":60000}'
```


## Troubleshooting

| Symptom | Cause |
|---|---|
| `502 upstream_error: The model service is unavailable` | the router (8090) isn't running |
| `401 Invalid API Key` in Groq's own format | the LLM gateway is still sending to Groq directly; close that terminal (clears `$env:UPSTREAM_URL`) and start it again |
| `python MCP\Customer.py` returns to the prompt straight away | the file is missing its `mcp.run(...)` lines at the bottom |
| `Connection refused` on `localhost` | use `127.0.0.1` instead (Windows can resolve `localhost` to IPv6) |
| `GET /mcp 405` and `DELETE /mcp 405` in the MCP gateway log | expected: the gateway only proxies POST, and the client carries on without those optional calls |
