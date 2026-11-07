import logging
import sys
from pathlib import Path
from typing import Annotated

import duckdb
from mcp import types
from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.exceptions import ToolError
from mcp.shared.exceptions import McpError
from pydantic import (BaseModel, ConfigDict, Field, StrictStr,
                      StringConstraints, ValidationError)


BASE_DIR = Path(__file__).resolve().parent.parent
data = BASE_DIR / "data.csv"

logging.basicConfig(
    level=logging.WARNING,
    stream=sys.stderr,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    force=True
)
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)


mcp = FastMCP("Customer")


# Validation rules, defined once and used by both the models and the tools
CustomerId = Annotated[StrictStr, Field(
    pattern=r"^CUST-[0-9]{5}$",
    description="Required customer ID: CUST-XXXXX"
)]
Amount = Annotated[float, Field(
    strict=True,
    gt=0,
    le=10_000,
    allow_inf_nan=False,
    description="Positive refund amount, max 10000"
)]
Reason = Annotated[str, StringConstraints(
    strict=True,
    strip_whitespace=True,
    min_length=11,
    max_length=500
)]


class CustomerInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    customer_id: CustomerId


class RefundInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    customer_id: CustomerId
    amount: Amount
    reason: Reason


@mcp.tool()
def get_customer_record(customer_id: CustomerId) -> dict:
    """Retrieve a customer record."""

    if not data.is_file():
        logger.error("CSV file not found: %s", data)
        raise ToolError("Customer dataset unavailable")

    try:
        with duckdb.connect() as con:
            result = con.execute(
                """
                SELECT *
                FROM read_csv_auto(?, all_varchar = true)
                WHERE customer_id = ?
                """,
                [str(data), customer_id]
            )
            # Read the results while the connection is still open
            columns = [col[0] for col in result.description]
            rows = result.fetchall()
    except Exception:
        logger.exception("Customer lookup failed")
        raise ToolError("Unable to retrieve customer data")

    if not rows:
        raise ToolError(f"Customer {customer_id} not found")

    records = [
        dict(zip(columns, row))
        for row in rows
    ]

    return {"records": records}


@mcp.tool()
def trigger_refund(customer_id: CustomerId, amount: Amount, reason: Reason) -> dict:
    """Validate a refund request without processing payment."""

    return {
        "status": "not_implemented",
        "message": "Refund validated but not processed",
        "customer_id": customer_id,
        "amount": amount,
        "reason": reason
    }


# Which tools exist, and which model checks each one's arguments
SCHEMAS = {
    "get_customer_record": CustomerInput,
    "trigger_refund": RefundInput,
}

# Save FastMCP's original tools/call handler so we can call it later
_original = mcp._mcp_server.request_handlers[types.CallToolRequest]


async def checked_call_tool(req):
    model = SCHEMAS.get(req.params.name)

    # Unknown tool -> JSON-RPC -32602
    if model is None:
        raise McpError(types.ErrorData(
            code=types.INVALID_PARAMS,
            message=f"Unknown tool: {req.params.name}",
        ))

    # Bad arguments -> JSON-RPC -32602, with Pydantic's explanation
    try:
        model.model_validate(req.params.arguments or {})
    except ValidationError as e:
        raise McpError(types.ErrorData(
            code=types.INVALID_PARAMS,
            message=str(e),
        ))

    # Everything is valid -> let FastMCP run the tool normally
    return await _original(req)


# Install the wrapper: every tools/call request now goes through it first
mcp._mcp_server.request_handlers[types.CallToolRequest] = checked_call_tool


if __name__ == "__main__":
    mcp.run(transport="stdio")