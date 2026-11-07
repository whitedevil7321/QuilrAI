from mcp.server.fastmcp import FastMCP
import logging
import sys
import duckdb
from pathlib import Path
from pydantic import (BaseModel, ConfigDict, Field, StrictStr,
                      StringConstraints, ValidationError)
from typing import Annotated
from mcp.server.fastmcp.exceptions import ToolError

mcp = FastMCP("Service_system", host="127.0.0.1", port=8000)

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


# ---------------------------------------------------------------- normal tools (everyone)
@mcp.tool()
def send_user_money(from_user: int, to_user: int, amount: float) -> str:
    """Send money from one user's account to another user's account.

    Args:
        from_user: ID of the user sending the money.
        to_user: ID of the user receiving the money.
        amount: Amount of money to send.
    """
    return f"Sent {amount} from user {from_user} to user {to_user}"


@mcp.tool()
def request_money(from_user: int, to_user: int, amount: float, reason: str = "") -> str:
    """Request money from another user.

    Args:
        from_user: ID of the user who is asked to pay.
        to_user: ID of the user requesting the money.
        amount: Amount of money requested.
        reason: Optional note explaining the request.
    """
    note = f" (reason: {reason})" if reason else ""
    return f"User {to_user} requested {amount} from user {from_user}{note}"




@mcp.tool()
def get_user_record(customer_id: CustomerId) -> dict:
    """Retrieve a user record."""

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




# ---------------------------------------------------------------- admin tools (gateway-protected)
@mcp.tool()
def admin_reset_key(customer_id: int, new_key: str) -> str:
    """Reset a customer's API key to a new value.

    Args:
        customer_id: ID of the customer whose key is reset.
        new_key: The new API key to set.
    """
    return f"API key for customer {customer_id} reset to {new_key}"


@mcp.tool()
def admin_send_money(admin_id: int, to_user: int, amount: float) -> str:
    """Send money from an admin account to a user's account.

    Args:
        admin_id: ID of the admin account sending the money.
        to_user: ID of the user receiving the money.
        amount: Amount of money to send.
    """
    return f"Sent {amount} from admin {admin_id} to user {to_user}"


if __name__ == "__main__":
    mcp.run(transport="streamable-http")