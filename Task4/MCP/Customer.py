from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.exceptions import ToolError
import logging
import re
import sys
from pathlib import Path

import duckdb

mcp = FastMCP("Service_system", host="127.0.0.1", port=8000)

BASE_DIR = Path(__file__).resolve().parent.parent
data = BASE_DIR / "data.csv"

logging.basicConfig(
    level=logging.WARNING,
    stream=sys.stderr,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    force=True,
)
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)


# ---------------------------------------------------------------- customer ID helper
# The format is checked INSIDE the tools (not in the tool schema), so a wrong ID
# becomes a normal tool error the LLM can explain, instead of Groq rejecting the
# whole request.
CUSTOMER_ID_DIGITS = 5                                   # CUST-00001
CUSTOMER_ID_FORMAT = re.compile(rf"^CUST-[0-9]{{{CUSTOMER_ID_DIGITS}}}$")


def clean_customer_id(customer_id: str) -> str:
    """Normalise and validate a customer ID.

    "cust-1", "CUST-0001", " CUST-00001 "  ->  "CUST-00001"
    Anything else raises a ToolError with a helpful message.
    """
    value = str(customer_id).strip().upper()
    prefix, _, number = value.partition("-")
    if prefix == "CUST" and number.isdigit() and len(number) <= CUSTOMER_ID_DIGITS:
        value = f"CUST-{number.zfill(CUSTOMER_ID_DIGITS)}"

    if not CUSTOMER_ID_FORMAT.match(value):
        raise ToolError(
            f"Invalid customer ID '{customer_id}'. "
            f"Use CUST- followed by {CUSTOMER_ID_DIGITS} digits, e.g. CUST-00001."
        )
    return value



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
def get_user_record(customer_id: str) -> dict:
    """Retrieve a customer's record.

    Args:
        customer_id: Customer ID in the format CUST-XXXXX (5 digits), for example CUST-00001.
    """
    customer_id = clean_customer_id(customer_id)

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
                [str(data), customer_id],
            )
            columns = [col[0] for col in result.description]
            rows = result.fetchall()
    except Exception:
        logger.exception("Customer lookup failed")
        raise ToolError("Unable to retrieve customer data")

    if not rows:
        raise ToolError(f"Customer {customer_id} not found")

    return {"records": [dict(zip(columns, row)) for row in rows]}


# ---------------------------------------------------------------- admin tools (gateway-protected)
@mcp.tool()
def admin_reset_key(customer_id: str, new_key: str) -> str:
    """Reset a customer's API key to a new value.

    Args:
        customer_id: Customer ID in the format CUST-XXXXX (5 digits), for example CUST-00001.
        new_key: The new API key to set.
    """
    customer_id = clean_customer_id(customer_id)
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