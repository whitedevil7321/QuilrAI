from mcp.server.fastmcp import FastMCP

mcp = FastMCP("Service_system", host="127.0.0.1", port=8000)


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