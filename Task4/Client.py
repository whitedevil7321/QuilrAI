import asyncio
import getpass
import hashlib
import hmac
import logging
import os
import secrets
import sqlite3
import uuid

import groq
from dotenv import load_dotenv

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_groq import ChatGroq
from langchain_mcp_adapters.client import MultiServerMCPClient

from langgraph.graph import StateGraph, START
from langgraph.graph import MessagesState
from langgraph.prebuilt import ToolNode, tools_condition
from langgraph.checkpoint.memory import MemorySaver


# --------------------------------------------------
# Configuration
# --------------------------------------------------

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
)

logger = logging.getLogger(__name__)

logging.getLogger("httpx").setLevel(logging.WARNING)


AUTH_DB = "Username_pass.db"

MCP_GATEWAY_URL = os.getenv(
    "MCP_GATEWAY_URL",
    "http://localhost:3000/mcp"
)

LLM_GATEWAY_URL = os.getenv(
    "LLM_GATEWAY_URL",
    "http://localhost:8080"
)

MCP_TOKEN = os.getenv("MCP_TOKEN", "viewer-token")

# Key for our own gateway. The router checks it and applies the
# per-tenant token limit; the real Groq key stays on the router.
TENANT_API_KEY = os.getenv("TENANT_API_KEY", "tenant-a-key")


SYSTEM = """
You are a customer support assistant.

You can use the MCP tools provided to retrieve customer records
and perform permitted customer support operations.

Customer IDs use the format CUST-00001 (CUST- followed by 5 digits).

Follow the permissions enforced by the MCP gateway.
If a tool returns an error (for example "Unauthorized Tool Call" or
"not found"), explain it to the user in simple words.

Do not invent customer information or claim an operation succeeded
unless the corresponding tool confirms it.
"""


# --------------------------------------------------
# Authentication
# --------------------------------------------------

def create_account(db, email, password):
    """Create a user account with a salted password hash."""

    email = email.strip().lower()

    if not email or not password:
        return "Issue"

    try:
        salt = secrets.token_bytes(16)

        password_hash = hashlib.pbkdf2_hmac(
            "sha256",
            password.encode("utf-8"),
            salt,
            600_000
        )

        db.execute(
            """
            INSERT INTO Username_pass (Email, Salt, Pass)
            VALUES (?, ?, ?)
            """,
            (
                email,
                salt.hex(),
                password_hash.hex()
            )
        )

        db.commit()

        return "Created"

    except sqlite3.IntegrityError:
        return "Exist"

    except sqlite3.Error:
        logger.exception("Account creation failed")
        return "Issue"


def login(db, email, password):
    """Verify user credentials and return email on success."""

    email = email.strip().lower()

    try:
        result = db.execute(
            """
            SELECT Salt, Pass
            FROM Username_pass
            WHERE Email = ?
            """,
            (email,)
        ).fetchone()

        if result is None:
            return None

        salt, stored_hash = result

        password_hash = hashlib.pbkdf2_hmac(
            "sha256",
            password.encode("utf-8"),
            bytes.fromhex(salt),
            600_000
        )

        if hmac.compare_digest(
            password_hash.hex(),
            stored_hash
        ):
            return email

        return None

    except (sqlite3.Error, ValueError):
        logger.exception("Login failed")
        return None


def start():
    """Initialize authentication database and authenticate the user."""

    try:
        db = sqlite3.connect(AUTH_DB)

        db.execute("""
            CREATE TABLE IF NOT EXISTS Username_pass (
                Email TEXT PRIMARY KEY,
                Salt TEXT NOT NULL,
                Pass TEXT NOT NULL
            )
        """)

        db.commit()

    except sqlite3.Error:
        logger.exception("Could not initialize authentication database")
        return None

    try:
        while True:

            print("\n========== ACCOUNT ==========")
            print("1. Login")
            print("2. Sign Up")
            print("3. Exit")

            choice = input("Choose an option: ").strip()

            if choice == "3":
                return None

            elif choice == "2":

                print("\n========== SIGN UP ==========")

                email = input("Enter email: ").strip().lower()
                password = getpass.getpass("Create password: ")

                if len(password) < 8:
                    print("Password must contain at least 8 characters.")
                    continue

                result = create_account(db, email, password)

                if result == "Created":
                    print("Account created successfully.")
                    print("Please log in.")

                elif result == "Exist":
                    print("This email is already registered.")

                else:
                    print("Account creation failed.")

            elif choice == "1":

                print("\n========== LOGIN ==========")

                email = input("Enter email: ").strip().lower()
                password = getpass.getpass("Enter password: ")

                authenticated_email = login(
                    db,
                    email,
                    password
                )

                if authenticated_email:
                    print("Login successful.")
                    return authenticated_email

                print("Invalid email or password.")

            else:
                print("Invalid option. Try again.")

    finally:
        db.close()


# --------------------------------------------------
# MCP + LangGraph application
# --------------------------------------------------

async def main(authenticated_email):

    # ------------------------------------------
    # MCP Gateway
    # ------------------------------------------

    client = MultiServerMCPClient(
        {
            "Service_system": {
                "transport": "streamable_http",
                "url": MCP_GATEWAY_URL,
                "headers": {
                    "Authorization": f"Bearer {MCP_TOKEN}"
                }
            }
        }
    )

    tools = await client.get_tools()

    logger.info(
        "Available MCP tools: %s",
        [tool.name for tool in tools]
    )

    # ------------------------------------------
    # LLM (through LLM gateway -> router -> Groq)
    # ------------------------------------------

    llm = ChatGroq(
        model="openai/gpt-oss-120b",
        api_key=TENANT_API_KEY,
        base_url=LLM_GATEWAY_URL,
        streaming=True,
        max_retries=0   # the router already does failover; don't retry here as well
    )

    llm_with_tools = llm.bind_tools(tools)

    # ------------------------------------------
    # LangGraph
    # ------------------------------------------

    async def tool_calling_llm(state: MessagesState):

        response = await llm_with_tools.ainvoke(
            state["messages"]
        )

        return {
            "messages": [response]
        }

    graph_builder = StateGraph(MessagesState)

    graph_builder.add_node(
        "tool_calling_llm",
        tool_calling_llm
    )

    graph_builder.add_node(
        "tools",
        ToolNode(
            tools,
            handle_tool_errors=True
        )
    )

    graph_builder.add_edge(
        START,
        "tool_calling_llm"
    )

    graph_builder.add_conditional_edges(
        "tool_calling_llm",
        tools_condition
    )

    graph_builder.add_edge(
        "tools",
        "tool_calling_llm"
    )

    graph = graph_builder.compile(
        checkpointer=MemorySaver()
    )

    # A separate conversation ID for this runtime session.
    thread_id = str(uuid.uuid4())

    config = {
        "configurable": {
            "thread_id": thread_id
        }
    }

    first_turn = True

    print(f"\nLogged in as {authenticated_email}. Type 'exit' to quit.")

    # ------------------------------------------
    # Conversation loop
    # ------------------------------------------

    while True:

        user_input = input("\nYou: ").strip()

        if user_input.lower() in {"exit", "quit"}:
            print("Goodbye!")
            break

        if not user_input:
            continue

        messages = []

        if first_turn:
            messages.append(
                SystemMessage(content=SYSTEM)
            )

        messages.append(
            HumanMessage(content=user_input)
        )

        print("\nAssistant: ", end="", flush=True)

        try:

            async for chunk, metadata in graph.astream(
                {"messages": messages},
                config=config,
                stream_mode="messages"
            ):

                if metadata.get("langgraph_node") != "tool_calling_llm":
                    continue

                content = getattr(chunk, "content", "")

                if isinstance(content, str) and content:
                    print(content, end="", flush=True)

                elif isinstance(content, list):
                    for item in content:
                        if isinstance(item, dict) and item.get("type") == "text":
                            print(item.get("text", ""), end="", flush=True)

            print()

            first_turn = False

        except groq.RateLimitError as error:

            wait = error.response.headers.get("retry-after", "a few")

            print(
                f"\nYou have reached the token limit for this minute. "
                f"Please try again in {wait} seconds."
            )

        except groq.APIError as error:

            # Rejected by the gateway or the model provider. The gateway
            # only sends a short, clean message, so it is safe to log.
            logger.warning("LLM request rejected: %s", error)

            print(
                "\nI couldn't process that request. Please check the "
                "details (for example the customer ID format "
                "CUST-00001) and try again."
            )

        except Exception:

            logger.exception("Request execution failed")

            print(
                "\nAn error occurred while processing "
                "your request."
            )


# --------------------------------------------------
# Application entry point
# --------------------------------------------------

if __name__ == "__main__":

    authenticated_email = start()

    if authenticated_email:

        try:
            asyncio.run(
                main(authenticated_email)
            )

        except KeyboardInterrupt:
            print("\nApplication stopped.")

        except Exception:
            logger.exception("Application failed")