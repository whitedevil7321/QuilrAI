import asyncio
import logging
import os
import sys

from dotenv import load_dotenv

from langchain_core.messages import AIMessageChunk, HumanMessage, SystemMessage
from langchain_mcp_adapters.client import MultiServerMCPClient
from langchain_groq import ChatGroq

from langgraph.graph import StateGraph, START, MessagesState
from langgraph.prebuilt import ToolNode, tools_condition
from langgraph.checkpoint.memory import MemorySaver


# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.WARNING,
    stream=sys.stderr,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    force=True,
)
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)

# ----------------------------------------------------------------- gateway settings
MCP_GATEWAY_URL = os.getenv("MCP_GATEWAY_URL", "http://localhost:3000/mcp")  # Task 2: tool auth
LLM_GATEWAY_URL = os.getenv("LLM_GATEWAY_URL", "http://localhost:8080")      # Task 3: PII guardrail
token = os.getenv("MCP_TOKEN", "viewer-token")

SYSTEM = SystemMessage(content=(
    "You are a customer-support assistant with access to tools. "
    "Permissions are enforced by a security gateway, not by you. "
    "If the user asks for an action that a tool can perform, call the tool. "
    "If the tool returns an authorization error, tell the user they are not authorized."
))


async def main():

    # 1. Connect to MCP server THROUGH the MCP gateway (Task 2)
    client = MultiServerMCPClient({
        "Service_system": {
            "url": MCP_GATEWAY_URL,
            "transport": "streamable_http",
            "headers": {"Authorization": f"Bearer {token}"},
        }
    })

    # 2. Retrieve MCP tools
    tools = await client.get_tools()
    for tool in tools:
        logger.info("Tools Available: %s", tool.name)

    # 3. Configure Groq LLM - calls go THROUGH the LLM gateway (Task 3)
    groq_api_key = os.getenv("GROQ_ENV")
    if not groq_api_key:
        raise ValueError("GROQ_ENV is missing from .env")

    model = ChatGroq(
        model="openai/gpt-oss-120b",
        api_key=groq_api_key,          
        base_url=LLM_GATEWAY_URL,     
        streaming=True,           
    )

    # 4. Bind MCP tools to LLM
    llm_with_tools = model.bind_tools(tools)

    # 5. Create LLM node
    async def tool_calling_llm(state: MessagesState):
        response = await llm_with_tools.ainvoke(state["messages"])
        return {"messages": [response]}

    # 6. Create LangGraph
    builder = StateGraph(MessagesState)
    builder.add_node("tool_calling_llm", tool_calling_llm)
    tool_node = ToolNode(tools, handle_tool_errors=True)

    async def execute_tools(state: MessagesState):
        logger.info("Tool calls requested: %s", state["messages"][-1].tool_calls)
        result = await tool_node.ainvoke(state)
        logger.info("MCP tool result: %s", result)
        return result

    builder.add_node("tools", execute_tools)

    # 7. Define graph flow
    builder.add_edge(START, "tool_calling_llm")
    builder.add_conditional_edges("tool_calling_llm", tools_condition)
    builder.add_edge("tools", "tool_calling_llm")

    # 8. Add conversation memory
    graph = builder.compile(checkpointer=MemorySaver())

    # 9. Configure conversation session
    config = {"configurable": {"thread_id": "customer_session_1"}}

    print(f"(role token: {token})")
    first_turn = True

    while True:
        user_input = input("You: ").strip()
        if user_input.lower() in ["exit", "quit"]:
            break
        if not user_input:
            continue

        messages = [HumanMessage(content=user_input)]
        if first_turn:                       # memory keeps it for later turns
            messages.insert(0, SYSTEM)
            first_turn = False

        try:
            print("\nAssistant: ", end="", flush=True)
            # stream_mode="messages" yields LLM tokens as they arrive (already redacted)
            async for chunk, meta in graph.astream(
                {"messages": messages}, config=config, stream_mode="messages"
            ):
                if (isinstance(chunk, AIMessageChunk)
                        and meta.get("langgraph_node") == "tool_calling_llm"
                        and chunk.content):
                    print(chunk.content, end="", flush=True)
            print("\n")

        except Exception:
            logger.exception("Error while processing user request")
            print("\nAssistant: Sorry, something went wrong.\n")


if __name__ == "__main__":
    asyncio.run(main())