import asyncio
import logging
import os
import sys

from dotenv import load_dotenv
from pathlib import Path

from langchain_mcp_adapters.client import MultiServerMCPClient
from langchain_groq import ChatGroq

from langgraph.graph import StateGraph, START, MessagesState
from langgraph.prebuilt import ToolNode, tools_condition
from langgraph.checkpoint.memory import MemorySaver


# Load environment variables
load_dotenv()
BASE_DIR = Path(__file__).resolve().parent

SERVER_PATH = BASE_DIR / "MCP" / "Customer.py"


# Configure logging
logging.basicConfig(

    level=logging.WARNING,
    stream=sys.stderr,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    force=True
)
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)


async def main():

    # 1. Connect to MCP server
    client = MultiServerMCPClient({
        "Customer": {
            "command": sys.executable,
            "args": [str(SERVER_PATH)],
            "transport": "stdio",
        }
    })

    # 2. Retrieve MCP tools
    tools = await client.get_tools()

    for tool in tools:
        logger.info("Tools Available: %s", tool.name)

    # 3. Configure Groq LLM
    groq_api_key = os.getenv("GROQ_ENV")

    if not groq_api_key:
        raise ValueError("GROQ_ENV is missing from .env")

    model = ChatGroq(
        model="openai/gpt-oss-120b",
        api_key=groq_api_key
    )

    # 4. Bind MCP tools to LLM
    llm_with_tools = model.bind_tools(tools)

    # 5. Create LLM node
    async def tool_calling_llm(state: MessagesState):

        response = await llm_with_tools.ainvoke(
            state["messages"]
        )

        return {"messages": [response]}

    # 6. Create LangGraph
    builder = StateGraph(MessagesState)

    builder.add_node("tool_calling_llm", tool_calling_llm)
    tool_node = ToolNode(tools)

    async def execute_tools(state: MessagesState):
        logger.info(
            "Tool calls requested: %s",
            state["messages"][-1].tool_calls
        )
        result = await tool_node.ainvoke(state)
        logger.info("MCP tool result: %s", result)
        return result

    builder.add_node("tools", execute_tools)

    # 7. Define graph flow
    builder.add_edge(START, "tool_calling_llm")

    builder.add_conditional_edges(
        "tool_calling_llm",
        tools_condition
    )

    builder.add_edge("tools", "tool_calling_llm")

    # 8. Add conversation memory
    memory = MemorySaver()

    graph = builder.compile(checkpointer=memory)

    # 9. Configure conversation session
    config = {
        "configurable": {
            "thread_id": "customer_session_1"
        }
    }



    while True:

        user_input = input("You: ").strip()

        if user_input.lower() in ["exit", "quit"]:
          
            break

        if not user_input:
            continue

        try:
            result = await graph.ainvoke(
                {
                    "messages": [
                        {
                            "role": "user",
                            "content": user_input
                        }
                    ]
                },
                config=config
            )

            print("\nAssistant:", result["messages"][-1].content)
            print()

        except Exception:
            logger.exception("Error while processing user request")
            print("\nAssistant: Sorry, something went wrong.\n")


if __name__ == "__main__":
    asyncio.run(main())