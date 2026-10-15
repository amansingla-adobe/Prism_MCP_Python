import os
import sys
import json
import asyncio
from dotenv import load_dotenv
from anthropic import AnthropicBedrock
from fastmcp import Client
from fastmcp.client.transports import StreamableHttpTransport
from llm.narrator import narrate

MCP_SERVER_URL = "https://14257-prismmcpserver-stage.adobeioruntime.net/api/v1/web/prism-mcp-server/mcp-server"
# MCP_API_KEY = os.getenv("MCP_SERVICE_API_KEY")  # the same value you put in the App Builder project's .env

load_dotenv()

llm_client = AnthropicBedrock(
    api_key=os.getenv("BEDROCK_API_KEY"),
    aws_region=os.getenv("AWS_REGION"),
)
MODEL_ID = os.getenv("BEDROCK_MODEL_ID")

SERVER_SCRIPT = "server.py"


def mcp_tool_to_anthropic_tool(mcp_tool):
    """Convert an MCP tool definition into the shape Claude's tool-use API expects."""
    return {
        "name": mcp_tool.name,
        "description": mcp_tool.description or "",
        "input_schema": mcp_tool.inputSchema,
    }


def extract_tool_result(tool_result):
    """Different fastmcp versions shape results slightly differently - handle all of them."""
    if getattr(tool_result, "data", None) is not None:
        return tool_result.data
    if getattr(tool_result, "structured_content", None) is not None:
        return tool_result.structured_content
    if hasattr(tool_result, "content"):
        text_parts = [b.text for b in tool_result.content if hasattr(b, "text")]
        combined = "\n".join(text_parts)
        try:
            return json.loads(combined)
        except json.JSONDecodeError:
            return combined
    return str(tool_result)


async def ask(question: str):
    transport = StreamableHttpTransport(
        url=MCP_SERVER_URL,
        # headers={"Authorization": f"Bearer {MCP_API_KEY}"}
    )
    mcp_client = Client(transport)

    async with mcp_client:
        mcp_tools = await mcp_client.list_tools()
        anthropic_tools = [mcp_tool_to_anthropic_tool(t) for t in mcp_tools]

        print("\n[Tools Claude can see]")
        for t in anthropic_tools:
            print(f"  - {t['name']}")

        messages = [{"role": "user", "content": question}]

        response = llm_client.messages.create(
            model=MODEL_ID,
            max_tokens=500,
            tools=anthropic_tools,
            messages=messages,
        )

        print("\n[Claude's decision - stop_reason]:", response.stop_reason)

        if response.stop_reason != "tool_use":
            final_text = "".join(b.text for b in response.content if b.type == "text")
            print("\n[Claude answered directly - no tool needed]")
            print(final_text)
            return

        tool_use_block = next(b for b in response.content if b.type == "tool_use")
        print(f"\n[Claude chose to call]: {tool_use_block.name}")
        print(f"[With arguments]: {json.dumps(tool_use_block.input, indent=2)}")

        tool_result = await mcp_client.call_tool(tool_use_block.name, tool_use_block.input)
        result_data = extract_tool_result(tool_result)

        print("\n[Real tool result returned]")
        print(json.dumps(result_data, indent=2, default=str))

        # messages.append({"role": "assistant", "content": response.content})
        # messages.append({
        #     "role": "user",
        #     "content": [{
        #         "type": "tool_result",
        #         "tool_use_id": tool_use_block.id,
        #         "content": json.dumps(result_data, default=str),
        #     }],
        # })

        # final_response = llm_client.messages.create(
        #     model=MODEL_ID,
        #     max_tokens=500,
        #     tools=anthropic_tools,
        #     messages=messages,
        # )
        

        # final_text = "".join(b.text for b in final_response.content if b.type == "text")
        # print("\n[Final narrated answer]")
        # print(final_text)
        answer = narrate(question, result_data)
        print("\n[Final narrated answer]")
        print(answer)

if __name__ == "__main__":
    question = " ".join(sys.argv[1:]) or "Are there open guardrail or governance breaches in xlg-prod?"
    asyncio.run(ask(question))