import os
import logging
import chainlit as cl
from dotenv import load_dotenv
from azure.identity.aio import DefaultAzureCredential
from azure.ai.projects.aio import AIProjectClient
from FinancialData import FinancialData
from stream_event_handler2 import StreamEventHandler2
from terminal_colors import TerminalColors as tc
from utilities import Utilities
from azure.ai.projects.models import (
    Agent,
    AgentThread,
    AsyncFunctionTool,
    AsyncToolSet,
    FileSearchTool,
    CodeInterpreterTool,
)

# Load env vars
load_dotenv()
logging.basicConfig(level=logging.ERROR)

AGENT_NAME = "MITR Support"
DATA_SHEET_FILE = "datasheet/Employee-Handbook.pdf"
FONTS_ZIP = "fonts/fonts.zip"
PROJECT_CONNECTION_STRING = os.environ["PROJECT_CONNECTION_STRING"]
API_DEPLOYMENT_NAME = os.getenv("MODEL_DEPLOYMENT_NAME")
MAX_COMPLETION_TOKENS = 10240
MAX_PROMPT_TOKENS = 20480
TEMPERATURE = 0.1
TOP_P = 0.1
INSTRUCTIONS_FILE = "instructions/code_interpreter.txt"

project_client = AIProjectClient.from_connection_string(
    conn_str=PROJECT_CONNECTION_STRING,
    credential=DefaultAzureCredential(exclude_managed_identity_credential=True)
)

utilities = Utilities()
FinancialData = FinancialData(utilities)


async def setup_agent_and_thread() -> tuple[Agent, AgentThread]:
    try:
        toolset = AsyncToolSet()

        await FinancialData.connect()
        db_schema = await FinancialData.get_database_info()

        instructions = utilities.load_instructions(INSTRUCTIONS_FILE)
        instructions = instructions.replace("{database_schema_string}", db_schema)

        functions = AsyncFunctionTool({FinancialData.async_fetch_data_using_sqlite_query})
        toolset.add(functions)

        # Save functions in user session
        cl.user_session.set("functions", functions)

        # Add vector store
        vs = await utilities.create_vector_store(
            project_client, [DATA_SHEET_FILE], "Contoso Vector"
        )
        toolset.add(FileSearchTool(vector_store_ids=[vs.id]))

        # Add code interpreter
        toolset.add(CodeInterpreterTool())

        agent = await project_client.agents.create_agent(
            name=AGENT_NAME,
            model=API_DEPLOYMENT_NAME,
            instructions=instructions,
            toolset=toolset,
            temperature=TEMPERATURE,
        )

        project_client.agents.enable_auto_function_calls(toolset=toolset)
        thread = await project_client.agents.create_thread()

        return agent, thread

    except Exception as e:
        logging.exception(f"⚠️ Error in setup_agent_and_thread: {e}")
        return None, None
    

@cl.on_chat_start
async def on_chat_start():
    agent, thread = await setup_agent_and_thread()
    if not agent or not thread:
        await cl.Message("🚫 Agent initialization failed. Check logs for details.").send()
        return

    cl.user_session.set("agent", agent)
    cl.user_session.set("thread", thread)
    await cl.Message(f"👋 Welcome! Agent `{agent.name}` is ready.").send()


@cl.on_message
async def on_message(message: cl.Message):
    agent: Agent = cl.user_session.get("agent")
    thread: AgentThread = cl.user_session.get("thread")
    functions = cl.user_session.get("functions")  # get functions from session

    # Create user message in backend
    await project_client.agents.create_message(
        thread_id=thread.id,
        role="user",
        content=message.content,
    )

    # Create the stream of the agent response
    stream = await project_client.agents.create_stream(
        thread_id=thread.id,
        agent_id=agent.id,
        event_handler=StreamEventHandler2(
            functions=functions,
            project_client=project_client,
            utilities=utilities
        ),
        max_completion_tokens=MAX_COMPLETION_TOKENS,
        max_prompt_tokens=MAX_PROMPT_TOKENS,
        temperature=TEMPERATURE,
        top_p=TOP_P,
        instructions=agent.instructions,
    )

    full_response = ""

    async with stream as s:
        async for event in s:
            if hasattr(event, "content"):
                full_response += event.content
                await cl.Message(event.content).send()
