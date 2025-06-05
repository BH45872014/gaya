from typing import Any
import chainlit as cl

from azure.ai.projects.aio import AIProjectClient
from azure.ai.projects.models import (
    AsyncAgentEventHandler,
    AsyncFunctionTool,
    MessageDeltaChunk,
    MessageStatus,
    RunStatus,
    RunStep,
    RunStepDeltaChunk,
    RunStepStatus,
    ThreadMessage,
    ThreadRun,
)

from utilities import Utilities


class StreamEventHandler2(AsyncAgentEventHandler[str]):
    """Handle LLM streaming events and tokens."""

    def __init__(self, functions: AsyncFunctionTool, project_client: AIProjectClient, utilities: Utilities) -> None:
        self.functions = functions
        self.project_client = project_client
        self.util = utilities
        self.msg = None
        super().__init__()





    async def on_message_delta(self, delta: MessageDeltaChunk) -> None:
        token = delta.text
        self.util.log_token_blue(token)

        # If this is the first token, create a streaming message
        if self.msg is None:
            self.msg = cl.Message(content="")
            await self.msg.send()

        # Stream token into the current message
        await self.msg.stream_token(token)

    async def on_done(self) -> None:
        # Mark the message as complete when done
        if self.msg:
            await self.msg.update()
            self.msg = None



    async def on_thread_run(self, run: ThreadRun) -> None:
        """Handle thread run events"""

        if run.status == RunStatus.FAILED:
            print(f"Run failed. Error: {run.last_error}")
            print(f"Thread ID: {run.thread_id}")
            print(f"Run ID: {run.id}")

    async def on_run_step(self, step: RunStep) -> None:
        pass
        # if step.status == RunStepStatus.COMPLETED:
        #     print()
        # self.util.log_msg_purple(f"RunStep type: {step.type}, Status: {step.status}")

    async def on_run_step_delta(self, delta: RunStepDeltaChunk) -> None:
        pass

    async def on_error(self, data: str) -> None:
        print(f"An error occurred: {data}")
        await cl.Message(f"⚠️ Error occurred during streaming: {data}").send()



    async def on_unhandled_event(self, event_type: str, event_data: Any) -> None:
        """Handle unhandled events."""
        # print(f"Unhandled Event Type: {event_type}, Data: {event_data}")
        print(f"Unhandled Event Type: {event_type}")



async def on_thread_message(self, message: ThreadMessage) -> None:
    if message.status == MessageStatus.COMPLETED:
        content_list = message.content or []
        for content in content_list:
            if content["type"] == "text":
                value = content["text"]["value"]
                await cl.Message(value).send()
            elif content["type"] == "image_file":
                image_id = content["image_file"]["file_id"]
                file = await self.project_client.files.get_file(file_id=image_id)
                await cl.Message(content="📊 Generated Pie Chart:", files=[cl.File(name=file.name, path=file.url)]).send()
