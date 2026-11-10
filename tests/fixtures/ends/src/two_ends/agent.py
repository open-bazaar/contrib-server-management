"""A channel and a runtime for an agent on the node, driven by a test through
files under the node's home: the channel connects, takes the messages left in
its inbox, from people met before or never, and writes down what the agent
sends; the runtime reports one model, makes the calls set for each turn, says
what the turn used, holds the turn while the test says so, and leaves its
session's environment where the test can act as the agent."""
# ruff: noqa: ASYNC109

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any, ClassVar
lazy import asyncio
lazy import json
lazy import os
lazy from collections.abc import AsyncIterator
lazy from pathlib import Path
lazy from time import time_ns

from bazaar_contrib_audit import AUDIT, AuditContext
from bazaar_contrib_channel import (
    CHANNEL,
    CHANNEL_BUILDERS,
    CHANNEL_INPUT,
    ChannelBuilderConfig,
    ChannelConnected,
    ChannelDeliveryReceipt,
    ChannelIdentity,
    ChannelInput,
    ChannelSendAttempt,
    ChannelSendPart,
    ChannelSendRequest,
    ChannelTarget,
    ChannelTargetPresentation,
    InboundAttachment,
    InboundMessage,
    SenderIdentity,
    channel_builder_key,
)
from bazaar_contrib_harness import (
    HARNESS,
    HARNESS_BUILDERS,
    HARNESS_INPUT,
    HarnessBuilderConfig,
    HarnessInspection,
    HarnessModel,
    HarnessOutput,
    HarnessSession,
    HarnessSessionReconciliation,
    HarnessTurn,
    ProviderCallResult,
    ProviderCallStatus,
    SessionInput,
    TokenUsage,
    ToolCall,
    ToolCallCompleted,
    ToolCallStarted,
    TurnCompleted,
    TurnStarted,
    UsageUpdated,
    harness_builder_key,
)
from bazaar_core import Context, Contrib
from pydantic import BaseModel, ConfigDict, Field, TypeAdapter


def ends(agent_id: str) -> Path:
    """Where a test talks to an agent: an inbox of messages, each a JSON file
    named by the message's id; the outbox the agent's sends are written to;
    the calls its next turns make; a hold that keeps turns from ending while
    it is there; and the environment of the agent's session."""
    return Path(os.environ["BCN_HOME"]) / "ends" / agent_id


class ChannelConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    bot: str = Field(min_length=1)
    token: str | None = Field(default=None, json_schema_extra={"writeOnly": True})


@asynccontextmanager
async def _content(path: Path) -> AsyncIterator[AsyncIterator[bytes]]:
    async def chunks() -> AsyncIterator[bytes]:
        yield await asyncio.to_thread(path.read_bytes)

    yield chunks()


def _write(path: Path, line: str) -> None:
    with path.open("a", encoding="utf-8") as file:
        file.write(line + "\n")


class EndsChannel:
    def __init__(self, input: ChannelInput, bot: str) -> None:
        self.input = input
        self.bot = bot
        self.stopped = asyncio.Event()

    def get_identity(self) -> ChannelIdentity:
        return ChannelIdentity(id=f"{self.input.agent_id}-{self.bot}", name=self.bot)

    def backfill_provider_thread_id(
        self, provider_thread_id: str, *, channel: str, channel_identity: str
    ) -> str:
        return provider_thread_id

    def dm_address(self, request: Any) -> None:
        return None

    async def receive(self) -> AsyncIterator[InboundMessage]:
        inbox = ends(self.input.agent_id) / "inbox"
        while not self.stopped.is_set():
            for path in await asyncio.to_thread(lambda: sorted(inbox.glob("*.json"))):
                message = json.loads(await asyncio.to_thread(path.read_text))
                await asyncio.to_thread(path.unlink)
                chat = f"ends:{self.input.agent_id}:{message['chat']}"
                yield InboundMessage(
                    id=path.stem,
                    provider_message_id=path.stem,
                    channel_session_id=chat,
                    thread_id=chat,
                    target=ChannelTarget(kind="dm", thread_id=message["chat"]),
                    target_presentation=ChannelTargetPresentation(
                        handle=message["chat"]
                    ),
                    sender=SenderIdentity(**message["sender"]),
                    body=message["text"],
                    attachments=tuple(
                        InboundAttachment(
                            name=Path(item["path"]).name,
                            source=_content(Path(item["path"])),
                            media_type=item["media_type"],
                        )
                        for item in message["attachments"]
                    ),
                    occurred_at_ms=time_ns() // 1_000_000,
                    mentions_agent=False,
                    metadata={"review": message["review"]},
                )
            try:
                async with asyncio.timeout(0.1):
                    await self.stopped.wait()
            except TimeoutError:
                pass

    async def prepare_send(
        self, request: ChannelSendRequest
    ) -> tuple[ChannelSendPart, ...]:
        outbox = ends(self.input.agent_id) / "outbox.jsonl"

        async def send(attempt: ChannelSendAttempt) -> ChannelDeliveryReceipt:
            await asyncio.to_thread(
                _write,
                outbox,
                json.dumps({"chat": request.target.thread_id, "text": request.body}),
            )
            return ChannelDeliveryReceipt(
                status="confirmed",
                provider_message_id=attempt.id,
                provider_thread_id=request.target.thread_id,
            )

        return (ChannelSendPart(send=send, message=request),)

    async def request_approval(self, request: Any, *, timeout: float) -> Any:
        raise NotImplementedError("this channel asks nothing")


@asynccontextmanager
async def channel_lifespan(ctx: Context, config: ChannelConfig):
    channel = EndsChannel(ctx.accept(CHANNEL_INPUT), config.bot)
    await asyncio.to_thread(
        (ends(channel.input.agent_id) / "inbox").mkdir, parents=True, exist_ok=True
    )
    ctx.offer(CHANNEL, channel)
    await ctx.accept(AUDIT).publish(
        ChannelConnected.event_name,
        context=AuditContext(agent_id=channel.input.agent_id, channel="ends-channel"),
        payload=ChannelConnected(identity=channel.get_identity()).model_dump(
            mode="json"
        ),
    )
    try:
        yield
    finally:
        channel.stopped.set()


channel = Contrib(
    config=ChannelConfig,
    accepts=[CHANNEL_INPUT, AUDIT],
    offers=[CHANNEL],
    lifespan=channel_lifespan,
)


class ChannelBuilderParameters(ChannelBuilderConfig):
    kind: ClassVar[str] = "ends-channel"
    entity_model: ClassVar[type[BaseModel]] = ChannelConfig
    entry: ClassVar[Contrib[Any]] = channel


@asynccontextmanager
async def channel_builder_lifespan(ctx: Context, config: ChannelBuilderParameters):
    with ctx.accept(CHANNEL_BUILDERS).register(config.builder()) as builder:
        ctx.offer(channel_builder_key(builder.kind), builder)
        yield


channel_builder = Contrib(
    config=ChannelBuilderParameters,
    accepts=[CHANNEL_BUILDERS],
    offers=[channel_builder_key("ends-channel")],
    lifespan=channel_builder_lifespan,
)


class RuntimeConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    model: str | None = Field(default=None, json_schema_extra={"x-inspect": "models"})


class EndsTurn:
    """A turn that makes the calls set for it, says what it used, and ends
    once no hold is there."""

    def __init__(self, turn: HarnessTurn, directory: Path) -> None:
        self.turn = turn
        self.directory = directory
        self.said: list[Any] | None = None

    def __aiter__(self) -> EndsTurn:
        return self

    async def __anext__(self) -> HarnessOutput:
        if self.said is None:
            path = self.directory / "calls.json"
            calls = (
                TypeAdapter(list[ToolCall]).validate_json(
                    await asyncio.to_thread(path.read_bytes)
                )
                if await asyncio.to_thread(path.exists)
                else []
            )
            self.said = [
                TurnStarted(event_name="ends.started"),
                *(
                    payload
                    for call in calls
                    for payload in (
                        ToolCallStarted(
                            call=call.model_copy(update={"duration_ms": None})
                        ),
                        ToolCallCompleted(call=call),
                    )
                ),
                UsageUpdated(
                    total=TokenUsage(input_tokens=1200, output_tokens=300),
                    cost_usd=0.01,
                ),
                None,
            ]
        if not self.said:
            raise StopAsyncIteration
        payload = self.said.pop(0)
        if payload is None:
            # the hold is a file another process removes: looked for again
            while True:
                if not await asyncio.to_thread((self.directory / "hold").exists):
                    break
                await asyncio.sleep(0.2)
            payload = TurnCompleted(event_name="ends.completed")
        return HarnessOutput(
            session_id=self.turn.session_id,
            turn_id=self.turn.id,
            payload=payload,
            provider="ends",
        )

    async def aclose(self) -> None:
        self.said = []


class EndsRuntime:
    def __init__(self, agent_id: str) -> None:
        self.agent_id = agent_id
        self.events: asyncio.Queue[Any] = asyncio.Queue()

    async def receive_event(self) -> Any:
        return await self.events.get()

    async def start_session(
        self, request: SessionInput, *, timeout: float
    ) -> ProviderCallResult[HarnessSession]:
        directory = ends(self.agent_id)
        await asyncio.to_thread(directory.mkdir, parents=True, exist_ok=True)
        # written aside and moved in, so the test never reads half of it
        staged = directory / "session.part"
        await asyncio.to_thread(staged.write_text, json.dumps(request.environment))
        await asyncio.to_thread(os.replace, staged, directory / "session.json")
        return ProviderCallResult(
            status=ProviderCallStatus.CONFIRMED,
            value=HarnessSession(id=request.id, provider_thread_id=request.id),
        )

    async def reconcile_session(
        self, session: HarnessSession, *args: Any, **kwargs: Any
    ) -> ProviderCallResult[HarnessSessionReconciliation]:
        return ProviderCallResult(
            status=ProviderCallStatus.CONFIRMED,
            value=HarnessSessionReconciliation(session),
        )

    async def start_turn(
        self, session: HarnessSession, turn: HarnessTurn, *args: Any, **kwargs: Any
    ) -> EndsTurn:
        return EndsTurn(turn, ends(self.agent_id))

    async def steer_turn(self, *args: Any, **kwargs: Any) -> bool:
        return False

    async def has_background_job(self, *args: Any, **kwargs: Any) -> bool:
        return False

    async def stop_session(
        self, session: HarnessSession, *, timeout: float
    ) -> ProviderCallResult[HarnessSession]:
        return ProviderCallResult(status=ProviderCallStatus.CONFIRMED, value=session)


@asynccontextmanager
async def runtime_lifespan(ctx: Context, config: RuntimeConfig):
    del config
    ctx.offer(HARNESS, EndsRuntime(ctx.accept(HARNESS_INPUT).agent_id))
    yield


runtime = Contrib(
    config=RuntimeConfig,
    accepts=[HARNESS_INPUT],
    offers=[HARNESS],
    lifespan=runtime_lifespan,
)


class RuntimeBuilderParameters(HarnessBuilderConfig):
    kind: ClassVar[str] = "ends-runtime"
    entity_model: ClassVar[type[BaseModel]] = RuntimeConfig
    entry: ClassVar[Contrib[Any]] = runtime

    @classmethod
    async def inspect(cls, workspace: Path, *, timeout: float) -> HarnessInspection:
        return HarnessInspection(
            available=True,
            version="1.0.0",
            models=(HarnessModel(id="ends-model", name="Ends Model", default=True),),
        )


@asynccontextmanager
async def runtime_builder_lifespan(ctx: Context, config: RuntimeBuilderParameters):
    with ctx.accept(HARNESS_BUILDERS).register(config.builder()) as builder:
        ctx.offer(harness_builder_key(builder.kind), builder)
        yield


runtime_builder = Contrib(
    config=RuntimeBuilderParameters,
    accepts=[HARNESS_BUILDERS],
    offers=[harness_builder_key("ends-runtime")],
    lifespan=runtime_builder_lifespan,
)
