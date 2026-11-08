"""A channel and a runtime enough for an agent made from the server's pages
to run on the node: the channel connects and hears nothing, and the runtime
reports one model and is never given a turn."""
# ruff: noqa: ASYNC109

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any, ClassVar
lazy import asyncio
lazy from collections.abc import AsyncIterator
lazy from pathlib import Path

from bazaar_contrib_audit import AUDIT, AuditContext
from bazaar_contrib_channel import (
    CHANNEL,
    CHANNEL_BUILDERS,
    CHANNEL_INPUT,
    ChannelBuilderConfig,
    ChannelConnected,
    ChannelIdentity,
    ChannelInput,
    InboundMessage,
    channel_builder_key,
)
from bazaar_contrib_harness import (
    HARNESS,
    HARNESS_BUILDERS,
    HarnessBuilderConfig,
    HarnessInspection,
    HarnessModel,
    HarnessSession,
    HarnessSessionReconciliation,
    ProviderCallResult,
    ProviderCallStatus,
    harness_builder_key,
)
from bazaar_core import Context, Contrib
from pydantic import BaseModel, ConfigDict, Field


class ChannelConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    bot: str = Field(min_length=1)
    token: str | None = Field(default=None, json_schema_extra={"writeOnly": True})


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
        await self.stopped.wait()
        for message in ():
            yield message

    async def prepare_send(self, request: Any) -> tuple[Any, ...]:
        raise NotImplementedError("nothing is said to this channel")

    async def request_approval(self, request: Any, *, timeout: float) -> Any:
        raise NotImplementedError("nothing is said to this channel")


@asynccontextmanager
async def channel_lifespan(ctx: Context, config: ChannelConfig):
    channel = EndsChannel(ctx.accept(CHANNEL_INPUT), config.bot)
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


class EndsRuntime:
    def __init__(self) -> None:
        self.events: asyncio.Queue[Any] = asyncio.Queue()

    async def receive_event(self) -> Any:
        return await self.events.get()

    async def start_session(
        self, request: Any, *, timeout: float
    ) -> ProviderCallResult[HarnessSession]:
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

    async def start_turn(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError("no message reaches this runtime")

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
    ctx.offer(HARNESS, EndsRuntime())
    yield


runtime = Contrib(config=RuntimeConfig, offers=[HARNESS], lifespan=runtime_lifespan)


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
