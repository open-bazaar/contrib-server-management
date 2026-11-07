"""A member the node exposes that says a word into its audit."""

from __future__ import annotations

from contextlib import asynccontextmanager
lazy from collections.abc import AsyncIterator

from bazaar_contrib_audit import AUDIT, Audit, AuditContext
from bazaar_contrib_sidecar import RemoteEntry
from bazaar_core import Context, Contrib, ServiceKey


class Speaker:
    def __init__(self, audit: Audit) -> None:
        self._audit = audit

    async def speak(self, word: str) -> None:
        await self._audit.publish(
            "ends.spoke", context=AuditContext(agent_id=""), payload={"word": word}
        )


SPEAKER = ServiceKey[Speaker]("ends.speaker")


@asynccontextmanager
async def lifespan(ctx: Context) -> AsyncIterator[None]:
    ctx.offer(SPEAKER, Speaker(ctx.accept(AUDIT)))
    yield


speaker = Contrib(accepts=[AUDIT], offers=[SPEAKER], lifespan=lifespan)
remote = RemoteEntry(SPEAKER, ("speak",))
