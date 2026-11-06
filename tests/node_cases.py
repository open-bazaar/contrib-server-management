"""Run a host that has both the node and the server installed."""

from __future__ import annotations

lazy import asyncio
lazy from pathlib import Path
lazy from typing import Any

from bazaar_compute_node.boot import defaults
from hosts import answers, document, host, legacy, ports


def configured(payload: dict[str, Any]) -> dict[str, object]:
    return {
        entry["entry"]: entry.get("config", {})
        for package in payload["contribs"]
        for entry in package.get("enabled", ())
        # the entries that carry over ~/.bcs's settings
        if entry["entry"] not in ("http", "server-database", "server-access")
    }


async def test_the_server_beside_the_node_leaves_the_node_as_it_was(tmp_path: Path):
    home = tmp_path / "home"
    (published,) = await asyncio.to_thread(ports, 1)
    await legacy(home, published)
    async with host(home):
        assert (await answers(published)).startswith(b"HTTP/")
        payload = await asyncio.to_thread(document, home)
    assert configured(payload) == configured(await asyncio.to_thread(defaults))
