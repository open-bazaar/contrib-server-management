"""Run a host that has both the node and the server installed."""

from __future__ import annotations

lazy import asyncio
lazy from pathlib import Path
lazy from typing import Any

from bazaar_compute_node.boot import defaults
from bazaar_contrib_server_migrations import MODULE
from hosts import answers, document, host, layout, legacy, ports, query


def configured(payload: dict[str, Any]) -> dict[str, object]:
    return {
        entry["entry"]: entry.get("config", {})
        for package in payload["contribs"]
        for entry in package.get("enabled", ())
        if entry["entry"] != "server-management"
    }


async def test_the_server_beside_the_node_leaves_the_node_as_it_was(tmp_path: Path):
    home = tmp_path / "home"
    (published,) = await asyncio.to_thread(ports, 1)
    await legacy(home, published)
    packages = "SELECT DISTINCT package FROM contrib_schema_migrations"
    async with host(home):
        assert (await answers(published)).startswith(b"HTTP/")
        payload = await asyncio.to_thread(document, home)
        node = await asyncio.to_thread(layout, home / ".bcn" / "bcn.sqlite3")
        ledgers = [
            {package for (package,) in await asyncio.to_thread(query, path, packages)}
            for path in (home / ".bcn" / "bcn.sqlite3", home / ".bcn" / "bcs.sqlite3")
        ]
    assert configured(payload) == configured(await asyncio.to_thread(defaults))
    assert not set(MODULE.tables) & node.keys()
    assert ledgers[0] and not ledgers[0] & ledgers[1]
    assert ledgers[1] == {"bazaar-contrib-server-migrations"}
