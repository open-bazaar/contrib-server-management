"""Run a host that has only the server installed, as new and as 0.2's server."""

from __future__ import annotations

lazy import asyncio
lazy from pathlib import Path

from hosts import answers, entries, host, legacy, ports


async def test_a_new_server_serves_with_the_original_defaults(tmp_path: Path):
    home = tmp_path / "home"
    async with host(home):
        assert (await answers(8765)).startswith(b"HTTP/")
        configured = await asyncio.to_thread(entries, home)
        assert configured["http"]["listen"] == "127.0.0.1:8765"
        assert configured["server-database"]["storage"] == "sqlite"
        assert configured["server-access"]["session_minutes"] == 10


async def test_a_02_server_keeps_its_settings(tmp_path: Path):
    home = tmp_path / "home"
    (published,) = await asyncio.to_thread(ports, 1)
    await legacy(home, published)
    async with host(home):
        assert (await answers(published)).startswith(b"HTTP/")
        configured = await asyncio.to_thread(entries, home)
        assert configured["http"]["listen"] == f"127.0.0.1:{published}"
        assert configured["server-database"]["storage"] == "sqlite"
        assert configured["server-access"]["session_minutes"] == 25
