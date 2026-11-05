"""Run a host that has only the server installed, as new and as 0.2's server."""

from __future__ import annotations

lazy import asyncio
lazy from pathlib import Path

import tomli_w
from hosts import (
    answers,
    document,
    entries,
    files,
    host,
    layout,
    legacy,
    ports,
    query,
    tables,
)


async def test_a_new_server_serves_with_the_original_defaults(tmp_path: Path):
    home, published = tmp_path / "home", tmp_path / "published"
    (port,) = await asyncio.to_thread(ports, 1)
    await legacy(published, port)
    history = await asyncio.to_thread(tables, published / ".bcs" / "bcs.sqlite3")
    async with host(home):
        assert (await answers(8765)).startswith(b"HTTP/")
        configured = await asyncio.to_thread(entries, home)
        assert configured["http"]["listen"] == "127.0.0.1:8765"
        assert configured["server-database"]["storage"] == "sqlite"
        created = await asyncio.to_thread(tables, home / ".bcn" / "bcs.sqlite3")
    assert "contrib_schema_migrations" in created
    assert not history & created


async def test_a_02_server_moves_into_bcn_once_and_stays_as_it_was(tmp_path: Path):
    home = tmp_path / "home"
    published, moved = await asyncio.to_thread(ports, 2)
    await legacy(home, published)
    original = await asyncio.to_thread(files, home / ".bcs")
    expected = await asyncio.to_thread(layout, home / ".bcs" / "bcs.sqlite3")
    database = home / ".bcn" / "bcs.sqlite3"
    ledger = "SELECT * FROM contrib_schema_migrations"
    async with host(home):
        assert (await answers(published)).startswith(b"HTTP/")
        configured = await asyncio.to_thread(entries, home)
        assert configured["http"]["listen"] == f"127.0.0.1:{published}"
        assert configured["server-database"]["storage"] == "sqlite"
        key = await asyncio.to_thread((home / ".bcn" / "session.key").read_bytes)
        assert key == original["session.key"]
        imported = await asyncio.to_thread(layout, database)
        assert expected.items() <= imported.items()
        history = await asyncio.to_thread(query, database, ledger)
    assert await asyncio.to_thread(files, home / ".bcs") == original

    configured = await asyncio.to_thread(document, home)
    for package in configured["contribs"]:
        for entry in package.get("enabled", ()):
            if entry["entry"] == "http":
                entry["config"]["listen"] = f"127.0.0.1:{moved}"
    await asyncio.to_thread(
        (home / ".bcn" / "config.toml").write_text, tomli_w.dumps(configured)
    )
    async with host(home):
        assert (await answers(moved)).startswith(b"HTTP/")
        assert (await asyncio.to_thread(entries, home))["http"]["listen"] == (
            f"127.0.0.1:{moved}"
        )
        assert await asyncio.to_thread(query, database, ledger) == history
    assert await asyncio.to_thread(files, home / ".bcs") == original
