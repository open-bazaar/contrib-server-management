"""Run the installed host and the published 0.2.3 server, and read what they leave."""

from __future__ import annotations

from contextlib import asynccontextmanager, closing, contextmanager
lazy import asyncio
lazy import os
lazy import shutil
lazy import socket
lazy import sqlite3
lazy import sys
lazy import tomllib
lazy from collections.abc import AsyncIterator, Iterator
lazy from pathlib import Path
lazy from tempfile import TemporaryDirectory
lazy from typing import Any

import tomli_w


@asynccontextmanager
async def host(home: Path) -> AsyncIterator[None]:
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        str(Path(__file__).with_name("host.py")),
        env=dict(os.environ) | {"BCN_HOME": str(home)},
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    assert process.stdin and process.stdout and process.stderr
    try:
        line = await process.stdout.readline()
        if not line:
            raise RuntimeError((await process.stderr.read()).decode())
        yield
    finally:
        if process.returncode is None:
            process.stdin.write(b"stop\n")
            await process.stdin.drain()
            _, stderr = await process.communicate()
            if process.returncode:
                raise RuntimeError(stderr.decode())


def ports(count: int) -> list[int]:
    sockets = [socket.socket() for _ in range(count)]
    try:
        for listener in sockets:
            listener.bind(("127.0.0.1", 0))
        return [listener.getsockname()[1] for listener in sockets]
    finally:
        for listener in sockets:
            listener.close()


async def answers(port: int) -> bytes:
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    writer.write(b"GET / HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n")
    await writer.drain()
    response = await reader.read()
    writer.close()
    await writer.wait_closed()
    return response


async def legacy(home: Path, port: int) -> None:
    """Leave ~/.bcs as the published 0.2.3 server writes it on its first run."""
    environment = home.parent / "bcs-0.2.3"
    python = environment / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    for command in (
        ("uv", "venv", "--python", "3.14", str(environment)),
        ("uv", "pip", "install", "--python", str(python), "bazaar-compute-node==0.2.3"),
    ):
        process = await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        output, _ = await process.communicate()
        assert process.returncode == 0, output.decode()
    data = home / ".bcs"
    await asyncio.to_thread(data.mkdir, parents=True)
    await asyncio.to_thread(
        (data / "config.toml").write_text,
        tomli_w.dumps(
            {
                "listen": f"127.0.0.1:{port}",
                "session_minutes": 25,
                "retention_days": 7,
                "storage": "sqlite",
            }
        ),
    )
    # The interpreter itself, not the launcher Windows puts in front of it, gets stopped.
    process = await asyncio.create_subprocess_exec(
        str(python),
        "-c",
        "import sys; from bazaar_compute_server.cli import main; sys.exit(main(['run']))",
        env=dict(os.environ) | {"HOME": str(home), "USERPROFILE": str(home)},
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )
    assert process.stderr is not None
    try:
        while b"Application startup complete" not in (
            line := await process.stderr.readline()
        ):
            assert line, "bcs run ended before it was ready"
    finally:
        process.terminate()
        await process.wait()


def files(root: Path) -> dict[str, bytes]:
    return {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in root.rglob("*")
        if path.is_file()
    }


def document(home: Path) -> dict[str, Any]:
    return tomllib.loads((home / ".bcn" / "config.toml").read_text(encoding="utf-8"))


def entries(home: Path) -> dict[str, dict[str, Any]]:
    """Each enabled entry's configuration as the host saved it."""
    return {
        entry["entry"]: entry.get("config", {})
        for package in document(home)["contribs"]
        for entry in package.get("enabled", ())
    }


@contextmanager
def reading(path: Path) -> Iterator[sqlite3.Connection]:
    """Open a copy, leaving the database and its write-ahead log as they are."""
    with TemporaryDirectory() as directory:
        for suffix in ("", "-wal"):
            if (source := Path(f"{path}{suffix}")).exists():
                shutil.copyfile(source, Path(directory) / f"{path.name}{suffix}")
        with closing(sqlite3.connect(Path(directory) / path.name)) as connection:
            yield connection


def query(path: Path, sql: str) -> list[tuple[Any, ...]]:
    with reading(path) as connection:
        return [tuple(row) for row in connection.execute(sql)]


def layout(path: Path) -> dict[str, tuple[str, str | None, int | None]]:
    """Each schema object with its definition, and each table with its row count."""
    with reading(path) as connection:
        return {
            name: (
                kind,
                sql,
                connection.execute(f'SELECT count(*) FROM "{name}"').fetchone()[0]
                if kind == "table"
                else None,
            )
            for kind, name, sql in connection.execute(
                "SELECT type, name, sql FROM sqlite_master"
            ).fetchall()
        }


def tables(path: Path) -> set[str]:
    """The database's own tables, without SQLite's internal ones."""
    return {
        name
        for name, (kind, _, _) in layout(path).items()
        if kind == "table" and not name.startswith("sqlite_")
    }
