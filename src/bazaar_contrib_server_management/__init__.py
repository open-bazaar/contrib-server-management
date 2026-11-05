"""Provide the server's default configuration and assemble its composition."""

from __future__ import annotations

from contextlib import asynccontextmanager
lazy import asyncio
lazy import os
lazy import shutil
lazy import sqlite3
lazy import tomllib
lazy from contextlib import closing
lazy from copy import deepcopy
lazy from importlib.resources import files
lazy from pathlib import Path
lazy from tempfile import TemporaryDirectory
lazy from typing import Any

from bazaar_contrib_config import CONFIGURATION
from bazaar_contrib_http import HTTP
from bazaar_contrib_schema import SCHEMA
from bazaar_contrib_server_migrations import SERVER_MIGRATIONS
from bazaar_contrib_storage import DB, STORAGE, backend_key
from bazaar_core import Contrib
from pydantic import BaseModel, ConfigDict, Field
lazy from bazaar_contrib_config import ConfigDocument
lazy from bazaar_core import Context, ContribSpec, load

__all__ = ["ServerConfig", "defaults", "server_management"]

DATABASE = "bcs.sqlite3"


class ServerConfig(BaseModel):
    """The settings of 0.2's ~/.bcs/config.toml, with the same defaults."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    listen: str = "127.0.0.1:8765"
    session_minutes: int = Field(default=10, ge=1, strict=True)
    retention_days: int = Field(default=30, ge=1, strict=True)
    storage: str = Field(default="sqlite", min_length=1)
    storage_options: dict[str, Any] = Field(default_factory=dict)


def defaults() -> dict[str, Any]:
    """The configuration a new server starts with."""
    return tomllib.loads(files(__package__).joinpath("defaults.toml").read_text())


def _settings(path: Path) -> ServerConfig:
    """Read 0.2's settings, where a storage's own live in a table named after it."""
    payload = tomllib.loads(path.read_text(encoding="utf-8"))
    return ServerConfig.model_validate(
        {name: payload[name] for name in ServerConfig.model_fields if name in payload}
        | {"storage_options": payload.get(payload.get("storage", "sqlite"), {})}
    )


def _copy(source: Path, target: Path) -> None:
    """Copy the signing key, then the database, reading the original files only."""
    target.mkdir(parents=True, exist_ok=True)
    key = os.open(target / "session.key", os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(key, "wb") as handle:
        handle.write((source / "session.key").read_bytes())
        handle.flush()
        os.fsync(handle.fileno())
    with TemporaryDirectory(prefix=".bcs-import-", dir=target) as directory:
        staged = Path(directory) / DATABASE
        for suffix in ("", "-wal"):
            if (path := source / f"{DATABASE}{suffix}").exists():
                shutil.copyfile(path, f"{staged}{suffix}")
        imported = Path(directory) / f"imported-{DATABASE}"
        with (
            closing(sqlite3.connect(staged)) as original,
            closing(sqlite3.connect(imported)) as copy,
        ):
            original.backup(copy)
        os.replace(imported, target / DATABASE)


@asynccontextmanager
async def lifespan(ctx: Context, config: ServerConfig):
    home = Path(os.environ.get("BCN_HOME") or await asyncio.to_thread(Path.home))
    source, target = home / ".bcs", home / ".bcn"
    # The database comes over last, so an interrupted import runs again.
    if await asyncio.to_thread(source.is_dir) and not await asyncio.to_thread(
        (target / DATABASE).exists
    ):
        config = await asyncio.to_thread(_settings, source / "config.toml")
        configuration = ctx.accept(CONFIGURATION)
        candidate, snapshot = await configuration.read()
        payload = deepcopy(candidate.payload)
        document = ConfigDocument.model_validate(payload)
        for index, package in enumerate(document.contribs):
            if not package.applies:
                continue
            for position, entry in enumerate(package.enabled):
                if entry.entry == ctx.name:
                    payload["contribs"][index]["enabled"][position]["config"] = (
                        config.model_dump(mode="json")
                    )
        await configuration.commit(
            payload,
            expected_revision=snapshot.revision if snapshot is not None else None,
        )
        await asyncio.to_thread(_copy, source, target)
    backend = config.storage_options | (
        {"name": DATABASE} if config.storage == "sqlite" else {}
    )
    composition = await asyncio.to_thread(
        load,
        [
            ContribSpec(contrib="storage"),
            ContribSpec(contrib=config.storage, config=backend),
            ContribSpec(contrib="database", config={"backend": config.storage}),
            ContribSpec(contrib="schema"),
            ContribSpec(contrib="server-migrations"),
            ContribSpec(contrib="http", config={"listen": config.listen}),
        ],
    )
    async with ctx.enable(
        composition,
        local=(
            STORAGE,
            backend_key(config.storage),
            DB,
            SCHEMA,
            SERVER_MIGRATIONS,
            HTTP,
        ),
    ):
        yield


server_management = Contrib(
    config=ServerConfig, accepts=[CONFIGURATION], lifespan=lifespan
)
