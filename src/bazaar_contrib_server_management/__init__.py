"""Provide the server's default configuration."""

from __future__ import annotations

lazy import os
lazy import tomllib
lazy from importlib.resources import files
lazy from pathlib import Path
lazy from typing import Any

__all__ = ["defaults"]


def defaults() -> dict[str, Any]:
    """The configuration a new server starts with, keeping the settings ~/.bcs has."""
    document = tomllib.loads(files(__package__).joinpath("defaults.toml").read_text())
    path = Path(os.environ.get("BCN_HOME") or Path.home()) / ".bcs" / "config.toml"
    if not path.exists():
        return document
    payload = tomllib.loads(path.read_text(encoding="utf-8"))
    storage = payload.get("storage", "sqlite")
    settings = {
        "http": {"listen": payload["listen"]} if "listen" in payload else {},
        # 0.2 read a storage's own settings from a table named after it
        "server-database": {
            "storage": storage,
            "storage_options": payload.get(storage, {}),
        },
        "server-access": {"session_minutes": payload["session_minutes"]}
        if "session_minutes" in payload
        else {},
    }
    for package in document["contribs"]:
        for entry in package.get("enabled", ()):
            entry.setdefault("config", {}).update(settings.get(entry["entry"], {}))
    return document
