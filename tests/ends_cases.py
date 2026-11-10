"""Run the server and a node as their installed hosts, each in a `bcn run` of
its own or both in one, connected the way the server's enrolment page says;
in runs of their own, an agent is made, changed and removed from the server's
pages, and what an agent hears, decides and does shows on them."""

from __future__ import annotations

from contextlib import asynccontextmanager, closing
lazy import asyncio
lazy import html
lazy import inspect
lazy import json
lazy import os
lazy import re
lazy import signal
lazy import sqlite3
lazy import struct
lazy import sys
lazy import tomllib
lazy import zlib
lazy from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
lazy from importlib.metadata import version
lazy from pathlib import Path
lazy from typing import Any
lazy from uuid import uuid7

import httpx
import pytest
import tomli_w
from bazaar_compute_node.boot import defaults
from bazaar_contrib_node_management import defaults as node_defaults
from bazaar_contrib_sidecar import CLIENT, Client
from bazaar_core import Context, Contrib, ContribEntry, ContribHub, ContribSpec, load
from hosts import ports

from bazaar_contrib_server_management import defaults as server_defaults

FIXTURE = Path(__file__).with_name("fixtures") / "ends"


class Host:
    """`bcn run` in a home of its own, its output kept as it comes."""

    def __init__(self, home: Path) -> None:
        self.home = home
        self.output = bytearray()
        self._process: asyncio.subprocess.Process | None = None
        self._reading: asyncio.Task[None] | None = None

    async def start(self) -> None:
        self.output.clear()
        # the interpreter itself, not the launcher Windows puts in front of it
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "bazaar_compute_node",
            "run",
            env=dict(os.environ) | {"BCN_HOME": str(self.home)},
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        assert process.stdout is not None
        self._process = process
        while b"bcn ready" not in self.output:
            line = await process.stdout.readline()
            assert line, self.output.decode(errors="replace")
            self.output += line
        self._reading = asyncio.create_task(self._read(process.stdout))

    async def _read(self, stream: asyncio.StreamReader) -> None:
        while line := await stream.readline():
            self.output += line

    def said(self, pattern: bytes) -> re.Match[bytes] | None:
        return re.search(pattern, self.output)

    async def stop(self) -> None:
        """End the run as Ctrl+C does; Windows has no Ctrl+C for another
        process, so there the process is ended."""
        assert self._process is not None and self._reading is not None
        if os.name == "nt":
            self._process.kill()
        else:
            self._process.send_signal(signal.SIGINT)
        await self._reading
        await self._process.wait()
        if os.name != "nt":
            assert b"Traceback" not in self.output, self.output.decode(errors="replace")

    async def end(self) -> None:
        if self._process is not None and self._process.returncode is None:
            self._process.kill()
            await self._process.wait()


@asynccontextmanager
async def hosts(*homes: Path) -> AsyncIterator[list[Host]]:
    started = [Host(home) for home in homes]
    try:
        yield started
    finally:
        for host in started:
            await host.end()


async def configure(
    home: Path, document: dict[str, Any], port: int, *entries: str
) -> None:
    """The home's configuration as its host would first write it, the server
    on the port and the fixture's entries enabled."""
    for package in document["contribs"]:
        for entry in package.get("enabled", ()):
            if entry["entry"] == "http":
                entry["config"] = {"listen": f"127.0.0.1:{port}"}
    document["contribs"].append(
        {
            "package": f"two-ends @ {FIXTURE.as_uri()}",
            "enabled": [{"entry": entry} for entry in entries],
        }
    )
    path = home / ".bcn" / "config.toml"
    await asyncio.to_thread(path.parent.mkdir, parents=True)
    await asyncio.to_thread(path.write_text, tomli_w.dumps(document))


@asynccontextmanager
async def browser() -> AsyncIterator[httpx.AsyncClient]:
    # building the client loads certificates from disk, which is not the loop's work
    async with await asyncio.to_thread(httpx.AsyncClient, timeout=90) as session:
        yield session


def password(server: Host) -> str:
    """The admin's, which only the server's first run prints."""
    created = server.said(rb"with password (\S+);")
    assert created is not None, server.output.decode(errors="replace")
    return created.group(1).decode()


async def login(session: httpx.AsyncClient, url: str, password: str) -> None:
    await session.post(f"{url}/login/", data={"name": "admin", "password": password})


async def enrol(session: httpx.AsyncClient, url: str, name: str) -> tuple[str, str]:
    """A computer the admin enrols; its ref and its token, as the page showing
    the commands gives them."""
    page = await session.post(f"{url}/ui/computers/", data={"name": name})
    assert page.status_code == 200, (page.status_code, page.text[:3000])
    token = re.search(r"--option token=([^\"&\s]+)\"", page.text)
    ref = re.search(r'hx-get="/ui/computers/(\d+)"', page.text)
    assert token is not None and ref is not None
    return ref.group(1), token.group(1)


async def connect(node: Path, url: str, token: str) -> None:
    """The second command of the enrolment page, the node's host installed."""
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "bazaar_compute_node",
        "contrib",
        "add",
        "bazaar-contrib-starlink",
        "--enable",
        "starlink",
        "--option",
        f"server={url}",
        "--option",
        f"token={token}",
        env=dict(os.environ) | {"BCN_HOME": str(node)},
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    output = (await process.communicate())[0].decode(errors="replace")
    assert process.returncode == 0, output


async def ask(
    session: httpx.AsyncClient, url: str, ref: str, member: str, *args: object
) -> dict[str, Any]:
    """Call the computer's member through the server, as a page would."""
    service, method = member.split(".")
    response = await session.post(
        f"{url}/ui/ends/{ref}/{service}/{method}", json=list(args)
    )
    return response.json()


async def online(session: httpx.AsyncClient, url: str, ref: str, release: str) -> bool:
    """Whether the computer's page shows it online, running the release."""
    page = (await session.get(f"{url}/ui/computers/{ref}")).text
    return 'class="online"' in page and release in page


async def reported(session: httpx.AsyncClient, url: str) -> list[dict[str, Any]]:
    return (await session.get(f"{url}/ui/ends/reported")).json()


def spoken(events: list[dict[str, Any]]) -> list[str]:
    return [
        event["payload"]["word"] for event in events if event["event"] == "ends.spoke"
    ]


async def heard(session: httpx.AsyncClient, url: str, *words: str) -> bool:
    """Whether the server has what the node said."""
    return set(words) <= set(spoken(await reported(session, url)))


async def speak(node: Path, word: str, monkeypatch: pytest.MonkeyPatch) -> None:
    """Have the node say the word, through its sidecar as bcc would."""
    clients: list[Client] = []

    @asynccontextmanager
    async def caller(ctx: Context):
        clients.append(ctx.accept(CLIENT))
        yield

    monkeypatch.setenv("BCN_HOME", str(node))
    composition = await asyncio.to_thread(
        load,
        [
            ContribSpec(contrib="timer-wheel"),
            ContribSpec(
                contrib="sidecar-windows" if sys.platform == "win32" else "sidecar-unix"
            ),
        ],
    )
    composition = composition.model_copy(
        update={
            "entries": (
                *composition.entries,
                ContribEntry(
                    name="caller", definition=Contrib(accepts=[CLIENT], lifespan=caller)
                ),
            )
        }
    )
    async with ContribHub() as hub, hub.enable(composition):
        await clients[0].call("ends", "speak", word)


def hidden(page: str, name: str) -> str:
    """A hidden field of the page's form, as the browser would send it."""
    found = re.search(rf"name=\"{name}\" value='([^']*)'", page) or re.search(
        rf'name="{name}" value="([^"]*)"', page
    )
    assert found, page[:3000]
    return found.group(1)


async def making(session: httpx.AsyncClient, url: str, page: str, **fields: str) -> str:
    """The next step of making an agent; once made, where its page is."""
    answer = await session.post(
        f"{url}/ui/agents/new",
        data={
            "draft": hidden(page, "draft"),
            "step": hidden(page, "step"),
            "opened": hidden(page, "opened"),
            **fields,
        },
    )
    assert answer.status_code in (200, 204), answer.text[:3000]
    return answer.text if answer.status_code == 200 else answer.headers["hx-redirect"]


def agents_on(home: Path) -> list[dict[str, Any]]:
    """The agents the node's configuration holds."""
    document = tomllib.loads(
        (home / ".bcn" / "config.toml").read_text(encoding="utf-8")
    )
    return next(
        entry.get("config", {}).get("agents", [])
        for package in document["contribs"]
        for entry in package.get("enabled", ())
        if entry["entry"] == "agent"
    )


async def until(
    check: Callable[[], Awaitable[object] | object], seconds: float = 120
) -> None:
    """Wait for the check to hold, looking again every tenth of a second."""
    for _ in range(int(seconds * 10)):
        result = check()
        if inspect.isawaitable(result):
            result = await result
        if result:
            return
        await asyncio.sleep(0.1)
    raise TimeoutError(f"not so after {seconds} seconds")


def agent(id: str, name: str) -> dict[str, Any]:
    """An agent on the fixture's channel and runtime, every conversation its own."""
    return {
        "id": id,
        "name": name,
        "mode": "dangerous_individual",
        "channel": [{"kind": "ends-channel", "bot": f"{id}-bot"}],
        "harness": [{"kind": "ends-runtime"}],
    }


def picture(path: Path) -> bytes:
    """A small PNG of one colour, written to the path; its bytes."""

    def chunk(kind: bytes, data: bytes) -> bytes:
        return (
            struct.pack(">I", len(data))
            + kind
            + data
            + struct.pack(">I", zlib.crc32(kind + data))
        )

    rows = b"".join(b"\x00" + b"\x2f\x81\xf7" * 48 for _ in range(32))
    content = (
        b"\x89PNG\r\n\x1a\n"
        + chunk(b"IHDR", struct.pack(">IIBBBBB", 48, 32, 8, 2, 0, 0, 0))
        + chunk(b"IDAT", zlib.compress(rows))
        + chunk(b"IEND", b"")
    )
    path.write_bytes(content)
    return content


async def say(
    node: Path,
    agent_id: str,
    chat: str,
    sender: dict[str, str],
    text: str,
    *,
    review: str = "approved",
    attachments: Sequence[dict[str, str]] = (),
) -> None:
    """Leave a message in the agent's inbox, from someone in a chat of two."""
    inbox = node / "ends" / agent_id / "inbox"
    await asyncio.to_thread(inbox.mkdir, parents=True, exist_ok=True)
    message = {
        "chat": chat,
        "sender": sender,
        "text": text,
        "review": review,
        "attachments": list(attachments),
    }
    # written aside and moved in, so the channel never reads half of it
    staged = inbox / f"{uuid7()}.part"
    await asyncio.to_thread(staged.write_text, json.dumps(message))
    await asyncio.to_thread(staged.rename, staged.with_suffix(".json"))


async def hold(node: Path, agent_id: str, held: bool) -> None:
    """Keep the agent's turns from ending, or let them."""
    directory = node / "ends" / agent_id
    await asyncio.to_thread(directory.mkdir, parents=True, exist_ok=True)
    if held:
        await asyncio.to_thread((directory / "hold").touch)
    else:
        await asyncio.to_thread((directory / "hold").unlink, missing_ok=True)


async def calls(node: Path, agent_id: str, made: Sequence[dict[str, Any]]) -> None:
    """The tool calls the agent's next turns make, as the runtime reports them."""
    directory = node / "ends" / agent_id
    await asyncio.to_thread(directory.mkdir, parents=True, exist_ok=True)
    await asyncio.to_thread((directory / "calls.json").write_text, json.dumps(made))


async def sent(node: Path, agent_id: str) -> list[str]:
    """What the agent sent on its channel, oldest first."""
    outbox = node / "ends" / agent_id / "outbox.jsonl"
    if not await asyncio.to_thread(outbox.exists):
        return []
    lines = (await asyncio.to_thread(outbox.read_text)).splitlines()
    return [json.loads(line)["text"] for line in lines]


async def act(
    session: httpx.AsyncClient,
    url: str,
    ref: str,
    node: Path,
    agent_id: str,
    service: str,
    resource: str,
    command: str,
    **values: Any,
) -> dict[str, Any]:
    """A command the agent runs in its latest session, as bcc runs it, sent
    through the server to the node."""
    environment = json.loads(
        await asyncio.to_thread((node / "ends" / agent_id / "session.json").read_text)
    )
    answer = await ask(
        session,
        url,
        ref,
        f"{service}.request",
        {
            "kind": "command",
            "resource": resource,
            "command": command,
            "agent_id": environment["BCN_AGENT_ID"],
            "actor_id": environment["BCN_ACTOR_ID"],
            "runtime_session_id": environment["BCN_RUNTIME_SESSION_ID"],
            "session_capability": environment["BCN_COMMAND_CAPABILITY"],
            **values,
        },
    )
    return answer["value"]


async def page(session: httpx.AsyncClient, url: str, path: str) -> str:
    answer = await session.get(f"{url}{path}")
    assert answer.status_code == 200, (answer.status_code, answer.text[:3000])
    return answer.text


async def shows(session: httpx.AsyncClient, url: str, path: str, *texts: str) -> bool:
    """Whether the page has all the texts yet."""
    answer = await session.get(f"{url}{path}")
    return answer.status_code == 200 and all(text in answer.text for text in texts)


def opened(listing: str, name: str) -> str:
    """Where a conversation the list shows opens, as its agent's ref and its own."""
    for address, row in re.findall(
        r'<a class="conv" href="/ui/chats/(\d+/\d+)[^"]*"(.*?)</a>',
        listing,
        re.DOTALL,
    ):
        if f'class="tn">{name}<' in row:
            return address
    raise AssertionError(listing[:3000])


async def test_a_server_and_a_node_in_runs_of_their_own(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    server_home, node_home = tmp_path / "server", tmp_path / "node"
    (port,) = await asyncio.to_thread(ports, 1)
    url = f"http://127.0.0.1:{port}"
    release = await asyncio.to_thread(version, "bazaar-compute")
    await configure(
        server_home, await asyncio.to_thread(server_defaults), port, "ends-server"
    )
    await configure(
        node_home,
        await asyncio.to_thread(node_defaults),
        port,
        "ends-node",
        "ends-channel-builder",
        "ends-runtime-builder",
    )
    async with hosts(server_home, node_home) as (server, node), browser() as admin:
        await server.start()
        secret = password(server)
        await login(admin, url, secret)
        ref, token = await enrol(admin, url, "kana-vm")
        await node.start()
        await connect(node_home, url, token)
        await until(lambda: online(admin, url, ref, release))

        assert await ask(admin, url, ref, "node.version") == {"value": release}
        info = (await ask(admin, url, ref, "node.info"))["value"]
        assert info["version"] == release and info["machine"]
        assert "value" in await ask(admin, url, ref, "agent-activity.agents")
        # a member agent-activity keeps to itself
        assert await ask(admin, url, ref, "agent-activity.observe") == {
            "failed": "LookupError"
        }
        assert "value" in await ask(admin, url, ref, "ends.speak", "hello")
        await until(lambda: heard(admin, url, "hello"))

        # an agent made from the pages is written to the node's configuration
        # and runs there
        page = (await admin.get(f"{url}/ui/agents/new")).text
        chosen = re.search(r'name="computer" value="(\d+)"', page)
        assert chosen, page[:3000]
        page = await making(
            admin,
            url,
            page,
            name="kana",
            idle_timeout="0",
            mode="session",
            sandbox="workspace-write",
            computer=chosen.group(1),
            action="go:2",
        )
        page = await making(admin, url, page, action="add:channel:ends-channel")
        page = await making(
            admin,
            url,
            page,
            **{"f.bot": "kana-bot", "s.token": "tok-1", "action": "go:3"},
        )
        page = await making(admin, url, page, action="add:harness:ends-runtime")
        page = await making(admin, url, page, action="go:4")
        made = await making(admin, url, page, action="create")
        assert made.startswith("/ui/agents/"), made
        (kana,) = await asyncio.to_thread(agents_on, node_home)
        assert kana["name"] == "kana"
        assert kana["channel"][0] == {
            "kind": "ends-channel",
            "bot": "kana-bot",
            "token": "tok-1",
        }

        async def running() -> bool:
            activity = (await ask(admin, url, ref, "agent-activity.agents"))["value"]
            known = activity.get(kana["id"])
            return known is not None and known["state"] == "running"

        await until(running)
        workspace = node_home / ".bcn" / "workspaces" / kana["id"]
        await asyncio.to_thread(workspace.mkdir, parents=True, exist_ok=True)
        await asyncio.to_thread((workspace / "notes.md").write_text, "# notes\n")

        # changed with its token left blank, the token stays
        tab = (await admin.get(f"{url}{made}?tab=config")).text
        saved = await admin.post(
            f"{url}{made}/config",
            data={
                "draft": hidden(tab, "draft"),
                "revision": hidden(tab, "revision"),
                "name": "kana two",
                "idle_timeout": "0",
                "mode": "session",
                "sandbox": "workspace-write",
                "action": "save",
            },
        )
        assert saved.status_code == 200, saved.text[:3000]
        (kana,) = await asyncio.to_thread(agents_on, node_home)
        assert kana["name"] == "kana two"
        assert kana["channel"][0]["token"] == "tok-1"

        # removed, its configuration goes and its workspace stays
        removed = await admin.post(f"{url}{made}/remove")
        assert removed.status_code == 204, removed.text[:3000]
        assert await asyncio.to_thread(agents_on, node_home) == []
        assert await asyncio.to_thread((workspace / "notes.md").exists)

        # the node keeps speaking while the server is away
        await server.stop()
        await until(lambda: node.said(rb'"event_name":"starlink.failed"'))
        for word in ("away", "still away"):
            await speak(node_home, word, monkeypatch)
        await server.start()
        await login(admin, url, secret)

        async def caught_up() -> bool:
            events = await reported(admin, url)
            return {"away", "still away"} <= set(spoken(events)) and any(
                event["event"] == "starlink.recovered" for event in events
            )

        await until(caught_up)
        events = await reported(admin, url)
        # each once, however often the node sent it
        assert sorted(spoken(events)) == ["away", "still away"]
        assert any(event["event"] == "starlink.failed" for event in events)
        assert await ask(admin, url, ref, "node.version") == {"value": release}

        await node.stop()
        await server.stop()


async def test_a_server_and_a_node_in_one_run(tmp_path: Path):
    home = tmp_path / "home"
    (port,) = await asyncio.to_thread(ports, 1)
    url = f"http://127.0.0.1:{port}"
    release = await asyncio.to_thread(version, "bazaar-compute")
    # a host with both installed starts from both defaults
    await configure(
        home, await asyncio.to_thread(defaults), port, "ends-server", "ends-node"
    )

    def tables(name: str) -> set[str]:
        (path,) = home.rglob(name)
        with closing(sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True)) as database:
            return {
                row[0]
                for row in database.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                )
            }

    async with hosts(home) as (host,), browser() as admin:
        await host.start()
        await login(admin, url, password(host))
        ref, token = await enrol(admin, url, "kana-vm")
        await connect(home, url, token)
        await until(lambda: online(admin, url, ref, release))
        assert "value" in await ask(admin, url, ref, "ends.speak", "together")
        await until(lambda: heard(admin, url, "together"))
        # the server keeps its computers in its own database, apart from the node's
        kept = await asyncio.to_thread(tables, "bcs.sqlite3")
        apart = await asyncio.to_thread(tables, "bcn.sqlite3")
        assert any("computers" in name for name in kept)
        assert not any("computers" in name for name in apart)
        await host.stop()


KANA = agent("kana", "有马佳奈")
YU = {"id": "u-yu", "name": "yuchanns", "display_name": "yuchanns"}
LI = {"id": "u-li", "name": "lilei", "display_name": "李雷"}


async def test_what_an_agent_hears_decides_and_does_shows_on_the_server(
    tmp_path: Path,
):
    server_home, node_home = tmp_path / "server", tmp_path / "node"
    (port,) = await asyncio.to_thread(ports, 1)
    url = f"http://127.0.0.1:{port}"
    release = await asyncio.to_thread(version, "bazaar-compute")
    await configure(
        server_home, await asyncio.to_thread(server_defaults), port, "ends-server"
    )
    document = await asyncio.to_thread(node_defaults)
    for package in document["contribs"]:
        for entry in package.get("enabled", ()):
            if entry["entry"] == "agent":
                entry["config"]["agents"] = [KANA]
    await configure(
        node_home,
        document,
        port,
        "ends-node",
        "ends-channel-builder",
        "ends-runtime-builder",
    )
    content = await asyncio.to_thread(picture, tmp_path / "schedule.png")
    async with hosts(server_home, node_home) as (server, node), browser() as admin:
        await server.start()
        secret = password(server)
        await login(admin, url, secret)
        ref, token = await enrol(admin, url, "kana-vm")
        await node.start()
        await connect(node_home, url, token)
        await until(lambda: online(admin, url, ref, release))

        # a first contact waits for a decision on the conversations page and on
        # the overview; the node is given its side of the conversations the
        # first time they are read
        await say(
            node_home, "kana", "lilei", LI, "你好，想请你看看排期", review="pending"
        )
        await until(lambda: shows(admin, url, "/ui/chats/?tab=contacts", "李雷"), 300)
        await until(lambda: shows(admin, url, "/ui/chats/attention", "李雷"))
        address = opened(await page(admin, url, "/ui/chats/?tab=contacts"), "李雷")
        let_in = await admin.post(
            f"{url}/ui/chats/{address}/review?tab=contacts",
            data={"decision": "approved", "on": "page"},
        )
        assert let_in.status_code == 204, let_in.text[:3000]

        # let in, the picture that comes next is read from the node, and a turn
        # of the agent runs a command and replies
        await hold(node_home, "kana", True)
        await calls(
            node_home,
            "kana",
            [
                {
                    "call_id": "c-1",
                    "name": "Bash",
                    "duration_ms": 800,
                    "detail": {
                        "kind": "command",
                        "command": "git fetch",
                        "exit_code": 0,
                    },
                }
            ],
        )
        await say(
            node_home,
            "kana",
            "lilei",
            LI,
            "排期在图里，能对上吗？",
            attachments=[
                {"path": str(tmp_path / "schedule.png"), "media_type": "image/png"}
            ],
        )
        await until(
            lambda: shows(admin, url, f"/ui/chats/{address}", "/ui/chats/files/")
        )
        image = re.search(
            r'<img src="(/ui/chats/files/[^"]+)"',
            await page(admin, url, f"/ui/chats/{address}"),
        )
        assert image is not None
        fetched = await admin.get(f"{url}{html.unescape(image.group(1))}")
        assert fetched.status_code == 200 and fetched.content == content
        heard: list[dict[str, Any]] = []

        async def checked() -> bool:
            session = node_home / "ends" / "kana" / "session.json"
            if not await asyncio.to_thread(session.exists):
                return False
            found = await act(
                admin, url, ref, node_home, "kana", "messages", "message", "check"
            )
            heard.extend(found["result"]["messages"])
            return bool(heard)

        await until(checked)
        replied = await act(
            admin,
            url,
            ref,
            node_home,
            "kana",
            "messages",
            "message",
            "send",
            target=heard[-1]["target"],
            body="能对上，按排期来",
        )
        assert replied["ok"], replied
        await hold(node_home, "kana", False)

        async def answered() -> bool:
            return "能对上，按排期来" in await sent(node_home, "kana")

        await until(answered)

        # the turn, its command, what it used and the reply show in the
        # agent's activity, its whole record, the market's feed and the overview
        kana = address.split("/")[0]
        await until(
            lambda: shows(
                admin,
                url,
                f"/ui/activity/log/{kana}",
                "Started on",
                "git fetch",
                "Replied in",
                "Turn done",
            )
        )
        await until(
            lambda: shows(admin, url, f"/ui/activity/today/{kana}", "DM with 李雷")
        )
        await until(lambda: shows(admin, url, f"/ui/activity/usage/{kana}", "$0.0"))
        await until(
            lambda: shows(
                admin,
                url,
                "/ui/activity/feed",
                "started on",
                "replied",
                "ran 1 commands",
            )
        )
        await until(lambda: shows(admin, url, "/ui/usage/", "k tokens", "$0.0"))

        # turned away again, the node holds the conversation denied
        listed = await ask(
            admin, url, ref, "conversation.conversations", "kana", "approved", 0, 50
        )
        (thread_id,) = [item["thread_id"] for item in listed["value"]["conversations"]]
        revoked = await admin.post(
            f"{url}/ui/chats/{address}/review",
            data={"decision": "denied", "on": "page"},
        )
        assert revoked.status_code == 204, revoked.text[:3000]
        held = await ask(
            admin, url, ref, "conversation.conversation", "kana", thread_id
        )
        assert held["value"]["conversation"]["review"] == "denied"

        # a turn while the server is away comes in once it is back, and what
        # was recorded before stays recorded once
        turns = (await page(admin, url, f"/ui/activity/log/{kana}")).count("Started on")
        await server.stop()
        await until(lambda: node.said(rb'"event_name":"starlink.failed"'))
        done = len(re.findall(rb'"event_name":"harness.completed"', node.output))
        await calls(node_home, "kana", [])
        await say(node_home, "kana", "yuchanns", YU, "晚上开会吗？")
        await until(
            lambda: (
                len(re.findall(rb'"event_name":"harness.completed"', node.output))
                > done
            )
        )
        await server.start()
        await login(admin, url, secret)

        async def recovered() -> bool:
            events = await reported(admin, url)
            return any(event["event"] == "starlink.recovered" for event in events)

        await until(recovered)

        async def counted() -> int:
            record = await page(admin, url, f"/ui/activity/log/{kana}")
            return record.count("Started on")

        async def caught_up() -> bool:
            return await counted() > turns

        await until(caught_up)
        assert await counted() == turns + 1

        # stopped, the computer leaves the market's feed a minute later
        await node.stop()
        await until(
            lambda: shows(admin, url, "/ui/activity/feed", "kana-vm", "disconnected"),
            180,
        )
        await server.stop()
