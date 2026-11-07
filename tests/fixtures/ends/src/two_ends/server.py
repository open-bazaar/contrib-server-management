"""What a page of the server would do with its computers: call a member of one
with POST /ui/ends/<ref>/<service>/<method> and the arguments as a JSON list,
and read what they reported with GET /ui/ends/reported."""

from __future__ import annotations

from contextlib import asynccontextmanager
lazy import json
lazy from collections.abc import AsyncIterator

from bazaar_contrib_server import SERVER
from bazaar_contrib_server_computer import COMPUTERS, REPORTED, Reported
from bazaar_contrib_sidecar import RemoteError
from bazaar_core import Context, Contrib
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

# what the computers reported, in the order it was announced
reported: list[Reported] = []


@asynccontextmanager
async def lifespan(ctx: Context) -> AsyncIterator[None]:
    server = ctx.accept(SERVER)
    computers = ctx.accept(COMPUTERS)

    async def call(request: Request) -> Response:
        ids = await server.refs.values([request.path_params["ref"]])
        assert ids is not None
        try:
            value = await computers.call(
                ids[0],
                request.path_params["service"],
                request.path_params["method"],
                *json.loads(await request.body()),
            )
        except TimeoutError:
            return JSONResponse({"silent": True}, status_code=504)
        except RemoteError as error:
            return JSONResponse({"failed": error.type}, status_code=502)
        return JSONResponse({"value": value})

    async def heard(request: Request) -> Response:
        del request
        return JSONResponse(
            [
                {"event": item.event.event_name, "payload": item.event.payload}
                for item in reported
            ]
        )

    async with server.module(
        "ends",
        ui=Starlette(
            routes=[
                Route("/reported", heard),
                Route("/{ref}/{service}/{method}", call, methods=["POST"]),
            ]
        ),
    ):
        yield


async def _heard(ctx: Context, value: Reported) -> None:
    del ctx
    reported.append(value)


probe = Contrib(
    accepts=[SERVER, COMPUTERS], lifespan=lifespan, listen={REPORTED: _heard}
)
