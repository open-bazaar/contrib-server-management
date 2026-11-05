"""Exercise the package through the host's defaults in isolated installations."""

from __future__ import annotations

lazy import asyncio
lazy import os
lazy from pathlib import Path


async def test_installed_server_runs_alone_and_beside_the_node(tmp_path: Path):
    root = (await asyncio.to_thread(Path(__file__).resolve)).parents[1]
    output = tmp_path / "distributions"
    environment = dict(os.environ) | {
        "UV_TOOL_DIR": str(tmp_path / "tools"),
        "UV_TOOL_BIN_DIR": str(tmp_path / "bin"),
        "UV_CACHE_DIR": str(tmp_path / "cache"),
    }
    process = await asyncio.create_subprocess_exec(
        "uv",
        "build",
        str(root),
        "--out-dir",
        str(output),
        cwd=tmp_path,
        env=environment,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    stdout, _ = await process.communicate()
    assert process.returncode == 0, stdout.decode()
    for index, (suffix, host, cases) in enumerate(
        (
            ("*.whl", "bazaar-compute~=0.3.0rc17", "server_cases.py"),
            ("*.tar.gz", "bazaar-compute~=0.3.0rc17", "server_cases.py"),
            ("*.whl", "bazaar-compute[node]~=0.3.0rc17", "node_cases.py"),
        )
    ):
        (artifact,) = await asyncio.to_thread(list, output.glob(suffix))
        prefix = tmp_path / f"environment-{index}"
        python = prefix / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
        # The host's manager installs the declared packages into this interpreter.
        for command in (
            ("uv", "venv", "--python", "3.15", str(prefix)),
            (
                "uv",
                "pip",
                "install",
                "--python",
                str(python),
                str(artifact),
                host,
                "pytest",
                "pytest-asyncio",
                "blockbuster>=1.5.29,<1.6",
            ),
            (
                str(python),
                str(root / "scripts" / "ci_pytest.py"),
                "-c",
                str(root / "pyproject.toml"),
                "--confcutdir",
                str(root / "tests"),
                "-q",
                str(root / "tests" / cases),
            ),
        ):
            process = await asyncio.create_subprocess_exec(
                *command,
                cwd=tmp_path,
                env=environment
                | {"PATH": str(python.parent) + os.pathsep + environment["PATH"]},
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
            stdout, _ = await process.communicate()
            assert process.returncode == 0, stdout.decode()
