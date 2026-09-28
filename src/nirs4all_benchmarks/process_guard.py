"""Bounded subprocess capture with process-group cleanup."""

from __future__ import annotations

import math
import os
import selectors
import signal
import subprocess
import tempfile
import time
from collections.abc import Mapping
from contextlib import suppress
from pathlib import Path


class OutputLimitExceeded(ValueError):
    """A child exceeded its combined stdout and stderr budget."""


def _stop_group(process: subprocess.Popen[bytes]) -> None:
    try:
        if os.name == "posix":
            os.killpg(process.pid, signal.SIGKILL)
        elif process.poll() is None:
            process.kill()
    except ProcessLookupError:
        pass
    process.wait()


def run_bounded(
    argv: list[str],
    *,
    input_text: str,
    env: Mapping[str, str],
    timeout: float,
    max_output_bytes: int,
    cwd: str | Path | None = None,
) -> subprocess.CompletedProcess[str]:
    """Capture child output within a byte and time limit, including descendants."""
    if not math.isfinite(timeout) or timeout <= 0 or max_output_bytes <= 0:
        raise ValueError("timeout and max_output_bytes must be positive")
    with tempfile.TemporaryFile(mode="w+b") as stdin:
        stdin.write(input_text.encode("utf-8"))
        stdin.seek(0)
        process = subprocess.Popen(
            argv,
            stdin=stdin,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=cwd,
            env=dict(env),
            start_new_session=os.name == "posix",
        )
        assert process.stdout is not None and process.stderr is not None
        chunks: dict[str, bytearray] = {"stdout": bytearray(), "stderr": bytearray()}
        deadline = time.monotonic() + timeout
        try:
            with selectors.DefaultSelector() as selector:
                selector.register(process.stdout, selectors.EVENT_READ, "stdout")
                selector.register(process.stderr, selectors.EVENT_READ, "stderr")
                while selector.get_map():
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise subprocess.TimeoutExpired(argv, timeout)
                    for key, _ in selector.select(min(remaining, 0.1)):
                        data = os.read(key.fd, 65536)
                        if not data:
                            selector.unregister(key.fileobj)
                            continue
                        chunks[key.data].extend(data)
                        if sum(map(len, chunks.values())) > max_output_bytes:
                            raise OutputLimitExceeded("adapter output exceeded its byte limit")
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise subprocess.TimeoutExpired(argv, timeout)
            returncode = process.wait(timeout=remaining)
            if os.name == "posix":
                with suppress(ProcessLookupError):
                    os.killpg(process.pid, signal.SIGKILL)
        except BaseException:
            _stop_group(process)
            raise
        return subprocess.CompletedProcess(
            argv,
            returncode,
            chunks["stdout"].decode("utf-8", errors="replace"),
            chunks["stderr"].decode("utf-8", errors="replace"),
        )
