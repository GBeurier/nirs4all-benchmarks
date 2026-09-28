"""Child adapters cannot exhaust capture memory or outlive a timeout."""

from __future__ import annotations

import subprocess
import sys

import pytest

from nirs4all_benchmarks.process_guard import OutputLimitExceeded, run_bounded


def test_output_is_stopped_at_byte_limit() -> None:
    with pytest.raises(OutputLimitExceeded):
        run_bounded(
            [sys.executable, "-c", "import sys; sys.stdout.write('x' * 1000000)"],
            input_text="",
            env={},
            timeout=5,
            max_output_bytes=1024,
        )


def test_timeout_stops_child_with_inherited_output_pipe() -> None:
    child = "import subprocess, sys; subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)']); print('ready', flush=True)"
    with pytest.raises(subprocess.TimeoutExpired):
        run_bounded(
            [sys.executable, "-c", child],
            input_text="",
            env={},
            timeout=0.2,
            max_output_bytes=1024,
        )
