"""Synthetic v2 fixtures and fail-closed offline test execution guards.

The guard bootstrap is invoked through a scratch sitecustomize BEFORE pytest or
project imports. It never grants producer training, network, builds or installs.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path


def install_execution_guards() -> None:
    """Install process-wide guards before any third-party or project import."""
    if getattr(sys, "_arena_test_guards", False):
        return
    import ast
    import builtins
    import hashlib
    import importlib.abc
    import json
    import shutil
    import stat
    import subprocess
    import venv

    scratch = Path(os.environ["ARENA_TEST_SCRATCH"]).resolve()
    scratch_stat = scratch.lstat()
    if scratch_stat.st_uid != os.geteuid() or not scratch.is_dir():
        raise PermissionError("arena_test_authorization_required:unowned_scratch")
    guard_dir = os.environ["ARENA_TEST_GUARD_DIR"]
    python = Path(sys.executable).resolve()
    node = Path("/usr/bin/node").resolve()
    node22 = Path("/home/delete/.nvm/versions/node/v22.21.1/bin/node").resolve()
    repository = Path(__file__).resolve().parents[1]
    node_metadata_argv = ("-p", "process.versions.node.split('.')[0]")
    stub_cases = {
        "tests/test_web_wasm_adapter.py::test_web_launcher_finalizes_one_multitarget_replay",
        "tests/test_web_wasm_adapter.py::test_web_launcher_refuses_a_missing_child_fit_counter",
    }
    fake_js_case = "tests/test_web_wasm_adapter.py::test_node_child_refuses_a_refit_call"

    def test_context():
        return getattr(sys, "_arena_test_context", "")

    def view_context():
        return test_context().startswith("tests/test_view_rendering.py::")

    def node_context():
        return view_context() or test_context() in stub_cases | {fake_js_case}

    def fixture_text(function_name, receiver):
        """Read a literal from the approved existing fixture, never evaluate code."""
        tree = ast.parse((repository / "tests/test_web_wasm_adapter.py").read_text())
        function = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == function_name)
        call = next(
            n
            for n in ast.walk(function)
            if isinstance(n, ast.Call)
            and isinstance(n.func, ast.Attribute)
            and n.func.attr == "write_text"
            and isinstance(n.func.value, ast.Name)
            and n.func.value.id == receiver
        )
        return call.args[0]

    def authorized_stub(executable, argv):
        if test_context() not in stub_cases:
            return False
        path = Path(executable)
        if not path.is_relative_to(scratch) or path.is_symlink() or path.name != "node":
            return False
        module = sys.modules.get("nirs4all_benchmarks.qualification.web_wasm_adapter")
        if module is None or list(argv[1:]) != ["--input-type=module", "--eval", module._CHILD_SCRIPT]:
            return False
        value = fixture_text("web_artifacts", "node")
        if not isinstance(value, ast.JoinedStr):
            return False
        digest = hashlib.sha256((path.parent / "model.n4a").read_bytes()).hexdigest()
        # The fixture has exactly one dynamic substitution: its archive SHA.
        if sum(isinstance(v, ast.FormattedValue) for v in value.values) != 1:
            return False
        expected = "".join(v.value if isinstance(v, ast.Constant) else digest for v in value.values)
        if test_context().endswith("test_web_launcher_refuses_a_missing_child_fit_counter"):
            expected = expected.replace(',"fitCalls":0', "")
        return path.read_text() == expected

    fixture_pins = {
        "tests/test_python_legacy_adapter.py": "a8c6366a0f8299d9bfb7879873f322af4ecc156fe06803da43bd9f4fd68a2afd",
        "tests/test_scientific_runtime_adapter.py": "d951ee8794f728d0d5bf04458f20fbb6562f0b2e9f74c02b96132647bb384c04",
        "tests/test_artifact_qualification.py": "f8423198d32722a550dd1da6b450d78359a5ef470475e0debf94fec91ee1826a",
        "tests/test_performance_compare.py": "0456801adc6daa820f3a57edc0161e695a16a30b1e64a71712b04047085a5d81",
        "src/nirs4all_benchmarks/qualification/scientific_runtime_adapter.py": "5fd9df44a9446fda9932fa1a24453b3d9edc703f0b2ba25d50a6ee46313eaf06",
        "src/nirs4all_benchmarks/qualification/json_result_adapter.py": "56186ef5adc15bd9610350fa8c5c3b6c79f4178cf83f0b3d3026964077ab50a1",
        "src/nirs4all_benchmarks/qualification/contract.py": "2d0249197f9643cefa824ffb94d588be755780751014ccd2b2a9cab706fa7e26",
        ".venv/bin/n4a-gate-json-adapter": "bebca613e0c987a2e507d20a6a0f41da48fca0bd370d9681f877ffb8f7ff3689",
    }
    child_ticket = None
    fixture_launches = {}

    def pinned_source(relative):
        path = repository / relative
        data = path.read_bytes()
        if path.is_symlink() or hashlib.sha256(data).hexdigest() != fixture_pins[relative]:
            denied("fixture_source_pin:" + relative)
        return data.decode()

    def fixture_module():
        return test_context().split("::", 1)[0]

    def fixture_context():
        return fixture_module() in {p for p in fixture_pins if p.startswith("tests/")}

    def owned_fixture_path(path):
        path = Path(path).absolute()
        root = Path(getattr(sys, "_arena_fixture_root", ""))
        if not root.is_absolute() or not root.is_relative_to(scratch) or not path.is_relative_to(root):
            denied("fixture_path_outside_owned_case")
        current = scratch
        if scratch.lstat().st_uid != os.geteuid():
            denied("fixture_unowned_scratch")
        for part in path.relative_to(scratch).parts:
            current = current / part
            try:
                info = current.lstat()
            except FileNotFoundError:
                continue
            if current.is_symlink() or info.st_uid != os.geteuid():
                denied("fixture_symlink_or_owner")
        if path.resolve() != path:
            denied("fixture_path_not_canonical")
        root_info = root.lstat()
        if (root_info.st_dev, root_info.st_ino) != getattr(sys, "_arena_fixture_inode", None):
            denied("fixture_case_replaced")
        return path

    def fixture_literal(name):
        tree = ast.parse(pinned_source("tests/test_scientific_runtime_adapter.py"))
        return next(
            n.value.value
            for n in tree.body
            if isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id == name for t in n.targets)
        )

    def fake_core_sources():
        return {fixture_literal("_CORE_SOURCE"), "def replay_methods_archive_v2(*_args, **_kwargs):\n    return {}\n"}

    def fixture_request():
        frame = sys._getframe(1)
        while frame is not None:
            if (
                Path(frame.f_code.co_filename) == repository / "src/nirs4all_benchmarks/process_guard.py"
                and frame.f_code.co_name == "run_bounded"
            ):
                return json.loads(frame.f_locals["input_text"])
            frame = frame.f_back
        denied("fixture_missing_bounded_request")

    def performance_stub(path):
        if fixture_module() != "tests/test_performance_compare.py":
            return False
        path = owned_fixture_path(path)
        tree = ast.parse(pinned_source(fixture_module()))
        function = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "_fixture_adapter")
        template = next(
            n.value
            for n in ast.walk(function)
            if isinstance(n, ast.Constant) and isinstance(n.value, str) and n.value.startswith("#!__PYTHON__")
        )
        expected = {
            template.replace("__DIVERGENCE__", repr(v)).replace("__PYTHON__", sys.executable)
            for v in (None, "web_wasm")
        }
        expected |= {v.replace("request['candidate']['commit_sha']", "'0' * 40") for v in expected}
        return path.name == "adapter" and path.is_file() and path.read_text() in expected

    def console_stub(path):
        if fixture_module() != "tests/test_artifact_qualification.py":
            return False
        pinned_source(fixture_module())
        for relative in (
            ".venv/bin/n4a-gate-json-adapter",
            "src/nirs4all_benchmarks/qualification/json_result_adapter.py",
            "src/nirs4all_benchmarks/qualification/contract.py",
        ):
            pinned_source(relative)
        return Path(path) == repository / ".venv/bin/n4a-gate-json-adapter"

    def venv_argv(argv):
        if fixture_module() != "tests/test_scientific_runtime_adapter.py" or len(argv) != 6:
            return False
        pinned_source(fixture_module())
        if str(argv[0]) != "/usr/bin/python3.11" or list(argv[1:5]) != ["-m", "venv", "--without-pip", "--copies"]:
            return False
        runtime = owned_fixture_path(argv[5])
        if runtime != Path(sys._arena_fixture_root) / "runtime" or runtime.exists():
            denied("fixture_venv_not_fresh")
        return True

    def scientific_argv(argv):
        if fixture_module() != "tests/test_scientific_runtime_adapter.py" or len(argv) != 6:
            return False
        pinned_source(fixture_module())
        pinned_source("src/nirs4all_benchmarks/qualification/scientific_runtime_adapter.py")
        module = sys.modules.get("nirs4all_benchmarks.qualification.scientific_runtime_adapter")
        if module is None or list(argv[1:5]) != ["-I", "-S", "-B", "-c"] or argv[5] != module._CHILD_SCRIPT:
            return False
        runtime = Path(sys._arena_fixture_root) / "runtime"
        executable = owned_fixture_path(argv[0])
        if executable != runtime / "bin/python" or executable.read_bytes() != Path("/usr/bin/python3.11").read_bytes():
            denied("fixture_interpreter_not_copy")
        payload = fixture_request()
        site = runtime / "lib/python3.11/site-packages"
        if payload["runtime_root"] != str(runtime) or payload["site_packages"] != str(site):
            denied("fixture_runtime_payload")
        for role, name, content in (
            ("archive_v2", "selected.n4a", b"content-addressed archive witness"),
            ("methods_library", "libn4m.so", b"content-addressed methods witness"),
        ):
            path = owned_fixture_path(payload[role])
            if path != runtime.parent / name or path.read_bytes() != content:
                denied("fixture_non_synthetic_witness")
        core = owned_fixture_path(site / "nirs4all_core/__init__.py")
        yaml = owned_fixture_path(site / "yaml/__init__.py")
        tampered_negative = (
            test_context()
            == "tests/test_scientific_runtime_adapter.py::test_launcher_refuses_installed_bytes_that_differ_from_wheel"
            and core.read_text() == "raise RuntimeError('tampered')\n"
        )
        if (core.read_text() not in fake_core_sources() and not tampered_negative) or yaml.read_bytes() != b"":
            denied("fixture_non_synthetic_package")
        return True

    def fixture_event(event, **fields):
        with (scratch / "authorized-fixture-operations.jsonl").open("a") as stream:
            stream.write(json.dumps({"event": event, "test": test_context(), **fields}) + "\n")

    def fixture_venv_admission():
        total = 0
        pending = [scratch]
        while pending:
            directory = pending.pop()
            fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_NOATIME)
            try:
                for name in os.listdir(fd):
                    try:
                        info = os.stat(name, dir_fd=fd, follow_symlinks=False)
                    except FileNotFoundError:
                        continue
                    total += max(info.st_size, info.st_blocks * 512)
                    if stat.S_ISDIR(info.st_mode):
                        pending.append(directory / name)
            finally:
                os.close(fd)
        # Three interpreter copies plus conservative fixture/config overhead.
        forecast = total + 3 * Path("/usr/bin/python3.11").stat().st_size + 2 * 1024**2
        if forecast >= 587202560 or shutil.disk_usage(scratch).free < 2684354560 + 67108864:
            denied("fixture_resource_admission_requires_parent")

    forbidden = {
        "pip",
        "ensurepip",
        "nirs4all",
        "nirs4all_core",
        "nirs4all_methods",
        "dag_ml",
        "dagml",
        "dmd",
        "sklearn",
        "scipy",
        "statsmodels",
        "catboost",
        "keras",
        "optuna",
        "torch",
        "tensorflow",
        "jax",
        "jaxlib",
        "lightgbm",
        "xgboost",
    }

    def denied(reason: str) -> None:
        raise PermissionError("arena_test_authorization_required:" + reason)

    class ScientificImports(importlib.abc.MetaPathFinder):
        def find_spec(self, fullname, path=None, target=None):
            if (
                child_ticket is not None
                and child_ticket["kind"] == "fake_core"
                and fullname in {"nirs4all_core", "yaml"}
            ):
                from importlib.machinery import PathFinder

                expected = owned_fixture_path(
                    Path(sys._arena_fixture_root) / "runtime/lib/python3.11/site-packages" / fullname / "__init__.py"
                )
                spec = PathFinder.find_spec(fullname, path)
                valid = (
                    expected.read_text() in fake_core_sources()
                    if fullname == "nirs4all_core"
                    else expected.read_bytes() == b""
                )
                if spec is None or spec.origin != str(expected) or not valid:
                    denied("fixture_import_escape")
                return spec
            if fullname.split(".")[0] in forbidden:
                denied("scientific_runtime:" + fullname)
            return None

    def fd_path(path, dir_fd=None):
        if dir_fd not in (None, -1) and not isinstance(path, int) and not os.path.isabs(path):
            # Preserve openat's directory binding. Audit events for open omit
            # dir_fd; using the fd path also makes that audit unambiguous.
            return os.path.join("/proc/self/fd", str(dir_fd), os.fsdecode(path))
        return path

    def writable(path, dir_fd=None):
        if isinstance(path, int) or path is None:
            return
        value = Path(os.fsdecode(fd_path(path, dir_fd)))
        if value == Path(os.devnull):
            return
        value = value.resolve()
        if not value.is_relative_to(scratch):
            denied("write_outside_scratch:" + str(value))

    def audit(event, args):
        if event == "open":
            path, _mode, flags = args
            if isinstance(path, (str, bytes)):
                value = os.fsdecode(path)
                if any(part in Path(value).parts for part in ("nirs4all-lab", "nirs4all-drafts")):
                    denied("private_source")
            if flags & (os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_APPEND):
                writable(path)
        elif event in {"os.remove", "os.rmdir"}:
            writable(args[0], args[1])
        elif event in {"os.mkdir", "os.chmod"}:
            writable(args[0], args[2])
        elif event == "os.utime":
            writable(args[0], args[3])
        elif event == "os.truncate":
            writable(args[0])
        elif event in {"os.rename", "os.link"}:
            writable(args[0], args[2])
            writable(args[1], args[3])
        elif event == "os.symlink":
            writable(args[1], args[2])
        elif event == "socket.__new__" and args[1] in {2, 10}:
            denied("network_socket")
        elif event in {"socket.getaddrinfo", "socket.gethostbyname", "socket.gethostbyaddr"}:
            denied("network_resolution")
        elif event == "shutil.copytree":
            source, destination = args
            source = Path(source).resolve()
            if not (
                view_context()
                and any(
                    source.is_relative_to(repository / "src/nirs4all_benchmarks/web" / part)
                    for part in ("views", "lib")
                )
            ):
                denied("tree_copy")
            writable(destination)
        elif event in {"os.system", "os.exec", "os.posix_spawn", "os.spawn"}:
            denied("external_execution")
        elif event == "ctypes.dlopen" and args[0] is not None:
            denied("native_library")
        elif event == "ctypes.dlsym" and args[1] in {"system", "socket", "connect", "execve", "posix_spawn", "fork"}:
            denied("native_execution_or_network")
        elif event == "subprocess.Popen":
            executable, argv, _cwd, env = args
            if not isinstance(argv, (list, tuple)):
                denied("process_argv_unsupported")
            if Path(executable).resolve() in {node, node22}:
                metadata = tuple(argv[1:]) == node_metadata_argv
                module = sys.modules.get("test_view_rendering")
                cwd = Path(_cwd or Path.cwd()).resolve()
                view = (
                    view_context()
                    and len(argv) == 4
                    and argv[1] == "driver.mjs"
                    and argv[2]
                    in {"matrix", "leaderboard", "statistics", "operator-effect", "robustness", "overview", "runs"}
                    and cwd.is_relative_to(scratch)
                    and module is not None
                    and (cwd / "driver.mjs").read_text() == module.DRIVER
                )
                wasm = sys.modules.get("nirs4all_benchmarks.qualification.web_wasm_adapter")
                fake = (
                    test_context() == fake_js_case
                    and wasm is not None
                    and list(argv[1:]) == ["--input-type=module", "--eval", wasm._CHILD_SCRIPT]
                )
                if not (metadata or view or fake):
                    denied("node_argv_not_authorized")
                if env is None or env.get("NODE_OPTIONS") or env.get("NODE_PATH"):
                    denied("node_preload_environment_not_authorized")
                if guard_dir not in env.get("PYTHONPATH", "").split(os.pathsep):
                    denied("unguarded_child_environment")
                return
            if authorized_stub(executable, argv):
                if env is None or guard_dir not in env.get("PYTHONPATH", "").split(os.pathsep):
                    denied("unguarded_child_environment")
                return
            if env is not None and env.get("ARENA_SYNTHETIC_CHILD"):
                ticket = json.loads(env["ARENA_SYNTHETIC_CHILD"])
                if fixture_launches.get(ticket.get("nonce")) == ticket and ticket.get("argv") == list(map(str, argv)):
                    return
                denied("fixture_child_argv_changed")
            if fixture_context():
                denied("unlisted_fixture_subprocess:" + repr(argv))
            if any(value in argv for value in ("pip", "ensurepip", "venv")):
                denied("package_installation")
            if Path(executable).resolve() != python:
                denied("non_python_process:" + str(executable))
            if any(value in {"-S", "-I", "-E"} for value in argv[1:] if isinstance(value, str)):
                denied("unguarded_python")
            if env is None or guard_dir not in env.get("PYTHONPATH", "").split(os.pathsep):
                denied("unguarded_child_environment")

    original_import = builtins.__import__

    def no_fit(*args, **kwargs):
        denied("scientific_fit")

    def guarded_import(*args, **kwargs):
        result = original_import(*args, **kwargs)
        for module_name, names in (
            ("numpy", ("polyfit",)),
            ("numpy.linalg", ("lstsq", "solve", "pinv")),
            ("numpy.polynomial.polyutils", ("_fit",)),
        ):
            module = sys.modules.get(module_name)
            if module is not None:
                for name in names:
                    if name in module.__dict__:
                        setattr(module, name, no_fit)
        base = sys.modules.get("numpy.polynomial._polybase")
        if base is not None and "ABCPolyBase" in base.__dict__:
            base.ABCPolyBase.fit = classmethod(no_fit)
        return result

    builtins.__import__ = guarded_import
    original_popen = subprocess.Popen
    original_run = subprocess.run
    original_which = shutil.which

    def guarded_which(command, *args, **kwargs):
        if command == "node" and node_context():
            return str(node22) if node22.is_file() else None
        return original_which(command, *args, **kwargs)

    def guarded_run(*args, **kwargs):
        if test_context() == fake_js_case:
            payload = json.loads(kwargs["input"])
            entrypoint = Path(payload["entrypoint"])
            archive = Path(payload["archive"])
            expected = fixture_text("test_node_child_refuses_a_refit_call", "entrypoint")
            if (
                not isinstance(expected, ast.Constant)
                or not entrypoint.is_relative_to(scratch)
                or entrypoint.is_symlink()
                or entrypoint.read_text() != expected.value
                or not archive.is_relative_to(scratch)
                or archive.read_bytes() != b"archive"
            ):
                denied("non_fake_wasm_payload")
        return original_run(*args, **kwargs)

    class GuardedPopen(original_popen):
        def __init__(self, args, *positional, **kwargs):
            if kwargs.get("shell"):
                denied("shell_execution")
            env = dict(kwargs["env"] if kwargs.get("env") is not None else os.environ)
            env["ARENA_TEST_SCRATCH"] = str(scratch)
            env["ARENA_TEST_GUARD_DIR"] = guard_dir
            env["PYTHONPATH"] = guard_dir + os.pathsep + env.get("PYTHONPATH", "")
            env["PYTHONDONTWRITEBYTECODE"] = "1"
            if node_context():
                env["PATH"] = str(node22.parent) + os.pathsep + env.get("PATH", "")
            argv = list(map(str, args)) if isinstance(args, (tuple, list)) else []
            kind = None
            if fixture_context():
                if venv_argv(argv):
                    kind = "venv"
                elif len(argv) == 1 and console_stub(argv[0]):
                    kind = "console"
                elif len(argv) == 1 and performance_stub(argv[0]):
                    kind = "performance"
                elif scientific_argv(argv):
                    kind = "fake_core"
                    original_script = argv[5]
                    argv[5] = (
                        "import runpy;runpy.run_path("
                        + repr(str(repository / "tests/conftest.py"))
                        + ",run_name='arena_guard_bootstrap');exec(compile("
                        + repr(original_script)
                        + ",'<synthetic-fixture-child>','exec'))"
                    )
                else:
                    denied("unlisted_fixture_subprocess:" + repr(argv))
                ticket = {
                    "kind": kind,
                    "test": test_context(),
                    "root": str(sys._arena_fixture_root),
                    "inode": list(sys._arena_fixture_inode),
                    "argv": argv,
                }
                ticket["nonce"] = os.urandom(16).hex()
                fixture_launches[ticket["nonce"]] = ticket
                env["ARENA_SYNTHETIC_CHILD"] = json.dumps(ticket)
                fixture_event("subprocess_admitted", argv=argv, kind=kind)
                args = argv
            else:
                env.pop("ARENA_SYNTHETIC_CHILD", None)
            kwargs["env"] = env
            try:
                super().__init__(args, *positional, **kwargs)
            finally:
                if kind is not None:
                    fixture_launches.pop(ticket["nonce"], None)

    original_create = venv.EnvBuilder.create

    def fixture_environment(builder, env_dir):
        module = fixture_module()
        if module not in {"tests/test_python_legacy_adapter.py", "tests/test_scientific_runtime_adapter.py"}:
            denied("environment_installation")
        pinned_source(module)
        runtime = owned_fixture_path(env_dir)
        if runtime != Path(sys._arena_fixture_root) / "runtime" or runtime.exists():
            denied("fixture_venv_not_fresh")
        expected_clear = module == "tests/test_python_legacy_adapter.py"
        if (
            type(builder) is not venv.EnvBuilder
            or builder.with_pip
            or builder.system_site_packages
            or builder.symlinks
            or builder.upgrade
            or builder.upgrade_deps
            or builder.orig_prompt is not None
            or builder.clear != expected_clear
        ):
            denied("fixture_venv_options")
        frame = sys._getframe(1)
        expected_origin = repository / module
        direct = frame.f_code.co_name == "legacy_artifacts" and Path(frame.f_code.co_filename) == expected_origin
        child = child_ticket is not None and child_ticket["kind"] == "venv"
        if not (direct or child):
            denied("fixture_venv_origin")
        fixture_event("venv_create_admitted", runtime=str(runtime), with_pip=False, clear=builder.clear, copies=True)
        fixture_venv_admission()
        result = original_create(builder, str(runtime))
        fixture_event("venv_created", runtime=str(runtime))
        return result

    venv.EnvBuilder.create = fixture_environment
    subprocess.Popen = GuardedPopen
    subprocess.run = guarded_run
    shutil.which = guarded_which
    original_open = os.open

    def guarded_open(path, flags, mode=0o777, *, dir_fd=None):
        bound = fd_path(path, dir_fd)
        if flags & (os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_APPEND):
            writable(bound)
            if Path(os.fsdecode(bound)) != Path(os.devnull):
                # Bind and check the actual parent before any creation/truncation;
                # do not follow a replaced final symlink after the audit check.
                parent = original_open(Path(os.fsdecode(bound)).parent, os.O_RDONLY | os.O_DIRECTORY)
                try:
                    writable(f"/proc/self/fd/{parent}")
                    if os.fstat(parent).st_uid != os.geteuid():
                        denied("unowned_scratch_parent")
                    return original_open(
                        f"/proc/self/fd/{parent}/{Path(os.fsdecode(bound)).name}", flags | os.O_NOFOLLOW, mode
                    )
                finally:
                    os.close(parent)
        return original_open(bound, flags, mode, dir_fd=None if bound != path else dir_fd)

    os.open = guarded_open
    if os.environ.get("ARENA_SYNTHETIC_CHILD"):
        candidate = json.loads(os.environ["ARENA_SYNTHETIC_CHILD"])
        expected_argv = candidate.get("argv")
        if candidate.get("kind") in {"console", "performance"}:
            expected_argv = [sys.executable, *expected_argv]
        if (
            candidate.get("kind") not in {"venv", "console", "performance", "fake_core"}
            or expected_argv != sys.orig_argv
        ):
            denied("fixture_child_bootstrap_argv")
        sys._arena_test_context = candidate["test"]
        sys._arena_fixture_root = candidate["root"]
        sys._arena_fixture_inode = tuple(candidate["inode"])
        owned_fixture_path(candidate["root"])
        pinned_source(fixture_module())
        child_ticket = candidate
    sys.meta_path.insert(0, ScientificImports())
    sys.addaudithook(audit)
    sys._arena_test_guards = True


if "ARENA_TEST_SCRATCH" in os.environ:
    install_execution_guards()


if __name__ != "arena_guard_bootstrap":
    import pytest

    from nirs4all_benchmarks.contract import ArenaRunExport
    from nirs4all_benchmarks.fixtures import generate_demo_exports, generate_fixture_exports
    from nirs4all_benchmarks.ingestion import IngestionPolicy, ingest_export
    from nirs4all_benchmarks.store import ArenaStore, Queries

    @pytest.hookimpl(hookwrapper=True)
    def pytest_fixture_setup(fixturedef, request):
        outcome = yield
        if getattr(sys, "_arena_test_guards", False) and fixturedef.argname == "tmp_path":
            root = outcome.get_result()
            sys._arena_fixture_root = str(root)
            info = root.lstat()
            sys._arena_fixture_inode = (info.st_dev, info.st_ino)

    @pytest.hookimpl(tryfirst=True)
    def pytest_runtest_setup(item):
        if not getattr(sys, "_arena_test_guards", False):
            return
        sys._arena_test_context = item.nodeid
        if item.nodeid.startswith("tests/test_static_engine_parity.py::"):
            # Collected by whole pytest, explicitly unexecuted, never skipped or
            # deselected. Its v1 site build is outside the parent's grant.
            raise PermissionError("arena_test_unexecuted_gate:joint_frontend_static_engine_parity")
        if item.nodeid == "tests/test_web_wasm_adapter.py::test_node_child_refuses_a_refit_call":
            item._arena_original_node = os.environ.get("N4A_TEST_NODE")
            os.environ["N4A_TEST_NODE"] = "/home/delete/.nvm/versions/node/v22.21.1/bin/node"

    @pytest.hookimpl(trylast=True)
    def pytest_runtest_teardown(item):
        if hasattr(item, "_arena_original_node"):
            if item._arena_original_node is None:
                os.environ.pop("N4A_TEST_NODE", None)
            else:
                os.environ["N4A_TEST_NODE"] = item._arena_original_node
        sys._arena_test_context = ""

    def rankable_fixture_exports(*, demo=False):
        """Explicit synthetic declarations for query tests, never owner evidence."""
        exports = []
        for original in generate_demo_exports() if demo else generate_fixture_exports():
            manifest = original.to_manifest()
            manifest["execution"].update(status="ok", terminal_status="completed")
            manifest["leakage_attestation"] = {
                "oof_enforced": True,
                "group_leakage_checked": True,
                "nested_cv_safe": True,
                "unsafe_flags": [],
            }
            manifest["dataset"].update(visibility="public", visibility_source="producer_reported")
            manifest["provenance"]["synthetic_test_declarations"] = "No owner execution or attestation was performed"
            exports.append(ArenaRunExport.model_validate(manifest))
        return exports

    def declare_synthetic_safety(export):
        """Copy an owner-shaped fixture and declare safety only for a query test."""
        raw = export.to_manifest()
        raw["leakage_attestation"] = {
            "oof_enforced": True,
            "group_leakage_checked": True,
            "nested_cv_safe": True,
            "unsafe_flags": [],
        }
        raw.setdefault("provenance", {})["synthetic_test_declarations"] = "No owner safety was observed"
        return ArenaRunExport.model_validate(raw)

    def seed_rankable_store(root, *, collection_id="fixtures", demo=False):
        """Seed declared synthetic query fixtures, not observed owner evidence."""
        with ArenaStore(root) as store:
            for export in rankable_fixture_exports(demo=demo):
                result = ingest_export(store, export, policy=IngestionPolicy(collection_id=collection_id))
                assert result.ok

    @pytest.fixture
    def empty_store(tmp_path: Path):
        with ArenaStore(tmp_path / "arena") as store:
            yield store

    @pytest.fixture
    def seeded_root(tmp_path: Path):
        root = tmp_path / "seeded"
        with ArenaStore(root) as store:
            for export in rankable_fixture_exports():
                result = ingest_export(store, export, policy=IngestionPolicy(collection_id="fixtures"))
                assert result.ok
        return root

    @pytest.fixture
    def seeded_store(seeded_root: Path):
        with ArenaStore(seeded_root) as store:
            yield store

    @pytest.fixture
    def queries(seeded_store):
        return Queries(seeded_store)
