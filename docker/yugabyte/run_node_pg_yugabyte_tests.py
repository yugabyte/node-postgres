#!/usr/bin/env python3
"""
node-postgres / YugabyteDB Docker Test Runner
----------------------------------------------
Runs YugabyteDB in one Compose service and the Yugabyte fork of node-postgres
tests in another, streaming all output to the terminal and to a log file.

Edit RUNNER_DEFAULTS below to change permanent settings.
Any value can also be overridden by the corresponding environment variable.

Environment variables
---------------------
  NODE_PG_DOCKER_DIR        Directory containing docker-compose.yml
                            (default: this script's directory)
  NODE_PG_COMPOSE_FILE      Full path to compose file
                            (default: NODE_PG_DOCKER_DIR/docker-compose.yml)
  COMPOSE_PROJECT_NAME      Docker Compose project name

  NODE_PG_TEST_SCOPE        all (default) | pg-only | unit-only | integration-only
  NODE_PG_SKIP_NATIVE       1/true: skip native test leg in pg-only scope (default: false)

  This runner does not set PGTESTNOSSL, SCRAM_TEST_*, or other upstream skip toggles.
  To match CI-style SSL skips, set PGTESTNOSSL yourself when invoking docker compose.

  YB_ENABLE_YSQL_CONN_MGR   0 (default): connection manager OFF
                             1: connection manager ON

  NODE_PG_TEST_OUTPUT_LOG   Full path for the log file
                            (default: docker/logs/node-pg-yugabyte-cm-off-<scope>.log
                            or node-pg-yugabyte-cm-on-<scope>.log when CM is on)

  NODE_PG_SKIP_CLEANUP      1/true: skip initial  `compose down`
  NODE_PG_SKIP_PULL         1/true: skip          `compose pull yugabyte`
  NODE_PG_SKIP_SETUP        1/true: skip pull + build (run stack/tests only)
  NODE_PG_REMOVE_VOLUMES    1/true: add -v to compose down calls
  NODE_PG_POST_DOWN         1/true: run compose down after tests (default)
                            0/false: leave stack up after tests

  YB_WAIT_SEC               Seconds to sleep before first YSQL readiness check
  YB_VERIFY_RETRIES         Max YSQL readiness attempts (each waits 5 s)

  NODE_PG_YB_HOST_YSQL_PORT  Host port → container 5433  (YSQL)
  NODE_PG_YB_HOST_ADMIN_PORT Host port → container 9000  (admin)
  NODE_PG_YB_HOST_UI_PORT    Host port → container 15433 (UI)
  NODE_PG_YB_HOST_YCQL_PORT  Host port → container 9042  (YCQL)

  YB_IMAGE                  YugabyteDB image (default: from RUNNER_DEFAULTS)
  YB_ENABLE_YSQL_CONN_MGR   1 → enable YSQL connection manager

Usage
-----
  python3 run_node_pg_yugabyte_tests.py [options]

  Options:
    --down-only          Tear down the Compose stack and exit.
    --no-down            Keep containers running after tests complete.
    --skip-setup         Skip image pull and build steps.
    --pg-only            Run only packages/pg tests (sets scope=pg-only).
    --unit-only          Run only packages/pg unit tests (scope=unit-only).
    --integration-only   Run only packages/pg integration tests.
    --all                Run the full monorepo test suite (scope=all).
"""

from __future__ import annotations

import argparse
import os
import shlex
import shutil
import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

# =============================================================================
# RUNNER_DEFAULTS
# Edit these values to change permanent defaults.
# Any key can be overridden by the corresponding environment variable above.
# =============================================================================
RUNNER_DEFAULTS: dict = {
    # Docker Compose
    "compose_file":  None,                  # None → docker-compose.yml next to this script
    "project_name":  "node-pg-yb",

    # Test target
    "test_scope":       "all",              # all | pg-only | unit-only | integration-only
    "skip_native":      False,              # pg-only: run pg-native leg; ignored for scope=all (Lerna runs make test-all)
    "ysql_conn_mgr":    0,                  # 0 = connection manager OFF, 1 = ON

    # YugabyteDB image
    "yb_image": "yugabytedb/yugabyte:2025.2.2.1-b1",

    # Log file
    "log_file": None,                       # None → docker/logs/node-pg-yugabyte-cm-off|on-<scope>.log

    # Lifecycle
    "skip_cleanup":   False,   # skip initial compose down
    "skip_pull":      False,   # skip compose pull yugabyte
    "skip_setup":     False,   # skip pull + build
    "remove_volumes": False,   # pass -v to compose down
    "no_post_down":   False,   # set True to leave the stack up after tests

    # YugabyteDB readiness
    "yb_wait_sec":       20,   # initial sleep before readiness probes
    "yb_verify_retries": 60,   # max probe attempts (each sleeps 5 s on failure)

    # Published host ports
    "yb_host_ysql_port":  45433,
    "yb_host_admin_port": 49000,
    "yb_host_ui_port":    45434,
    "yb_host_ycql_port":  49042,
}

TOTAL_STEPS = 8


# =============================================================================
# Config helpers
# =============================================================================

def _flag(key: str, default: bool) -> bool:
    v = os.environ.get(key, "").strip().lower()
    if not v:
        return default
    return v in ("1", "true", "yes", "on")


def _int(key: str, default: int) -> int:
    v = os.environ.get(key, "").strip()
    if not v:
        return default
    try:
        return int(v)
    except ValueError:
        return default


def _str(key: str, default: str | None) -> str | None:
    v = os.environ.get(key, "").strip()
    return v if v else default


def _resolve_docker() -> str | None:
    """Return path to docker CLI, checking PATH then common macOS locations."""
    found = shutil.which("docker")
    if found:
        return found
    candidates = [
        Path("/Applications/Docker.app/Contents/Resources/bin/docker"),
        Path("/Applications/OrbStack.app/Contents/MacOS/xbin/docker"),
        Path.home() / ".orbstack" / "bin" / "docker",
        Path("/usr/local/bin/docker"),
        Path("/opt/homebrew/bin/docker"),
    ]
    for p in candidates:
        if p.is_file() and os.access(p, os.X_OK):
            return str(p)
    return None


# =============================================================================
# Config dataclass
# =============================================================================

@dataclass
class RunnerConfig:
    script_dir:         Path
    docker_dir:         Path
    repo_root:          Path
    compose_file:       Path
    project_name:       str
    test_scope:         str
    skip_native:        bool
    connection_manager: str   # "on" | "off"
    yb_image:           str
    log_file:           Path | None
    skip_cleanup:       bool
    skip_pull:          bool
    skip_setup:         bool
    remove_volumes:     bool
    post_down:          bool
    yb_wait_sec:        int
    yb_verify_retries:  int
    yb_host_ysql_port:  int
    yb_host_admin_port: int
    yb_host_ui_port:    int
    yb_host_ycql_port:  int


def load_config(script_dir: Path, args: argparse.Namespace) -> RunnerConfig:
    d = RUNNER_DEFAULTS

    docker_dir = Path(os.environ.get("NODE_PG_DOCKER_DIR", str(script_dir))).resolve()
    repo_root  = docker_dir.parent  # local_runs/JavaScript/

    cf_raw = _str("NODE_PG_COMPOSE_FILE", None)
    if cf_raw:
        compose_file = Path(cf_raw).expanduser().resolve()
    elif d.get("compose_file"):
        compose_file = Path(str(d["compose_file"])).expanduser().resolve()
    else:
        compose_file = (docker_dir / "docker-compose.yml").resolve()

    # Test scope: CLI flag wins, then env var, then default.
    if getattr(args, "all", False):
        scope = "all"
    elif getattr(args, "pg_only", False):
        scope = "pg-only"
    elif getattr(args, "unit_only", False):
        scope = "unit-only"
    elif getattr(args, "integration_only", False):
        scope = "integration-only"
    else:
        scope = _str("NODE_PG_TEST_SCOPE", str(d["test_scope"])) or str(d["test_scope"])

    log_env = _str("NODE_PG_TEST_OUTPUT_LOG", None)
    if log_env:
        log_file: Path | None = Path(log_env).expanduser().resolve()
    elif d.get("log_file"):
        log_file = Path(str(d["log_file"])).expanduser().resolve()
    else:
        log_file = None

    skip_setup = _flag("NODE_PG_SKIP_SETUP", bool(d["skip_setup"])) or getattr(args, "skip_setup", False)

    return RunnerConfig(
        script_dir         = script_dir,
        docker_dir         = docker_dir,
        repo_root          = repo_root,
        compose_file       = compose_file,
        project_name       = os.environ.get("COMPOSE_PROJECT_NAME", d["project_name"]),
        test_scope         = scope,
        skip_native        = _flag("NODE_PG_SKIP_NATIVE", bool(d["skip_native"])),
        connection_manager = "on" if _int("YB_ENABLE_YSQL_CONN_MGR", int(d.get("ysql_conn_mgr", 0))) == 1 else "off",
        yb_image           = _str("YB_IMAGE", str(d["yb_image"])) or str(d["yb_image"]),
        log_file           = log_file,
        skip_cleanup       = _flag("NODE_PG_SKIP_CLEANUP",   bool(d["skip_cleanup"])),
        skip_pull          = _flag("NODE_PG_SKIP_PULL",      bool(d["skip_pull"])),
        skip_setup         = skip_setup,
        remove_volumes     = _flag("NODE_PG_REMOVE_VOLUMES", bool(d["remove_volumes"])),
        post_down          = _flag("NODE_PG_POST_DOWN", not bool(d["no_post_down"])),
        yb_wait_sec        = _int("YB_WAIT_SEC",                   int(d["yb_wait_sec"])),
        yb_verify_retries  = _int("YB_VERIFY_RETRIES",             int(d["yb_verify_retries"])),
        yb_host_ysql_port  = _int("NODE_PG_YB_HOST_YSQL_PORT",    int(d["yb_host_ysql_port"])),
        yb_host_admin_port = _int("NODE_PG_YB_HOST_ADMIN_PORT",   int(d["yb_host_admin_port"])),
        yb_host_ui_port    = _int("NODE_PG_YB_HOST_UI_PORT",      int(d["yb_host_ui_port"])),
        yb_host_ycql_port  = _int("NODE_PG_YB_HOST_YCQL_PORT",    int(d["yb_host_ycql_port"])),
    )


# =============================================================================
# I/O helpers
# =============================================================================

def tee(log_fp, msg: str) -> None:
    """Write msg to stdout and the log file simultaneously."""
    sys.stdout.write(msg)
    sys.stdout.flush()
    log_fp.write(msg)
    log_fp.flush()


def tee_run(
    cmd: list[str],
    *,
    cwd: Path,
    log_fp,
    env: dict[str, str] | None = None,
) -> int:
    """Run cmd, streaming stdout+stderr to terminal and log file. Returns exit code."""
    proc = subprocess.Popen(
        cmd,
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        env=env,
    )
    assert proc.stdout is not None
    try:
        for line in proc.stdout:
            sys.stdout.write(line)
            sys.stdout.flush()
            log_fp.write(line)
            log_fp.flush()
    finally:
        proc.stdout.close()
    return proc.wait()


def is_port_free(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s.bind(("127.0.0.1", port))
            return True
        except OSError:
            return False


# =============================================================================
# Main
# =============================================================================

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Run node-postgres upstream tests against YugabyteDB in Docker."
    )
    scope = p.add_mutually_exclusive_group()
    scope.add_argument("--pg-only",           dest="pg_only",           action="store_true",
                       help="Run packages/pg tests only (make test-all minus native).")
    scope.add_argument("--unit-only",         dest="unit_only",         action="store_true",
                       help="Run packages/pg unit tests only.")
    scope.add_argument("--integration-only",  dest="integration_only",  action="store_true",
                       help="Run packages/pg integration tests only.")
    scope.add_argument("--all",               dest="all",               action="store_true",
                       help="Run the full monorepo test suite (yarn lerna exec yarn test).")
    p.add_argument("--down-only",   action="store_true", help="Tear down Compose stack and exit.")
    p.add_argument("--no-down",     action="store_true", help="Leave containers running after tests.")
    p.add_argument("--skip-setup",  action="store_true", help="Skip image pull and build.")
    return p.parse_args()


def main() -> int:
    args = parse_args()

    docker_bin = _resolve_docker()
    if not docker_bin:
        print(
            "Docker was not found. Install Docker Desktop for Mac and start it, then retry:\n"
            "  https://docs.docker.com/desktop/setup/install/mac-install/\n"
            "After install, ensure `docker compose version` works in this terminal.",
            file=sys.stderr,
        )
        return 127

    cfg = load_config(Path(__file__).resolve().parent, args)

    if not cfg.compose_file.is_file():
        print(f"error: compose file not found: {cfg.compose_file}", file=sys.stderr)
        return 1

    # ── Compose base command ───────────────────────────────────────────────────
    compose = [docker_bin, "compose", "-f", str(cfg.compose_file)]

    # ── Compose env ───────────────────────────────────────────────────────────
    compose_env = os.environ.copy()
    compose_env["COMPOSE_PROJECT_NAME"]    = cfg.project_name
    compose_env["YB_IMAGE"]                = cfg.yb_image
    compose_env["YB_PUBLISH_YSQL"]         = str(cfg.yb_host_ysql_port)
    compose_env["YB_PUBLISH_9000"]         = str(cfg.yb_host_admin_port)
    compose_env["YB_PUBLISH_UI"]           = str(cfg.yb_host_ui_port)
    compose_env["YB_PUBLISH_YCQL"]         = str(cfg.yb_host_ycql_port)
    compose_env["NODE_PG_TEST_SCOPE"]      = cfg.test_scope
    compose_env["NODE_PG_SKIP_NATIVE"]     = "true" if cfg.skip_native else "false"
    compose_env["YB_ENABLE_YSQL_CONN_MGR"] = "1" if cfg.connection_manager == "on" else "0"

    # ── down-only shortcut ─────────────────────────────────────────────────────
    if args.down_only:
        print("[down-only] docker compose down ...")
        cmd = compose + ["down", "--remove-orphans"] + (["-v"] if cfg.remove_volumes else [])
        subprocess.run(cmd, cwd=cfg.docker_dir, env=compose_env)
        print("Done.")
        return 0

    # ── Port check ────────────────────────────────────────────────────────────
    busy = [
        (name, port)
        for name, port in [
            ("YSQL",  cfg.yb_host_ysql_port),
            ("Admin", cfg.yb_host_admin_port),
            ("UI",    cfg.yb_host_ui_port),
            ("YCQL",  cfg.yb_host_ycql_port),
        ]
        if not is_port_free(port)
    ]
    if busy:
        for name, port in busy:
            print(f"error: {name} host port {port} is already in use", file=sys.stderr)
        print(
            "Set NODE_PG_YB_HOST_*_PORT environment variables to choose different ports.",
            file=sys.stderr,
        )
        return 2

    # ── Log path ─────────────────────────────────────────────────────────────
    logs_dir = cfg.docker_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    if cfg.log_file is not None:
        log_path = cfg.log_file
        log_path.parent.mkdir(parents=True, exist_ok=True)
    else:
        scope_label = cfg.test_scope.replace("-", "_")
        # Stable names: cm-off / cm-on (no timestamp — each run overwrites the same file).
        cm_slug = "cm-on" if cfg.connection_manager == "on" else "cm-off"
        log_path = logs_dir / f"node-pg-yugabyte-{cm_slug}-{scope_label}.log"

    # ── Banner ────────────────────────────────────────────────────────────────
    print("=== node-postgres / YugabyteDB Docker Test Runner ===")
    print(f"  docker dir:         {cfg.docker_dir}")
    print(f"  compose file:       {cfg.compose_file}")
    print(f"  project:            {cfg.project_name}")
    print(f"  test scope:         {cfg.test_scope}")
    print(f"  skip native:        {cfg.skip_native}")
    print(f"  connection manager: {cfg.connection_manager}")
    print(f"  yb image:           {cfg.yb_image}")
    print(f"  ports:              ysql={cfg.yb_host_ysql_port}  admin={cfg.yb_host_admin_port}"
          f"  ui={cfg.yb_host_ui_port}  ycql={cfg.yb_host_ycql_port}")
    print(f"  log file:           {log_path}")
    print()

    exit_code = 0

    with log_path.open("w", encoding="utf-8") as log_fp:
        log_fp.write(
            f"=== node-postgres / YugabyteDB test run ===\n"
            f"UTC time:           {datetime.now(timezone.utc).isoformat()}\n"
            f"compose file:       {cfg.compose_file}\n"
            f"project:            {cfg.project_name}\n"
            f"test scope:         {cfg.test_scope}\n"
            f"skip native:        {cfg.skip_native}\n"
            f"connection manager: {cfg.connection_manager}\n"
            f"yb image:           {cfg.yb_image}\n"
            f"ports:              ysql={cfg.yb_host_ysql_port}  admin={cfg.yb_host_admin_port}"
            f"  ui={cfg.yb_host_ui_port}  ycql={cfg.yb_host_ycql_port}\n"
            f"log file:           {log_path}\n"
            f"===\n\n"
        )

        def run_step(step: int, label: str, cmd: list[str]) -> int:
            tee(log_fp, f"\n[{step}/{TOTAL_STEPS}] {label}\n$ {' '.join(cmd)}\n\n")
            code = tee_run(cmd, cwd=cfg.docker_dir, log_fp=log_fp, env=compose_env)
            tee(log_fp, f"\n----- finished (exit {code}) -----\n")
            return code

        # ── Step 1: Cleanup ───────────────────────────────────────────────────
        if not cfg.skip_cleanup:
            cmd = compose + ["down", "--remove-orphans"] + (["-v"] if cfg.remove_volumes else [])
            if run_step(1, "Cleanup (compose down)", cmd) != 0:
                tee(log_fp, "\nERROR: cleanup step failed.\n")
                return 1
        else:
            tee(log_fp, f"\n[1/{TOTAL_STEPS}] Cleanup skipped (NODE_PG_SKIP_CLEANUP)\n\n")

        # ── Steps 2–3: Pull + build ───────────────────────────────────────────
        if not cfg.skip_setup:
            if not cfg.skip_pull:
                if run_step(2, "Pull yugabyte image", compose + ["pull", "yugabyte"]) != 0:
                    tee(log_fp, "\nERROR: image pull failed.\n")
                    return 1
            else:
                tee(log_fp, f"\n[2/{TOTAL_STEPS}] Pull skipped (NODE_PG_SKIP_PULL)\n\n")

            if run_step(3, "Build node-pg-tests image", compose + ["build", "node-pg-tests"]) != 0:
                tee(log_fp, "\nERROR: image build failed.\n")
                return 1
        else:
            tee(log_fp, f"\n[2-3/{TOTAL_STEPS}] Setup skipped (NODE_PG_SKIP_SETUP)\n\n")

        # ── Step 4: Start YugabyteDB ──────────────────────────────────────────
        if run_step(4, "Start yugabyte (compose up -d)", compose + ["up", "-d", "yugabyte"]) != 0:
            run_step(5, "Diagnostics (compose logs)", compose + ["logs", "--no-color"])
            tee(log_fp, "\nERROR: yugabyte container failed to start.\n")
            return 1

        # ── Step 5: YSQL readiness ────────────────────────────────────────────
        tee(log_fp, f"\n[5/{TOTAL_STEPS}] Waiting {cfg.yb_wait_sec}s before readiness checks\n")
        time.sleep(max(0, cfg.yb_wait_sec))

        ysql_probe = compose + [
            "exec", "-T", "yugabyte",
            "/home/yugabyte/bin/ysqlsh",
            "-h", "yugabyte", "-p", "5433", "-U", "yugabyte", "-c", "SELECT 1", "-q",
        ]
        ready = False
        for attempt in range(1, cfg.yb_verify_retries + 1):
            if run_step(5, f"YSQL readiness {attempt}/{cfg.yb_verify_retries}", ysql_probe) == 0:
                ready = True
                break
            if attempt < cfg.yb_verify_retries:
                tee(log_fp, "YSQL not ready yet; retrying in 5s...\n")
                time.sleep(5)

        if not ready:
            run_step(5, "Diagnostics (compose logs)", compose + ["logs", "--no-color"])
            tee(log_fp, "\nERROR: YSQL did not become ready.\n")
            return 1

        # ── Step 6: Run tests ─────────────────────────────────────────────────
        test_cmd = compose + ["run", "--rm", "node-pg-tests"]
        exit_code = run_step(6, f"Run node-postgres tests (scope={cfg.test_scope})", test_cmd)

        # ── Step 7: Diagnostics (on failure) ─────────────────────────────────
        if exit_code != 0:
            run_step(7, "Diagnostics (compose logs)", compose + ["logs", "--no-color"])
        else:
            tee(log_fp, f"\n[7/{TOTAL_STEPS}] Tests passed — skipping diagnostics.\n")

        # ── Step 8: Post-run cleanup ──────────────────────────────────────────
        if not args.no_down and cfg.post_down:
            cmd = compose + ["down", "--remove-orphans"] + (["-v"] if cfg.remove_volumes else [])
            run_step(8, "Post-run cleanup (compose down)", cmd)
        else:
            reason = "--no-down flag" if args.no_down else "NODE_PG_POST_DOWN=0"
            tee(log_fp, f"\n[8/{TOTAL_STEPS}] Post-run cleanup skipped ({reason})\n\n")

        tee(log_fp, f"\n=== finished: test exit code {exit_code} | log: {log_path} ===\n")

    # ── Final summary ─────────────────────────────────────────────────────────
    W = 70
    print()
    print("=" * W)
    print(f"Full test log: {log_path}")
    print(f"Exit code:     {exit_code}")
    print("=" * W)

    return exit_code


if __name__ == "__main__":
    sys.exit(main())
