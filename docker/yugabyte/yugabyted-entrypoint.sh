#!/bin/bash
# Custom entrypoint for the yugabyte service.
# Builds --tserver_flags dynamically from environment variables, then execs yugabyted.
#
# Environment variables read:
#   YB_ENABLE_YSQL_CONN_MGR  1/true → add enable_ysql_conn_mgr=true
#   YB_TSERVER_FLAGS_EXTRA   Any additional comma-separated tserver flags to append
set -euo pipefail

# ---------------------------------------------------------------------------
# Base tserver flags – always applied
# ---------------------------------------------------------------------------
TSERVER_FLAGS="ysql_max_connections=300,ysql_pg_conf_csv=logical_decoding_work_mem=64kB"

# ---------------------------------------------------------------------------
# Optional: YSQL connection manager
# ---------------------------------------------------------------------------
if [ "${YB_ENABLE_YSQL_CONN_MGR:-0}" != "0" ]; then
    TSERVER_FLAGS="${TSERVER_FLAGS},enable_ysql_conn_mgr=true"
    echo "[entrypoint] YSQL connection manager: ON"
else
    echo "[entrypoint] YSQL connection manager: OFF"
fi

# ---------------------------------------------------------------------------
# Optional: extra tserver flags passed via env
# ---------------------------------------------------------------------------
if [ -n "${YB_TSERVER_FLAGS_EXTRA:-}" ]; then
    TSERVER_FLAGS="${TSERVER_FLAGS},${YB_TSERVER_FLAGS_EXTRA}"
    echo "[entrypoint] extra tserver flags: ${YB_TSERVER_FLAGS_EXTRA}"
fi

echo "[entrypoint] --tserver_flags=${TSERVER_FLAGS}"
echo "[entrypoint] Starting yugabyted ..."
exec bin/yugabyted start --background=false "--tserver_flags=${TSERVER_FLAGS}"
