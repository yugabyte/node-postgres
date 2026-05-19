#!/usr/bin/env bash
# Container-side entrypoint for the node-pg-tests service.
#
# Responsibilities:
#   1. Wait for YugabyteDB YSQL to accept connections.
#   2. Run the appropriate test scope (all / pg-only / unit-only / integration-only).
#
# Environment variables (all have defaults):
#   PGHOST                  DB hostname  (default: yugabyte)
#   PGPORT                  DB port      (default: 5433)
#   PGUSER                  DB user      (default: yugabyte)
#   PGPASSWORD              DB password  (default: yugabyte)
#   PGDATABASE              DB name      (default: yugabyte)
#
#   This script does not set PGTESTNOSSL or SCRAM_TEST_* — those are only read
#   if you supply them (e.g. docker compose -e). Otherwise the repo’s own
#   guards control skips (e.g. 2085-tests.js skips when PGTESTNOSSL is set).
#
#   NODE_PG_TEST_SCOPE      What to run:
#                             all              - full monorepo: yarn build + yarn lerna exec yarn test (default)
#                             pg-only          - packages/pg only (custom runner + optional native leg)
#                             unit-only        - packages/pg unit tests only
#                             integration-only - packages/pg integration tests only
#
#   NODE_PG_SKIP_NATIVE     1/true → skip pg-native leg in pg-only scope (default: false)
#
#   DB_WAIT_RETRIES         Max connection attempts before giving up (default: 120)
#   DB_WAIT_INTERVAL_SEC    Seconds between attempts (default: 2)

set -euo pipefail

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
PGHOST="${PGHOST:-yugabyte}"
PGPORT="${PGPORT:-5433}"
PGUSER="${PGUSER:-yugabyte}"
PGPASSWORD="${PGPASSWORD:-yugabyte}"
PGDATABASE="${PGDATABASE:-yugabyte}"

NODE_PG_TEST_SCOPE="${NODE_PG_TEST_SCOPE:-all}"
NODE_PG_SKIP_NATIVE="${NODE_PG_SKIP_NATIVE:-false}"

DB_WAIT_RETRIES="${DB_WAIT_RETRIES:-120}"
DB_WAIT_INTERVAL_SEC="${DB_WAIT_INTERVAL_SEC:-2}"

export PGHOST PGPORT PGUSER PGPASSWORD PGDATABASE

# ---------------------------------------------------------------------------
# Readiness wait
# ---------------------------------------------------------------------------
echo "[run-tests] Waiting for YSQL at ${PGHOST}:${PGPORT} ..."
for i in $(seq 1 "${DB_WAIT_RETRIES}"); do
    if PGPASSWORD="${PGPASSWORD}" psql \
            -h "${PGHOST}" -p "${PGPORT}" \
            -U "${PGUSER}" -d "${PGDATABASE}" \
            -c "SELECT 1" >/dev/null 2>&1; then
        echo "[run-tests] Database is ready (attempt ${i})."
        break
    fi
    if [ "${i}" -eq "${DB_WAIT_RETRIES}" ]; then
        echo "[run-tests] ERROR: database did not become ready after ${DB_WAIT_RETRIES} attempts." >&2
        exit 1
    fi
    echo "[run-tests] Not ready yet (attempt ${i}/${DB_WAIT_RETRIES}); retrying in ${DB_WAIT_INTERVAL_SEC}s ..."
    sleep "${DB_WAIT_INTERVAL_SEC}"
done

# ---------------------------------------------------------------------------
# Install dependencies (source is volume-mounted, not baked into image)
# ---------------------------------------------------------------------------
echo "[run-tests] Installing dependencies (yarn install) ..."
yarn install --frozen-lockfile

# ---------------------------------------------------------------------------
# Test runner helper
#
# The upstream Makefile pipes test files through xargs, which aborts all
# remaining files when any single test exits with code 255 (the code
# produced by process.exit(-1) in suite.js on assertion failure).
#
# run_test_files runs every discovered *-tests.js file individually so that
# a failure in one file does not prevent the remaining files from executing.
# Failed file names are collected and printed in a summary at the end.
# ---------------------------------------------------------------------------
OVERALL_EXIT=0
FAILED_FILES=()
TOTAL_RUN=0
TOTAL_FAILED=0
TEST_TIMEOUT="${TEST_TIMEOUT:-120}"

run_test_files() {
    local search_dir="$1"
    shift
    local extra_args=("$@")

    while IFS= read -r test_file; do
        TOTAL_RUN=$((TOTAL_RUN + 1))
        echo ""
        echo "--- running: ${test_file} ---"
        if timeout "${TEST_TIMEOUT}" node "${test_file}" "${extra_args[@]}"; then
            echo "--- passed:  ${test_file} ---"
        else
            local rc=$?
            if [ "${rc}" -eq 124 ]; then
                echo "--- TIMEOUT (>${TEST_TIMEOUT}s): ${test_file} ---"
                FAILED_FILES+=("${test_file} [TIMEOUT]")
            else
                echo "--- FAILED (exit ${rc}): ${test_file} ---"
                FAILED_FILES+=("${test_file}")
            fi
            TOTAL_FAILED=$((TOTAL_FAILED + 1))
            OVERALL_EXIT=1
        fi
    done < <(find "${search_dir}" -name "*-tests.js" | sort)
}

install_pg_native() {
    echo "[run-tests] Installing pg-native from workspace root ..."
    # libpq (transitive dep of pg-native) bundles node-gyp 5.x whose gyp
    # scripts use Python's 'rU' mode, removed in 3.11.  Work around it:
    #   1. Install without running lifecycle scripts.
    #   2. Rebuild libpq's native addon with the modern global node-gyp.
    (
        cd "${REPO_ROOT}"
        npm i --no-save --ignore-scripts pg-native
        echo "[run-tests] Rebuilding libpq native addon with global node-gyp ..."
        (cd node_modules/libpq && /usr/local/lib/node_modules/node-gyp/bin/node-gyp.js rebuild)
    )
    if node -e "require('pg-native')" 2>/dev/null; then
        echo "[run-tests] pg-native installed and loadable."
    else
        echo "[run-tests] WARNING: pg-native installed but not loadable; native tests will fail." >&2
    fi
}

run_missing_native() {
    echo "***Testing optional native install***"
    rm -rf node_modules/pg-native node_modules/libpq
    rm -rf "${REPO_ROOT}/node_modules/pg-native" "${REPO_ROOT}/node_modules/libpq"
    node test/native/missing-native.js
    rm -rf node_modules/pg-native node_modules/libpq
    rm -rf "${REPO_ROOT}/node_modules/pg-native" "${REPO_ROOT}/node_modules/libpq"
}

run_create_test_tables() {
    echo "***Testing connection***"
    node script/create-test-tables.js "${CONNECTION_STRING}"
}

print_summary() {
    echo ""
    echo "======================================================================"
    echo "  Test Summary"
    echo "======================================================================"
    echo "  Files run:    ${TOTAL_RUN}"
    echo "  Files passed: $((TOTAL_RUN - TOTAL_FAILED))"
    echo "  Files failed: ${TOTAL_FAILED}"
    if [ ${#FAILED_FILES[@]} -gt 0 ]; then
        echo ""
        echo "  Failed files:"
        for f in "${FAILED_FILES[@]}"; do
            echo "    - ${f}"
        done
    fi
    echo "======================================================================"
    echo ""
}

# ---------------------------------------------------------------------------
# Run tests
# ---------------------------------------------------------------------------
echo "[run-tests] Test scope: ${NODE_PG_TEST_SCOPE}"
echo "[run-tests] Skip native: ${NODE_PG_SKIP_NATIVE}"
echo "[run-tests] PGHOST=${PGHOST}  PGPORT=${PGPORT}  PGUSER=${PGUSER}  PGDATABASE=${PGDATABASE}"
if [ -v PGTESTNOSSL ]; then
    echo "[run-tests] PGTESTNOSSL=${PGTESTNOSSL} (set — repo may skip SSL tests in 2085-tests.js)"
else
    echo "[run-tests] PGTESTNOSSL unset (not injected by this runner)"
fi
echo ""

REPO_ROOT="$(pwd)"
PG_DIR="packages/pg"
CONNECTION_STRING="postgres://${PGUSER}:${PGPASSWORD}@${PGHOST}:${PGPORT}/${PGDATABASE}"

case "${NODE_PG_TEST_SCOPE}" in

    pg-only)
        echo "[run-tests] Running packages/pg tests ..."
        cd "${PG_DIR}"

        run_missing_native

        echo "***Running unit tests***"
        run_test_files test/unit "${CONNECTION_STRING}"

        run_create_test_tables

        echo "***Running integration tests (Pure Javascript)***"
        run_test_files test/integration "${CONNECTION_STRING}"

        if [ "${NODE_PG_SKIP_NATIVE}" = "1" ] || [ "${NODE_PG_SKIP_NATIVE}" = "true" ]; then
            echo "[run-tests] Skipping native test leg (NODE_PG_SKIP_NATIVE=${NODE_PG_SKIP_NATIVE})"
        else
            install_pg_native
            echo "***Running native tests***"
            run_test_files test/native "${CONNECTION_STRING}"
            echo "***Running integration tests (native)***"
            run_test_files test/integration "${CONNECTION_STRING}" native
        fi

        print_summary
        ;;

    unit-only)
        echo "[run-tests] Running packages/pg unit tests only ..."
        cd "${PG_DIR}"
        run_test_files test/unit "${CONNECTION_STRING}"
        print_summary
        ;;

    integration-only)
        echo "[run-tests] Running packages/pg integration tests only ..."
        cd "${PG_DIR}"
        run_create_test_tables
        run_test_files test/integration "${CONNECTION_STRING}"
        print_summary
        ;;

    all)
        echo "[run-tests] Running ALL monorepo package tests ..."
        echo "[run-tests] Each package is run individually so a failure in one does not abort the rest."
        echo ""

        # Build TypeScript packages first (pg-protocol, pg-query-stream).
        yarn build

        # ── Other packages (Mocha-based, run via yarn test) ───────────────
        OTHER_PACKAGES=(
            packages/pg-connection-string
            packages/pg-protocol
            packages/pg-cursor
            packages/pg-pool
            packages/pg-query-stream
        )
        for pkg_dir in "${OTHER_PACKAGES[@]}"; do
            pkg_name=$(node -e "process.stdout.write(require('./${pkg_dir}/package.json').name)")
            echo ""
            echo "======================================================================"
            echo "  Package: ${pkg_name}  (${pkg_dir})"
            echo "======================================================================"
            set +e
            (cd "${pkg_dir}" && yarn test)
            pkg_rc=$?
            set -e
            if [ "${pkg_rc}" -ne 0 ]; then
                echo "--- FAILED: ${pkg_name} (exit ${pkg_rc}) ---"
                FAILED_FILES+=("${pkg_dir} (${pkg_name})")
                TOTAL_FAILED=$((TOTAL_FAILED + 1))
                OVERALL_EXIT=1
            else
                echo "--- passed: ${pkg_name} ---"
            fi
            TOTAL_RUN=$((TOTAL_RUN + 1))
        done

        # ── packages/pg (custom runner — avoids xargs abort on failure) ───
        echo ""
        echo "======================================================================"
        echo "  Package: @yugabytedb/pg  (packages/pg)"
        echo "======================================================================"
        cd "${PG_DIR}"

        run_missing_native

        echo "***Running unit tests***"
        run_test_files test/unit "${CONNECTION_STRING}"

        run_create_test_tables

        echo "***Running integration tests (Pure Javascript)***"
        run_test_files test/integration "${CONNECTION_STRING}"

        if [ "${NODE_PG_SKIP_NATIVE}" = "1" ] || [ "${NODE_PG_SKIP_NATIVE}" = "true" ]; then
            echo "[run-tests] Skipping native test leg (NODE_PG_SKIP_NATIVE=${NODE_PG_SKIP_NATIVE})"
        else
            install_pg_native
            echo "***Running native tests***"
            run_test_files test/native "${CONNECTION_STRING}"
            echo "***Running integration tests (native)***"
            run_test_files test/integration "${CONNECTION_STRING}" native
        fi

        print_summary
        ;;

    *)
        echo "[run-tests] ERROR: unknown NODE_PG_TEST_SCOPE='${NODE_PG_TEST_SCOPE}'" >&2
        echo "  Valid values: all | pg-only | unit-only | integration-only" >&2
        exit 2
        ;;
esac

exit "${OVERALL_EXIT}"
