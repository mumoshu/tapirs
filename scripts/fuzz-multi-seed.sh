#!/usr/bin/env bash
#
# Run fuzz_tapir_transactions with many different seeds and report results.
#
# Usage:
#   ./scripts/fuzz-multi-seed.sh
#   FUZZ_ITERATIONS=100 FUZZ_PARALLEL=4 ./scripts/fuzz-multi-seed.sh
#
# Environment variables:
#   FUZZ_ITERATIONS  - Number of seeds to test (default: 20)
#   FUZZ_PARALLEL    - Max parallel jobs (default: 1)
#   FUZZ_TEST_NAME   - Test function name (default: fuzz_tapir_transactions)
#   FUZZ_KEEP_PASS   - If set to 1, keep logs for passing seeds (default: 0)
#   FUZZ_BASE_SEED   - Starting seed for sequential seeds (default: random independent seeds)
#   FUZZ_VERBOSE     - Passed through to test (default: 0)
#   FUZZ_CARGO_FLAGS - Extra cargo flags

set -euo pipefail

ITERATIONS="${FUZZ_ITERATIONS:-20}"
PARALLEL="${FUZZ_PARALLEL:-1}"
TEST_NAME="${FUZZ_TEST_NAME:-fuzz_tapir_transactions}"
KEEP_PASS="${FUZZ_KEEP_PASS:-0}"
BASE_SEED="${FUZZ_BASE_SEED:-}"
VERBOSE="${FUZZ_VERBOSE:-0}"
CARGO_FLAGS="${FUZZ_CARGO_FLAGS:-}"

echo "=== Multi-Seed Fuzz Runner ==="
echo "Iterations: ${ITERATIONS}"
echo "Parallel:   ${PARALLEL}"
echo "Test:       ${TEST_NAME}"
if [[ -n "${CARGO_FLAGS}" ]]; then
    echo "Cargo flags: ${CARGO_FLAGS}"
fi
echo ""

# --- Create output directory ---
LOGDIR=$(mktemp -d "/tmp/fuzz-multi-XXXXXX")
echo "Logs: ${LOGDIR}"
echo ""

# --- Pre-compile ---
echo "Pre-compiling test binary..."
if ! cargo test --lib ${CARGO_FLAGS} --no-run 2>"${LOGDIR}/build.log"; then
    echo "ERROR: Build failed. See ${LOGDIR}/build.log"
    exit 1
fi
echo "Build successful."
echo ""

# --- Generate seeds ---
declare -a SEEDS
if [[ -n "${BASE_SEED}" ]]; then
    for (( i=0; i<ITERATIONS; i++ )); do
        SEEDS+=( $(( BASE_SEED + i )) )
    done
else
    for (( i=0; i<ITERATIONS; i++ )); do
        SEEDS+=( "$(od -An -tu8 -N8 /dev/urandom | tr -d ' ')" )
    done
fi

# --- Run jobs with parallelism ---
declare -i completed=0
declare -i passed=0
declare -i failed=0
declare -a FAILED_SEEDS
declare -a PIDS
declare -A PID_TO_SEED
declare -A PID_TO_IDX

collect_finished_pid() {
    local pid="$1"
    local exit_code="$2"
    local seed="${PID_TO_SEED[${pid}]}"
    local idx="${PID_TO_IDX[${pid}]}"
    completed=$((completed + 1))

    if [[ ${exit_code} -eq 0 ]]; then
        passed=$((passed + 1))
        local status="PASS"
        if [[ "${KEEP_PASS}" != "1" ]]; then
            rm -f "${LOGDIR}/seed-${seed}.log"
        fi
    else
        failed=$((failed + 1))
        local status="FAIL"
        FAILED_SEEDS+=("${seed}")
    fi

    printf "[%d/%d] seed=%-20s %s\n" "${completed}" "${ITERATIONS}" "${seed}" "${status}"
}

# Launch jobs, respecting parallelism limit.
running_pids=()

for (( i=0; i<ITERATIONS; i++ )); do
    seed="${SEEDS[$i]}"
    log="${LOGDIR}/seed-${seed}.log"

    # Wait if at max parallelism.
    while (( ${#running_pids[@]} >= PARALLEL )); do
        # Poll for any finished PID.
        new_running=()
        found=0
        for rpid in "${running_pids[@]}"; do
            if ! kill -0 "${rpid}" 2>/dev/null; then
                set +e
                wait "${rpid}"
                ec=$?
                set -e
                collect_finished_pid "${rpid}" "${ec}"
                found=1
            else
                new_running+=("${rpid}")
            fi
        done
        running_pids=("${new_running[@]+"${new_running[@]}"}")
        if (( found == 0 )); then
            sleep 0.1
        fi
    done

    # Launch in background.
    (
        TAPI_TEST_SEED="${seed}" TAPI_WATCHDOG_SECS=8 FUZZ_VERBOSE="${VERBOSE}" \
            cargo test --lib ${CARGO_FLAGS} "${TEST_NAME}" -- --nocapture \
            >"${log}" 2>&1
    ) &
    local_pid=$!
    running_pids+=("${local_pid}")
    PID_TO_SEED[${local_pid}]="${seed}"
    PID_TO_IDX[${local_pid}]="${i}"
done

# Collect remaining jobs.
for rpid in "${running_pids[@]}"; do
    set +e
    wait "${rpid}"
    ec=$?
    set -e
    collect_finished_pid "${rpid}" "${ec}"
done

# --- Summary ---
echo ""
echo "================================"
echo "  SUMMARY"
echo "================================"
echo "Total:   ${ITERATIONS}"
echo "Passed:  ${passed}"
echo "Failed:  ${failed}"
echo ""

if (( failed > 0 )); then
    echo "Failed seeds:"
    for s in "${FAILED_SEEDS[@]}"; do
        echo "  ${s}  (log: ${LOGDIR}/seed-${s}.log)"
    done
    echo ""
    echo "Reproduce a failure:"
    echo "  TAPI_TEST_SEED=<seed> FUZZ_VERBOSE=1 cargo test --lib ${CARGO_FLAGS} ${TEST_NAME} -- --nocapture"
    echo ""
    echo "Logs preserved in: ${LOGDIR}"
    exit 1
else
    echo "All seeds passed."
    rm -f "${LOGDIR}/build.log"
    if [[ "${KEEP_PASS}" != "1" ]]; then
        rmdir "${LOGDIR}" 2>/dev/null || true
    fi
    exit 0
fi
