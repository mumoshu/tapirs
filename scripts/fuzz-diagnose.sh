#!/usr/bin/env bash
#
# Combined fuzz diagnostic: detects both correctness bugs and indeterminism
# in a single run by repeating the same seed multiple times.
#
# Usage:
#   ./scripts/fuzz-diagnose.sh
#   TAPI_TEST_SEED=42 TAPI_WATCHDOG_SECS=15 FUZZ_RUNS=20 ./scripts/fuzz-diagnose.sh
#
# Environment variables:
#   TAPI_TEST_SEED      - Random seed (default: 0)
#   TAPI_WATCHDOG_SECS  - Watchdog timeout in seconds (default: 30)
#   FUZZ_RUNS           - Number of repetitions (default: 10)
#   FUZZ_TEST_NAME      - Test function name (default: fuzz_tapir_transactions)
#   FUZZ_VERBOSE        - Pass through FUZZ_VERBOSE to test (default: 0)

set -euo pipefail

# --- Configuration ---
SEED="${TAPI_TEST_SEED:-0}"
WATCHDOG="${TAPI_WATCHDOG_SECS:-30}"
RUNS="${FUZZ_RUNS:-10}"
TEST_NAME="${FUZZ_TEST_NAME:-fuzz_tapir_transactions}"
VERBOSE="${FUZZ_VERBOSE:-0}"

echo "=== Fuzz Diagnostic ==="
echo "Seed:     ${SEED}"
echo "Watchdog: ${WATCHDOG}s"
echo "Runs:     ${RUNS}"
echo "Test:     ${TEST_NAME}"
echo ""

# --- Temp directory ---
LOGDIR=$(mktemp -d "/tmp/fuzz-diagnose-seed${SEED}-XXXXXX")
echo "Logs:     ${LOGDIR}"
echo ""

# --- Pre-compile to keep build output out of test logs ---
echo "Pre-compiling test binary (release)..."
if ! cargo test --lib --release --no-run 2>"${LOGDIR}/build.log"; then
    echo "ERROR: Build failed. See ${LOGDIR}/build.log"
    exit 1
fi
echo "Build successful."
echo ""

# --- Run loop ---
declare -i passed=0
declare -i failed=0
declare -i timed_out=0
declare -a summaries       # "attempted=N committed=M ..." per run
declare -a exit_codes
declare -a statuses        # PASS / FAIL / TIMEOUT

for (( i=1; i<=RUNS; i++ )); do
    run_log="${LOGDIR}/run-${i}.log"

    set +e
    TAPI_TEST_SEED="${SEED}" TAPI_WATCHDOG_SECS="${WATCHDOG}" FUZZ_VERBOSE="${VERBOSE}" \
        timeout "$((WATCHDOG + 10))s" \
        cargo test --lib --release "${TEST_NAME}" -- --nocapture \
        >"${run_log}" 2>&1
    ec=$?
    set -e

    exit_codes[${i}]=${ec}

    # Extract the summary line (attempted=N committed=M ...)
    summary=$(grep -oE 'attempted=[0-9]+ committed=[0-9]+ timed_out=[0-9]+' "${run_log}" | tail -1 || echo "")
    summaries[${i}]="${summary}"

    if [[ ${ec} -eq 0 ]]; then
        passed=$((passed + 1))
        statuses[${i}]="PASS"
    elif [[ ${ec} -eq 124 ]]; then
        timed_out=$((timed_out + 1))
        statuses[${i}]="TIMEOUT"
    else
        failed=$((failed + 1))
        statuses[${i}]="FAIL"
    fi

    printf "  Run %2d/%d: %-7s  exit=%-3d  %s\n" "${i}" "${RUNS}" "${statuses[${i}]}" "${ec}" "${summary}"
done

echo ""

# --- Collect unique summary signatures (for determinism check) ---
# A "signature" is the summary line, which captures attempted/committed/timed_out counts.
declare -A sig_counts      # signature -> count
declare -A sig_runs        # signature -> "1 2 3"

for (( i=1; i<=RUNS; i++ )); do
    sig="${summaries[${i}]:-<no-summary>}"
    if [[ -v "sig_counts[${sig}]" ]]; then
        sig_counts["${sig}"]=$(( sig_counts["${sig}"] + 1 ))
        sig_runs["${sig}"]+=" ${i}"
    else
        sig_counts["${sig}"]=1
        sig_runs["${sig}"]="${i}"
    fi
done

num_variants=${#sig_counts[@]}

# --- Detect correctness failures ---
has_correctness_bug=false
for (( i=1; i<=RUNS; i++ )); do
    if [[ "${statuses[${i}]}" == "FAIL" ]]; then
        # Check if it's a counter invariant violation (correctness bug)
        if grep -qE 'COUNTER INVARIANT|counter mismatch|panicked.*assert' "${LOGDIR}/run-${i}.log" 2>/dev/null; then
            has_correctness_bug=true
            break
        fi
    fi
done

# --- Summary ---
echo "================================"
echo "  RESULTS"
echo "================================"
echo ""
echo "Runs:      ${RUNS}"
echo "Passed:    ${passed}"
echo "Failed:    ${failed}"
echo "Timed out: ${timed_out}"
echo ""

# --- Per-signature breakdown ---
echo "--- Summary Signatures (${num_variants} variant(s)) ---"
echo ""
printf "  %-6s  %-50s  %s\n" "Count" "Signature" "Runs"
printf "  %-6s  %-50s  %s\n" "-----" "---------" "----"
for sig in "${!sig_counts[@]}"; do
    runs_display=$(echo "${sig_runs[${sig}]}" | sed 's/^ //;s/ /,/g')
    printf "  %-6d  %-50s  %s\n" "${sig_counts[${sig}]}" "${sig}" "${runs_display}"
done
echo ""

# --- Diagnosis ---
echo "================================"
echo "  DIAGNOSIS"
echo "================================"
echo ""

diagnosis_items=()

# Check correctness
if (( failed > 0 )); then
    if ${has_correctness_bug}; then
        diagnosis_items+=("CORRECTNESS BUG: ${failed} run(s) failed with invariant violations")
    else
        diagnosis_items+=("FAILURES: ${failed} run(s) failed (check logs for details)")
    fi
fi

if (( timed_out > 0 )); then
    diagnosis_items+=("TIMEOUTS: ${timed_out} run(s) timed out (possible hot loop or slow CI)")
fi

# Check determinism
if (( num_variants > 1 )); then
    diagnosis_items+=("INDETERMINISM: ${num_variants} distinct outcomes from same seed=${SEED}")
fi

if (( ${#diagnosis_items[@]} == 0 )); then
    echo "  ALL CLEAR: ${RUNS} runs with seed=${SEED} all passed with identical outcomes."
    echo ""
    echo "  Cleaning up logs..."
    rm -rf "${LOGDIR}"
    exit 0
fi

for item in "${diagnosis_items[@]}"; do
    echo "  * ${item}"
done
echo ""

# --- Next Actions ---
echo "================================"
echo "  NEXT ACTIONS"
echo "================================"
echo ""

if ${has_correctness_bug}; then
    # Find first failing run
    first_fail=""
    for (( i=1; i<=RUNS; i++ )); do
        if [[ "${statuses[${i}]}" == "FAIL" ]]; then
            first_fail="${i}"
            break
        fi
    done
    echo "  1. Inspect the failing run's full output:"
    echo "       less ${LOGDIR}/run-${first_fail}.log"
    echo ""
    echo "  2. Reproduce with verbose logging:"
    echo "       TAPI_TEST_SEED=${SEED} FUZZ_VERBOSE=1 cargo test --lib --release ${TEST_NAME} -- --nocapture"
    echo ""
fi

if (( timed_out > 0 )); then
    first_timeout=""
    for (( i=1; i<=RUNS; i++ )); do
        if [[ "${statuses[${i}]}" == "TIMEOUT" ]]; then
            first_timeout="${i}"
            break
        fi
    done
    echo "  - Inspect timed-out run (look for watchdog dump or last event):"
    echo "       less ${LOGDIR}/run-${first_timeout}.log"
    echo ""
    echo "  - Try increasing watchdog: TAPI_WATCHDOG_SECS=60 ./scripts/fuzz-diagnose.sh"
    echo ""
fi

if (( num_variants > 1 )); then
    echo "  - Indeterminism detected. Compare two runs with different outcomes:"

    # Find two runs with different signatures
    run_a=""
    run_b=""
    sig_a=""
    for sig in "${!sig_runs[@]}"; do
        first_run=$(echo "${sig_runs[${sig}]}" | awk '{print $1}')
        if [[ -z "${run_a}" ]]; then
            run_a="${first_run}"
            sig_a="${sig}"
        elif [[ "${sig}" != "${sig_a}" ]]; then
            run_b="${first_run}"
            break
        fi
    done

    if [[ -n "${run_a}" && -n "${run_b}" ]]; then
        echo "       diff <(grep -v '^\s*Finished' ${LOGDIR}/run-${run_a}.log) \\"
        echo "            <(grep -v '^\s*Finished' ${LOGDIR}/run-${run_b}.log) | head -60"
        echo ""
    fi

    echo "  - Run with verbose logging to capture structured event timeline:"
    echo "       TAPI_TEST_SEED=${SEED} FUZZ_VERBOSE=1 TAPI_WATCHDOG_SECS=${WATCHDOG} \\"
    echo "         cargo test --lib --release ${TEST_NAME} -- --nocapture > /tmp/fuzz-v1.txt 2>&1"
    echo "       # (repeat, then diff the two)"
    echo ""
    echo "  - For deeper investigation, use detect-fuzz-indeterminism.sh which"
    echo "    hashes filtered output and groups runs by behavioral equivalence class."
    echo ""
fi

echo "Logs preserved in: ${LOGDIR}"
exit 1
