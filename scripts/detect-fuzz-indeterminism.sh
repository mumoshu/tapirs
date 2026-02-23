#!/usr/bin/env bash
#
# Detect indeterminism in fuzz tests by running them multiple times with the
# same seed and comparing output hashes.
#
# Usage:
#   ./scripts/detect-fuzz-indeterminism.sh
#   FUZZ_SEED=12345 FUZZ_RUNS=10 ./scripts/detect-fuzz-indeterminism.sh
#
# Environment variables:
#   FUZZ_SEED             - Random seed (default: randomly generated)
#   FUZZ_RUNS             - Number of repetitions (default: 5)
#   FUZZ_TEST_NAME        - Test function name (default: fuzz_tapir_transactions)
#   FUZZ_STOP_ON_DETECTED - If set to 1, stop immediately on first divergence (default: 0)

set -euo pipefail

# --- Configuration ---
SEED="${FUZZ_SEED:-$(od -An -tu8 -N8 /dev/urandom | tr -d ' ')}"
RUNS="${FUZZ_RUNS:-5}"
TEST_NAME="${FUZZ_TEST_NAME:-fuzz_tapir_transactions}"
STOP_ON_DETECTED="${FUZZ_STOP_ON_DETECTED:-0}"

echo "=== Fuzz Determinism Detector ==="
echo "Seed:  ${SEED}"
echo "Runs:  ${RUNS}"
echo "Test:  ${TEST_NAME}"
echo ""

# --- Temp directory (use LOGDIR to avoid shadowing system TMPDIR) ---
LOGDIR=$(mktemp -d "/tmp/fuzz-determinism-${SEED}-XXXXXX")
echo "Logs:  ${LOGDIR}"
echo ""

# --- Pre-compile to keep build output out of test logs ---
echo "Pre-compiling test binary..."
if ! cargo test --lib --no-run 2>"${LOGDIR}/build.log"; then
    echo "ERROR: Build failed. See ${LOGDIR}/build.log"
    exit 1
fi
echo "Build successful."
echo ""

# --- Filter function: strips lines that may vary between runs ---
# Categories:
#   1. Cargo test harness lines (running N tests, test result, etc.)
#   2. Cargo build output that leaks through (Compiling, Finished with timing, etc.)
FILTER_PATTERNS=(
    # Test harness
    '^running [0-9]+ tests?'
    '^test .+ \.\.\. (ok|FAILED)'
    '^test result: '
    '^failures:'
    '^---- .+ ----'
    # Cargo build output (timing varies between runs)
    '^\s*Compiling '
    '^\s*Finished '
    '^\s*Downloading '
    '^\s*Downloaded '
    '^\s*Fresh '
    '^\s*Dirty '
    'target\(s\) in [0-9]'
)

FILTER_REGEX=$(IFS='|'; echo "${FILTER_PATTERNS[*]}")

filter_log() {
    grep -vE "${FILTER_REGEX}" "$1" || true
}

# --- Run loop ---
declare -A hash_to_runs    # hash -> "1 2 3"
declare -A hash_to_first   # hash -> first run number
declare -a run_hashes      # indexed by run number
declare -a run_exits       # indexed by run number

for (( i=1; i<=RUNS; i++ )); do
    run_log="${LOGDIR}/run-${i}.log"

    set +e
    TAPI_TEST_SEED="${SEED}" TAPI_WATCHDOG_SECS=8 cargo test --lib "${TEST_NAME}" -- --nocapture \
        >"${run_log}" 2>&1
    exit_code=$?
    set -e

    run_exits[${i}]=${exit_code}

    # Hash meaningful output + exit code
    run_hash=$( (echo "EXIT:${exit_code}"; filter_log "${run_log}") \
        | sha256sum | awk '{print $1}' )
    run_hashes[${i}]="${run_hash}"

    short_hash="${run_hash:0:12}"
    status="PASS"
    [[ ${exit_code} -ne 0 ]] && status="FAIL"

    printf "Run %d/%d: %-4s  hash=%s\n" "${i}" "${RUNS}" "${status}" "${short_hash}"

    # Group by hash
    if [[ -v "hash_to_runs[${run_hash}]" ]]; then
        hash_to_runs["${run_hash}"]+=" ${i}"
    else
        hash_to_runs["${run_hash}"]="${i}"
        hash_to_first["${run_hash}"]="${i}"
    fi

    # Early exit: stop as soon as we see a second distinct hash
    if [[ "${STOP_ON_DETECTED}" == "1" ]] && (( ${#hash_to_runs[@]} > 1 )); then
        echo ""
        echo "Indeterminism detected on run ${i}, stopping early (FUZZ_STOP_ON_DETECTED=1)."
        RUNS="${i}"
        break
    fi
done

echo ""

# --- Analysis ---
num_variants=${#hash_to_runs[@]}

if (( num_variants == 1 )); then
    # --- DETERMINISTIC ---
    echo "================================"
    echo "  DETERMINISTIC"
    echo "================================"
    echo ""
    echo "All ${RUNS} runs produced identical meaningful output."
    the_hash="${run_hashes[1]}"
    echo "Hash: ${the_hash:0:12}"
    echo "Exit: ${run_exits[1]}"
    echo ""
    echo "Cleaning up ${LOGDIR}"
    rm -rf "${LOGDIR}"
    exit 0
fi

# --- INDETERMINISM DETECTED ---
echo "================================"
echo "  INDETERMINISM DETECTED"
echo "================================"
echo ""
echo "Variants: ${num_variants}"
echo "Runs:     ${RUNS}"
echo "Seed:     ${SEED}"
echo ""

printf "%-14s %-6s %s\n" "Result Hash" "Count" "Runs"
printf "%-14s %-6s %s\n" "------------" "-----" "----"

# Organize logs into per-hash directories and build summary table
declare -a hash_dirs  # ordered list of per-hash directory paths

for hash in "${!hash_to_runs[@]}"; do
    runs_str="${hash_to_runs[${hash}]}"
    count=$(echo "${runs_str}" | wc -w)
    short="${hash:0:12}"
    runs_display=$(echo "${runs_str}" | sed 's/^ //;s/ /,/g')

    printf "%-14s %-6d %s\n" "${short}" "${count}" "${runs_display}"

    # Create per-hash directory and move matching run logs into it
    hash_dir="${LOGDIR}/${short}"
    mkdir -p "${hash_dir}"
    for run_num in ${runs_str}; do
        mv "${LOGDIR}/run-${run_num}.log" "${hash_dir}/run-${run_num}.log"
    done
    # Create filtered version from the first run's log
    first_run="${hash_to_first[${hash}]}"
    filter_log "${hash_dir}/run-${first_run}.log" > "${hash_dir}/filtered.log"

    hash_dirs+=("${hash_dir}")
done

echo ""
echo "Log directories:"
for dir in "${hash_dirs[@]}"; do
    echo "  ${dir}/"
done
echo ""

# Diff first two variants (filtered to remove harness noise)
echo "--- Diff (filtered) ---"
diff -u "${hash_dirs[0]}/filtered.log" "${hash_dirs[1]}/filtered.log" | head -80 || true
echo ""

cat <<GUIDANCE
--- Investigation Guidance ---

Common sources of indeterminism in Tokio (current_thread + start_paused=true):

  1. HashMap iteration order — HashMap uses RandomState by default.
     Even with a seeded StdRng, HashMap::new() gets random SipHasher keys.
     Fix: Use BTreeMap, IndexMap, or HashMap with a deterministic hasher.

  2. tokio::select! — when multiple branches are ready simultaneously,
     select! uses thread_rng() to pick which branch runs first.
     Fix: Use biased; in select!, or restructure to avoid simultaneous readiness.

  3. FuturesUnordered / JoinSet — task completion order may vary even
     with deterministic time if tasks yield at the same simulated instant.
     Fix: Use ordered join (join_all preserves order) or sequentialize.

  4. Arc<AtomicU64> with Relaxed ordering — interleaving of fetch_add
     across concurrent tasks depends on scheduler decisions.

  5. Channel receive ordering — when multiple senders enqueue concurrently,
     receive order may not be deterministic across runs.

GUIDANCE

echo "Debug steps:"
echo "  1. Re-run with FUZZ_VERBOSE=1 for structured event timeline:"
echo "       TAPI_TEST_SEED=${SEED} FUZZ_VERBOSE=1 cargo test --lib ${TEST_NAME} -- --nocapture"
echo ""
echo "  2. Re-run with RUST_LOG=debug for protocol-level tracing:"
echo "       TAPI_TEST_SEED=${SEED} RUST_LOG=debug cargo test --lib ${TEST_NAME} -- --nocapture"
echo ""
echo "  3. Reduce iterations_per_client or num_clients for a minimal reproduction."
echo ""
echo "  4. Full diff (pick any run log from each hash directory):"
echo "       diff -u ${hash_dirs[0]}/filtered.log ${hash_dirs[1]}/filtered.log | less"
echo ""
echo "Logs preserved in: ${LOGDIR}"
exit 1
