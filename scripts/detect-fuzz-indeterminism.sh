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

# --- Filter function: strips cargo test harness lines ---
# These lines contain wall-clock timing or boilerplate that varies between runs
# even when the test itself is deterministic.
HARNESS_PATTERN='^(running [0-9]+ tests?|test .+ \.\.\. (ok|FAILED)|test result: .+|failures:|---- .+ ----)$'

filter_log() {
    grep -vE "${HARNESS_PATTERN}" "$1" || true
}

# --- Run loop ---
declare -A hash_to_runs    # hash -> "1 2 3"
declare -A hash_to_first   # hash -> first run number
declare -a run_hashes      # indexed by run number
declare -a run_exits       # indexed by run number

for (( i=1; i<=RUNS; i++ )); do
    run_log="${LOGDIR}/run-${i}.log"

    set +e
    TAPI_TEST_SEED="${SEED}" cargo test --lib "${TEST_NAME}" -- --nocapture \
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

printf "%-8s %-14s %-6s %s\n" "Variant" "Hash" "Count" "Runs"
printf "%-8s %-14s %-6s %s\n" "-------" "------------" "-----" "----"

variant_num=0
declare -a variant_filtered

for hash in "${!hash_to_runs[@]}"; do
    (( variant_num++ ))
    runs_str="${hash_to_runs[${hash}]}"
    count=$(echo "${runs_str}" | wc -w)
    short="${hash:0:12}"
    runs_display=$(echo "${runs_str}" | sed 's/^ //;s/ /,/g')

    printf "%-8d %-14s %-6d %s\n" "${variant_num}" "${short}" "${count}" "${runs_display}"

    # Create representative log and filtered version for this variant
    first_run="${hash_to_first[${hash}]}"
    cp "${LOGDIR}/run-${first_run}.log" "${LOGDIR}/variant-${variant_num}.log"
    filter_log "${LOGDIR}/run-${first_run}.log" > "${LOGDIR}/variant-${variant_num}.filtered"

    variant_filtered[${variant_num}]="${LOGDIR}/variant-${variant_num}.filtered"
done

echo ""
echo "Representative logs:"
for (( v=1; v<=variant_num; v++ )); do
    echo "  ${LOGDIR}/variant-${v}.log"
done
echo ""

# Diff first two variants (filtered to remove harness noise)
echo "--- Diff (variant 1 vs variant 2, filtered) ---"
diff -u "${variant_filtered[1]}" "${variant_filtered[2]}" | head -80 || true
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
echo "  1. Re-run with RUST_LOG=debug:"
echo "       FUZZ_SEED=${SEED} RUST_LOG=debug cargo test --lib ${TEST_NAME} -- --nocapture"
echo ""
echo "  2. Add eprintln! at suspected divergence points to narrow where output first differs."
echo ""
echo "  3. Reduce iterations_per_client or num_clients for a minimal reproduction."
echo ""
echo "  4. Full diff:"
echo "       diff -u ${LOGDIR}/variant-1.log ${LOGDIR}/variant-2.log | less"
echo ""
echo "Logs preserved in: ${LOGDIR}"
exit 1
