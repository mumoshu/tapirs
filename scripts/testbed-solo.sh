#!/usr/bin/env bash
#
# testbed-solo.sh — Create or destroy a standalone single-shard TAPIR cluster.
#
# Usage:
#   scripts/testbed-solo.sh up      Build binaries, create cluster, smoke-test, print guide
#   scripts/testbed-solo.sh down    Tear down all testbed processes and temp files
#
# The cluster runs 3 nodes locally (no Docker), each hosting 1 replica of
# shard 0 with static membership. No discovery service or shard-manager
# required — all replicas know the full membership from their config files.
#
# This testbed is for developing and testing the TAPIR-based discovery store.
# For the full multi-shard production-like stack, use scripts/testbed-docker-compose.sh.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
WORK_DIR="/tmp/tapi-solo"
PID_FILE="${WORK_DIR}/pids"

# Ports
ADMIN_PORTS=(9011 9012 9013)
TAPIR_PORTS=(6000 6001 6002)

# ---------------------------------------------------------------------------
# Colors (disabled when stdout is not a TTY)
# ---------------------------------------------------------------------------
if [[ -t 1 ]]; then
    BOLD='\033[1m'  GREEN='\033[32m'  YELLOW='\033[33m'
    RED='\033[31m'  CYAN='\033[36m'   DIM='\033[2m'
    RESET='\033[0m'
else
    BOLD='' GREEN='' YELLOW='' RED='' CYAN='' DIM='' RESET=''
fi

step()  { printf "\n${BOLD}${CYAN}==> %s${RESET}\n" "$1"; }
info()  { printf "${DIM}    %s${RESET}\n" "$1"; }
ok()    { printf "${GREEN}    OK: %s${RESET}\n" "$1"; }
warn()  { printf "${YELLOW}    WARN: %s${RESET}\n" "$1"; }
fail()  { printf "${RED}ERROR: %s${RESET}\n" "$1" >&2; exit 1; }

run_cmd() {
    printf "${YELLOW}    \$ %s${RESET}\n" "$*"
    "$@"
}

separator() {
    printf "\n${DIM}%s${RESET}\n" "────────────────────────────────────────────────────────────"
}

# Run a one-shot client transaction against the solo cluster.
run_client() {
    run_cmd "${TAPI}" client --config "${WORK_DIR}/client.toml" -e "$1"
}

# Run a client transaction and capture stdout+stderr for verification.
run_client_capture() {
    "${TAPI}" client --config "${WORK_DIR}/client.toml" -e "$1" 2>&1
}

# ---------------------------------------------------------------------------
# Locate or build binaries
# ---------------------------------------------------------------------------
TAPI=""
TAPICTL=""

find_or_build_binary() {
    step "Locating tapi and tapictl binaries..."

    for dir in "${PROJECT_ROOT}/target/release" "${PROJECT_ROOT}/target/debug"; do
        if [[ -x "${dir}/tapi" ]] && [[ -x "${dir}/tapictl" ]]; then
            TAPI="${dir}/tapi"
            TAPICTL="${dir}/tapictl"
            ok "Found ${TAPI} and ${TAPICTL}"
            return
        fi
    done

    info "Not found. Building (this may take a few minutes)..."
    run_cmd cargo build --release --bin tapi --bin tapictl --manifest-path "${PROJECT_ROOT}/Cargo.toml"
    TAPI="${PROJECT_ROOT}/target/release/tapi"
    TAPICTL="${PROJECT_ROOT}/target/release/tapictl"
    ok "Binaries built."
}

# ---------------------------------------------------------------------------
# Check ports
# ---------------------------------------------------------------------------
check_ports() {
    local all_ports=("${ADMIN_PORTS[@]}" "${TAPIR_PORTS[@]}")
    local busy=()

    for port in "${all_ports[@]}"; do
        if command -v ss &>/dev/null; then
            if ss -tlnH 2>/dev/null | grep -qE ":${port}\b"; then
                busy+=("${port}")
            fi
        elif command -v lsof &>/dev/null; then
            if lsof -iTCP:"${port}" -sTCP:LISTEN -P -n &>/dev/null; then
                busy+=("${port}")
            fi
        fi
    done

    if (( ${#busy[@]} > 0 )); then
        fail "Port(s) ${busy[*]} already in use. Run 'scripts/testbed-solo.sh down' first."
    fi
}

# ---------------------------------------------------------------------------
# Generate configs
# ---------------------------------------------------------------------------
generate_configs() {
    step "Generating configs in ${WORK_DIR}..."
    mkdir -p "${WORK_DIR}"

    local membership="[\"127.0.0.1:${TAPIR_PORTS[0]}\", \"127.0.0.1:${TAPIR_PORTS[1]}\", \"127.0.0.1:${TAPIR_PORTS[2]}\"]"

    for i in 0 1 2; do
        local node_num=$((i + 1))
        cat > "${WORK_DIR}/node${node_num}.toml" <<TOML
admin_listen_addr = "127.0.0.1:${ADMIN_PORTS[$i]}"
persist_dir = "${WORK_DIR}/data${node_num}"

[[replicas]]
shard = 0
listen_addr = "127.0.0.1:${TAPIR_PORTS[$i]}"
membership = ${membership}
TOML
        info "node${node_num}.toml  admin=${ADMIN_PORTS[$i]}  tapir=${TAPIR_PORTS[$i]}"
    done

    # Client config
    cat > "${WORK_DIR}/client.toml" <<TOML
[[shards]]
id = 0
replicas = ["127.0.0.1:${TAPIR_PORTS[0]}", "127.0.0.1:${TAPIR_PORTS[1]}", "127.0.0.1:${TAPIR_PORTS[2]}"]
TOML
    info "client.toml"
}

# ---------------------------------------------------------------------------
# Start / stop nodes
# ---------------------------------------------------------------------------
start_nodes() {
    step "Starting 3 nodes..."
    > "${PID_FILE}"

    for i in 0 1 2; do
        local node_num=$((i + 1))
        local log="${WORK_DIR}/node${node_num}.log"
        mkdir -p "${WORK_DIR}/data${node_num}"

        "${TAPI}" node --config "${WORK_DIR}/node${node_num}.toml" \
            > "${log}" 2>&1 &
        local pid=$!
        echo "${pid}" >> "${PID_FILE}"
        info "node${node_num} started (pid=${pid}, log=${log})"
    done

    # Wait for nodes to bind their ports.
    info "Waiting for nodes to start..."
    local max_wait=10
    local waited=0
    while (( waited < max_wait )); do
        local ready=0
        for port in "${TAPIR_PORTS[@]}"; do
            if command -v ss &>/dev/null; then
                ss -tlnH 2>/dev/null | grep -qE ":${port}\b" && (( ready++ )) || true
            elif command -v lsof &>/dev/null; then
                lsof -iTCP:"${port}" -sTCP:LISTEN -P -n &>/dev/null && (( ready++ )) || true
            else
                (( ready++ ))  # Can't check, assume ready
            fi
        done
        if (( ready >= 3 )); then
            break
        fi
        sleep 1
        (( waited++ ))
    done

    if (( waited >= max_wait )); then
        warn "Timed out waiting for all nodes to start. Check logs in ${WORK_DIR}/"
    else
        ok "All 3 nodes listening."
    fi
}

stop_nodes() {
    if [[ -f "${PID_FILE}" ]]; then
        while read -r pid; do
            if kill -0 "${pid}" 2>/dev/null; then
                kill "${pid}" 2>/dev/null || true
                info "Stopped pid ${pid}"
            fi
        done < "${PID_FILE}"
        rm -f "${PID_FILE}"
    fi
}

# ---------------------------------------------------------------------------
# Smoke test
# ---------------------------------------------------------------------------
smoke_test() {
    step "Running smoke test..."

    info "Writing key 'hello' = 'world'..."
    local output
    output=$("${TAPI}" client --config "${WORK_DIR}/client.toml" \
        -e "begin; put hello world; commit" 2>&1) || true

    if echo "${output}" | grep -qi "committed\|ok"; then
        ok "Write committed."
    else
        warn "Write result: ${output}"
    fi

    info "Reading key 'hello'..."
    output=$("${TAPI}" client --config "${WORK_DIR}/client.toml" \
        -e "begin ro; get hello; abort" 2>&1) || true

    if echo "${output}" | grep -q "world"; then
        ok "Read returned 'world'."
    else
        warn "Read result: ${output}"
    fi

    ok "Smoke test done."
}

# ---------------------------------------------------------------------------
# Demo scenarios (matching testbed-docker-compose.sh where applicable)
# ---------------------------------------------------------------------------
demo_delete() {
    step "Demo: Delete operation..."
    info "Deleting key 'hello' (written by smoke test)..."
    run_client "begin; delete hello; commit"

    info "Verifying 'hello' is deleted (RO validated quorum read)..."
    local output
    output=$(run_client_capture "begin ro; get hello") || true
    if echo "${output}" | grep -q "(not found)"; then
        ok "Key 'hello' confirmed deleted."
    else
        warn "Expected '(not found)', got: ${output}"
    fi
    ok "Delete demo complete."
}

demo_seed_data() {
    step "Seeding data for demo scenarios..."
    info "Putting 6 keys: alice=100, bob=200, charlie=300, grapes=fruit, mango=tropical, orange=citrus"
    run_client "begin; put alice 100; put bob 200; put charlie 300; put grapes fruit; put mango tropical; put orange citrus; commit"
    ok "Data seeded (6 keys in shard 0)."
}

demo_multi_key_txns() {
    step "Demo: Multi-key transactions..."
    info "RO multi-key read (alice + orange)..."
    run_client "begin ro; get alice; get orange"
    info "RW multi-key write (fruit=apple, snack=pretzel)..."
    run_client "begin; put fruit apple; put snack pretzel; commit"
    ok "Multi-key transactions complete."
}

demo_range_scan() {
    step "Demo: Range scan..."
    info "Scanning all keys from 'a' to 'z' (RO validated quorum read)..."
    run_client "begin ro; scan a z"
    ok "Range scan complete."
}

demo_admin_status() {
    step "Querying node1 admin status..."
    run_cmd "${TAPI}" admin status --admin-listen-addr "127.0.0.1:${ADMIN_PORTS[0]}"
}

demo_view_change() {
    step "Demo: Triggering a view change on shard 0..."
    info "View changes synchronize the IR consensus record across replicas."
    run_cmd "${TAPI}" admin view-change \
        --admin-listen-addr "127.0.0.1:${ADMIN_PORTS[0]}" --shard 0
    info "Waiting for view change to settle (3s)..."
    sleep 3
    info "Verifying data is still accessible after view change..."
    run_client "begin ro; get alice"
    ok "View change complete. Data intact."
}

demo_backup() {
    step "Demo: Solo backup..."
    local backup_dir="${WORK_DIR}/backup"
    local admin_addrs="127.0.0.1:${ADMIN_PORTS[0]},127.0.0.1:${ADMIN_PORTS[1]},127.0.0.1:${ADMIN_PORTS[2]}"

    run_cmd "${TAPICTL}" solo backup cluster \
        --admin-addrs "${admin_addrs}" --output "${backup_dir}"

    if [[ -f "${backup_dir}/cluster.json" ]] && [[ -f "${backup_dir}/shard_0_delta_0.bin" ]]; then
        ok "Backup created: cluster.json + shard_0_delta_0.bin"
    else
        warn "Expected backup files not found in ${backup_dir}"
        ls -la "${backup_dir}" 2>/dev/null || true
    fi
}

# ---------------------------------------------------------------------------
# Getting-started guide
# ---------------------------------------------------------------------------
print_guide() {
    separator
    printf "\n${BOLD}    Solo TAPIR Testbed — Getting Started Guide${RESET}\n"
    separator

    cat <<EOF

  Node admin ports: 127.0.0.1:${ADMIN_PORTS[0]}, 127.0.0.1:${ADMIN_PORTS[1]}, 127.0.0.1:${ADMIN_PORTS[2]}
  TAPIR ports:      127.0.0.1:${TAPIR_PORTS[0]}, 127.0.0.1:${TAPIR_PORTS[1]}, 127.0.0.1:${TAPIR_PORTS[2]}

${BOLD}1. INTERACTIVE REPL${RESET}

   Start an interactive client session:

     ${TAPI} client --config ${WORK_DIR}/client.toml

   Then at the tapi> prompt:

     begin
     put alice 100
     put bob 200
     commit
     begin ro
     get alice
     get bob
     begin ro
     scan a z
     begin
     delete alice
     commit

   Type 'help' for all commands, 'quit' to exit.
   Read-only transactions don't need explicit abort.

${BOLD}2. SCRIPTED TRANSACTIONS${RESET}

   Run one-liner transactions (no interactive session needed):

     ${TAPI} client --config ${WORK_DIR}/client.toml \\
       -e "begin; put counter 42; commit"

     ${TAPI} client --config ${WORK_DIR}/client.toml \\
       -e "begin ro; get counter"

   Multi-key transaction (all keys in shard 0):

     ${TAPI} client --config ${WORK_DIR}/client.toml \\
       -e "begin; put apple fruit; put orange citrus; commit"

   Run a command file (one command per line):

     ${TAPI} client --config ${WORK_DIR}/client.toml \\
       -s /path/to/script.txt

${BOLD}3. CLUSTER INSPECTION${RESET}

     ${TAPI} admin status --admin-listen-addr 127.0.0.1:${ADMIN_PORTS[0]}
     ${TAPI} admin status --admin-listen-addr 127.0.0.1:${ADMIN_PORTS[1]}
     ${TAPI} admin status --admin-listen-addr 127.0.0.1:${ADMIN_PORTS[2]}

${BOLD}4. VIEW CHANGES${RESET}

   Trigger a view change to synchronize the IR consensus record:

     ${TAPI} admin view-change \\
       --admin-listen-addr 127.0.0.1:${ADMIN_PORTS[0]} --shard 0

${BOLD}5. BACKUP & RESTORE${RESET}

   Back up all shards via direct node access (no shard manager needed):

     ${TAPICTL} solo backup cluster \\
       --admin-addrs 127.0.0.1:${ADMIN_PORTS[0]},127.0.0.1:${ADMIN_PORTS[1]},127.0.0.1:${ADMIN_PORTS[2]} \\
       --output /path/to/backup

   Restore from backup to target nodes:

     ${TAPICTL} solo restore cluster \\
       --admin-addrs 127.0.0.1:${ADMIN_PORTS[0]},127.0.0.1:${ADMIN_PORTS[1]},127.0.0.1:${ADMIN_PORTS[2]} \\
       --input /path/to/backup

${BOLD}6. SOLO CLONE (BLUE-GREEN COMPACTION)${RESET}

   Clone shard 0 from this cluster to a second solo cluster.

   1. Start a second solo cluster on different ports (edit configs).
   2. Clone shard 0:

     tapictl solo clone \\
       --source-nodes-admin-addrs 127.0.0.1:${ADMIN_PORTS[0]},127.0.0.1:${ADMIN_PORTS[1]},127.0.0.1:${ADMIN_PORTS[2]} \\
       --source-shard 0 \\
       --dest-nodes-admin-addrs 127.0.0.1:9021,127.0.0.1:9022,127.0.0.1:9023 \\
       --dest-shard 0 \\
       --dest-base-port 7000

   3. Switch clients to the new cluster, tear down the old one.

${BOLD}7. LIMITATIONS${RESET}

   The solo testbed runs a single shard with static membership. The
   following operations require the full multi-shard stack (shard-manager,
   discovery service) provided by scripts/testbed-docker-compose.sh:

     - Add/remove nodes (dynamic membership changes)
     - Shard split / merge / compact
     - Cross-shard transactions (requires 2+ shards)

${BOLD}8. TEARDOWN${RESET}

     scripts/testbed-solo.sh down

EOF
}

# ---------------------------------------------------------------------------
# up
# ---------------------------------------------------------------------------
cmd_up() {
    check_ports
    find_or_build_binary
    generate_configs
    start_nodes
    smoke_test
    demo_delete
    demo_seed_data
    demo_multi_key_txns
    demo_range_scan
    demo_admin_status
    demo_view_change
    demo_backup
    print_guide
}

# ---------------------------------------------------------------------------
# down
# ---------------------------------------------------------------------------
cmd_down() {
    step "Tearing down solo testbed..."
    stop_nodes

    if [[ -d "${WORK_DIR}" ]]; then
        rm -rf "${WORK_DIR}"
        ok "Removed ${WORK_DIR}"
    else
        info "No testbed found to tear down."
    fi

    ok "Solo testbed removed."
}

# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------
usage() {
    printf "Usage: %s <command>\n\n" "$(basename "$0")"
    printf "Commands:\n"
    printf "  up      Create solo cluster, run smoke test, print getting-started guide\n"
    printf "  down    Tear down all testbed processes and temp files\n"
    exit 1
}

case "${1:-}" in
    up)   cmd_up   ;;
    down) cmd_down ;;
    *)    usage    ;;
esac
