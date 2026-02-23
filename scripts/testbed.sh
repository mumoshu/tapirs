#!/usr/bin/env bash
#
# testbed.sh — Create or destroy a demo TAPIR cluster.
#
# Usage:
#   scripts/testbed.sh up      Build, create cluster, run full demo, print guide
#   scripts/testbed.sh down    Tear down all testbed resources
#
# Two-tier deployment:
#   Tier 1 — Discovery store: 3-node single-shard TAPIR cluster (static membership)
#   Tier 2 — Main cluster: 2 shards x 3 replicas across 3 nodes
#
# Services: shard-manager (port 9001), node admin ports 9011-9013.
# Discovery store is internal (no host port mapping).
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
WORK_DIR="${PROJECT_ROOT}/.tapiadm"

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

# Print a command then execute it.
run_cmd() {
    printf "${YELLOW}    \$ %s${RESET}\n" "$*"
    "$@"
}

separator() {
    printf "\n${DIM}%s${RESET}\n" "────────────────────────────────────────────────────────────"
}

# ---------------------------------------------------------------------------
# Locate or build the three CLI binaries
# ---------------------------------------------------------------------------
TAPIADM="" TAPI="" TAPICTL=""

find_or_build_binaries() {
    step "Locating CLI binaries..."

    for dir in "${PROJECT_ROOT}/target/release" "${PROJECT_ROOT}/target/debug"; do
        if [[ -x "${dir}/tapiadm" && -x "${dir}/tapi" && -x "${dir}/tapictl" ]]; then
            TAPIADM="${dir}/tapiadm"
            TAPI="${dir}/tapi"
            TAPICTL="${dir}/tapictl"
            ok "Found in ${dir}"
            return
        fi
    done

    info "Not found in target/. Building (this may take a few minutes on first run)..."
    run_cmd cargo build --release --no-default-features \
        --bin tapiadm --bin tapi --bin tapictl \
        --manifest-path "${PROJECT_ROOT}/Cargo.toml"

    TAPIADM="${PROJECT_ROOT}/target/release/tapiadm"
    TAPI="${PROJECT_ROOT}/target/release/tapi"
    TAPICTL="${PROJECT_ROOT}/target/release/tapictl"
    ok "Binaries built."
}

# ---------------------------------------------------------------------------
# Check that required host ports are free
# ---------------------------------------------------------------------------
check_ports() {
    local ports=(9001 9011 9012 9013)
    local busy=()

    for port in "${ports[@]}"; do
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
        fail "Port(s) ${busy[*]} already in use. Run 'scripts/testbed.sh down' first, or free them manually."
    fi
}

# Run a one-shot client transaction via the Docker client container.
# Uses --discovery-tapir-endpoint so the client reads fresh shard topology
# (membership + key ranges) from the TAPIR discovery cluster at startup.
run_client() {
    run_cmd docker compose -f "${WORK_DIR}/docker-compose.yml" \
        --profile tools run --rm -T client \
        client --discovery-tapir-endpoint "172.28.0.4:6000,172.28.0.5:6000,172.28.0.6:6000" \
        -e "$1"
}

# ---------------------------------------------------------------------------
# up
# ---------------------------------------------------------------------------
cmd_up() {
    check_ports
    find_or_build_binaries

    # --- Create the cluster ---
    step "Creating TAPIR cluster (2 shards x 3 replicas)..."
    info "Builds Docker image, starts 5 containers, bootstraps replicas."
    info "First run takes several minutes (Rust compile inside Docker)."
    info "Subsequent runs reuse the cached image layer."
    (cd "${PROJECT_ROOT}" && run_cmd "${TAPIADM}" docker up)

    # --- Verify cluster health ---
    step "Verifying cluster health..."
    (cd "${PROJECT_ROOT}" && run_cmd "${TAPIADM}" docker get nodes)
    printf "\n"
    (cd "${PROJECT_ROOT}" && run_cmd "${TAPIADM}" docker get replicas)

    # --- Smoke test ---
    step "Running smoke test..."
    info "Writing key 'hello' with value 'world'..."
    run_client "begin; put hello world; commit"

    info "Reading key 'hello' back..."
    run_client "begin ro; get hello"
    ok "Smoke test passed."

    # --- Delete ---
    step "Demo: Delete operation..."
    info "Deleting key 'hello'..."
    run_client "begin; delete hello; commit"
    info "Verifying 'hello' is deleted (RO validated quorum read)..."
    run_client "begin ro; get hello"
    ok "Delete demo complete."

    # --- Seed data ---
    step "Seeding data for demo scenarios..."
    info "Shard 0 keys (below future split point 'g'): alice, bob, charlie"
    info "Shard 0 keys (above 'g'): grapes, mango"
    info "Shard 1 keys (>= 'n'): orange, pear, zebra"
    run_client "begin; put alice 100; put bob 200; put charlie 300; put grapes fruit; put mango tropical; commit"
    run_client "begin; put orange citrus; put pear green; put zebra animal; commit"
    ok "Data seeded (8 keys across 2 shards)."

    # --- Cross-shard transaction ---
    step "Demo: Cross-shard transaction..."
    info "RO read across shards (alice=shard0, orange=shard1)..."
    run_client "begin ro; get alice; get orange"
    info "RW write across shards (fruit→shard0, snack→shard1)..."
    run_client "begin; put fruit apple; put snack pretzel; commit"
    ok "Cross-shard transactions complete."

    # --- Range scan ---
    step "Demo: Range scan..."
    info "Scanning all keys from 'a' to 'z' (RO validated quorum read across both shards)..."
    run_client "begin ro; scan a z"
    ok "Range scan complete."

    # --- Admin status ---
    step "Querying node1 admin status..."
    run_cmd "${TAPI}" admin status --admin-listen-addr 127.0.0.1:9011

    # --- Add 4th node ---
    step "Demo: Adding a 4th node to the cluster..."
    (cd "${PROJECT_ROOT}" && run_cmd "${TAPIADM}" docker add node --name node4)

    info "Fetching node4 details..."
    NODE4_LINE=$( (cd "${PROJECT_ROOT}" && "${TAPIADM}" docker get nodes 2>/dev/null) | grep "^node4 ")
    NODE4_IP=$(echo "${NODE4_LINE}" | awk '{print $2}')
    NODE4_HOST_ADMIN=$(echo "${NODE4_LINE}" | awk '{print $3}')
    NODE4_ADMIN_PORT="${NODE4_HOST_ADMIN##*:}"
    info "node4: IP=${NODE4_IP}, admin=127.0.0.1:${NODE4_ADMIN_PORT}"

    info "Adding shard 0 replica on node4..."
    run_cmd "${TAPI}" admin add-replica \
        --admin-listen-addr "127.0.0.1:${NODE4_ADMIN_PORT}" \
        --shard 0 --listen-addr "${NODE4_IP}:6000"
    info "Waiting for view change settlement (5s)..."
    sleep 5

    info "Adding shard 1 replica on node4..."
    run_cmd "${TAPI}" admin add-replica \
        --admin-listen-addr "127.0.0.1:${NODE4_ADMIN_PORT}" \
        --shard 1 --listen-addr "${NODE4_IP}:6001"
    info "Waiting for view change settlement (5s)..."
    sleep 5

    info "Verifying cluster with 4 nodes..."
    (cd "${PROJECT_ROOT}" && run_cmd "${TAPIADM}" docker get nodes)
    printf "\n"
    (cd "${PROJECT_ROOT}" && run_cmd "${TAPIADM}" docker get replicas)
    ok "Node4 added with replicas for both shards."

    # --- View change ---
    step "Demo: Triggering a view change on shard 0..."
    info "View changes synchronize the IR consensus record across replicas."
    run_cmd "${TAPI}" admin view-change \
        --admin-listen-addr 127.0.0.1:9011 --shard 0
    info "Waiting for view change to settle (5s)..."
    sleep 5
    info "Verifying data is still accessible after view change..."
    run_client "begin ro; get alice"
    ok "View change complete. Data intact."

    # --- Shard split ---
    step "Demo: Splitting shard 0 at key 'g' into new shard 2..."
    info "Shard 0 currently handles keys < 'n'."
    info "After split: shard 0 → keys < 'g', shard 2 → keys ['g', 'n')."

    info "Creating shard 2 replicas on existing nodes (port 6002)..."
    run_cmd "${TAPI}" admin add-replica \
        --admin-listen-addr 127.0.0.1:9011 \
        --shard 2 --listen-addr 172.28.0.11:6002
    info "Waiting for initial replica (3s)..."
    sleep 3

    run_cmd "${TAPI}" admin add-replica \
        --admin-listen-addr 127.0.0.1:9012 \
        --shard 2 --listen-addr 172.28.0.12:6002
    info "Waiting for view change settlement (5s)..."
    sleep 5

    run_cmd "${TAPI}" admin add-replica \
        --admin-listen-addr 127.0.0.1:9013 \
        --shard 2 --listen-addr 172.28.0.13:6002
    info "Waiting for view change settlement (5s)..."
    sleep 5

    info "Executing shard split via shard-manager..."
    run_cmd "${TAPICTL}" split shard \
        --source 0 --split-key g --new-shard 2 \
        --new-replicas 172.28.0.11:6002,172.28.0.12:6002,172.28.0.13:6002
    ok "Shard split complete."

    # --- Verify post-split ---
    step "Verifying data after shard split..."
    info "Each client invocation reads latest key ranges from TAPIR discovery at startup."

    info "Reading 'alice' (a < g → still on shard 0)..."
    run_client "begin ro; get alice"

    info "Reading 'grapes' (g >= g → now routes to shard 2 via TAPIR discovery key ranges)..."
    run_client "begin ro; get grapes"

    info "Reading 'orange' (o >= n → shard 1, unaffected by split)..."
    run_client "begin ro; get orange"

    info "Full range scan across all 3 shards..."
    run_client "begin ro; scan a z"
    ok "Post-split verification complete."

    # --- Backup ---
    step "Demo: Full cluster backup..."
    BACKUP_DIR="${WORK_DIR}/backup"
    ADMIN_ADDRS="127.0.0.1:9011,127.0.0.1:9012,127.0.0.1:9013"
    if [[ -n "${NODE4_ADMIN_PORT:-}" ]]; then
        ADMIN_ADDRS="${ADMIN_ADDRS},127.0.0.1:${NODE4_ADMIN_PORT}"
    fi

    info "Backing up all shards to ${BACKUP_DIR}..."
    info "(This triggers view changes on each shard for consistency.)"
    run_cmd "${TAPI}" admin backup \
        --admin-addrs "${ADMIN_ADDRS}" \
        --output "${BACKUP_DIR}"

    info "Verifying backup files..."
    for f in "${BACKUP_DIR}/cluster.json" "${BACKUP_DIR}/shard_0.json" \
             "${BACKUP_DIR}/shard_1.json" "${BACKUP_DIR}/shard_2.json"; do
        if [[ -f "${f}" ]]; then
            ok "Found $(basename "${f}") ($(wc -c < "${f}") bytes)"
        else
            warn "Missing $(basename "${f}")"
        fi
    done
    ok "Backup complete."

    # --- Guide ---
    print_getting_started_guide
}

# ---------------------------------------------------------------------------
# down
# ---------------------------------------------------------------------------
cmd_down() {
    step "Tearing down testbed..."

    # Prefer tapiadm (handles dynamic nodes + compose down cleanly).
    local tapiadm=""
    for dir in "${PROJECT_ROOT}/target/release" "${PROJECT_ROOT}/target/debug"; do
        if [[ -x "${dir}/tapiadm" ]]; then
            tapiadm="${dir}/tapiadm"
            break
        fi
    done

    if [[ -n "${tapiadm}" ]]; then
        (cd "${PROJECT_ROOT}" && run_cmd "${tapiadm}" docker down)
    elif [[ -f "${WORK_DIR}/docker-compose.yml" ]]; then
        warn "tapiadm binary not found; falling back to docker compose."
        run_cmd docker compose -f "${WORK_DIR}/docker-compose.yml" down -v
        rm -f "${PROJECT_ROOT}/.dockerignore"
    else
        info "No cluster found to tear down."
    fi

    if [[ -d "${WORK_DIR}/backup" ]]; then
        rm -rf "${WORK_DIR}/backup"
        info "Removed backup directory."
    fi

    ok "Testbed removed."
}

# ---------------------------------------------------------------------------
# Getting-started guide
# ---------------------------------------------------------------------------
print_getting_started_guide() {
    # Use short relative-to-project-root paths for display.
    local bin_dir
    bin_dir="$(dirname "${TAPIADM}")"
    # Strip PROJECT_ROOT prefix and leading slash to get e.g. "target/release"
    local rel_bin="${bin_dir#"${PROJECT_ROOT}"/}"
    local dc="docker compose -f .tapiadm/docker-compose.yml"

    separator
    printf "\n${BOLD}    TAPIR Testbed — Getting Started Guide${RESET}\n"
    printf "    (run commands from the project root)\n"
    separator

    local disc_ep="172.28.0.4:6000,172.28.0.5:6000,172.28.0.6:6000"
    cat <<EOF

${BOLD}1. INTERACTIVE REPL${RESET}

   Start an interactive client session inside the Docker network:

     cd .tapiadm && docker compose --profile tools run --rm client \\
       client --discovery-tapir-endpoint "${disc_ep}"

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

     ${dc} \\
       --profile tools run --rm -T client \\
       client --discovery-tapir-endpoint "${disc_ep}" \\
       -e "begin; put counter 42; commit"

     ${dc} \\
       --profile tools run --rm -T client \\
       client --discovery-tapir-endpoint "${disc_ep}" \\
       -e "begin ro; get counter"

   Cross-shard transaction (keys < 'n' -> shard 0, >= 'n' -> shard 1):

     ${dc} \\
       --profile tools run --rm -T client \\
       client --discovery-tapir-endpoint "${disc_ep}" \\
       -e "begin; put apple fruit; put orange citrus; commit"

   Run a command file (one command per line):

     ${dc} \\
       --profile tools run --rm -T client \\
       client --discovery-tapir-endpoint "${disc_ep}" \\
       -s /path/to/script.txt

${BOLD}3. CLUSTER INSPECTION${RESET}

     ${rel_bin}/tapiadm docker get nodes
     ${rel_bin}/tapiadm docker get replicas
     ${rel_bin}/tapi admin status --admin-listen-addr 127.0.0.1:9011
     ${rel_bin}/tapi admin status --admin-listen-addr 127.0.0.1:9012
     ${rel_bin}/tapi admin status --admin-listen-addr 127.0.0.1:9013

${BOLD}4. ADD / REMOVE NODES${RESET}

   Add a new node:

     ${rel_bin}/tapiadm docker add node --name node4

   Check its IP and admin port:

     ${rel_bin}/tapiadm docker get nodes

   Add replicas on the new node (substitute <PORT> and <IP> from above):

     ${rel_bin}/tapi admin add-replica --admin-listen-addr 127.0.0.1:<PORT> \\
       --shard 0 --listen-addr <IP>:6000
     ${rel_bin}/tapi admin add-replica --admin-listen-addr 127.0.0.1:<PORT> \\
       --shard 1 --listen-addr <IP>:6001

   Remove a node:

     ${rel_bin}/tapiadm docker remove node node4

${BOLD}5. VIEW CHANGES${RESET}

   Trigger a view change to synchronize the IR consensus record:

     ${rel_bin}/tapi admin view-change \\
       --admin-listen-addr 127.0.0.1:9011 --shard 0

${BOLD}6. SHARD OPERATIONS${RESET}

   Split shard 0 at key 'g' into new shard 2:

     ${rel_bin}/tapictl split shard --source 0 --split-key g --new-shard 2 \\
       --new-replicas 172.28.0.11:6002,172.28.0.12:6002,172.28.0.13:6002

   Merge shard 2 back into shard 0:

     ${rel_bin}/tapictl merge shard --absorbed 2 --surviving 0

   Compact shard 0 onto fresh replicas:

     ${rel_bin}/tapictl compact shard --source 0 --new-shard 3 \\
       --new-replicas 172.28.0.11:6003,172.28.0.12:6003,172.28.0.13:6003

${BOLD}7. BACKUP & RESTORE${RESET}

   Full cluster backup (triggers view changes for consistency):

     ${rel_bin}/tapi admin backup \\
       --admin-addrs 127.0.0.1:9011,127.0.0.1:9012,127.0.0.1:9013 \\
       --output .tapiadm/backup

   Restore from backup:

     ${rel_bin}/tapi admin restore \\
       --admin-addrs 127.0.0.1:9011,127.0.0.1:9012,127.0.0.1:9013 \\
       --input .tapiadm/backup

${BOLD}8. TEAR DOWN${RESET}

     scripts/testbed.sh down

EOF
}

# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------
usage() {
    printf "Usage: %s <command>\n\n" "$(basename "$0")"
    printf "Commands:\n"
    printf "  up      Build, create cluster, run full demo, print getting-started guide\n"
    printf "  down    Tear down all testbed resources\n"
    exit 1
}

case "${1:-}" in
    up)   cmd_up   ;;
    down) cmd_down ;;
    *)    usage    ;;
esac
