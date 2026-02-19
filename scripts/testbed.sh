#!/usr/bin/env bash
#
# testbed.sh — Create or destroy a demo TAPIR cluster.
#
# Usage:
#   scripts/testbed.sh up      Build binaries, create cluster, smoke-test, print guide
#   scripts/testbed.sh down    Tear down all testbed resources
#
# The cluster runs in Docker: 2 shards x 3 replicas across 3 nodes,
# with discovery (port 8080), shard-manager (port 9001), and admin
# ports on 9011-9013.
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
    local ports=(8080 9001 9011 9012 9013)
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
    run_cmd docker compose -f "${WORK_DIR}/docker-compose.yml" \
        --profile tools run --rm -T client \
        client --config /etc/tapi/client.toml \
        -e "begin; put hello world; commit"

    info "Reading key 'hello' back..."
    run_cmd docker compose -f "${WORK_DIR}/docker-compose.yml" \
        --profile tools run --rm -T client \
        client --config /etc/tapi/client.toml \
        -e "begin ro; get hello; abort"
    ok "Smoke test passed."

    # --- Admin status ---
    step "Querying node1 admin status..."
    run_cmd "${TAPI}" admin status --admin-listen-addr 127.0.0.1:9011

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

    cat <<EOF

${BOLD}1. INTERACTIVE REPL${RESET}

   Start an interactive client session inside the Docker network:

     cd .tapiadm && docker compose --profile tools run --rm client

   Then at the tapi> prompt:

     begin
     put alice 100
     put bob 200
     commit
     begin ro
     get alice
     get bob
     abort
     begin ro
     scan a z
     abort

   Type 'help' for all commands, 'quit' to exit.

${BOLD}2. SCRIPTED TRANSACTIONS${RESET}

   Run one-liner transactions (no interactive session needed):

     ${dc} \\
       --profile tools run --rm -T client \\
       client --config /etc/tapi/client.toml \\
       -e "begin; put counter 42; commit"

     ${dc} \\
       --profile tools run --rm -T client \\
       client --config /etc/tapi/client.toml \\
       -e "begin ro; get counter; abort"

   Cross-shard transaction (keys < 'n' -> shard 0, >= 'n' -> shard 1):

     ${dc} \\
       --profile tools run --rm -T client \\
       client --config /etc/tapi/client.toml \\
       -e "begin; put apple fruit; put orange citrus; commit"

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

${BOLD}5. SHARD OPERATIONS${RESET}

   Split shard 0 at key 'g' into new shard 2:

     ${rel_bin}/tapictl split shard --source 0 --split-key g --new-shard 2 \\
       --new-replicas 172.28.0.11:6002,172.28.0.12:6002,172.28.0.13:6002

   Merge shard 2 back into shard 0:

     ${rel_bin}/tapictl merge shard --absorbed 2 --surviving 0

   Compact shard 0 onto fresh replicas:

     ${rel_bin}/tapictl compact shard --source 0 --new-shard 3 \\
       --new-replicas 172.28.0.11:6003,172.28.0.12:6003,172.28.0.13:6003

${BOLD}6. TEAR DOWN${RESET}

     scripts/testbed.sh down

EOF
}

# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------
usage() {
    printf "Usage: %s <command>\n\n" "$(basename "$0")"
    printf "Commands:\n"
    printf "  up      Create demo cluster, run smoke test, print getting-started guide\n"
    printf "  down    Tear down all testbed resources\n"
    exit 1
}

case "${1:-}" in
    up)   cmd_up   ;;
    down) cmd_down ;;
    *)    usage    ;;
esac
