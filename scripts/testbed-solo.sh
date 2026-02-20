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
# For the full multi-shard production-like stack, use scripts/testbed.sh.
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

# ---------------------------------------------------------------------------
# Locate or build binaries
# ---------------------------------------------------------------------------
TAPI=""

find_or_build_binary() {
    step "Locating tapi binary..."

    for dir in "${PROJECT_ROOT}/target/release" "${PROJECT_ROOT}/target/debug"; do
        if [[ -x "${dir}/tapi" ]]; then
            TAPI="${dir}/tapi"
            ok "Found ${TAPI}"
            return
        fi
    done

    info "Not found. Building (this may take a few minutes)..."
    run_cmd cargo build --release --bin tapi --manifest-path "${PROJECT_ROOT}/Cargo.toml"
    TAPI="${PROJECT_ROOT}/target/release/tapi"
    ok "Binary built."
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
# Getting-started guide
# ---------------------------------------------------------------------------
print_guide() {
    separator
    printf "\n${BOLD}    Solo TAPIR cluster is running (shard 0, 3 replicas)${RESET}\n"
    separator

    cat <<EOF

  Node admin ports: 127.0.0.1:${ADMIN_PORTS[0]}, 127.0.0.1:${ADMIN_PORTS[1]}, 127.0.0.1:${ADMIN_PORTS[2]}
  TAPIR ports:      127.0.0.1:${TAPIR_PORTS[0]}, 127.0.0.1:${TAPIR_PORTS[1]}, 127.0.0.1:${TAPIR_PORTS[2]}

  ${BOLD}Interactive client REPL:${RESET}
    ${TAPI} client --config ${WORK_DIR}/client.toml

  ${BOLD}Scripted put/get:${RESET}
    echo -e "put hello world\nget hello\ncommit" | ${TAPI} client --config ${WORK_DIR}/client.toml

  ${BOLD}Node status:${RESET}
    ${TAPI} admin status --admin-listen-addr 127.0.0.1:${ADMIN_PORTS[0]}

  ${BOLD}Teardown:${RESET}
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
