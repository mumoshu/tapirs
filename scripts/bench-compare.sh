#!/usr/bin/env bash
#
# TAPIR vs TiKV Benchmark Comparison
#
# Orchestrates: build images → start TAPIR → bootstrap shards → bench →
# tear down → start TiKV → bench → tear down → print comparison.
#
# Usage:
#   scripts/bench-compare.sh
#   BENCH_CPUS=6 BENCH_MEM=12 BENCH_DELAY_MS=2 scripts/bench-compare.sh
#
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
COMPOSE_DIR="$SCRIPT_DIR/bench-compare"
GENERATED_DIR="$COMPOSE_DIR/generated"

# ── Configuration ──────────────────────────────────────────────────────
BENCH_CPUS="${BENCH_CPUS:-6}"
BENCH_MEM="${BENCH_MEM:-12}"          # GB
BENCH_DELAY_MS="${BENCH_DELAY_MS:-2}"
BENCH_DURATION="${BENCH_DURATION:-10}"
BENCH_CLIENTS="${BENCH_CLIENTS:-100}"
BENCH_SHARDS="${BENCH_SHARDS:-1}"
BENCH_REPLICAS="${BENCH_REPLICAS:-3}" # data replicas per shard
BENCH_KEY_SPACE="${BENCH_KEY_SPACE:-10000}"

# ── IPs (must match docker-compose files) ─────────────────────────────
TAPIR_DISC_IPS=("172.30.1.4" "172.30.1.5" "172.30.1.6")
TAPIR_MGR_IP="172.30.1.3"
TAPIR_NODE_IPS=("172.30.1.11" "172.30.1.12" "172.30.1.13")
TIKV_PD_IP="172.30.0.10"
TIKV_NODE_IPS=("172.30.0.11" "172.30.0.12" "172.30.0.13")

REPLICA_BASE_PORT=6000

# ── Resource allocation ───────────────────────────────────────────────
# TAPIR: 3 discovery (small fixed) + 1 shard-mgr (small fixed) + N data nodes
TAPIR_DISC_CPUS="0.20"
TAPIR_DISC_MEM="0.064"  # GB — 3 disc + 1 mgr = 256MB total (matches PD budget)
TAPIR_MGR_CPUS="0.20"
TAPIR_MGR_MEM="0.064"   # GB

compute_tapir_resources() {
    local total_cpu="$1" total_mem="$2" n_replicas="$3"
    local overhead_cpu overhead_mem
    overhead_cpu=$(echo "3 * $TAPIR_DISC_CPUS + $TAPIR_MGR_CPUS" | bc)
    overhead_mem=$(echo "3 * $TAPIR_DISC_MEM + $TAPIR_MGR_MEM" | bc)
    TAPIR_NODE_CPUS=$(echo "scale=2; ($total_cpu - $overhead_cpu) / $n_replicas" | bc)
    TAPIR_NODE_MEM=$(echo "scale=2; ($total_mem - $overhead_mem) / $n_replicas" | bc)
}

# TiKV: 1 PD (small fixed) + N TiKV nodes
PD_CPUS="0.50"
PD_MEM="0.25"  # GB

TIKV_MIN_NODE_MEM="1.00"  # GB — TiKV OOMs below ~1GB (RocksDB + gRPC + threads)

compute_tikv_resources() {
    local total_cpu="$1" total_mem="$2" n_replicas="$3"
    TIKV_NODE_CPUS=$(echo "scale=2; ($total_cpu - $PD_CPUS) / $n_replicas" | bc)
    TIKV_NODE_MEM=$(echo "scale=2; ($total_mem - $PD_MEM) / $n_replicas" | bc)
    # Enforce minimum memory per TiKV node — TiKV OOMs below ~1GB
    local below
    below=$(echo "$TIKV_NODE_MEM < $TIKV_MIN_NODE_MEM" | bc)
    if [ "$below" = "1" ]; then
        local min_total
        min_total=$(echo "scale=1; $PD_MEM + $n_replicas * $TIKV_MIN_NODE_MEM" | bc)
        echo "ERROR: TiKV node memory ${TIKV_NODE_MEM}GB < minimum ${TIKV_MIN_NODE_MEM}GB." >&2
        echo "  Increase BENCH_MEM to at least ${min_total}GB for ${n_replicas} TiKV replicas." >&2
        echo "  (PD=${PD_MEM}GB + ${n_replicas}x${TIKV_MIN_NODE_MEM}GB)" >&2
        exit 1
    fi
}

# Format GB to docker memory string (e.g. 3.67 -> 3670m)
gb_to_docker_mem() {
    echo "${1}" | awk '{printf "%dm", $1 * 1000}'
}

# ── Split key computation (mirrors tapiadm docker.rs:compute_split_key) ──
# N shards → N-1 split keys dividing a-z evenly.
# E.g. 2 shards → split at "n" (26/2=13, chars[13]='n')
compute_split_keys() {
    local n="$1"
    if [ "$n" -le 1 ]; then
        echo ""
        return
    fi
    python3 -c "
chars = [chr(c) for c in range(ord('a'), ord('z')+1)]
per = len(chars) // $n
print(','.join(chars[i * per] for i in range(1, $n)))
"
}

# Get shard key range (start, end) as JSON-compatible strings.
# shard_key_range <shard_index> <total_shards> <comma-separated split_keys>
shard_key_range() {
    local shard="$1" total="$2" split_keys="$3"
    IFS=',' read -ra keys <<< "$split_keys"
    local start="" end=""
    if [ "$shard" -gt 0 ]; then
        start="${keys[$((shard - 1))]}"
    fi
    if [ "$shard" -lt "$((total - 1))" ]; then
        end="${keys[$shard]}"
    fi
    echo "$start" "$end"
}

# ── Helpers ───────────────────────────────────────────────────────────
tapir_compose() {
    docker compose -f "$COMPOSE_DIR/docker-compose-tapir.yml" "$@"
}

tikv_compose() {
    docker compose -f "$COMPOSE_DIR/docker-compose-tikv.yml" "$@"
}

# Send admin JSON command via TCP (like tapiadm docker.rs:send_admin_command).
send_admin_command() {
    local addr="$1" json="$2"
    echo "$json" | nc -q1 "$addr" 9000 2>/dev/null || echo '{"error":"connection failed"}'
}

# HTTP POST (like tapiadm docker.rs:http_post).
http_post() {
    local url="$1" body="$2"
    curl -sf -X POST "$url" \
        -H "Content-Type: application/json" \
        -d "$body" || echo '{"error":"request failed"}'
}

wait_tcp() {
    local host="$1" port="$2" timeout="${3:-60}"
    local elapsed=0
    while [ $elapsed -lt "$timeout" ]; do
        if timeout 1 bash -c "</dev/tcp/$host/$port" 2>/dev/null; then
            return 0
        fi
        sleep 1
        elapsed=$((elapsed + 1))
    done
    echo "WARN: $host:$port not ready after ${timeout}s" >&2
    return 1
}

# ── Generate TAPIR discovery configs ──────────────────────────────────
generate_discovery_configs() {
    mkdir -p "$GENERATED_DIR"
    local disc_endpoint
    disc_endpoint=$(printf "%s:6000," "${TAPIR_DISC_IPS[@]}")
    disc_endpoint="${disc_endpoint%,}"  # trim trailing comma

    for i in 0 1 2; do
        local ip="${TAPIR_DISC_IPS[$i]}"
        local membership
        membership=$(printf '"%s:6000", ' "${TAPIR_DISC_IPS[@]}")
        membership="[${membership%, }]"
        cat > "$GENERATED_DIR/discovery$((i+1)).toml" <<EOF
admin_listen_addr = "0.0.0.0:9000"
persist_dir = "/data"

[[replicas]]
shard = 0
listen_addr = "${ip}:6000"
membership = ${membership}
EOF
    done
    echo "  Generated discovery configs in $GENERATED_DIR/"
}

# ── Generate TiKV config with memory-appropriate settings ────────────
generate_tikv_config() {
    mkdir -p "$GENERATED_DIR"
    # Compute block-cache capacity: ~40% of TiKV node memory, at least 64MB
    local mem_mb
    mem_mb=$(echo "$TIKV_NODE_MEM" | awk '{printf "%d", $1 * 1000}')
    local block_cache_mb=$((mem_mb * 40 / 100))
    if [ "$block_cache_mb" -lt 64 ]; then
        block_cache_mb=64
    fi
    cat > "$GENERATED_DIR/tikv.toml" <<EOF
[storage.block-cache]
capacity = "${block_cache_mb}MB"

[rocksdb.defaultcf]
write-buffer-size = "4MB"
max-write-buffer-number = 2

[rocksdb.writecf]
write-buffer-size = "4MB"
max-write-buffer-number = 2

[rocksdb.lockcf]
write-buffer-size = "4MB"
max-write-buffer-number = 2

[raftdb.defaultcf]
write-buffer-size = "4MB"
max-write-buffer-number = 2
EOF
}

# ── TAPIR bootstrap (mirrors tapiadm docker.rs lines 278-340) ────────
bootstrap_tapir_shards() {
    local n_shards="$1"
    local split_keys
    split_keys=$(compute_split_keys "$n_shards")

    echo "  Bootstrapping $n_shards shard(s) x ${#TAPIR_NODE_IPS[@]} replicas..."

    for shard in $(seq 0 $((n_shards - 1))); do
        local listen_port=$((REPLICA_BASE_PORT + shard))
        # Build membership for this shard
        local membership=""
        for ip in "${TAPIR_NODE_IPS[@]}"; do
            membership="${membership}\"${ip}:${listen_port}\","
        done
        membership="[${membership%,}]"

        for ip in "${TAPIR_NODE_IPS[@]}"; do
            local listen_addr="${ip}:${listen_port}"
            local cmd="{\"command\":\"add_replica\",\"shard\":${shard},\"listen_addr\":\"${listen_addr}\",\"membership\":${membership}}"
            local resp
            resp=$(send_admin_command "$ip" "$cmd")
            if echo "$resp" | grep -q '"ok"'; then
                echo "    Shard $shard: replica on $ip at $listen_addr"
            else
                echo "    WARN: add_replica shard $shard on $ip: $resp" >&2
            fi
        done

        echo "    Waiting 5s for view change settlement..."
        sleep 5
    done

    # Register shards with shard-manager
    echo "  Registering shard layout with shard-manager..."
    for shard in $(seq 0 $((n_shards - 1))); do
        local listen_port=$((REPLICA_BASE_PORT + shard))
        local range
        range=($(shard_key_range "$shard" "$n_shards" "$split_keys"))
        local start="${range[0]:-}"
        local end="${range[1]:-}"

        local replicas=""
        for ip in "${TAPIR_NODE_IPS[@]}"; do
            replicas="${replicas}\"${ip}:${listen_port}\","
        done
        replicas="[${replicas%,}]"

        # Build JSON body. null for unbounded range ends.
        local start_json="null" end_json="null"
        if [ -n "$start" ]; then start_json="\"$start\""; fi
        if [ -n "$end" ]; then end_json="\"$end\""; fi

        local body="{\"shard\":${shard},\"key_range_start\":${start_json},\"key_range_end\":${end_json},\"replicas\":${replicas}}"
        local resp
        resp=$(http_post "http://${TAPIR_MGR_IP}:9001/v1/register" "$body")
        if echo "$resp" | grep -q '"ok"'; then
            echo "    Shard $shard: range [${start:-<min>}, ${end:-<max>})"
        else
            echo "    WARN: register shard $shard: $resp" >&2
        fi
    done
}

# ── Build connection string for TAPIR bench ───────────────────────────
tapir_bench_cluster() {
    # For single-shard, point directly at shard 0 replicas.
    local addrs=""
    for ip in "${TAPIR_NODE_IPS[@]}"; do
        addrs="${addrs}${ip}:${REPLICA_BASE_PORT},"
    done
    echo "${addrs%,}"
}

# ── Run bench and capture output ──────────────────────────────────────
run_tapir_bench() {
    local output_file="$1"
    echo "  Running TAPIR bench (${BENCH_CLIENTS} clients, ${BENCH_DURATION}s, ${BENCH_KEY_SPACE} keys)..."
    local cluster
    cluster=$(tapir_bench_cluster)
    BENCH_CLUSTER="$cluster" \
    BENCH_CLIENTS="$BENCH_CLIENTS" \
    BENCH_DURATION="$BENCH_DURATION" \
    BENCH_KEY_SPACE="$BENCH_KEY_SPACE" \
    timeout -k 10s 300s cargo test bench_rw --release -- --nocapture --include-ignored \
        > "$output_file" 2>&1 || true
}

run_tikv_bench() {
    local output_file="$1"
    echo "  Running TiKV bench (${BENCH_CLIENTS} clients, ${BENCH_DURATION}s, ${BENCH_KEY_SPACE} keys)..."
    BENCH_TIKV="${TIKV_PD_IP}:2379" \
    BENCH_CLIENTS="$BENCH_CLIENTS" \
    BENCH_DURATION="$BENCH_DURATION" \
    BENCH_KEY_SPACE="$BENCH_KEY_SPACE" \
    timeout -k 10s 300s cargo test --features tikv bench_tikv_rw --release -- --nocapture --include-ignored \
        > "$output_file" 2>&1 || true
}

# ── Parse TPUT lines from bench output ────────────────────────────────
# Extracts "TPUT RW: X attempted/s, Y committed/s" lines.
parse_tput_lines() {
    local file="$1" label="${2:-RW}"
    grep "TPUT $label:" "$file" 2>/dev/null || true
}

# Compute average of last N TPUT lines (skip warmup).
avg_tput() {
    local file="$1" label="${2:-RW}" skip="${3:-5}"
    local lines
    lines=$(parse_tput_lines "$file" "$label")
    if [ -z "$lines" ]; then
        echo "0 0"
        return
    fi
    local total
    total=$(echo "$lines" | wc -l)
    local take=$((total - skip))
    if [ "$take" -le 0 ]; then take="$total"; fi

    echo "$lines" | tail -n "$take" | awk -F'[: ,]+' '
    {
        for (i=1; i<=NF; i++) {
            if ($(i+1) == "attempted/s") att += $i;
            if ($(i+1) == "committed/s") com += $i;
        }
        n++
    }
    END {
        if (n > 0) printf "%.0f %.0f\n", att/n, com/n;
        else print "0 0"
    }'
}

# ── Print comparison report ───────────────────────────────────────────
print_report() {
    local tapir_out="$1" tikv_out="$2"

    compute_tapir_resources "$BENCH_CPUS" "$BENCH_MEM" "$BENCH_REPLICAS"
    compute_tikv_resources "$BENCH_CPUS" "$BENCH_MEM" "$BENCH_REPLICAS"

    echo ""
    echo "================================================================"
    echo "  TAPIR vs TiKV Benchmark Comparison"
    echo "================================================================"
    echo ""
    echo "Configuration:"
    echo "  Total CPU: ${BENCH_CPUS} cores   Total Memory: ${BENCH_MEM} GB"
    echo "  Network delay: ${BENCH_DELAY_MS}ms   Shards: ${BENCH_SHARDS}"
    echo "  Workload: RW (1 read + 1 write per txn)"
    echo "  Key space: ${BENCH_KEY_SPACE} keys   Clients: ${BENCH_CLIENTS}   Duration: ${BENCH_DURATION}s"
    echo ""
    echo "--- TAPIR (3 disc ${TAPIR_DISC_CPUS} CPU + 1 mgr ${TAPIR_MGR_CPUS} CPU + ${BENCH_REPLICAS} data x ${TAPIR_NODE_CPUS} CPU, ${TAPIR_NODE_MEM} GB) ---"
    echo ""
    parse_tput_lines "$tapir_out" "RW" | head -20 | while IFS= read -r line; do
        echo "  $line"
    done
    echo ""
    echo "--- TiKV (1 PD ${PD_CPUS} CPU ${PD_MEM} GB + ${BENCH_REPLICAS} nodes x ${TIKV_NODE_CPUS} CPU, ${TIKV_NODE_MEM} GB) ---"
    echo ""
    parse_tput_lines "$tikv_out" "RW" | head -20 | while IFS= read -r line; do
        echo "  $line"
    done

    echo ""
    echo "================================================================"
    echo "  Summary (last 5s average, excluding warmup)"
    echo "================================================================"

    local tapir_avg tikv_avg
    tapir_avg=($(avg_tput "$tapir_out" "RW" 5))
    tikv_avg=($(avg_tput "$tikv_out" "RW" 5))

    local tapir_att="${tapir_avg[0]}" tapir_com="${tapir_avg[1]}"
    local tikv_att="${tikv_avg[0]}" tikv_com="${tikv_avg[1]}"

    local ratio_att="N/A" ratio_com="N/A"
    if [ "$tikv_att" -gt 0 ] 2>/dev/null; then
        ratio_att=$(echo "scale=2; $tapir_att / $tikv_att" | bc)x
    fi
    if [ "$tikv_com" -gt 0 ] 2>/dev/null; then
        ratio_com=$(echo "scale=2; $tapir_com / $tikv_com" | bc)x
    fi

    printf "%-24s %-16s %-16s %s\n" "" "TAPIR" "TiKV" "Ratio"
    printf "%-24s %-16s %-16s %s\n" "RW attempted/s:" "$tapir_att" "$tikv_att" "$ratio_att"
    printf "%-24s %-16s %-16s %s\n" "RW committed/s:" "$tapir_com" "$tikv_com" "$ratio_com"

    # Commit rates
    local tapir_rate="N/A" tikv_rate="N/A"
    if [ "$tapir_att" -gt 0 ] 2>/dev/null; then
        tapir_rate=$(echo "scale=1; $tapir_com * 100 / $tapir_att" | bc)%
    fi
    if [ "$tikv_att" -gt 0 ] 2>/dev/null; then
        tikv_rate=$(echo "scale=1; $tikv_com * 100 / $tikv_att" | bc)%
    fi
    printf "%-24s %-16s %-16s %s\n" "Commit rate:" "$tapir_rate" "$tikv_rate" "-"
    echo "================================================================"
}

# ── Wait for TiKV stores to register ──────────────────────────────────
# Polls PD's store API until the expected number of TiKV stores are Up.
wait_tikv_stores() {
    local target="$1" timeout=90 elapsed=0
    while [ $elapsed -lt $timeout ]; do
        local count
        count=$(curl -sf "http://${TIKV_PD_IP}:2379/pd/api/v1/stores" \
            | python3 -c "
import sys, json
data = json.load(sys.stdin)
up = sum(1 for s in data.get('stores', []) if s.get('store', {}).get('state_name') == 'Up')
print(up)
" 2>/dev/null || echo 0)
        if [ "$count" -ge "$target" ]; then
            echo "  $count TiKV store(s) registered and Up."
            return 0
        fi
        sleep 2
        elapsed=$((elapsed + 2))
    done
    echo "  WARN: only $count TiKV store(s) Up after ${timeout}s (expected $target)" >&2
}

# ── TiKV region pre-splitting ─────────────────────────────────────────
PD_SPLIT_DIR="$COMPOSE_DIR/pd-split"
PD_SPLIT_BIN="$PD_SPLIT_DIR/pd-split"

build_pd_split() {
    if [ -x "$PD_SPLIT_BIN" ]; then
        return 0
    fi
    echo "  Building pd-split helper..."
    if ! (cd "$PD_SPLIT_DIR" && go build -o pd-split . 2>&1); then
        echo "  WARN: failed to build pd-split helper" >&2
        return 1
    fi
}

presplit_tikv_regions() {
    local pd_addr="$1" split_keys="$2"
    if [ -x "$PD_SPLIT_BIN" ]; then
        echo "  Pre-splitting TiKV regions at keys: ${split_keys}"
        if "$PD_SPLIT_BIN" --pd-addr "${pd_addr}" --split-keys "${split_keys}"; then
            return 0
        fi
        echo "  WARN: pd-split failed, falling back to auto-split" >&2
    fi
    return 1
}

# Fallback: wait for TiKV to auto-split via small region-split-size.
wait_tikv_regions() {
    local target="$1" timeout=60 elapsed=0
    while [ $elapsed -lt $timeout ]; do
        local count
        count=$(curl -sf "http://${TIKV_PD_IP}:2379/pd/api/v1/stats/region" \
            | python3 -c "import sys,json; print(json.load(sys.stdin)['count'])" 2>/dev/null || echo 0)
        if [ "$count" -ge "$target" ]; then
            echo "  TiKV region count: $count (target: $target)"
            return 0
        fi
        sleep 2
        elapsed=$((elapsed + 2))
    done
    echo "  WARN: TiKV region count $count < target $target after ${timeout}s" >&2
}

# ══════════════════════════════════════════════════════════════════════
#  Main
# ══════════════════════════════════════════════════════════════════════
main() {
    echo "================================================================"
    echo "  TAPIR vs TiKV Benchmark Comparison"
    echo "================================================================"
    echo ""
    echo "Config: CPUS=${BENCH_CPUS} MEM=${BENCH_MEM}GB DELAY=${BENCH_DELAY_MS}ms"
    echo "        SHARDS=${BENCH_SHARDS} REPLICAS=${BENCH_REPLICAS}"
    echo "        CLIENTS=${BENCH_CLIENTS} DURATION=${BENCH_DURATION}s KEYS=${BENCH_KEY_SPACE}"
    echo ""

    compute_tapir_resources "$BENCH_CPUS" "$BENCH_MEM" "$BENCH_REPLICAS"
    compute_tikv_resources "$BENCH_CPUS" "$BENCH_MEM" "$BENCH_REPLICAS"

    local tapir_node_mem_docker tikv_node_mem_docker pd_mem_docker
    local tapir_disc_mem_docker tapir_mgr_mem_docker
    tapir_node_mem_docker=$(gb_to_docker_mem "$TAPIR_NODE_MEM")
    tapir_disc_mem_docker=$(gb_to_docker_mem "$TAPIR_DISC_MEM")
    tapir_mgr_mem_docker=$(gb_to_docker_mem "$TAPIR_MGR_MEM")
    tikv_node_mem_docker=$(gb_to_docker_mem "$TIKV_NODE_MEM")
    pd_mem_docker=$(gb_to_docker_mem "$PD_MEM")

    echo "TAPIR allocation: disc=${TAPIR_DISC_CPUS}CPU/${tapir_disc_mem_docker} mgr=${TAPIR_MGR_CPUS}CPU/${tapir_mgr_mem_docker} node=${TAPIR_NODE_CPUS}CPU/${tapir_node_mem_docker}"
    echo "TiKV allocation:  pd=${PD_CPUS}CPU/${pd_mem_docker} node=${TIKV_NODE_CPUS}CPU/${tikv_node_mem_docker}"
    echo ""

    local output_dir
    output_dir=$(mktemp -d /tmp/bench-compare.XXXXXX)
    local tapir_out="$output_dir/tapir.txt"
    local tikv_out="$output_dir/tikv.txt"

    # ── 1. Build TAPIR base image ─────────────────────────────────────
    echo "[1/8] Building TAPIR base image..."
    (cd "$PROJECT_ROOT" && docker build -f .tapiadm/Dockerfile -t tapiadm-tapi . -q) >/dev/null
    echo "  tapiadm-tapi image built."

    # ── 2. Build bench images ─────────────────────────────────────────
    echo "[2/8] Building bench images..."
    tapir_compose build -q 2>/dev/null
    tikv_compose build -q 2>/dev/null
    echo "  All bench images built."

    # ── 3. Generate TAPIR discovery configs ───────────────────────────
    echo "[3/8] Generating TAPIR discovery configs..."
    generate_discovery_configs

    # ── 4. Start TAPIR cluster ────────────────────────────────────────
    echo "[4/8] Starting TAPIR cluster..."
    TAPIR_DISC_CPUS="$TAPIR_DISC_CPUS" \
    TAPIR_DISC_MEM="$tapir_disc_mem_docker" \
    TAPIR_MGR_CPUS="$TAPIR_MGR_CPUS" \
    TAPIR_MGR_MEM="$tapir_mgr_mem_docker" \
    TAPIR_NODE_CPUS="$TAPIR_NODE_CPUS" \
    TAPIR_NODE_MEM="$tapir_node_mem_docker" \
    NETWORK_DELAY_MS="$BENCH_DELAY_MS" \
    tapir_compose up -d

    echo "  Waiting for discovery nodes..."
    for ip in "${TAPIR_DISC_IPS[@]}"; do
        wait_tcp "$ip" 9000 60
    done

    echo "  Waiting for data nodes..."
    for ip in "${TAPIR_NODE_IPS[@]}"; do
        wait_tcp "$ip" 9000 60
    done
    echo "  All TAPIR containers healthy."

    # ── 5. Bootstrap TAPIR shards ─────────────────────────────────────
    echo "[5/8] Bootstrapping TAPIR shards..."
    bootstrap_tapir_shards "$BENCH_SHARDS"
    echo "  TAPIR shards bootstrapped."

    # ── 6. Run TAPIR bench ────────────────────────────────────────────
    echo "[6/8] Running TAPIR benchmark..."
    run_tapir_bench "$tapir_out"
    echo "  TAPIR bench complete. Output: $tapir_out"

    # Tear down TAPIR
    tapir_compose down -v --remove-orphans 2>/dev/null || true

    # ── 7. Start TiKV cluster and run bench ───────────────────────────
    echo "[7/8] Starting TiKV cluster..."
    generate_tikv_config
    PD_CPUS="$PD_CPUS" \
    PD_MEM="$pd_mem_docker" \
    TIKV_NODE_CPUS="$TIKV_NODE_CPUS" \
    TIKV_NODE_MEM="$tikv_node_mem_docker" \
    NETWORK_DELAY_MS="$BENCH_DELAY_MS" \
    tikv_compose up -d

    echo "  Waiting for PD..."
    wait_tcp "$TIKV_PD_IP" 2379 60
    echo "  Waiting for TiKV stores to register with PD..."
    wait_tikv_stores "$BENCH_REPLICAS"
    echo "  TiKV cluster ready."

    # Pre-split TiKV regions if multiple shards requested.
    if [ "$BENCH_SHARDS" -gt 1 ]; then
        local split_keys
        split_keys=$(compute_split_keys "$BENCH_SHARDS")
        build_pd_split || true
        if ! presplit_tikv_regions "${TIKV_PD_IP}:2379" "$split_keys"; then
            echo "  Falling back to auto-split via small region-split-size..."
            wait_tikv_regions "$BENCH_SHARDS"
        fi
    fi

    echo "[8/8] Running TiKV benchmark..."
    run_tikv_bench "$tikv_out"
    echo "  TiKV bench complete. Output: $tikv_out"

    # Tear down TiKV
    tikv_compose down -v --remove-orphans 2>/dev/null || true

    # ── Print report ──────────────────────────────────────────────────
    print_report "$tapir_out" "$tikv_out"

    echo ""
    echo "Raw output saved to: $output_dir/"
}

main "$@"
