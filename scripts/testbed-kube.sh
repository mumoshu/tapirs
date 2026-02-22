#!/usr/bin/env bash
#
# testbed-kube.sh — Create or destroy a TAPIR cluster on Kubernetes.
#
# Usage:
#   scripts/testbed-kube.sh up       Deploy cluster, bootstrap, smoke-test, print guide
#   scripts/testbed-kube.sh down     Tear down all testbed resources
#   scripts/testbed-kube.sh status   Show cluster health
#
# Two-tier deployment (same architecture as scripts/testbed.sh):
#   Tier 1 — Discovery store: 3-node single-shard TAPIR cluster (StatefulSet)
#   Tier 2 — Main cluster: N shards x 3 replicas across M nodes (StatefulSet)
#
# Works against existing Kind cluster or remote K8s cluster.
# Set TAPIR_KIND=1 for automatic Kind cluster lifecycle.
#
# Environment variables:
#   TAPIR_KIND=0            Set to 1 to auto-create/destroy Kind cluster
#   TAPIR_KIND_CLUSTER=tapir  Kind cluster name
#   TAPIR_NAMESPACE=tapir   K8s namespace
#   TAPIR_NODES=3           Number of data nodes
#   TAPIR_SHARDS=2          Number of shards
#   TAPIR_IMAGE=tapir:latest  Container image
#   TAPIR_BUILD_IMAGE=1     Build Docker image (set 0 to skip)
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Defaults
: "${TAPIR_KIND:=0}"
: "${TAPIR_KIND_CLUSTER:=tapir}"
: "${TAPIR_NAMESPACE:=tapir}"
: "${TAPIR_NODES:=3}"
: "${TAPIR_SHARDS:=2}"
: "${TAPIR_IMAGE:=tapir:latest}"
: "${TAPIR_BUILD_IMAGE:=1}"

DISCOVERY_REPLICAS=3
DISCOVERY_TAPIR_PORT=6000
ADMIN_PORT=9000
SHARD_MANAGER_PORT=9001
REPLICA_BASE_PORT=6000

NS="${TAPIR_NAMESPACE}"

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
# Helpers
# ---------------------------------------------------------------------------
kube() {
    kubectl --namespace "${NS}" "$@"
}

# Wait for all pods matching a label to be Ready (with timeout).
wait_pods_ready() {
    local label="$1"
    local timeout="${2:-120s}"
    info "Waiting for pods (${label}) to be ready (timeout: ${timeout})..."
    kube wait --for=condition=Ready pod -l "${label}" --timeout="${timeout}" 2>/dev/null || {
        warn "Timed out waiting for pods (${label}). Current pod status:"
        kube get pods -l "${label}" -o wide 2>/dev/null || true
        fail "Pods not ready: ${label}"
    }
}

# Get pod IP for a named pod.
pod_ip() {
    kube get pod "$1" -o jsonpath='{.status.podIP}'
}

# Run tapi admin command inside a pod via kubectl exec.
exec_tapi() {
    local pod="$1"
    shift
    kube exec "${pod}" -- tapi "$@"
}

# Run tapictl command inside a pod via kubectl exec.
exec_tapictl() {
    local pod="$1"
    shift
    kube exec "${pod}" -- tapictl "$@"
}

# Compute split key for shard partitioning (a-z, evenly distributed).
# Usage: split_key <shard_index> <total_shards>
# Returns the split key for the boundary between shard i-1 and shard i.
compute_split_keys() {
    local total="$1"
    local chars=(a b c d e f g h i j k l m n o p q r s t u v w x y z)
    local per=$(( ${#chars[@]} / total ))
    local keys=()
    for (( i=1; i<total; i++ )); do
        local idx=$(( i * per ))
        keys+=("${chars[$idx]}")
    done
    echo "${keys[@]}"
}

# Get key_range_end for a shard (empty string for last shard).
shard_key_range_end() {
    local shard="$1"
    local total="$2"
    local -a split_keys
    read -ra split_keys <<< "$(compute_split_keys "${total}")"

    if (( shard >= total - 1 )); then
        echo ""
    else
        echo "${split_keys[$shard]}"
    fi
}

# Get key_range_start for a shard (empty string for first shard).
shard_key_range_start() {
    local shard="$1"
    local total="$2"
    local -a split_keys
    read -ra split_keys <<< "$(compute_split_keys "${total}")"

    if (( shard == 0 )); then
        echo ""
    else
        echo "${split_keys[$((shard - 1))]}"
    fi
}

# ---------------------------------------------------------------------------
# Kind cluster management
# ---------------------------------------------------------------------------
kind_create() {
    if [[ "${TAPIR_KIND}" != "1" ]]; then return; fi
    step "Creating Kind cluster '${TAPIR_KIND_CLUSTER}'..."

    if kind get clusters 2>/dev/null | grep -q "^${TAPIR_KIND_CLUSTER}$"; then
        info "Kind cluster '${TAPIR_KIND_CLUSTER}' already exists."
        return
    fi

    run_cmd kind create cluster --name "${TAPIR_KIND_CLUSTER}" --wait 60s
    ok "Kind cluster created."
}

kind_delete() {
    if [[ "${TAPIR_KIND}" != "1" ]]; then return; fi
    step "Deleting Kind cluster '${TAPIR_KIND_CLUSTER}'..."

    if kind get clusters 2>/dev/null | grep -q "^${TAPIR_KIND_CLUSTER}$"; then
        run_cmd kind delete cluster --name "${TAPIR_KIND_CLUSTER}"
        ok "Kind cluster deleted."
    else
        info "Kind cluster '${TAPIR_KIND_CLUSTER}' not found."
    fi
}

# ---------------------------------------------------------------------------
# Docker image build and load
# ---------------------------------------------------------------------------
build_and_load_image() {
    if [[ "${TAPIR_BUILD_IMAGE}" != "1" ]]; then
        info "Skipping image build (TAPIR_BUILD_IMAGE=0)."
        return
    fi

    step "Building Docker image '${TAPIR_IMAGE}'..."
    run_cmd docker build -t "${TAPIR_IMAGE}" \
        -f "${PROJECT_ROOT}/src/bin/tapiadm/docker/Dockerfile" \
        "${PROJECT_ROOT}"
    ok "Docker image built."

    if [[ "${TAPIR_KIND}" == "1" ]]; then
        step "Loading image into Kind cluster..."
        run_cmd kind load docker-image "${TAPIR_IMAGE}" --name "${TAPIR_KIND_CLUSTER}"
        ok "Image loaded into Kind."
    fi
}

# ---------------------------------------------------------------------------
# K8s manifest generation and application
# ---------------------------------------------------------------------------
apply_namespace() {
    step "Creating namespace '${NS}'..."
    kubectl apply -f - <<EOF
apiVersion: v1
kind: Namespace
metadata:
  name: ${NS}
EOF
    ok "Namespace created."
}

apply_discovery() {
    step "Deploying discovery store (${DISCOVERY_REPLICAS} pods)..."
    kubectl apply -f - <<EOF
---
apiVersion: v1
kind: Service
metadata:
  name: tapir-discovery
  namespace: ${NS}
spec:
  clusterIP: None
  publishNotReadyAddresses: true
  selector:
    app: tapir-discovery
  ports:
    - name: tapir
      port: ${DISCOVERY_TAPIR_PORT}
    - name: admin
      port: ${ADMIN_PORT}
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: tapir-discovery
  namespace: ${NS}
spec:
  serviceName: tapir-discovery
  replicas: ${DISCOVERY_REPLICAS}
  selector:
    matchLabels:
      app: tapir-discovery
  template:
    metadata:
      labels:
        app: tapir-discovery
    spec:
      containers:
        - name: tapir
          image: ${TAPIR_IMAGE}
          imagePullPolicy: IfNotPresent
          command: ["tapi", "node"]
          args:
            - "--admin-listen-addr=0.0.0.0:${ADMIN_PORT}"
            - "--persist-dir=/data"
          env:
            - name: POD_IP
              valueFrom:
                fieldRef:
                  fieldPath: status.podIP
            - name: RUST_LOG
              value: "info"
          ports:
            - containerPort: ${DISCOVERY_TAPIR_PORT}
              name: tapir
            - containerPort: ${ADMIN_PORT}
              name: admin
          readinessProbe:
            tcpSocket:
              port: ${ADMIN_PORT}
            initialDelaySeconds: 2
            periodSeconds: 3
          volumeMounts:
            - name: data
              mountPath: /data
      volumes:
        - name: data
          emptyDir: {}
EOF
    ok "Discovery store manifests applied."
}

apply_shard_manager_and_nodes() {
    local disc_endpoint="srv://tapir-discovery.${NS}.svc.cluster.local:${DISCOVERY_TAPIR_PORT}"
    local sm_url="http://tapir-shard-manager.${NS}.svc.cluster.local:${SHARD_MANAGER_PORT}"

    step "Deploying shard-manager and data nodes (${TAPIR_NODES} pods)..."
    kubectl apply -f - <<EOF
---
apiVersion: v1
kind: Service
metadata:
  name: tapir-shard-manager
  namespace: ${NS}
spec:
  selector:
    app: tapir-shard-manager
  ports:
    - port: ${SHARD_MANAGER_PORT}
      name: http
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: tapir-shard-manager
  namespace: ${NS}
spec:
  replicas: 1
  selector:
    matchLabels:
      app: tapir-shard-manager
  template:
    metadata:
      labels:
        app: tapir-shard-manager
    spec:
      containers:
        - name: shard-manager
          image: ${TAPIR_IMAGE}
          imagePullPolicy: IfNotPresent
          command: ["tapi", "shard-manager"]
          args:
            - "--listen-addr=0.0.0.0:${SHARD_MANAGER_PORT}"
            - "--discovery-tapir-endpoint=${disc_endpoint}"
          env:
            - name: RUST_LOG
              value: "info"
          ports:
            - containerPort: ${SHARD_MANAGER_PORT}
              name: http
          readinessProbe:
            tcpSocket:
              port: ${SHARD_MANAGER_PORT}
            initialDelaySeconds: 5
            periodSeconds: 5
---
apiVersion: v1
kind: Service
metadata:
  name: tapir-node
  namespace: ${NS}
spec:
  clusterIP: None
  selector:
    app: tapir-node
  ports:
    - name: admin
      port: ${ADMIN_PORT}
    - name: tapir-0
      port: 6000
    - name: tapir-1
      port: 6001
    - name: tapir-2
      port: 6002
    - name: tapir-3
      port: 6003
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: tapir-node
  namespace: ${NS}
spec:
  serviceName: tapir-node
  replicas: ${TAPIR_NODES}
  selector:
    matchLabels:
      app: tapir-node
  template:
    metadata:
      labels:
        app: tapir-node
    spec:
      containers:
        - name: tapir
          image: ${TAPIR_IMAGE}
          imagePullPolicy: IfNotPresent
          command: ["tapi", "node"]
          args:
            - "--admin-listen-addr=0.0.0.0:${ADMIN_PORT}"
            - "--persist-dir=/data"
            - "--discovery-tapir-endpoint=${disc_endpoint}"
            - "--shard-manager-url=${sm_url}"
          env:
            - name: POD_IP
              valueFrom:
                fieldRef:
                  fieldPath: status.podIP
            - name: RUST_LOG
              value: "info"
          ports:
            - containerPort: ${ADMIN_PORT}
              name: admin
            - containerPort: 6000
              name: tapir-0
            - containerPort: 6001
              name: tapir-1
            - containerPort: 6002
              name: tapir-2
            - containerPort: 6003
              name: tapir-3
          readinessProbe:
            tcpSocket:
              port: ${ADMIN_PORT}
            initialDelaySeconds: 2
            periodSeconds: 3
          volumeMounts:
            - name: data
              mountPath: /data
      volumes:
        - name: data
          emptyDir: {}
EOF
    ok "Shard-manager and data node manifests applied."
}

# ---------------------------------------------------------------------------
# Bootstrap discovery store
# ---------------------------------------------------------------------------
bootstrap_discovery() {
    step "Bootstrapping discovery store..."

    # Collect pod IPs.
    local disc_ips=()
    for (( i=0; i<DISCOVERY_REPLICAS; i++ )); do
        local pod="tapir-discovery-${i}"
        local ip
        ip=$(pod_ip "${pod}")
        if [[ -z "${ip}" ]]; then
            fail "Could not get IP for ${pod}"
        fi
        disc_ips+=("${ip}")
        info "${pod} -> ${ip}"
    done

    # Build membership string (comma-separated).
    local membership=""
    for ip in "${disc_ips[@]}"; do
        if [[ -n "${membership}" ]]; then membership="${membership},"; fi
        membership="${membership}${ip}:${DISCOVERY_TAPIR_PORT}"
    done

    # Add replica on each discovery node with static membership.
    for (( i=0; i<DISCOVERY_REPLICAS; i++ )); do
        local pod="tapir-discovery-${i}"
        local ip="${disc_ips[$i]}"
        info "Adding discovery replica on ${pod} (${ip}:${DISCOVERY_TAPIR_PORT})..."
        exec_tapi "${pod}" admin add-replica \
            --admin-listen-addr "127.0.0.1:${ADMIN_PORT}" \
            --shard 0 \
            --listen-addr "${ip}:${DISCOVERY_TAPIR_PORT}" \
            --membership "${membership}"
    done

    ok "Discovery store bootstrapped (${DISCOVERY_REPLICAS} replicas)."
}

# ---------------------------------------------------------------------------
# Bootstrap data nodes (static membership — initial bootstrap)
# ---------------------------------------------------------------------------
# All pod IPs are known upfront. Each replica is started with the full
# membership list via --membership. No shard-manager coordination needed.
# TAPIR is leaderless — replicas sharing the same view number (0) and
# membership form an operational shard immediately.
#
# Matches tapiadm's proven approach (docker.rs:278-313).
bootstrap_data_nodes() {
    step "Bootstrapping data node replicas (static membership)..."

    local nodes="${TAPIR_NODES}"
    local shards="${TAPIR_SHARDS}"

    # Collect all pod IPs upfront.
    local node_ips=()
    for (( n=0; n<nodes; n++ )); do
        local pod="tapir-node-${n}"
        local ip
        ip=$(pod_ip "${pod}")
        if [[ -z "${ip}" ]]; then
            fail "Could not get IP for ${pod}"
        fi
        node_ips+=("${ip}")
        info "${pod} -> ${ip}"
    done

    # For each shard: compute full membership, then start all replicas.
    for (( s=0; s<shards; s++ )); do
        local port=$(( REPLICA_BASE_PORT + s ))

        # Build membership string: all nodes' IP:port for this shard.
        local membership=""
        for ip in "${node_ips[@]}"; do
            if [[ -n "${membership}" ]]; then membership="${membership},"; fi
            membership="${membership}${ip}:${port}"
        done

        info "Shard ${s}: membership=${membership}"

        for (( n=0; n<nodes; n++ )); do
            local pod="tapir-node-${n}"
            local ip="${node_ips[$n]}"

            info "  ${pod} (${ip}:${port}) -> shard ${s}..."
            exec_tapi "${pod}" admin add-replica \
                --admin-listen-addr "127.0.0.1:${ADMIN_PORT}" \
                --shard "${s}" \
                --listen-addr "${ip}:${port}" \
                --membership "${membership}"
        done
        ok "Shard ${s}: ${nodes} replicas created."
    done

    ok "All data node replicas bootstrapped."
}

# ---------------------------------------------------------------------------
# Register shard layout (explicit replicas — initial bootstrap)
# ---------------------------------------------------------------------------
# Passes --replicas so the shard-manager uses addresses directly instead
# of querying the discovery cluster (which may not have membership yet
# because CachingShardDirectory pushes every 10 seconds).
#
# Matches tapiadm's proven approach (docker.rs:326-330).
register_shards() {
    step "Registering shard layout with shard-manager..."

    local sm_pod
    sm_pod=$(kube get pods -l app=tapir-shard-manager -o jsonpath='{.items[0].metadata.name}')

    local nodes="${TAPIR_NODES}"
    local shards="${TAPIR_SHARDS}"

    # Collect pod IPs for --replicas computation.
    local node_ips=()
    for (( n=0; n<nodes; n++ )); do
        node_ips+=("$(pod_ip "tapir-node-${n}")")
    done

    for (( s=0; s<shards; s++ )); do
        local port=$(( REPLICA_BASE_PORT + s ))
        local key_end
        key_end=$(shard_key_range_end "${s}" "${shards}")
        local key_start
        key_start=$(shard_key_range_start "${s}" "${shards}")

        # Build replicas string: all nodes' IP:port for this shard.
        local replicas=""
        for ip in "${node_ips[@]}"; do
            if [[ -n "${replicas}" ]]; then replicas="${replicas},"; fi
            replicas="${replicas}${ip}:${port}"
        done

        local args=(create shard
            --shard-manager-url "http://127.0.0.1:${SHARD_MANAGER_PORT}"
            --shard "${s}"
            --replicas "${replicas}")

        if [[ -n "${key_start}" ]]; then
            args+=(--key-range-start "${key_start}")
        fi
        if [[ -n "${key_end}" ]]; then
            args+=(--key-range-end "${key_end}")
        fi

        info "Shard ${s}: key_range=[${key_start:-<min>}, ${key_end:-<max>}) replicas=${replicas}"
        exec_tapictl "${sm_pod}" "${args[@]}"
    done

    ok "Shard layout registered."
}

# ---------------------------------------------------------------------------
# Smoke test
# ---------------------------------------------------------------------------
smoke_test() {
    step "Running smoke test..."

    local disc_endpoint="srv://tapir-discovery.${NS}.svc.cluster.local:${DISCOVERY_TAPIR_PORT}"

    # Delete stale smoke test pods from failed previous runs.
    kube delete pod tapir-smoke-write tapir-smoke-read 2>/dev/null || true

    info "Writing key 'hello' with value 'world'..."
    kube run tapir-smoke-write --rm -i --restart=Never \
        --image="${TAPIR_IMAGE}" --image-pull-policy=IfNotPresent -- \
        tapi client \
            --discovery-tapir-endpoint "${disc_endpoint}" \
            -e "begin; put hello world; commit" \
        2>/dev/null || true

    info "Reading key 'hello' back..."
    local output
    output=$(kube run tapir-smoke-read --rm -i --restart=Never \
        --image="${TAPIR_IMAGE}" --image-pull-policy=IfNotPresent -- \
        tapi client \
            --discovery-tapir-endpoint "${disc_endpoint}" \
            -e "begin ro; get hello; abort" \
        2>/dev/null) || true

    if echo "${output}" | grep -q "world"; then
        ok "Smoke test passed: read returned 'world'."
    else
        warn "Smoke test result: ${output}"
    fi
}

# ---------------------------------------------------------------------------
# Getting-started guide
# ---------------------------------------------------------------------------
print_guide() {
    local disc_endpoint="srv://tapir-discovery.${NS}.svc.cluster.local:${DISCOVERY_TAPIR_PORT}"
    local sm_url="http://tapir-shard-manager.${NS}.svc.cluster.local:${SHARD_MANAGER_PORT}"

    separator
    printf "\n${BOLD}    TAPIR Kubernetes Testbed — Getting Started Guide${RESET}\n"
    printf "    (namespace: ${NS})\n"
    separator

    cat <<EOF

${BOLD}1. INTERACTIVE REPL${RESET}

   Start an interactive client session inside the cluster:

     kubectl run -n ${NS} tapir-client -it --rm --restart=Never \\
       --image=${TAPIR_IMAGE} --image-pull-policy=IfNotPresent -- \\
       tapi client --discovery-tapir-endpoint ${disc_endpoint}

   Then at the tapi> prompt:

     begin
     put alice 100
     put bob 200
     commit
     begin ro
     get alice
     get bob
     abort

   Type 'help' for all commands, 'quit' to exit.

${BOLD}2. SCRIPTED TRANSACTIONS${RESET}

   Run one-liner transactions:

     kubectl run -n ${NS} tapir-txn -i --rm --restart=Never \\
       --image=${TAPIR_IMAGE} --image-pull-policy=IfNotPresent -- \\
       tapi client --discovery-tapir-endpoint ${disc_endpoint} \\
       -e "begin; put counter 42; commit"

     kubectl run -n ${NS} tapir-txn -i --rm --restart=Never \\
       --image=${TAPIR_IMAGE} --image-pull-policy=IfNotPresent -- \\
       tapi client --discovery-tapir-endpoint ${disc_endpoint} \\
       -e "begin ro; get counter; abort"

${BOLD}3. LOCAL CLIENT (port-forward)${RESET}

   Forward a data node's TAPIR port to localhost and connect directly:

     kubectl port-forward -n ${NS} pod/tapir-node-0 6000:6000 &
     tapi client --discovery-tapir-endpoint ${disc_endpoint}

   (Requires tapi binary built locally.)

${BOLD}4. CLUSTER INSPECTION${RESET}

   Pod status:

     kubectl get pods -n ${NS} -o wide

   Node admin status:

     kubectl exec -n ${NS} tapir-node-0 -- tapi admin status
     kubectl exec -n ${NS} tapir-node-1 -- tapi admin status
     kubectl exec -n ${NS} tapir-node-2 -- tapi admin status

   Cluster topology:

     kubectl exec -n ${NS} tapir-node-0 -- tapictl get topology \\
       --discovery-tapir-endpoint ${disc_endpoint}

${BOLD}5. SHARD OPERATIONS${RESET}

   All shard operations go through the shard-manager pod:

   Split shard 0 at key 'g':

     SM_POD=\$(kubectl get pods -n ${NS} -l app=tapir-shard-manager -o jsonpath='{.items[0].metadata.name}')
     kubectl exec -n ${NS} \$SM_POD -- tapictl split shard \\
       --shard-manager-url ${sm_url} \\
       --source 0 --split-key g --new-shard 2 \\
       --new-replicas <IP1>:6002,<IP2>:6002,<IP3>:6002

   Merge shard 2 back into shard 0:

     kubectl exec -n ${NS} \$SM_POD -- tapictl merge shard \\
       --shard-manager-url ${sm_url} \\
       --absorbed 2 --surviving 0

${BOLD}6. TEAR DOWN${RESET}

     scripts/testbed-kube.sh down

EOF
}

# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------
cmd_status() {
    step "Pod status..."
    kube get pods -o wide 2>/dev/null || {
        warn "Namespace '${NS}' not found or no pods."
        return
    }

    step "Data node replicas..."
    local node_count
    node_count=$(kube get pods -l app=tapir-node --no-headers 2>/dev/null | wc -l)
    for (( i=0; i<node_count; i++ )); do
        local pod="tapir-node-${i}"
        printf "\n    ${BOLD}${pod}${RESET}\n"
        exec_tapi "${pod}" admin status --admin-listen-addr "127.0.0.1:${ADMIN_PORT}" 2>/dev/null || true
    done

    step "Discovery topology..."
    local disc_endpoint="srv://tapir-discovery.${NS}.svc.cluster.local:${DISCOVERY_TAPIR_PORT}"
    if (( node_count > 0 )); then
        exec_tapictl "tapir-node-0" get topology \
            --discovery-tapir-endpoint "${disc_endpoint}" 2>/dev/null || true
    fi
}

# ---------------------------------------------------------------------------
# up
# ---------------------------------------------------------------------------
cmd_up() {
    kind_create
    build_and_load_image
    apply_namespace

    # Phase 1: Discovery store
    apply_discovery
    wait_pods_ready "app=tapir-discovery" "120s"
    bootstrap_discovery

    # Brief pause for discovery cluster to stabilize.
    info "Waiting for discovery cluster to stabilize..."
    sleep 5

    # Phase 2: Shard-manager + data nodes
    apply_shard_manager_and_nodes
    wait_pods_ready "app=tapir-shard-manager" "120s"
    wait_pods_ready "app=tapir-node" "120s"

    # Phase 3: Bootstrap
    bootstrap_data_nodes
    register_shards

    # Phase 4: Verify
    smoke_test
    cmd_status
    print_guide
}

# ---------------------------------------------------------------------------
# down
# ---------------------------------------------------------------------------
cmd_down() {
    step "Tearing down Kubernetes testbed..."

    if kubectl get namespace "${NS}" &>/dev/null; then
        run_cmd kubectl delete namespace "${NS}" --wait=true
        ok "Namespace '${NS}' deleted."
    else
        info "Namespace '${NS}' not found."
    fi

    kind_delete
    ok "Testbed removed."
}

# ---------------------------------------------------------------------------
# add-node (dynamic shard-manager — runtime operation)
# ---------------------------------------------------------------------------
# Scales the tapir-node StatefulSet by 1 and uses the dynamic shard-manager
# path (no --membership) to join each shard. This works because discovery
# already has shard memberships from the initial bootstrap's
# CachingShardDirectory push.
#
# Flow: tapi admin add-replica (no --membership) → Node::create_replica()
# → shard_manager_join() → POST /v1/join → discovery found → manager.join()
# → AddMember view change on existing replicas.
cmd_add_node() {
    step "Adding a new data node..."

    local current_count
    current_count=$(kube get statefulset tapir-node -o jsonpath='{.spec.replicas}')
    local new_count=$(( current_count + 1 ))
    local new_idx=$(( current_count ))
    local new_pod="tapir-node-${new_idx}"

    info "Scaling tapir-node StatefulSet: ${current_count} -> ${new_count}..."
    kube scale statefulset tapir-node --replicas="${new_count}"

    info "Waiting for ${new_pod} to be ready..."
    # Wait specifically for the new pod (label selector matches all, but
    # wait --for=condition=Ready covers newly created pods too).
    local retries=0
    while ! kube get pod "${new_pod}" &>/dev/null; do
        retries=$((retries + 1))
        if (( retries > 30 )); then
            fail "Timed out waiting for ${new_pod} to appear"
        fi
        sleep 2
    done
    kube wait --for=condition=Ready pod "${new_pod}" --timeout=120s

    local new_ip
    new_ip=$(pod_ip "${new_pod}")
    info "${new_pod} ready at ${new_ip}"

    # Add replicas for each shard via the dynamic shard-manager path.
    # Discovery already has shard membership from the initial bootstrap,
    # so create_replica → shard_manager_join → /v1/join finds the shard
    # and coordinates AddMember with existing replicas.
    local shards="${TAPIR_SHARDS}"
    for (( s=0; s<shards; s++ )); do
        local port=$(( REPLICA_BASE_PORT + s ))
        info "Joining shard ${s} on ${new_pod} (${new_ip}:${port})..."
        exec_tapi "${new_pod}" admin add-replica \
            --admin-listen-addr "127.0.0.1:${ADMIN_PORT}" \
            --shard "${s}" \
            --listen-addr "${new_ip}:${port}"

        # Wait for AddMember view change to propagate.
        sleep 3
    done

    ok "Node ${new_pod} added to all ${shards} shard(s)."
    info "Run 'scripts/testbed-kube.sh status' to verify."
}

# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------
usage() {
    printf "Usage: %s <command>\n\n" "$(basename "$0")"
    printf "Commands:\n"
    printf "  up        Deploy cluster, bootstrap, smoke test, print guide\n"
    printf "  down      Tear down all testbed resources\n"
    printf "  status    Show cluster health\n"
    printf "  add-node  Add a new data node (dynamic shard-manager join)\n"
    printf "\nEnvironment variables:\n"
    printf "  TAPIR_KIND=1          Auto-create/destroy Kind cluster\n"
    printf "  TAPIR_KIND_CLUSTER    Kind cluster name (default: tapir)\n"
    printf "  TAPIR_NAMESPACE       K8s namespace (default: tapir)\n"
    printf "  TAPIR_NODES           Number of data nodes (default: 3)\n"
    printf "  TAPIR_SHARDS          Number of shards (default: 2)\n"
    printf "  TAPIR_IMAGE           Container image (default: tapir:latest)\n"
    printf "  TAPIR_BUILD_IMAGE     Build Docker image (default: 1, set 0 to skip)\n"
    exit 1
}

case "${1:-}" in
    up)       cmd_up       ;;
    down)     cmd_down     ;;
    status)   cmd_status   ;;
    add-node) cmd_add_node ;;
    *)        usage        ;;
esac
