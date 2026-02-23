#!/usr/bin/env bash
#
# testbed-kube-operator.sh — Deploy TAPIR via the Kubernetes operator (Helm charts).
#
# Usage:
#   scripts/testbed-kube-operator.sh up       Deploy operator + cluster, smoke test, print guide
#   scripts/testbed-kube-operator.sh down     Tear down everything
#   scripts/testbed-kube-operator.sh status   Show cluster health
#
# Deploys the operator via the tapirs-operator Helm chart, then creates a
# TAPIRCluster via the tapirs-cluster Helm chart. The operator reconciler
# handles all sub-resource creation and bootstrapping.
#
# Environment variables:
#   TAPIR_KIND=0                          Auto-create/destroy Kind cluster
#   TAPIR_KIND_CLUSTER=tapir-op           Kind cluster name
#   TAPIR_NAMESPACE=tapir                 Namespace for TAPIRCluster
#   TAPIR_OPERATOR_NS=tapirs-operator-system  Operator namespace
#   TAPIR_IMAGE=tapir:latest              Container image for TAPIR components
#   TAPIR_OPERATOR_IMAGE=tapirs-operator:latest  Operator image
#   TAPIR_BUILD_IMAGES=1                  Build Docker images (set 0 to skip)
#   TAPIR_CLUSTER_NAME=tapir              TAPIRCluster resource name
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Defaults
: "${TAPIR_KIND:=0}"
: "${TAPIR_KIND_CLUSTER:=tapir-op}"
: "${TAPIR_NAMESPACE:=tapir}"
: "${TAPIR_OPERATOR_NS:=tapirs-operator-system}"
: "${TAPIR_IMAGE:=tapir:latest}"
: "${TAPIR_OPERATOR_IMAGE:=tapirs-operator:latest}"
: "${TAPIR_BUILD_IMAGES:=1}"
: "${TAPIR_CLUSTER_NAME:=tapir}"

DISCOVERY_TAPIR_PORT=6000
ADMIN_PORT=9000

NS="${TAPIR_NAMESPACE}"
OP_NS="${TAPIR_OPERATOR_NS}"

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

kube_op() {
    kubectl --namespace "${OP_NS}" "$@"
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
build_and_load_images() {
    if [[ "${TAPIR_BUILD_IMAGES}" != "1" ]]; then
        info "Skipping image build (TAPIR_BUILD_IMAGES=0)."
        return
    fi

    step "Building TAPIR image '${TAPIR_IMAGE}'..."
    run_cmd docker build -t "${TAPIR_IMAGE}" \
        -f "${PROJECT_ROOT}/src/bin/tapiadm/docker/Dockerfile" \
        "${PROJECT_ROOT}"
    ok "TAPIR image built."

    step "Building operator image '${TAPIR_OPERATOR_IMAGE}'..."
    run_cmd docker build -t "${TAPIR_OPERATOR_IMAGE}" \
        -f "${PROJECT_ROOT}/kubernetes/operator/Dockerfile" \
        "${PROJECT_ROOT}/kubernetes/operator"
    ok "Operator image built."

    if [[ "${TAPIR_KIND}" == "1" ]]; then
        step "Loading images into Kind cluster..."
        run_cmd kind load docker-image "${TAPIR_IMAGE}" --name "${TAPIR_KIND_CLUSTER}"
        run_cmd kind load docker-image "${TAPIR_OPERATOR_IMAGE}" --name "${TAPIR_KIND_CLUSTER}"
        ok "Images loaded into Kind."
    fi
}

# ---------------------------------------------------------------------------
# Helm install / uninstall
# ---------------------------------------------------------------------------
install_operator() {
    step "Installing tapirs-operator Helm chart..."

    # Split image into repository:tag for Helm values.
    local repo="${TAPIR_OPERATOR_IMAGE%:*}"
    local tag="${TAPIR_OPERATOR_IMAGE##*:}"

    run_cmd helm upgrade --install tapirs-operator \
        "${PROJECT_ROOT}/kubernetes/charts/tapirs-operator" \
        --namespace "${OP_NS}" \
        --create-namespace \
        --set "image.repository=${repo}" \
        --set "image.tag=${tag}" \
        --set "image.pullPolicy=IfNotPresent" \
        --wait --timeout 120s
    ok "Operator chart installed."
}

install_cluster() {
    step "Installing tapirs-cluster Helm chart..."
    run_cmd helm upgrade --install tapirs-cluster \
        "${PROJECT_ROOT}/kubernetes/charts/tapirs-cluster" \
        --namespace "${NS}" \
        --create-namespace \
        --set "name=${TAPIR_CLUSTER_NAME}" \
        --set "image=${TAPIR_IMAGE}" \
        --wait --timeout 30s
    ok "Cluster chart installed."
}

uninstall_cluster() {
    step "Uninstalling tapirs-cluster..."
    if helm status tapirs-cluster --namespace "${NS}" &>/dev/null; then
        run_cmd helm uninstall tapirs-cluster --namespace "${NS}" --wait --timeout 60s
        ok "Cluster chart uninstalled."
    else
        info "tapirs-cluster release not found in namespace '${NS}'."
    fi
}

uninstall_operator() {
    step "Uninstalling tapirs-operator..."
    if helm status tapirs-operator --namespace "${OP_NS}" &>/dev/null; then
        run_cmd helm uninstall tapirs-operator --namespace "${OP_NS}" --wait --timeout 60s
        ok "Operator chart uninstalled."
    else
        info "tapirs-operator release not found in namespace '${OP_NS}'."
    fi
}

# ---------------------------------------------------------------------------
# Wait for TAPIRCluster to reach Running phase
# ---------------------------------------------------------------------------
wait_cluster_running() {
    local timeout=180
    local interval=5
    local elapsed=0

    step "Waiting for TAPIRCluster '${TAPIR_CLUSTER_NAME}' to reach Running phase (timeout: ${timeout}s)..."

    while (( elapsed < timeout )); do
        local phase
        phase=$(kube get tapircluster "${TAPIR_CLUSTER_NAME}" \
            -o jsonpath='{.status.phase}' 2>/dev/null) || true

        if [[ "${phase}" == "Running" ]]; then
            ok "TAPIRCluster is Running."
            return 0
        fi

        if [[ "${phase}" == "Failed" ]]; then
            warn "TAPIRCluster entered Failed phase."
            kube get tapircluster "${TAPIR_CLUSTER_NAME}" -o yaml 2>/dev/null || true
            fail "TAPIRCluster failed."
        fi

        info "Phase: ${phase:-Pending} (${elapsed}s / ${timeout}s)"
        sleep "${interval}"
        elapsed=$(( elapsed + interval ))
    done

    warn "Timed out. Current status:"
    kube get tapircluster "${TAPIR_CLUSTER_NAME}" -o yaml 2>/dev/null || true
    kube get pods -o wide 2>/dev/null || true
    kube_op logs deployment/tapirs-operator --tail=50 2>/dev/null || true
    fail "TAPIRCluster did not reach Running within ${timeout}s."
}

# ---------------------------------------------------------------------------
# Smoke test
# ---------------------------------------------------------------------------
smoke_test() {
    step "Running smoke test..."

    local disc_endpoint="srv://${TAPIR_CLUSTER_NAME}-discovery.${NS}.svc.cluster.local:${DISCOVERY_TAPIR_PORT}"

    # Delete stale smoke test pods from failed previous runs.
    kube delete pod tapir-smoke-write tapir-smoke-read 2>/dev/null || true

    info "Writing key 'hello' with value 'world'..."
    kube run tapir-smoke-write --rm -i --restart=Never \
        --image="${TAPIR_IMAGE}" --image-pull-policy=IfNotPresent -- \
        client \
            --discovery-tapir-endpoint "${disc_endpoint}" \
            -e "begin; put hello world; commit" \
        2>/dev/null || true

    info "Reading key 'hello' back..."
    local output
    output=$(kube run tapir-smoke-read --rm -i --restart=Never \
        --image="${TAPIR_IMAGE}" --image-pull-policy=IfNotPresent -- \
        client \
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
    local disc_endpoint="srv://${TAPIR_CLUSTER_NAME}-discovery.${NS}.svc.cluster.local:${DISCOVERY_TAPIR_PORT}"

    separator
    printf "\n${BOLD}    TAPIR Operator Testbed — Getting Started Guide${RESET}\n"
    printf "    (cluster: ${TAPIR_CLUSTER_NAME}, namespace: ${NS})\n"
    separator

    cat <<EOF

${BOLD}1. INTERACTIVE REPL${RESET}

   Start an interactive client session inside the cluster:

     kubectl run -n ${NS} tapir-client -it --rm --restart=Never \\
       --image=${TAPIR_IMAGE} --image-pull-policy=IfNotPresent -- \\
       client --discovery-tapir-endpoint ${disc_endpoint}

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
       client --discovery-tapir-endpoint ${disc_endpoint} \\
       -e "begin; put counter 42; commit"

     kubectl run -n ${NS} tapir-txn -i --rm --restart=Never \\
       --image=${TAPIR_IMAGE} --image-pull-policy=IfNotPresent -- \\
       client --discovery-tapir-endpoint ${disc_endpoint} \\
       -e "begin ro; get counter; abort"

${BOLD}3. CLUSTER INSPECTION${RESET}

   TAPIRCluster status:

     kubectl get tapircluster -n ${NS} -o wide
     kubectl describe tapircluster ${TAPIR_CLUSTER_NAME} -n ${NS}

   Pod status:

     kubectl get pods -n ${NS} -o wide

   Node admin status:

     kubectl exec -n ${NS} ${TAPIR_CLUSTER_NAME}-default-0 -- tapi admin status

${BOLD}4. OPERATOR LOGS${RESET}

   Watch operator reconciliation:

     kubectl logs -n ${OP_NS} deployment/tapirs-operator -f

${BOLD}5. SCALING${RESET}

   Scale data nodes via Helm:

     helm upgrade tapirs-cluster kubernetes/charts/tapirs-cluster \\
       -n ${NS} --reuse-values --set 'nodePools[0].replicas=5'

   Or edit the TAPIRCluster directly:

     kubectl edit tapircluster ${TAPIR_CLUSTER_NAME} -n ${NS}

${BOLD}6. TEAR DOWN${RESET}

     scripts/testbed-kube-operator.sh down

EOF
}

# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------
cmd_status() {
    step "Operator status..."
    kube_op get pods -o wide 2>/dev/null || {
        warn "Namespace '${OP_NS}' not found or no pods."
    }

    step "TAPIRCluster status..."
    kube get tapircluster -o wide 2>/dev/null || {
        warn "No TAPIRCluster resources in namespace '${NS}'."
    }

    step "Data plane pods..."
    kube get pods -o wide 2>/dev/null || {
        warn "Namespace '${NS}' not found or no pods."
    }

    step "Data node replicas..."
    local node_count
    node_count=$(kube get pods -l "app.kubernetes.io/component=node-default" \
        --no-headers 2>/dev/null | wc -l) || true
    for (( i=0; i<node_count; i++ )); do
        local pod="${TAPIR_CLUSTER_NAME}-default-${i}"
        printf "\n    ${BOLD}${pod}${RESET}\n"
        kube exec "${pod}" -- tapi admin status \
            --admin-listen-addr "127.0.0.1:${ADMIN_PORT}" 2>/dev/null || true
    done
}

# ---------------------------------------------------------------------------
# up
# ---------------------------------------------------------------------------
cmd_up() {
    kind_create
    build_and_load_images

    # Phase 1: Install operator
    install_operator

    # Phase 2: Install TAPIRCluster
    install_cluster

    # Phase 3: Wait for operator to reconcile cluster to Running
    wait_cluster_running

    # Phase 4: Verify
    smoke_test
    cmd_status
    print_guide
}

# ---------------------------------------------------------------------------
# down
# ---------------------------------------------------------------------------
cmd_down() {
    step "Tearing down operator testbed..."

    uninstall_cluster

    # Wait for sub-resources to be garbage-collected.
    if kubectl get namespace "${NS}" &>/dev/null; then
        info "Waiting for sub-resources to be deleted..."
        local retries=0
        while kube get statefulsets --no-headers 2>/dev/null | grep -q .; do
            retries=$((retries + 1))
            if (( retries > 30 )); then
                warn "Sub-resources still present after 30 retries."
                break
            fi
            sleep 2
        done
    fi

    uninstall_operator

    # Clean up namespaces.
    if kubectl get namespace "${NS}" &>/dev/null; then
        run_cmd kubectl delete namespace "${NS}" --wait=true 2>/dev/null || true
    fi
    if kubectl get namespace "${OP_NS}" &>/dev/null; then
        run_cmd kubectl delete namespace "${OP_NS}" --wait=true 2>/dev/null || true
    fi

    kind_delete
    ok "Testbed removed."
}

# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------
usage() {
    printf "Usage: %s <command>\n\n" "$(basename "$0")"
    printf "Commands:\n"
    printf "  up        Deploy operator + cluster, smoke test, print guide\n"
    printf "  down      Tear down all testbed resources\n"
    printf "  status    Show cluster health\n"
    printf "\nEnvironment variables:\n"
    printf "  TAPIR_KIND=1                      Auto-create/destroy Kind cluster\n"
    printf "  TAPIR_KIND_CLUSTER                Kind cluster name (default: tapir-op)\n"
    printf "  TAPIR_NAMESPACE                   Namespace for TAPIRCluster (default: tapir)\n"
    printf "  TAPIR_OPERATOR_NS                 Operator namespace (default: tapirs-operator-system)\n"
    printf "  TAPIR_IMAGE                       TAPIR image (default: tapir:latest)\n"
    printf "  TAPIR_OPERATOR_IMAGE              Operator image (default: tapirs-operator:latest)\n"
    printf "  TAPIR_BUILD_IMAGES                Build images (default: 1, set 0 to skip)\n"
    printf "  TAPIR_CLUSTER_NAME                TAPIRCluster name (default: tapir)\n"
    exit 1
}

case "${1:-}" in
    up)     cmd_up     ;;
    down)   cmd_down   ;;
    status) cmd_status ;;
    *)      usage      ;;
esac
