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
#   TAPIR_TLS=0                           Enable mTLS via cert-manager
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
: "${TAPIR_TLS:=0}"

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
# Pre-flight checks
# ---------------------------------------------------------------------------
preflight_check_tools() {
    step "Pre-flight: checking required tools..."
    local missing=()
    for tool in kubectl helm docker; do
        command -v "${tool}" &>/dev/null || missing+=("${tool}")
    done
    if [[ "${TAPIR_KIND}" == "1" ]]; then
        command -v kind &>/dev/null || missing+=("kind")
    fi
    if (( ${#missing[@]} > 0 )); then
        fail "Missing required tools: ${missing[*]}"
    fi
    ok "All required tools found."
}

preflight_check_sysctl() {
    step "Pre-flight: checking kernel inotify limits..."

    local max_instances max_watches
    max_instances=$(cat /proc/sys/fs/inotify/max_user_instances 2>/dev/null) || max_instances=0
    max_watches=$(cat /proc/sys/fs/inotify/max_user_watches 2>/dev/null) || max_watches=0

    info "fs.inotify.max_user_instances = ${max_instances}"
    info "fs.inotify.max_user_watches   = ${max_watches}"

    if (( max_instances < 256 )); then
        fail "fs.inotify.max_user_instances is ${max_instances} (need >= 256). Fix: sudo sysctl fs.inotify.max_user_instances=512"
    fi
    if (( max_watches < 100000 )); then
        fail "fs.inotify.max_user_watches is ${max_watches} (need >= 100000). Fix: sudo sysctl fs.inotify.max_user_watches=524288"
    fi
    ok "Kernel limits OK."
}

preflight_check_docker() {
    step "Pre-flight: checking Docker daemon..."
    if ! docker info &>/dev/null; then
        fail "Docker daemon is not running."
    fi
    ok "Docker daemon is reachable."
}

# ---------------------------------------------------------------------------
# Cluster health check — verifies kube-system pods are healthy.
# Catches issues like kube-proxy CrashLoopBackOff (e.g. from exhausted
# inotify instances) before they cascade into mysterious timeouts later.
# Waits up to 30s for pods to settle before checking.
# ---------------------------------------------------------------------------
check_cluster_health() {
    local label="${1:-}"
    local wait_timeout=30
    local interval=5
    local elapsed=0

    step "Health check: kube-system pods${label:+ (${label})}..."

    while (( elapsed < wait_timeout )); do
        local unhealthy
        unhealthy=$(kubectl get pods -n kube-system --no-headers 2>/dev/null \
            | awk '$3 == "CrashLoopBackOff" || $3 == "Error" || $3 == "ImagePullBackOff" { print $1, $3 }') || true

        if [[ -n "${unhealthy}" ]]; then
            # Hard failures — don't wait, bail immediately.
            warn "Unhealthy kube-system pods:"
            echo "${unhealthy}" | while read -r pod status; do
                info "  ${pod}: ${status}"
                kubectl logs -n kube-system "${pod}" --tail=5 2>/dev/null | while read -r line; do
                    info "    ${line}"
                done
            done
            fail "Cluster is not healthy. Check kube-system pods before proceeding."
        fi

        # Check if all pods are Running and fully ready.
        local not_ready
        not_ready=$(kubectl get pods -n kube-system --no-headers 2>/dev/null \
            | awk '$3 != "Running" && $3 != "Completed" { print $1, $3 }
                   $3 == "Running" { split($2, a, "/"); if (a[1] != a[2]) print $1, $2 }') || true

        if [[ -z "${not_ready}" ]]; then
            ok "All kube-system pods healthy."
            return 0
        fi

        info "Waiting for pods to settle... (${elapsed}s / ${wait_timeout}s)"
        sleep "${interval}"
        elapsed=$(( elapsed + interval ))
    done

    warn "kube-system pods not fully ready after ${wait_timeout}s:"
    kubectl get pods -n kube-system --no-headers 2>/dev/null \
        | awk '$3 != "Running" && $3 != "Completed" { print "  " $1 ": " $3 }
               $3 == "Running" { split($2, a, "/"); if (a[1] != a[2]) print "  " $1 ": " $2 }' || true
    fail "Cluster health check timed out."
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
# cert-manager (for TLS mode)
# ---------------------------------------------------------------------------
CERT_MANAGER_VERSION="v1.19.3"
CERT_MANAGER_URL="https://github.com/cert-manager/cert-manager/releases/download/${CERT_MANAGER_VERSION}/cert-manager.yaml"
CERT_MANAGER_ISSUER="tapir-ca-issuer"

install_cert_manager() {
    if [[ "${TAPIR_TLS}" != "1" ]]; then return; fi

    step "Installing cert-manager ${CERT_MANAGER_VERSION}..."

    # Check if already installed.
    if kubectl get crd certificates.cert-manager.io &>/dev/null; then
        info "cert-manager CRDs already present."
    else
        run_cmd kubectl apply -f "${CERT_MANAGER_URL}"
    fi

    info "Waiting for cert-manager deployments to be ready..."
    run_cmd kubectl wait deployment.apps/cert-manager \
        --for=condition=Available --namespace=cert-manager --timeout=60s
    run_cmd kubectl wait deployment.apps/cert-manager-cainjector \
        --for=condition=Available --namespace=cert-manager --timeout=60s
    run_cmd kubectl wait deployment.apps/cert-manager-webhook \
        --for=condition=Available --namespace=cert-manager --timeout=60s
    ok "cert-manager deployments are ready."

    # Create a proper CA chain. A plain self-signed issuer makes each cert its
    # own CA — no shared trust root for mTLS. Instead we create:
    #   1. Self-signed ClusterIssuer (bootstrap — only signs the CA cert)
    #   2. CA Certificate in the cert-manager namespace (isCA: true)
    #   3. CA ClusterIssuer referencing the CA secret
    # All component certs then share the same CA for mutual verification.
    #
    # The deployment may report Available before the webhook endpoint is truly
    # reachable. Retry the apply with backoff until the webhook responds.
    step "Creating CA chain for mTLS..."
    local ca_yaml
    ca_yaml=$(cat <<CAEOF
apiVersion: cert-manager.io/v1
kind: ClusterIssuer
metadata:
  name: tapir-selfsigned-bootstrap
spec:
  selfSigned: {}
---
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: tapir-ca
  namespace: cert-manager
spec:
  isCA: true
  commonName: tapir-ca
  secretName: tapir-ca-secret
  privateKey:
    algorithm: ECDSA
    size: 256
  issuerRef:
    name: tapir-selfsigned-bootstrap
    kind: ClusterIssuer
---
apiVersion: cert-manager.io/v1
kind: ClusterIssuer
metadata:
  name: ${CERT_MANAGER_ISSUER}
spec:
  ca:
    secretName: tapir-ca-secret
CAEOF
)
    local ca_retries=0
    while ! echo "${ca_yaml}" | kubectl apply -f - 2>/dev/null; do
        ca_retries=$((ca_retries + 1))
        if (( ca_retries > 24 )); then
            fail "Failed to create CA chain after 120s (cert-manager webhook not ready?)"
        fi
        info "Waiting for cert-manager webhook... (${ca_retries}/24)"
        sleep 5
    done

    info "Waiting for CA certificate to be issued..."
    run_cmd kubectl wait certificate/tapir-ca \
        --for=condition=Ready --namespace=cert-manager --timeout=60s
    ok "CA chain created — ClusterIssuer '${CERT_MANAGER_ISSUER}' ready."
}

uninstall_cert_manager() {
    if [[ "${TAPIR_TLS}" != "1" ]]; then return; fi

    step "Cleaning up cert-manager resources..."
    kubectl delete clusterissuer "${CERT_MANAGER_ISSUER}" --ignore-not-found 2>/dev/null || true
    kubectl delete certificate tapir-ca -n cert-manager --ignore-not-found 2>/dev/null || true
    kubectl delete secret tapir-ca-secret -n cert-manager --ignore-not-found 2>/dev/null || true
    kubectl delete clusterissuer tapir-selfsigned-bootstrap --ignore-not-found 2>/dev/null || true
    kubectl delete -f "${CERT_MANAGER_URL}" --ignore-not-found 2>/dev/null || true
    ok "cert-manager cleaned up."
}

# ---------------------------------------------------------------------------
# Docker image build and load
# ---------------------------------------------------------------------------
build_and_load_images() {
    if [[ "${TAPIR_BUILD_IMAGES}" == "1" ]]; then
        local build_args=()
        if [[ "${TAPIR_TLS}" == "1" ]]; then
            build_args=(--build-arg "FEATURES=tls")
            info "Building with TLS feature enabled."
        fi

        step "Building TAPIR image '${TAPIR_IMAGE}'..."
        run_cmd docker build -t "${TAPIR_IMAGE}" \
            -f "${PROJECT_ROOT}/src/bin/tapiadm/docker/Dockerfile" \
            "${build_args[@]}" \
            "${PROJECT_ROOT}"
        ok "TAPIR image built."

        step "Building operator image '${TAPIR_OPERATOR_IMAGE}'..."
        run_cmd docker build -t "${TAPIR_OPERATOR_IMAGE}" \
            -f "${PROJECT_ROOT}/kubernetes/operator/Dockerfile" \
            "${PROJECT_ROOT}/kubernetes/operator"
        ok "Operator image built."
    else
        info "Skipping image build (TAPIR_BUILD_IMAGES=0)."
    fi

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

    local tls_flags=()
    if [[ "${TAPIR_TLS}" == "1" ]]; then
        tls_flags=(
            --set "tls.enabled=true"
            --set "tls.issuerRef.name=${CERT_MANAGER_ISSUER}"
            --set "tls.issuerRef.kind=ClusterIssuer"
        )
        info "TLS mode enabled — using issuer '${CERT_MANAGER_ISSUER}'."
    fi

    run_cmd helm upgrade --install tapirs-cluster \
        "${PROJECT_ROOT}/kubernetes/charts/tapirs-cluster" \
        --namespace "${NS}" \
        --create-namespace \
        --set "name=${TAPIR_CLUSTER_NAME}" \
        --set "image=${TAPIR_IMAGE}" \
        "${tls_flags[@]}" \
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

    # Build a pod YAML for the smoke test. When TLS is enabled, we add volume
    # mounts and TLS CLI flags. We use kubectl apply instead of kubectl run
    # --overrides because strategic merge on containers loses the image/args fields.
    local tls_args=""
    local tls_volumes=""
    local tls_volume_mounts=""
    if [[ "${TAPIR_TLS}" == "1" ]]; then
        local tls_secret="${TAPIR_CLUSTER_NAME}-operator-client-tls"
        local internal_san="${TAPIR_CLUSTER_NAME}-internal.${NS}.svc.cluster.local"
        tls_args="\"--tls-cert\", \"/tls/tls.crt\", \"--tls-key\", \"/tls/tls.key\", \"--tls-ca\", \"/tls/ca.crt\", \"--tls-server-name\", \"${internal_san}\","

        # Wait for the operator-client TLS secret to exist.
        info "Waiting for TLS secret '${tls_secret}' to be created by cert-manager..."
        if ! kubectl wait --for=jsonpath='{.type}'=kubernetes.io/tls \
            secret/"${tls_secret}" -n "${NS}" --timeout=60s 2>/dev/null; then
            warn "TLS secret not ready. Skipping smoke test."
            return
        fi

        tls_volumes="
  - name: tls
    secret:
      secretName: ${tls_secret}"
        tls_volume_mounts="
    volumeMounts:
    - name: tls
      mountPath: /tls
      readOnly: true"
    fi

    _smoke_pod() {
        local name="$1"; shift
        local expr="$1"; shift
        cat <<PODEOF
apiVersion: v1
kind: Pod
metadata:
  name: ${name}
  namespace: ${NS}
spec:
  restartPolicy: Never
  containers:
  - name: smoke
    image: ${TAPIR_IMAGE}
    imagePullPolicy: IfNotPresent
    command: ["tapi"]
    args: ["client", "--discovery-tapir-endpoint", "${disc_endpoint}", ${tls_args} "-e", "${expr}"]
    stdin: true${tls_volume_mounts}
  volumes:${tls_volumes:-" []"}
PODEOF
    }

    info "Writing key 'hello' with value 'world'..."
    _smoke_pod tapir-smoke-write "begin; put hello world; commit" | kube apply -f -
    kube wait --for=jsonpath='{.status.phase}'=Succeeded pod/tapir-smoke-write --timeout=60s 2>/dev/null || true
    kube logs tapir-smoke-write 2>/dev/null || true
    kube delete pod tapir-smoke-write --wait=false 2>/dev/null || true

    info "Reading key 'hello' back..."
    _smoke_pod tapir-smoke-read "begin ro; get hello; abort" | kube apply -f -
    kube wait --for=jsonpath='{.status.phase}'=Succeeded pod/tapir-smoke-read --timeout=60s 2>/dev/null || true
    local output
    output=$(kube logs tapir-smoke-read 2>/dev/null) || true
    kube delete pod tapir-smoke-read --wait=false 2>/dev/null || true

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
    preflight_check_tools
    preflight_check_sysctl
    preflight_check_docker

    kind_create
    check_cluster_health "after Kind create"

    install_cert_manager
    build_and_load_images

    # Phase 1: Install operator
    install_operator
    check_cluster_health "after operator install"

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

    uninstall_cert_manager
    kind_delete
    ok "Testbed removed."
}

# ---------------------------------------------------------------------------
# apply-code-changes — rebuild images, load into Kind, rolling restart pods
# ---------------------------------------------------------------------------
cmd_apply_code_changes() {
    build_and_load_images

    step "Rolling restart data plane StatefulSets..."
    if kube get statefulset "${TAPIR_CLUSTER_NAME}-discovery" &>/dev/null; then
        run_cmd kube rollout restart statefulset/"${TAPIR_CLUSTER_NAME}-discovery"
    fi
    for sts in $(kube get statefulsets -l "app.kubernetes.io/instance=${TAPIR_CLUSTER_NAME}" \
        --no-headers -o custom-columns=NAME:.metadata.name 2>/dev/null); do
        if [[ "${sts}" == "${TAPIR_CLUSTER_NAME}-discovery" ]]; then continue; fi
        run_cmd kube rollout restart statefulset/"${sts}"
    done

    step "Rolling restart operator Deployment..."
    if kube_op get deployment tapirs-operator &>/dev/null; then
        run_cmd kube_op rollout restart deployment/tapirs-operator
    fi

    step "Waiting for rollouts to complete..."
    if kube get statefulset "${TAPIR_CLUSTER_NAME}-discovery" &>/dev/null; then
        run_cmd kube rollout status statefulset/"${TAPIR_CLUSTER_NAME}-discovery" --timeout=120s
    fi
    for sts in $(kube get statefulsets -l "app.kubernetes.io/instance=${TAPIR_CLUSTER_NAME}" \
        --no-headers -o custom-columns=NAME:.metadata.name 2>/dev/null); do
        if [[ "${sts}" == "${TAPIR_CLUSTER_NAME}-discovery" ]]; then continue; fi
        run_cmd kube rollout status statefulset/"${sts}" --timeout=120s
    done
    if kube_op get deployment tapirs-operator &>/dev/null; then
        run_cmd kube_op rollout status deployment/tapirs-operator --timeout=120s
    fi

    ok "All components restarted with new images."
}

# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------
usage() {
    printf "Usage: %s <command>\n\n" "$(basename "$0")"
    printf "Commands:\n"
    printf "  up                  Deploy operator + cluster, smoke test, print guide\n"
    printf "  down                Tear down all testbed resources\n"
    printf "  status              Show cluster health\n"
    printf "  apply-code-changes  Rebuild images, load into Kind, rolling restart\n"
    printf "\nEnvironment variables:\n"
    printf "  TAPIR_KIND=1                      Auto-create/destroy Kind cluster\n"
    printf "  TAPIR_KIND_CLUSTER                Kind cluster name (default: tapir-op)\n"
    printf "  TAPIR_NAMESPACE                   Namespace for TAPIRCluster (default: tapir)\n"
    printf "  TAPIR_OPERATOR_NS                 Operator namespace (default: tapirs-operator-system)\n"
    printf "  TAPIR_IMAGE                       TAPIR image (default: tapir:latest)\n"
    printf "  TAPIR_OPERATOR_IMAGE              Operator image (default: tapirs-operator:latest)\n"
    printf "  TAPIR_BUILD_IMAGES                Build images (default: 1, set 0 to skip)\n"
    printf "  TAPIR_CLUSTER_NAME                TAPIRCluster name (default: tapir)\n"
    printf "  TAPIR_TLS=1                       Enable mTLS via cert-manager\n"
    exit 1
}

case "${1:-}" in
    up)                 cmd_up                 ;;
    down)               cmd_down               ;;
    status)             cmd_status             ;;
    apply-code-changes) cmd_apply_code_changes ;;
    *)                  usage                  ;;
esac
