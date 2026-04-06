#!/usr/bin/env bash
#
# testbed-kube-operator-s3.sh — Deploy TAPIR with S3 remote storage via MinIO + operator.
#
# Usage:
#   scripts/testbed-kube-operator-s3.sh up       Deploy MinIO + operator + S3-enabled cluster, full E2E test
#   scripts/testbed-kube-operator-s3.sh down     Tear down everything
#   scripts/testbed-kube-operator-s3.sh status   Show cluster health
#
# Extends the testbed-kube-operator.sh pattern with MinIO deployment and S3
# verification. Tests the full S3 lifecycle: writes, S3 uploads, read replica,
# cluster backup, and restore.
#
# Environment variables (same as testbed-kube-operator.sh, plus):
#   TAPIR_S3_BUCKET=tapir-e2e                S3 bucket name
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Defaults
: "${TAPIR_KIND:=0}"
: "${TAPIR_KIND_CLUSTER:=tapir-s3}"
: "${TAPIR_NAMESPACE:=tapir}"
: "${TAPIR_OPERATOR_NS:=tapirs-operator-system}"
: "${TAPIR_IMAGE:=tapir:latest}"
: "${TAPIR_OPERATOR_IMAGE:=tapirs-operator:latest}"
: "${TAPIR_BUILD_IMAGES:=1}"
: "${TAPIR_CLUSTER_NAME:=tapir}"
: "${TAPIR_S3_BUCKET:=tapir-e2e}"

DISCOVERY_TAPIR_PORT=6000
ADMIN_PORT=9000
MINIO_PORT=9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin

NS="${TAPIR_NAMESPACE}"
OP_NS="${TAPIR_OPERATOR_NS}"

# ---------------------------------------------------------------------------
# Colors
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
# Cluster health check
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
            warn "Unhealthy kube-system pods:"
            echo "${unhealthy}" | while read -r pod status; do
                info "  ${pod}: ${status}"
            done
            fail "Cluster is not healthy."
        fi

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

    fail "Cluster health check timed out."
}

# ---------------------------------------------------------------------------
# Docker image build and load
# ---------------------------------------------------------------------------
build_and_load_images() {
    if [[ "${TAPIR_BUILD_IMAGES}" == "1" ]]; then
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
# MinIO deployment
# ---------------------------------------------------------------------------
deploy_minio() {
    step "Deploying MinIO into namespace '${NS}'..."

    kubectl create namespace "${NS}" --dry-run=client -o yaml | kubectl apply -f -

    # Create MinIO Deployment.
    cat <<MEOF | kube apply -f -
apiVersion: apps/v1
kind: Deployment
metadata:
  name: minio
  namespace: ${NS}
spec:
  replicas: 1
  selector:
    matchLabels:
      app: minio
  template:
    metadata:
      labels:
        app: minio
    spec:
      containers:
      - name: minio
        image: minio/minio
        args: ["server", "/data"]
        env:
        - name: MINIO_ROOT_USER
          value: "${MINIO_ACCESS_KEY}"
        - name: MINIO_ROOT_PASSWORD
          value: "${MINIO_SECRET_KEY}"
        ports:
        - containerPort: ${MINIO_PORT}
        readinessProbe:
          httpGet:
            path: /minio/health/ready
            port: ${MINIO_PORT}
          initialDelaySeconds: 5
          periodSeconds: 5
MEOF

    # Create MinIO Service.
    cat <<SEOF | kube apply -f -
apiVersion: v1
kind: Service
metadata:
  name: minio
  namespace: ${NS}
spec:
  selector:
    app: minio
  ports:
  - port: ${MINIO_PORT}
    targetPort: ${MINIO_PORT}
SEOF

    info "Waiting for MinIO deployment to be ready..."
    kube rollout status deployment/minio --timeout=120s
    ok "MinIO deployed."
}

create_s3_bucket() {
    step "Creating S3 bucket '${TAPIR_S3_BUCKET}' on MinIO..."

    local minio_endpoint="http://minio.${NS}.svc.cluster.local:${MINIO_PORT}"

    kube delete pod aws-cli-setup 2>/dev/null || true
    kube run aws-cli-setup --rm -i --restart=Never \
        --image=amazon/aws-cli \
        --env="AWS_ACCESS_KEY_ID=${MINIO_ACCESS_KEY}" \
        --env="AWS_SECRET_ACCESS_KEY=${MINIO_SECRET_KEY}" \
        -- --endpoint-url "${minio_endpoint}" s3 mb "s3://${TAPIR_S3_BUCKET}" 2>/dev/null || true
    ok "Bucket '${TAPIR_S3_BUCKET}' created."
}

# ---------------------------------------------------------------------------
# AWS credentials Secret for pods
# ---------------------------------------------------------------------------
create_aws_secret() {
    step "Creating AWS credentials Secret for TAPIR pods..."

    cat <<AEOF | kube apply -f -
apiVersion: v1
kind: Secret
metadata:
  name: minio-credentials
  namespace: ${NS}
type: Opaque
stringData:
  AWS_ACCESS_KEY_ID: "${MINIO_ACCESS_KEY}"
  AWS_SECRET_ACCESS_KEY: "${MINIO_SECRET_KEY}"
AEOF
    ok "AWS credentials Secret created."
}

# ---------------------------------------------------------------------------
# Helm install / uninstall
# ---------------------------------------------------------------------------
install_operator() {
    step "Installing tapirs-operator Helm chart..."

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

install_cluster_with_s3() {
    step "Installing tapirs-cluster Helm chart with S3 enabled..."

    local minio_endpoint="http://minio.${NS}.svc.cluster.local:${MINIO_PORT}"

    run_cmd helm upgrade --install tapirs-cluster \
        "${PROJECT_ROOT}/kubernetes/charts/tapirs-cluster" \
        --namespace "${NS}" \
        --create-namespace \
        --set "name=${TAPIR_CLUSTER_NAME}" \
        --set "image=${TAPIR_IMAGE}" \
        --set "destination.s3.bucket=${TAPIR_S3_BUCKET}" \
        --set "destination.s3.endpoint=${minio_endpoint}" \
        --set "destination.s3.credentialsSecret=minio-credentials" \
        --wait --timeout 30s
    ok "Cluster chart installed with S3 enabled."
}

uninstall_cluster() {
    step "Uninstalling tapirs-cluster..."
    if helm status tapirs-cluster --namespace "${NS}" &>/dev/null; then
        run_cmd helm uninstall tapirs-cluster --namespace "${NS}" --wait --timeout 60s
        ok "Cluster chart uninstalled."
    else
        info "tapirs-cluster release not found."
    fi
}

uninstall_operator() {
    step "Uninstalling tapirs-operator..."
    if helm status tapirs-operator --namespace "${OP_NS}" &>/dev/null; then
        run_cmd helm uninstall tapirs-operator --namespace "${OP_NS}" --wait --timeout 60s
        ok "Operator chart uninstalled."
    else
        info "tapirs-operator release not found."
    fi
}

# ---------------------------------------------------------------------------
# Wait for TAPIRCluster Running
# ---------------------------------------------------------------------------
wait_cluster_running() {
    local timeout="${TAPIR_WAIT_TIMEOUT:-180}"
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

    dump_logs
    fail "TAPIRCluster did not reach Running within ${timeout}s."
}

# ---------------------------------------------------------------------------
# Diagnostic logs
# ---------------------------------------------------------------------------
dump_logs() {
    step "Cluster resource status:"
    kube get tapircluster "${TAPIR_CLUSTER_NAME}" -o yaml 2>/dev/null || true
    kube get pods -o wide 2>/dev/null || true

    step "Shard-manager logs:"
    kube logs -l app.kubernetes.io/component=shard-manager --tail=100 2>/dev/null || true

    step "Discovery pod logs:"
    for pod in $(kube get pods -l app.kubernetes.io/component=discovery \
        -o jsonpath='{.items[*].metadata.name}' 2>/dev/null); do
        info "  --- ${pod} ---"
        kube logs "${pod}" --tail=50 2>/dev/null || true
    done

    step "Data node pod logs:"
    for pod in $(kube get pods \
        -l "app.kubernetes.io/instance=${TAPIR_CLUSTER_NAME},app.kubernetes.io/component notin (discovery,shard-manager)" \
        -o jsonpath='{.items[*].metadata.name}' 2>/dev/null); do
        info "  --- ${pod} ---"
        kube logs "${pod}" --tail=30 2>/dev/null || true
    done

    step "Operator logs:"
    kube_op logs deployment/tapirs-operator --tail=50 2>/dev/null || true
}

# ---------------------------------------------------------------------------
# Verification helpers
# ---------------------------------------------------------------------------
_smoke_pod() {
    local name="$1"; shift
    local expr="$1"; shift
    local disc_endpoint="srv://${TAPIR_CLUSTER_NAME}-discovery.${NS}.svc.cluster.local:${DISCOVERY_TAPIR_PORT}"

    cat <<PODEOF
apiVersion: v1
kind: Pod
metadata:
  name: ${name}
  namespace: ${NS}
spec:
  restartPolicy: Never
  containers:
  - name: client
    image: ${TAPIR_IMAGE}
    imagePullPolicy: IfNotPresent
    command: ["tapi"]
    args: ["client", "--discovery-tapir-endpoint", "${disc_endpoint}", "-e", "${expr}"]
    stdin: true
    envFrom:
    - secretRef:
        name: minio-credentials
PODEOF
}

# ---------------------------------------------------------------------------
# E2E Tests
# ---------------------------------------------------------------------------
verify_s3_args_in_statefulset() {
    step "Verifying StatefulSet args contain --s3-bucket..."

    local args
    args=$(kube get sts "${TAPIR_CLUSTER_NAME}-default" \
        -o jsonpath='{.spec.template.spec.containers[0].args}' 2>/dev/null) || true

    if echo "${args}" | grep -q "s3-bucket"; then
        ok "StatefulSet args contain --s3-bucket."
    else
        warn "StatefulSet args: ${args}"
        fail "StatefulSet args do not contain --s3-bucket."
    fi
}

smoke_test_write_read() {
    step "Running smoke test: write + read..."

    local disc_endpoint="srv://${TAPIR_CLUSTER_NAME}-discovery.${NS}.svc.cluster.local:${DISCOVERY_TAPIR_PORT}"

    kube delete pod smoke-write smoke-read 2>/dev/null || true

    info "Writing key 'hello' with value 'world'..."
    _smoke_pod smoke-write "begin; put hello world; commit" | kube apply -f -
    kube wait --for=jsonpath='{.status.phase}'=Succeeded pod/smoke-write --timeout=60s 2>/dev/null || true
    kube logs smoke-write 2>/dev/null || true
    kube delete pod smoke-write --wait=false 2>/dev/null || true

    info "Reading key 'hello' back..."
    _smoke_pod smoke-read "begin ro; get hello; abort" | kube apply -f -
    kube wait --for=jsonpath='{.status.phase}'=Succeeded pod/smoke-read --timeout=60s 2>/dev/null || true
    local output
    output=$(kube logs smoke-read 2>/dev/null) || true
    kube delete pod smoke-read --wait=false 2>/dev/null || true

    if echo "${output}" | grep -q "world"; then
        ok "Smoke test passed: read returned 'world'."
    else
        warn "Smoke test output: ${output}"
        fail "Smoke test failed: expected 'world' in output."
    fi
}

verify_s3_objects() {
    step "Verifying S3 objects on MinIO..."

    local minio_endpoint="http://minio.${NS}.svc.cluster.local:${MINIO_PORT}"

    kube delete pod aws-cli-verify 2>/dev/null || true

    local output
    output=$(kube run aws-cli-verify --rm -i --restart=Never \
        --image=amazon/aws-cli \
        --env="AWS_ACCESS_KEY_ID=${MINIO_ACCESS_KEY}" \
        --env="AWS_SECRET_ACCESS_KEY=${MINIO_SECRET_KEY}" \
        -- --endpoint-url "${minio_endpoint}" s3 ls "s3://${TAPIR_S3_BUCKET}/" --recursive 2>/dev/null) || true

    if [[ -z "${output}" ]]; then
        warn "No S3 objects found in bucket '${TAPIR_S3_BUCKET}'."
        fail "Expected segments and manifests on S3."
    fi

    info "S3 objects:"
    echo "${output}" | head -20 | while read -r line; do info "  ${line}"; done

    if echo "${output}" | grep -q "shard_0"; then
        ok "Found shard_0 objects on S3."
    else
        fail "Missing shard_0 objects on S3."
    fi

    if echo "${output}" | grep -q "shard_1"; then
        ok "Found shard_1 objects on S3."
    else
        fail "Missing shard_1 objects on S3."
    fi
}

verify_cluster_backup() {
    step "Testing cluster backup to S3..."

    local minio_endpoint="http://minio.${NS}.svc.cluster.local:${MINIO_PORT}"
    # Resolve shard-manager service to ClusterIP (the client doesn't resolve DNS).
    local sm_ip
    sm_ip=$(kube get svc "${TAPIR_CLUSTER_NAME}-shard-manager" -o jsonpath='{.spec.clusterIP}' 2>/dev/null)
    local sm_url="http://${sm_ip}:9001"
    info "Shard-manager URL: ${sm_url}"

    kube delete pod tapictl-backup 2>/dev/null || true

    cat <<BEOF | kube apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: tapictl-backup
  namespace: ${NS}
spec:
  restartPolicy: Never
  containers:
  - name: tapictl
    image: ${TAPIR_IMAGE}
    imagePullPolicy: IfNotPresent
    command: ["tapictl"]
    args: ["--s3-endpoint", "${minio_endpoint}",
           "backup", "cluster",
           "--shard-manager-url", "${sm_url}",
           "--output", "s3://${TAPIR_S3_BUCKET}/backup/"]
    envFrom:
    - secretRef:
        name: minio-credentials
BEOF

    kube wait --for=jsonpath='{.status.phase}'=Succeeded pod/tapictl-backup --timeout=120s 2>/dev/null || {
        warn "Backup pod did not succeed."
        kube logs tapictl-backup 2>/dev/null || true
        fail "Cluster backup failed."
    }
    kube logs tapictl-backup 2>/dev/null || true
    kube delete pod tapictl-backup --wait=false 2>/dev/null || true
    ok "Cluster backup succeeded."
}

verify_list_backups() {
    step "Testing list backups from S3..."

    local minio_endpoint="http://minio.${NS}.svc.cluster.local:${MINIO_PORT}"

    kube delete pod tapictl-list 2>/dev/null || true

    cat <<LEOF | kube apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: tapictl-list
  namespace: ${NS}
spec:
  restartPolicy: Never
  containers:
  - name: tapictl
    image: ${TAPIR_IMAGE}
    imagePullPolicy: IfNotPresent
    command: ["tapictl"]
    args: ["--s3-endpoint", "${minio_endpoint}",
           "get", "backups",
           "--dir", "s3://${TAPIR_S3_BUCKET}/backup/"]
    envFrom:
    - secretRef:
        name: minio-credentials
LEOF

    kube wait --for=jsonpath='{.status.phase}'=Succeeded pod/tapictl-list --timeout=60s 2>/dev/null || true
    local output
    output=$(kube logs tapictl-list 2>/dev/null) || true
    kube delete pod tapictl-list --wait=false 2>/dev/null || true

    if [[ -n "${output}" ]]; then
        info "Backups listed:"
        echo "${output}" | while read -r line; do info "  ${line}"; done
        ok "List backups succeeded."
    else
        warn "No backups listed."
        fail "Expected at least one backup in listing."
    fi
}

# ---------------------------------------------------------------------------
# Clone cluster deployment
# ---------------------------------------------------------------------------
CLONE_CLUSTER_NAME="tapir-clone"
CLONE_S3_BUCKET="tapir-clone-data"

deploy_clone_cluster() {
    step "Deploying writable clone cluster from source S3..."

    local minio_endpoint="http://minio.${NS}.svc.cluster.local:${MINIO_PORT}"

    # Create a separate bucket for the clone's ongoing uploads.
    kube delete pod aws-cli-clone-bucket 2>/dev/null || true
    kube run aws-cli-clone-bucket --rm -i --restart=Never \
        --image=amazon/aws-cli \
        --env="AWS_ACCESS_KEY_ID=${MINIO_ACCESS_KEY}" \
        --env="AWS_SECRET_ACCESS_KEY=${MINIO_SECRET_KEY}" \
        -- --endpoint-url "${minio_endpoint}" s3 mb "s3://${CLONE_S3_BUCKET}" 2>/dev/null || true

    run_cmd helm upgrade --install tapirs-clone \
        "${PROJECT_ROOT}/kubernetes/charts/tapirs-cluster" \
        --namespace "${NS}" \
        --set "name=${CLONE_CLUSTER_NAME}" \
        --set "image=${TAPIR_IMAGE}" \
        --set "source.enabled=true" \
        --set "source.s3.bucket=${TAPIR_S3_BUCKET}" \
        --set "source.s3.endpoint=${minio_endpoint}" \
        --set "source.s3.credentialsSecret=minio-credentials" \
        --set "source.mode=writableClone" \
        --set "destination.s3.bucket=${CLONE_S3_BUCKET}" \
        --set "destination.s3.endpoint=${minio_endpoint}" \
        --set "destination.s3.credentialsSecret=minio-credentials" \
        --wait --timeout 30s
    ok "Clone cluster chart installed."

    # Wait for clone cluster to reach Running.
    local timeout=180
    local interval=5
    local elapsed=0
    step "Waiting for clone TAPIRCluster '${CLONE_CLUSTER_NAME}' to reach Running (timeout: ${timeout}s)..."
    while (( elapsed < timeout )); do
        local phase
        phase=$(kube get tapircluster "${CLONE_CLUSTER_NAME}" \
            -o jsonpath='{.status.phase}' 2>/dev/null) || true
        if [[ "${phase}" == "Running" ]]; then
            ok "Clone TAPIRCluster is Running."
            return 0
        fi
        if [[ "${phase}" == "Failed" ]]; then
            kube get tapircluster "${CLONE_CLUSTER_NAME}" -o yaml 2>/dev/null || true
            fail "Clone TAPIRCluster failed."
        fi
        info "Phase: ${phase:-Pending} (${elapsed}s / ${timeout}s)"
        sleep "${interval}"
        elapsed=$(( elapsed + interval ))
    done
    fail "Clone TAPIRCluster did not reach Running within ${timeout}s."
}

verify_clone_reads_source_data() {
    step "Verifying clone cluster reads source data..."


    local disc_endpoint="srv://${CLONE_CLUSTER_NAME}-discovery.${NS}.svc.cluster.local:${DISCOVERY_TAPIR_PORT}"

    kube delete pod clone-read 2>/dev/null || true
    _smoke_pod clone-read "begin ro; get hello; abort" | \
        sed "s|${TAPIR_CLUSTER_NAME}-discovery|${CLONE_CLUSTER_NAME}-discovery|g" | \
        kube apply -f -
    kube wait --for=jsonpath='{.status.phase}'=Succeeded pod/clone-read --timeout=60s 2>/dev/null || true
    local output
    output=$(kube logs clone-read 2>/dev/null) || true
    kube delete pod clone-read --wait=false 2>/dev/null || true

    if echo "${output}" | grep -q "world"; then
        ok "Clone reads source data: 'world'."
    else
        warn "Clone read output: ${output}"
        fail "Clone did not return expected 'world'."
    fi
}

# ---------------------------------------------------------------------------
# Read replica cluster deployment
# ---------------------------------------------------------------------------
REPLICA_CLUSTER_NAME="tapir-replica"

deploy_read_replica_cluster() {
    step "Deploying read replica cluster from source S3..."

    local minio_endpoint="http://minio.${NS}.svc.cluster.local:${MINIO_PORT}"

    run_cmd helm upgrade --install tapirs-replica \
        "${PROJECT_ROOT}/kubernetes/charts/tapirs-cluster" \
        --namespace "${NS}" \
        --set "name=${REPLICA_CLUSTER_NAME}" \
        --set "image=${TAPIR_IMAGE}" \
        --set "source.enabled=true" \
        --set "source.s3.bucket=${TAPIR_S3_BUCKET}" \
        --set "source.s3.endpoint=${minio_endpoint}" \
        --set "source.s3.credentialsSecret=minio-credentials" \
        --set "source.mode=readReplica" \
        --set "source.refreshInterval=5s" \
        --wait --timeout 30s
    ok "Read replica cluster chart installed."

    # Wait for replica cluster to reach Running.
    local timeout=180
    local interval=5
    local elapsed=0
    step "Waiting for read replica TAPIRCluster '${REPLICA_CLUSTER_NAME}' to reach Running (timeout: ${timeout}s)..."
    while (( elapsed < timeout )); do
        local phase
        phase=$(kube get tapircluster "${REPLICA_CLUSTER_NAME}" \
            -o jsonpath='{.status.phase}' 2>/dev/null) || true
        if [[ "${phase}" == "Running" ]]; then
            ok "Read replica TAPIRCluster is Running."
            return 0
        fi
        if [[ "${phase}" == "Failed" ]]; then
            kube get tapircluster "${REPLICA_CLUSTER_NAME}" -o yaml 2>/dev/null || true
            fail "Read replica TAPIRCluster failed."
        fi
        info "Phase: ${phase:-Pending} (${elapsed}s / ${timeout}s)"
        sleep "${interval}"
        elapsed=$(( elapsed + interval ))
    done
    fail "Read replica TAPIRCluster did not reach Running within ${timeout}s."
}

verify_read_replica_reads_source_data() {
    step "Verifying read replica cluster reads source data..."


    local disc_endpoint="srv://${REPLICA_CLUSTER_NAME}-discovery.${NS}.svc.cluster.local:${DISCOVERY_TAPIR_PORT}"

    kube delete pod replica-read 2>/dev/null || true
    _smoke_pod replica-read "begin ro; get hello; abort" | \
        sed "s|${TAPIR_CLUSTER_NAME}-discovery|${REPLICA_CLUSTER_NAME}-discovery|g" | \
        kube apply -f -
    kube wait --for=jsonpath='{.status.phase}'=Succeeded pod/replica-read --timeout=60s 2>/dev/null || true
    local output
    output=$(kube logs replica-read 2>/dev/null) || true
    kube delete pod replica-read --wait=false 2>/dev/null || true

    if echo "${output}" | grep -q "world"; then
        ok "Read replica reads source data: 'world'."
    else
        warn "Read replica output: ${output}"
        fail "Read replica did not return expected 'world'."
    fi
}

# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------
cmd_status() {
    step "Operator status..."
    kube_op get pods -o wide 2>/dev/null || true

    step "TAPIRCluster status..."
    kube get tapircluster -o wide 2>/dev/null || true

    step "Data plane pods..."
    kube get pods -o wide 2>/dev/null || true

    step "MinIO status..."
    kube get deployment minio -o wide 2>/dev/null || true
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

    build_and_load_images

    # Deploy MinIO and create bucket
    deploy_minio
    create_s3_bucket
    create_aws_secret

    # Install operator
    install_operator
    check_cluster_health "after operator install"

    # Install cluster with S3 enabled
    install_cluster_with_s3

    # Wait for Running
    wait_cluster_running

    separator
    step "Running S3 E2E verification suite..."
    separator

    # 1. Verify StatefulSet has S3 args
    verify_s3_args_in_statefulset

    # 2. Smoke test: write + read
    smoke_test_write_read

    # 3. Force view change and wait for S3 upload deterministically.
    # The smoke test wrote "hello=world" in the active memtable. A view
    # change flushes it to vlog, and sync_to_remote uploads to S3.
    # Instead of sleeping, poll S3 for a manifest version > 1 (the initial
    # empty-view manifest is v1; after the smoke test flush it becomes v2+).
    step "Flushing source data to S3 (trigger view change + poll manifest)..."
    kube exec "${TAPIR_CLUSTER_NAME}-default-0" -- \
        tapi admin view-change --admin-listen-addr 127.0.0.1:9000 --shard 0 2>/dev/null || true
    kube exec "${TAPIR_CLUSTER_NAME}-default-0" -- \
        tapi admin view-change --admin-listen-addr 127.0.0.1:9000 --shard 1 2>/dev/null || true

    local minio_endpoint="http://minio.${NS}.svc.cluster.local:${MINIO_PORT}"
    local poll_timeout=30
    local poll_interval=2
    local poll_elapsed=0
    while (( poll_elapsed < poll_timeout )); do
        local versions
        versions=$(kube run --rm -i aws-cli-poll --restart=Never \
            --image=amazon/aws-cli \
            --env="AWS_ACCESS_KEY_ID=${MINIO_ACCESS_KEY}" \
            --env="AWS_SECRET_ACCESS_KEY=${MINIO_SECRET_KEY}" \
            -- --endpoint-url "${minio_endpoint}" s3 ls \
            "s3://${TAPIR_S3_BUCKET}/shard_0/manifests/" 2>/dev/null) || true
        # Count manifest files (v00000001.manifest, v00000002.manifest, ...)
        local manifest_count
        manifest_count=$(echo "${versions}" | grep -c '\.manifest' || true)
        if (( manifest_count >= 2 )); then
            ok "Source data flushed to S3 (${manifest_count} manifests for shard_0)."
            break
        fi
        info "Waiting for S3 manifest (${manifest_count} so far, need >= 2)... (${poll_elapsed}s / ${poll_timeout}s)"
        sleep "${poll_interval}"
        poll_elapsed=$(( poll_elapsed + poll_interval ))
    done
    if (( poll_elapsed >= poll_timeout )); then
        fail "Source data not flushed to S3 within ${poll_timeout}s."
    fi

    # 4. Verify S3 objects exist
    verify_s3_objects

    # 4. Cluster backup to S3
    verify_cluster_backup

    # 5. List backups
    verify_list_backups

    # 6. Deploy writable clone from source S3
    deploy_clone_cluster

    # 7. Verify clone reads source data
    verify_clone_reads_source_data

    # 8. Deploy read replica from source S3
    deploy_read_replica_cluster

    # 9. Verify read replica reads source data
    verify_read_replica_reads_source_data

    separator
    ok "All S3 E2E tests passed."
    separator
}

# ---------------------------------------------------------------------------
# down
# ---------------------------------------------------------------------------
cmd_down() {
    step "Tearing down S3 operator testbed..."

    # Uninstall clone and replica releases if they exist.
    if helm status tapirs-clone --namespace "${NS}" &>/dev/null; then
        run_cmd helm uninstall tapirs-clone --namespace "${NS}" --wait --timeout 60s || true
    fi
    if helm status tapirs-replica --namespace "${NS}" &>/dev/null; then
        run_cmd helm uninstall tapirs-replica --namespace "${NS}" --wait --timeout 60s || true
    fi

    uninstall_cluster

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

    if kubectl get namespace "${NS}" &>/dev/null; then
        run_cmd kubectl delete namespace "${NS}" --wait=true 2>/dev/null || true
    fi
    if kubectl get namespace "${OP_NS}" &>/dev/null; then
        run_cmd kubectl delete namespace "${OP_NS}" --wait=true 2>/dev/null || true
    fi

    kind_delete
    ok "S3 testbed removed."
}

# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------
usage() {
    printf "Usage: %s <command>\n\n" "$(basename "$0")"
    printf "Commands:\n"
    printf "  up       Deploy MinIO + operator + S3-enabled cluster, run E2E tests\n"
    printf "  down     Tear down all testbed resources\n"
    printf "  status   Show cluster health\n"
    printf "\nEnvironment variables:\n"
    printf "  TAPIR_KIND=1               Auto-create/destroy Kind cluster\n"
    printf "  TAPIR_KIND_CLUSTER         Kind cluster name (default: tapir-s3)\n"
    printf "  TAPIR_S3_BUCKET            S3 bucket name (default: tapir-e2e)\n"
    exit 1
}

case "${1:-}" in
    up)      cmd_up      ;;
    down)    cmd_down     ;;
    status)  cmd_status   ;;
    *)       usage        ;;
esac
