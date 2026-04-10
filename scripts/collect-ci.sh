#!/usr/bin/env bash
#
# Collect GitHub Actions CI results for a given commit or branch.
# Usage: scripts/collect-ci.sh <commit-sha|branch-name> [--repo OWNER/REPO]
#
set -euo pipefail

# --- Color helpers ---
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m' # No Color

info()  { echo -e "${CYAN}==> $*${NC}"; }
ok()    { echo -e "${GREEN}  ✓ $*${NC}"; }
fail()  { echo -e "${RED}  ✗ $*${NC}"; }
warn()  { echo -e "${YELLOW}  ! $*${NC}"; }

# --- Parse args ---
REF=""
REPO=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo) REPO="$2"; shift 2 ;;
    --help|-h)
      echo "Usage: $0 <commit-sha|branch-name> [--repo OWNER/REPO]"
      echo ""
      echo "Collects GitHub Actions workflow results into a local directory."
      echo "Requires: gh CLI (authenticated)"
      echo ""
      echo "Examples:"
      echo "  $0 main"
      echo "  $0 abc1234"
      echo "  $0 feature-branch --repo mumoshu/tapirs"
      exit 0
      ;;
    *) REF="$1"; shift ;;
  esac
done

if [[ -z "$REF" ]]; then
  echo "Error: must specify a commit SHA or branch name" >&2
  echo "Usage: $0 <commit-sha|branch-name> [--repo OWNER/REPO]" >&2
  exit 1
fi

# --- Detect repo ---
if [[ -z "$REPO" ]]; then
  # Parse origin remote URL (prefer this over gh repo view which returns the parent for forks)
  REPO=$(git remote get-url origin 2>/dev/null | sed -E 's#.*github\.com[:/]##; s/\.git$//' || true)
  if [[ -z "$REPO" ]]; then
    REPO=$(gh repo view --json nameWithOwner -q '.nameWithOwner' 2>/dev/null || true)
  fi
fi

if [[ -z "$REPO" ]]; then
  echo "Error: could not detect repository. Use --repo OWNER/REPO" >&2
  exit 1
fi

info "Repository: $REPO"

# --- Resolve ref to SHA ---
RESOLVED_SHA=""
IS_BRANCH=false
DISPLAY_REF="$REF"

if git rev-parse --verify "$REF^{commit}" &>/dev/null; then
  RESOLVED_SHA=$(git rev-parse "$REF^{commit}")
  # Check if this is also a branch name
  if git show-ref --verify "refs/heads/$REF" &>/dev/null || \
     git show-ref --verify "refs/remotes/origin/$REF" &>/dev/null; then
    IS_BRANCH=true
  fi
else
  # Assume it's a remote branch we don't have locally
  IS_BRANCH=true
fi

# --- Fetch workflow runs ---
if [[ -n "$RESOLVED_SHA" ]]; then
  info "Resolved SHA: ${RESOLVED_SHA:0:12}"
  RUNS_JSON=$(gh api "repos/$REPO/actions/runs?head_sha=$RESOLVED_SHA&per_page=100" \
    --jq '.workflow_runs | map({id, name, status, conclusion, html_url, run_started_at, head_sha, head_branch})')
elif [[ "$IS_BRANCH" == true ]]; then
  info "Fetching latest runs for branch: $REF"
  RUNS_JSON=$(gh api "repos/$REPO/actions/runs?branch=$REF&per_page=100" \
    --jq '[.workflow_runs[:50] | group_by(.name) | .[] | sort_by(.run_started_at) | last] | map({id, name, status, conclusion, html_url, run_started_at, head_sha, head_branch})')
  RESOLVED_SHA=$(echo "$RUNS_JSON" | jq -r '.[0].head_sha // empty')
else
  echo "Error: could not resolve '$REF' as commit or branch" >&2
  exit 1
fi

RUN_COUNT=$(echo "$RUNS_JSON" | jq 'length')
if [[ "$RUN_COUNT" -eq 0 ]]; then
  echo "No workflow runs found for '$REF'" >&2
  exit 1
fi

info "Found $RUN_COUNT workflow run(s)"

# --- Create output directory ---
TIMESTAMP=$(date -u +%Y%m%d-%H%M%S)
SAFE_REF=$(echo "$DISPLAY_REF" | tr '/' '_' | tr -cd '[:alnum:]_.-')
if [[ -n "$RESOLVED_SHA" ]]; then
  SAFE_REF="${SAFE_REF}_${RESOLVED_SHA:0:8}"
fi
OUTDIR="/tmp/ci-collect/${TIMESTAMP}_${SAFE_REF}"
mkdir -p "$OUTDIR"

info "Output directory: $OUTDIR"

# --- Collect results ---
SUMMARY_FILE="$OUTDIR/summary.txt"
{
  echo "CI Collection for $REF ($REPO)"
  echo "Collected at: $(date -u -Iseconds)"
  echo "SHA: ${RESOLVED_SHA:-unknown}"
  echo "========================================"
  echo ""
} > "$SUMMARY_FILE"

# Process each workflow run
echo "$RUNS_JSON" | jq -c '.[]' | while read -r RUN; do
  RUN_ID=$(echo "$RUN" | jq -r '.id')
  WF_NAME=$(echo "$RUN" | jq -r '.name')
  STATUS=$(echo "$RUN" | jq -r '.status')
  CONCLUSION=$(echo "$RUN" | jq -r '.conclusion')

  # Sanitize workflow name for directory
  SAFE_WF=$(echo "$WF_NAME" | tr ' ' '-' | tr -cd '[:alnum:]_.-')
  WF_DIR="$OUTDIR/$SAFE_WF"
  mkdir -p "$WF_DIR"

  if [[ "$STATUS" != "completed" ]]; then
    warn "Workflow '$WF_NAME' is still $STATUS"
    echo "[PENDING] $WF_NAME ($STATUS)" >> "$SUMMARY_FILE"
    echo "" >> "$SUMMARY_FILE"
    continue
  fi

  if [[ "$CONCLUSION" == "success" ]]; then
    ok "Workflow '$WF_NAME': $CONCLUSION"
  else
    fail "Workflow '$WF_NAME': $CONCLUSION"
  fi

  echo "[$CONCLUSION] $WF_NAME" >> "$SUMMARY_FILE"

  # Fetch jobs
  JOBS_JSON=$(gh api "repos/$REPO/actions/runs/$RUN_ID/jobs?per_page=100" \
    --jq '.jobs | map({id, name, status, conclusion, started_at, completed_at})')

  echo "$JOBS_JSON" | jq -c '.[]' | while read -r JOB; do
    JOB_ID=$(echo "$JOB" | jq -r '.id')
    JOB_NAME=$(echo "$JOB" | jq -r '.name')
    JOB_CONCLUSION=$(echo "$JOB" | jq -r '.conclusion')

    # Extract matrix info from job name: "build (ubuntu, 3.10)" -> job=build, matrix=ubuntu_3.10
    if [[ "$JOB_NAME" =~ ^(.+)\ \((.+)\)$ ]]; then
      BASE_NAME="${BASH_REMATCH[1]}"
      MATRIX_PART="${BASH_REMATCH[2]}"
      SAFE_MATRIX=$(echo "$MATRIX_PART" | tr ', ' '_' | tr -cd '[:alnum:]_.-')
      SAFE_JOB=$(echo "$BASE_NAME" | tr ' ' '-' | tr -cd '[:alnum:]_.-')
      JOB_DIR_NAME="${SAFE_JOB}-${SAFE_MATRIX}"
    else
      SAFE_JOB=$(echo "$JOB_NAME" | tr ' ' '-' | tr -cd '[:alnum:]_.-')
      JOB_DIR_NAME="$SAFE_JOB"
    fi

    JOB_DIR="$WF_DIR/$JOB_DIR_NAME"
    mkdir -p "$JOB_DIR"

    # Download job log
    gh api "repos/$REPO/actions/jobs/$JOB_ID/logs" > "$JOB_DIR/job.log" 2>/dev/null || true

    if [[ "$JOB_CONCLUSION" == "success" ]]; then
      ok "  Job '$JOB_NAME': $JOB_CONCLUSION"
      echo "  [pass] $JOB_NAME" >> "$SUMMARY_FILE"
    elif [[ "$JOB_CONCLUSION" == "skipped" ]]; then
      echo "  [skip] $JOB_NAME" >> "$SUMMARY_FILE"
    else
      fail "  Job '$JOB_NAME': $JOB_CONCLUSION"
      echo "  [FAIL] $JOB_NAME -> $JOB_DIR/job.log" >> "$SUMMARY_FILE"
    fi
  done

  # Download artifacts for this run
  ARTIFACT_COUNT=$(gh api "repos/$REPO/actions/runs/$RUN_ID/artifacts" --jq '.artifacts | length' 2>/dev/null || echo "0")
  if [[ "$ARTIFACT_COUNT" -gt 0 ]]; then
    ARTIFACT_DIR="$WF_DIR/artifacts"
    mkdir -p "$ARTIFACT_DIR"
    gh run download "$RUN_ID" -R "$REPO" -D "$ARTIFACT_DIR" 2>/dev/null || warn "  Failed to download some artifacts for '$WF_NAME'"
    ok "  Downloaded $ARTIFACT_COUNT artifact(s) -> $ARTIFACT_DIR/"
    echo "  [artifacts] -> $ARTIFACT_DIR/" >> "$SUMMARY_FILE"
  fi

  echo "" >> "$SUMMARY_FILE"
done

# --- Print summary ---
echo ""
echo -e "${BOLD}========================================${NC}"
echo -e "${BOLD}  CI Collection Summary${NC}"
echo -e "${BOLD}========================================${NC}"
echo ""
cat "$SUMMARY_FILE"
echo ""
echo -e "${BOLD}Guidance:${NC}"
echo "  - Each job log is at: <workflow>/<job>/job.log"
echo "  - Artifacts (if any) are at: <workflow>/artifacts/"
echo "  - For failed jobs, check the [FAIL] lines above for exact log paths"
echo "  - Full summary saved to: $SUMMARY_FILE"
echo ""
info "Results saved to: $OUTDIR"
