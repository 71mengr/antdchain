#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: scripts/release-gate.sh [options]

Production go/no-go gate for ANTDChain.

Options:
  --branch-regex <regex>      Allowed release branch pattern.
                              Default: ^(development|release/.+|hotfix/.+)$
  --rollback-target <ref>     Required. Previous known-good tag/SHA used for rollback.
  --smoke-plan <path>         Required. Path to smoke-test plan/document.
  --staging-report <path>     Required unless --skip-staging. Evidence file that must include:
                                STAKE_EXACT
                                REGISTER_STAKE_FLOW
                                MINING_REQUIRES_STAKE
  --skip-staging              Skip staging evidence checks.
  --skip-race                 Skip go test -race (not recommended for prod).
  --help                      Show this help.

Exit codes:
  0 = GO
  1 = NO-GO
USAGE
}

BRANCH_REGEX='^(development|release/.+|hotfix/.+)$'
ROLLBACK_TARGET=''
SMOKE_PLAN=''
STAGING_REPORT=''
SKIP_STAGING=0
SKIP_RACE=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --branch-regex)
      BRANCH_REGEX="${2:-}"
      shift 2
      ;;
    --rollback-target)
      ROLLBACK_TARGET="${2:-}"
      shift 2
      ;;
    --smoke-plan)
      SMOKE_PLAN="${2:-}"
      shift 2
      ;;
    --staging-report)
      STAGING_REPORT="${2:-}"
      shift 2
      ;;
    --skip-staging)
      SKIP_STAGING=1
      shift
      ;;
    --skip-race)
      SKIP_RACE=1
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

failures=()
passes=()

pass() {
  passes+=("$1")
  printf '✅ %s\n' "$1"
}

fail() {
  failures+=("$1")
  printf '❌ %s\n' "$1"
}

run_check() {
  local label="$1"
  shift
  if "$@"; then
    pass "$label"
  else
    fail "$label"
  fi
}

run_shell_check() {
  local label="$1"
  local cmd="$2"
  if bash -lc "$cmd"; then
    pass "$label"
  else
    fail "$label"
  fi
}

echo "== ANTDChain Production Gate =="

# 1) Release hygiene
if [[ -n "$(git status --porcelain)" ]]; then
  fail "Working tree is clean"
else
  pass "Working tree is clean"
fi

current_branch="$(git rev-parse --abbrev-ref HEAD)"
if [[ "$current_branch" =~ $BRANCH_REGEX ]]; then
  pass "Branch '$current_branch' matches release policy"
else
  fail "Branch '$current_branch' matches release policy ($BRANCH_REGEX)"
fi

if git describe --tags --exact-match >/dev/null 2>&1; then
  pass "Release commit is exactly tagged"
else
  fail "Release commit is exactly tagged"
fi

run_shell_check "go.mod/go.sum are tidy" "go mod tidy >/dev/null 2>&1 && git diff --quiet -- go.mod go.sum"

# 2) Build and code health
run_shell_check "go test ./..." "go test ./..."

if [[ "$SKIP_RACE" -eq 1 ]]; then
  echo "⚠️ Skipping race tests by flag (--skip-race)"
else
  run_shell_check "go test -race ./..." "go test -race ./..."
fi

run_shell_check "go vet ./..." "go vet ./..."
run_shell_check "make build" "make build"

# 3) Lint (hard gate)
if command -v golangci-lint >/dev/null 2>&1; then
  run_shell_check "make lint" "make lint"
else
  fail "golangci-lint installed (required for production gate)"
fi

# 4) Behavior-specific staging evidence
if [[ "$SKIP_STAGING" -eq 1 ]]; then
  echo "⚠️ Skipping staging evidence checks by flag (--skip-staging)"
else
  if [[ -z "$STAGING_REPORT" ]]; then
    fail "Staging report provided (--staging-report)"
  elif [[ ! -f "$STAGING_REPORT" ]]; then
    fail "Staging report file exists: $STAGING_REPORT"
  else
    run_shell_check "Staging includes STAKE_EXACT" "grep -q 'STAKE_EXACT' '$STAGING_REPORT'"
    run_shell_check "Staging includes REGISTER_STAKE_FLOW" "grep -q 'REGISTER_STAKE_FLOW' '$STAGING_REPORT'"
    run_shell_check "Staging includes MINING_REQUIRES_STAKE" "grep -q 'MINING_REQUIRES_STAKE' '$STAGING_REPORT'"
  fi
fi

# 5) Deployment safety
if [[ -z "$ROLLBACK_TARGET" ]]; then
  fail "Rollback target provided (--rollback-target)"
else
  run_shell_check "Rollback target resolves in git" "git rev-parse --verify '$ROLLBACK_TARGET^{commit}' >/dev/null 2>&1"
fi

if [[ -z "$SMOKE_PLAN" ]]; then
  fail "Smoke plan provided (--smoke-plan)"
elif [[ ! -s "$SMOKE_PLAN" ]]; then
  fail "Smoke plan file exists and is non-empty: $SMOKE_PLAN"
else
  pass "Smoke plan file exists and is non-empty: $SMOKE_PLAN"
fi

echo
if [[ ${#failures[@]} -eq 0 ]]; then
  echo "�� GO: All hard gates passed (${#passes[@]} checks)."
  exit 0
fi

echo "�� NO-GO: ${#failures[@]} check(s) failed."
printf '   - %s\n' "${failures[@]}"
exit 1
