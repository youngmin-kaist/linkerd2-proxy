#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="${ROOT:-$(cd "$SCRIPT_DIR/.." && pwd)}"
LOG_DIR="${LOG_DIR:-$ROOT/target/local-outbound-h2load}"

mkdir -p "$LOG_DIR"
cd "$ROOT"

source scripts/dev-proxy-env.sh

# This script is intended to be reproducible even if the caller's shell has
# stale Linkerd env vars from earlier manual runs.
export LINKERD2_PROXY_IDENTITY_SVC_ADDR="127.0.0.1:8088"
export LINKERD2_PROXY_DESTINATION_SVC_ADDR="127.0.0.1:8089"
export LINKERD2_PROXY_POLICY_SVC_ADDR="127.0.0.1:8087"
export LINKERD2_PROXY_OUTBOUND_LISTEN_ADDR="127.0.0.1:4140"
export LINKERD2_PROXY_POLICY_WORKLOAD="local-dev"
export LINKERD2_PROXY_DESTINATION_PROFILE_NETWORKS="127.0.0.0/24"
export MOCK_DESTINATION_ADDR="127.0.0.1:8089"
export MOCK_DESTINATION_BACKEND="127.0.0.1:8086"
export MOCK_POLICY_ADDR="127.0.0.1:8087"
export MOCK_POLICY_BACKEND="127.0.0.1:8086"

PIDS=()

cleanup() {
  local pid
  for pid in "${PIDS[@]:-}"; do
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
    fi
  done
  sleep 0.2
  for pid in "${PIDS[@]:-}"; do
    if kill -0 "$pid" 2>/dev/null; then
      kill -KILL "$pid" 2>/dev/null || true
    fi
  done
  wait "${PIDS[@]:-}" 2>/dev/null || true
}
trap cleanup EXIT

dump_logs() {
  echo
  echo "Effective addresses:"
  echo "  identity:    $LINKERD2_PROXY_IDENTITY_SVC_ADDR"
  echo "  destination: $LINKERD2_PROXY_DESTINATION_SVC_ADDR"
  echo "  policy:      $LINKERD2_PROXY_POLICY_SVC_ADDR"
  echo "  outbound:    $LINKERD2_PROXY_OUTBOUND_LISTEN_ADDR"
  echo "  backend:     $MOCK_POLICY_BACKEND"
  echo
  echo "Recent proxy log:"
  tail -n 160 "$LOG_DIR/linkerd2-proxy.log" 2>/dev/null || true
  echo
  echo "Recent destination log:"
  tail -n 80 "$LOG_DIR/mock-destination.log" 2>/dev/null || true
  echo
  echo "Recent policy log:"
  tail -n 80 "$LOG_DIR/mock-policy.log" 2>/dev/null || true
}

wait_for_port() {
  local name="$1"
  local port="$2"
  local i

  for i in $(seq 1 80); do
    if (echo >"/dev/tcp/127.0.0.1/$port") >/dev/null 2>&1; then
      echo "$name is listening on 127.0.0.1:$port"
      return 0
    fi
    sleep 0.25
  done

  echo "Timed out waiting for $name on 127.0.0.1:$port" >&2
  echo "Logs are in $LOG_DIR" >&2
  return 1
}

if ! (echo >"/dev/tcp/127.0.0.1/8086") >/dev/null 2>&1; then
  echo "Backend nginx is not listening on 127.0.0.1:8086" >&2
  exit 1
fi
echo "backend is listening on 127.0.0.1:8086"

source scripts/run-local-mock-services.sh

PROXY_FEATURES="${PROXY_FEATURES:-allow-loopback}"
cargo run -p linkerd2-proxy --features "$PROXY_FEATURES" \
  >"$LOG_DIR/linkerd2-proxy.log" 2>&1 &
PIDS+=("$!")
wait_for_port linkerd2-proxy 4140

echo "Running h2load..."
echo "Proxy features: $PROXY_FEATURES"
read -r -a H2LOAD_ARGS_ARRAY <<< "${H2LOAD_ARGS:--c100 -n10000 http://localhost:4140/}"
echo "+ h2load ${H2LOAD_ARGS_ARRAY[*]}"

set +e
timeout "${H2LOAD_TIMEOUT:-20s}" h2load "${H2LOAD_ARGS_ARRAY[@]}" 2>&1 \
  | tee "$LOG_DIR/h2load.log"
status=${PIPESTATUS[0]}
set -e

if [ "$status" -ne 0 ]; then
  echo "h2load failed or timed out with status $status"
  dump_logs
  exit "$status"
fi

echo
echo "Recent proxy routing log:"
grep -E "Detected|Using ClientPolicy|Connecting server.addr=127.0.0.1:8086|method=GET|status" \
  "$LOG_DIR/linkerd2-proxy.log" | tail -n 20 || true

echo
echo "Logs are in $LOG_DIR"
