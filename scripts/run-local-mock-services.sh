#!/usr/bin/env bash
set -euo pipefail

RUNNING_STANDALONE=0
if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  RUNNING_STANDALONE=1
fi

if [[ "$RUNNING_STANDALONE" -eq 1 ]]; then
  SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  ROOT="${ROOT:-$(cd "$SCRIPT_DIR/.." && pwd)}"
  LOG_DIR="${LOG_DIR:-$ROOT/target/local-outbound-h2load}"

  mkdir -p "$LOG_DIR"
  cd "$ROOT"

  source scripts/dev-proxy-env.sh

  export LINKERD2_PROXY_IDENTITY_SVC_ADDR="${LINKERD2_PROXY_IDENTITY_SVC_ADDR:-127.0.0.1:8088}"
  export LINKERD2_PROXY_DESTINATION_SVC_ADDR="${LINKERD2_PROXY_DESTINATION_SVC_ADDR:-127.0.0.1:8089}"
  export LINKERD2_PROXY_POLICY_SVC_ADDR="${LINKERD2_PROXY_POLICY_SVC_ADDR:-127.0.0.1:8087}"
  export MOCK_DESTINATION_ADDR="${MOCK_DESTINATION_ADDR:-127.0.0.1:8089}"
  export MOCK_DESTINATION_BACKEND="${MOCK_DESTINATION_BACKEND:-127.0.0.1:8086}"
  export MOCK_POLICY_ADDR="${MOCK_POLICY_ADDR:-127.0.0.1:8087}"
  export MOCK_POLICY_BACKEND="${MOCK_POLICY_BACKEND:-127.0.0.1:8086}"

  PIDS=()

  cleanup() {
    local pid
    for pid in "${PIDS[@]:-}"; do
      if kill -0 "$pid" 2>/dev/null; then
        kill "$pid" 2>/dev/null || true
      fi
    done
    wait "${PIDS[@]:-}" 2>/dev/null || true
  }
  trap cleanup EXIT
fi

wait_for_port() {
  local name="$1"
  local port="$2"
  local pid="${3:-}"
  local i
  local timeout_secs="${MOCK_SERVICE_STARTUP_TIMEOUT_SECS:-300}"
  local attempts=$((timeout_secs * 4))

  for i in $(seq 1 "$attempts"); do
    if (echo >"/dev/tcp/127.0.0.1/$port") >/dev/null 2>&1; then
      echo "$name is listening on 127.0.0.1:$port"
      return 0
    fi
    if [[ -n "$pid" ]] && ! kill -0 "$pid" 2>/dev/null; then
      echo "$name exited before listening on 127.0.0.1:$port" >&2
      echo "Recent $name log:" >&2
      tail -n 80 "$LOG_DIR/$name.log" >&2 || true
      return 1
    fi
    sleep 0.25
  done

  echo "Timed out after ${timeout_secs}s waiting for $name on 127.0.0.1:$port" >&2
  echo "Logs are in $LOG_DIR" >&2
  echo "Recent $name log:" >&2
  tail -n 80 "$LOG_DIR/$name.log" >&2 || true
  return 1
}

start_mock_service() {
  local name="$1"
  local port="$2"
  local bin="$3"

  cargo run -p linkerd-app-integration --bin "$bin" \
    >"$LOG_DIR/$name.log" 2>&1 &
  PIDS+=("$!")
  wait_for_port "$name" "$port" "$!"
}

start_mock_service mock-identity 8088 mock-identity
start_mock_service mock-destination 8089 mock-destination
start_mock_service mock-policy 8087 mock-policy

if [[ "$RUNNING_STANDALONE" -eq 1 ]]; then
  echo
  echo "Mock services are running. Logs are in $LOG_DIR"
  echo "Press Ctrl-C to stop them."
  wait
fi
