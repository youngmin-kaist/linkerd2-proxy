#!/usr/bin/env bash

# Development environment for running linkerd2-proxy with the local mock
# identity service in linkerd/app/integration/src/bin/mock-identity.rs.
#
# Usage:
#   source scripts/dev-proxy-env.sh
#   cargo run -p linkerd-app-integration --bin mock-identity
#   cargo run -p linkerd-app-integration --bin mock-destination
#   cargo run -p linkerd-app-integration --bin mock-policy
#   cargo run -p linkerd2-proxy --features allow-loopback
#   h2load -p http/1.1 -n100 -c10 http://127.0.0.1:4140/

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEFAULT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
ROOT="${LINKERD2_PROXY_ROOT:-${ROOT:-$DEFAULT_ROOT}}"
DATA="$ROOT/linkerd/app/integration/src/data"

if [[ ! -f "$DATA/ca1.pem" ]]; then
  DEFAULT_DATA="$DEFAULT_ROOT/linkerd/app/integration/src/data"
  if [[ -f "$DEFAULT_DATA/ca1.pem" ]]; then
    echo "Ignoring stale ROOT=$ROOT; using $DEFAULT_ROOT" >&2
    ROOT="$DEFAULT_ROOT"
    DATA="$DEFAULT_DATA"
  else
    echo "Missing identity trust anchor: $DATA/ca1.pem" >&2
    echo "Set LINKERD2_PROXY_ROOT to your linkerd2-proxy checkout if needed." >&2
    return 1 2>/dev/null || exit 1
  fi
fi
IDENTITY="default.default.serviceaccount.identity.linkerd.cluster.local"

export LINKERD2_PROXY_IDENTITY_LOCAL_NAME="$IDENTITY"
export LINKERD2_PROXY_IDENTITY_TRUST_ANCHORS="$(cat "$DATA/ca1.pem")"
export LINKERD2_PROXY_IDENTITY_DIR="$DATA/default-default"
export LINKERD2_PROXY_IDENTITY_TOKEN_FILE="$DATA/default-default/token.txt"
export LINKERD2_PROXY_IDENTITY_SVC_ADDR="${LINKERD2_PROXY_IDENTITY_SVC_ADDR:-127.0.0.1:8088}"

export LINKERD2_PROXY_OUTBOUND_LISTEN_ADDR="${LINKERD2_PROXY_OUTBOUND_LISTEN_ADDR:-127.0.0.1:4140}"

# Loopback control-plane addresses do not use TLS names. Keep the destination
# control service separate from the backend nginx service.
export LINKERD2_PROXY_DESTINATION_SVC_ADDR="${LINKERD2_PROXY_DESTINATION_SVC_ADDR:-127.0.0.1:8089}"
export LINKERD2_PROXY_POLICY_SVC_ADDR="${LINKERD2_PROXY_POLICY_SVC_ADDR:-127.0.0.1:8087}"
export LINKERD2_PROXY_POLICY_WORKLOAD="${LINKERD2_PROXY_POLICY_WORKLOAD:-local-dev}"
export LINKERD2_PROXY_DESTINATION_PROFILE_NETWORKS="${LINKERD2_PROXY_DESTINATION_PROFILE_NETWORKS:-127.0.0.0/24}"

# The mock control-plane binaries use the same backend default. Override these
# before starting them if your local backend is not nginx on 127.0.0.1:8086.
export MOCK_DESTINATION_BACKEND="${MOCK_DESTINATION_BACKEND:-127.0.0.1:8086}"
export MOCK_POLICY_BACKEND="${MOCK_POLICY_BACKEND:-127.0.0.1:8086}"

export LINKERD2_PROXY_LOG="${LINKERD2_PROXY_LOG:-linkerd=debug,info}"
export RUSTFLAGS="${RUSTFLAGS:---cfg tokio_unstable}"


export LINKERD2_PROXY_DOCA_DEV_PCI_ADDR="03:00.1"
export LINKERD2_PROXY_DOCA_REP_PCI_ADDR="94:00.1"