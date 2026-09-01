#!/bin/bash
set -u
cd /home/youngmin/DPUMesh/linkerd2-proxy
LOG=${DMESH_LOG_DIR:-/tmp}
HOST=192.168.100.1
step(){ echo "[$(date +%H:%M:%S)] $*"; }
die(){ step "FATAL: $*"; exit 1; }

step "0. clean"
pkill -f "release/linkerd2-proxy" 2>/dev/null; pkill -f "release/mock-" 2>/dev/null
timeout 20 ssh $HOST 'pkill -f "echo-ser""ver"; pkill -f "build/dpu""mesh"; true' </dev/null >/dev/null 2>&1
sleep 10

step "1. mocks + proxy (sharded W=1)"
source scripts/dev-proxy-env.sh >/dev/null 2>&1
export LINKERD2_PROXY_LOG=warn MOCK_POLICY_ECHO_TARGET=1
setsid ./target/release/mock-identity    > $LOG/mock-identity.log 2>&1 </dev/null &
setsid ./target/release/mock-destination > $LOG/mock-destination.log 2>&1 </dev/null &
setsid ./target/release/mock-policy      > $LOG/mock-policy.log 2>&1 </dev/null &
for p in 8087 8088 8089; do for i in $(seq 1 50); do (echo > /dev/tcp/127.0.0.1/$p) 2>/dev/null && break; sleep 0.2; done; done
DMESH_SHARDED=1 DMESH_NUM_WORKERS=1 LINKERD2_PROXY_CORES=1 \
    LINKERD2_PROXY_ADMIN_LISTEN_ADDR=127.0.0.1:4991 \
    LINKERD2_PROXY_INBOUND_LISTEN_ADDR=127.0.0.1:5143 \
    setsid taskset -c 15 ./target/release/linkerd2-proxy > $LOG/dproxy.log 2>&1 </dev/null &
for i in $(seq 1 20); do grep -aq "Started DOCA comch server" $LOG/dproxy.log 2>/dev/null && break; sleep 1; done
grep -aq "Started DOCA comch server" $LOG/dproxy.log || die "proxy server not up"
step "   proxy up"

step "2. echo-server (host, DMA backend channel)"
timeout 8 ssh $HOST "cd ~/bf-workspace/dmeshgo && setsid go run ./cmd/echo-server > /tmp/ym_echo_srv.log 2>&1 </dev/null & exit 0" </dev/null >/dev/null 2>&1
for i in $(seq 1 40); do grep -aq "Push channel ready (mode 1)" $LOG/dproxy.log 2>/dev/null && break; sleep 1; done
grep -aq "Push channel ready (mode 1)" $LOG/dproxy.log || die "backend channel not registered (srv log: $(timeout 10 ssh $HOST 'tail -3 /tmp/ym_echo_srv.log' </dev/null 2>/dev/null | tr '\n' ' '))"
step "   backend channel registered"

step "3. echo-client: single RPC through the proxy"
timeout 60 ssh $HOST "cd ~/bf-workspace/dmeshgo && go run ./cmd/echo-client 2>&1" </dev/null 2>&1 | grep -a "ECHO\|failed"
step "4. h2 종단 검증 (admin metrics)"
curl -s --max-time 5 http://127.0.0.1:4991/metrics | grep -a '^request_total{direction="outbound"' | head -2
step "done (server/proxy left running for inspection)"
