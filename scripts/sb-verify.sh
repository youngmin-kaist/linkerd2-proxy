#!/bin/bash
# 11x decomposition cells — stepwise, gated. usage: sb-verify.sh <push|dpa> <timed|count> <CORES> [M] [load|curl]
set -u
TRANSPORT=$1; MEASURE=$2; CORES=$3; MCONC=${4:-100}; MODE=${5:-load}
cd /home/youngmin/DPUMesh/linkerd2-proxy
LOG=/home/youngmin/.claude/jobs/7d781695/tmp
HOST=192.168.100.1
step(){ echo "[$(date +%H:%M:%S)] $*"; }
die(){ step "FATAL: $*"; cleanup; exit 1; }
cleanup(){
  pkill -f "release/linkerd2-proxy" 2>/dev/null; pkill -f "release/mock-" 2>/dev/null
  timeout 20 ssh $HOST 'pkill -f "build/dpu""mesh"; true' </dev/null >/dev/null 2>&1
}

step "0. clean both sides"
cleanup; sleep 8

step "1. mocks + proxy"
source scripts/dev-proxy-env.sh >/dev/null 2>&1
export LINKERD2_PROXY_LOG=warn MOCK_POLICY_ECHO_TARGET=1
setsid ./target/release/mock-identity    > $LOG/mock-identity.log 2>&1 </dev/null &
setsid ./target/release/mock-destination > $LOG/mock-destination.log 2>&1 </dev/null &
setsid ./target/release/mock-policy      > $LOG/mock-policy.log 2>&1 </dev/null &
for p in 8087 8088 8089; do for i in $(seq 1 50); do (echo > /dev/tcp/127.0.0.1/$p) 2>/dev/null && break; sleep 0.2; done; done
TSK=""; [ "$CORES" = "1" ] && TSK="taskset -c 15"
DMESH_NUM_WORKERS=1 LINKERD2_PROXY_CORES=$CORES \
    LINKERD2_PROXY_ADMIN_LISTEN_ADDR=127.0.0.1:4991 \
    LINKERD2_PROXY_INBOUND_LISTEN_ADDR=127.0.0.1:5143 \
    setsid $TSK ./target/release/linkerd2-proxy > $LOG/dproxy.log 2>&1 </dev/null &
for i in $(seq 1 20); do grep -aq "Started DOCA comch server" $LOG/dproxy.log 2>/dev/null && break; sleep 1; done
grep -aq "Started DOCA comch server" $LOG/dproxy.log || die "comch server not up"
step "   proxy server up"

step "2. backend bridge (nginx client_header_timeout=60s => curl must fire within 60s of THIS)"
timeout 12 ssh $HOST "cd ~/bf-workspace && rm -f /tmp/ym_be_0.log && setsid env DMESH_BACKEND_CONNECT=127.0.0.1:8086 DMESH_DST_IP=10.0.0.1 DMESH_DST_PORT=8086 DMESH_SERVER_IDX=0 ./build/dpumesh -p 94:00.1 -t 1 -d 1 > /tmp/ym_be_0.log 2>&1 </dev/null & exit 0" </dev/null >/dev/null 2>&1
for i in $(seq 1 25); do grep -aq "Push channel ready (mode 1)" $LOG/dproxy.log 2>/dev/null && break; sleep 1; done
grep -aq "Push channel ready (mode 1)" $LOG/dproxy.log || die "backend channel not ready (dproxy: $(tail -2 $LOG/dproxy.log | tr '\n' ' '))"
step "   backend channel ready"

step "3. ingress bridge"
if [ "$TRANSPORT" = "push" ]; then ING="DMESH_PUSH_BRIDGE_PORT=28080"; else ING="DMESH_BRIDGE_PORT=28080 DMESH_REV_PCI=94:00.1"; fi
timeout 12 ssh $HOST "cd ~/bf-workspace && rm -f /tmp/ym_in_0.log && setsid env $ING DMESH_DST_IP=10.0.0.1 DMESH_DST_PORT=8086 DMESH_SERVER_IDX=0 ./build/dpumesh -p 94:00.1 -t 1 -d 1 > /tmp/ym_in_0.log 2>&1 </dev/null & exit 0" </dev/null >/dev/null 2>&1
for i in $(seq 1 25); do grep -aq "Push channel ready (mode 2)" $LOG/dproxy.log 2>/dev/null && break; sleep 1; done
LIS=$(timeout 20 ssh $HOST "grep -ac 'listening' /tmp/ym_in_0.log 2>/dev/null" </dev/null 2>/dev/null | tr -dc 0-9)
[ "${LIS:-0}" -ge 1 ] || die "ingress not listening (remote log: $(timeout 20 ssh $HOST 'tail -2 /tmp/ym_in_0.log' </dev/null 2>/dev/null | tr '\n' ' '))"
step "   ingress listening on :28080"

if [ "$MODE" != "curl" ]; then step "4. (skip preflight in load cycle: bridge is one-connection-only)"; else
step "4. single-request preflight"
RES=$(timeout 20 ssh $HOST 'curl -s -o /dev/null -w "%{http_code} in %{time_total}s" --max-time 10 --http2-prior-knowledge http://127.0.0.1:28080/' </dev/null 2>/dev/null)
step "   single request: ${RES:-ssh-failed}"
echo "$RES" | grep -q "^200" || die "preflight failed (proxy $(pgrep -f 'release/linkerd2-proxy' >/dev/null && echo alive || echo DEAD); dproxy tail: $(grep -av 'Consumer failed' $LOG/dproxy.log | tail -2 | tr '\n' ' '))"

step "PREFLIGHT OK"; cleanup; exit 0; fi

step "5. h2load ($MEASURE)"
if [ "$MEASURE" = "timed" ]; then ARGS="--duration=10 --warm-up-time=3 -c1 -m$MCONC"; else ARGS="-c1 -m$MCONC -n20000"; fi
timeout 120 ssh $HOST "h2load $ARGS http://127.0.0.1:28080/ 2>&1 | grep -E 'finished in|succeeded|status codes'" </dev/null 2>&1
step "done"; cleanup
