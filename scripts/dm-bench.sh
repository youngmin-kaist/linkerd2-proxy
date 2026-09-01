#!/bin/bash
# 64B gRPC echo throughput over the DMA transport. usage: dm-bench.sh <CORES> <P> [M]
set -u
CORES=$1; P=$2; M=${3:-64}
W=$CORES
cd /home/youngmin/DPUMesh/linkerd2-proxy
LOG=/home/youngmin/.claude/jobs/7d781695/tmp
HOST=192.168.100.1
step(){ echo "[$(date +%H:%M:%S)] $*"; }
die(){ step "FATAL: $*"; exit 1; }

step "0. clean (C=$CORES W=$W P=$P M=$M)"
pkill -f "release/linkerd2-proxy" 2>/dev/null; pkill -f "release/mock-" 2>/dev/null
timeout 20 ssh $HOST 'pkill -f bench-ser; pkill -f bench-cli; pkill -f "build/dpumesh"; true' </dev/null >/dev/null 2>&1
sleep 10

step "1. mocks + proxy (sharded W=$W, no-teardown)"
source scripts/dev-proxy-env.sh >/dev/null 2>&1
export LINKERD2_PROXY_LOG=warn MOCK_POLICY_ECHO_TARGET=1
setsid ./target/release/mock-identity    > $LOG/mock-identity.log 2>&1 </dev/null &
setsid ./target/release/mock-destination > $LOG/mock-destination.log 2>&1 </dev/null &
setsid ./target/release/mock-policy      > $LOG/mock-policy.log 2>&1 </dev/null &
for pt in 8087 8088 8089; do for i in $(seq 1 50); do (echo > /dev/tcp/127.0.0.1/$pt) 2>/dev/null && break; sleep 0.2; done; done
DMESH_NO_TEARDOWN=1 DMESH_SHARDED=1 DMESH_NUM_WORKERS=$W LINKERD2_PROXY_CORES=1 \
    LINKERD2_PROXY_ADMIN_LISTEN_ADDR=127.0.0.1:4991 \
    LINKERD2_PROXY_INBOUND_LISTEN_ADDR=127.0.0.1:5143 \
    setsid taskset -c $((16-CORES))-15 ./target/release/linkerd2-proxy > $LOG/dproxy.log 2>&1 </dev/null &
for i in $(seq 1 25); do [ "$(grep -ac 'Started DOCA comch server' $LOG/dproxy.log 2>/dev/null)" -ge "$W" ] && break; sleep 1; done
[ "$(grep -ac 'Started DOCA comch server' $LOG/dproxy.log)" -ge "$W" ] || die "proxy servers"
step "   proxy up ($W workers)"

step "2. bench-server ($P listeners)"
timeout 8 ssh $HOST "cd ~/bf-workspace/dmeshgo && BENCH_K=$P BENCH_W=$W setsid ./bin/bench-server > /tmp/ym_bsrv.log 2>&1 </dev/null & exit 0" </dev/null >/dev/null 2>&1
for i in $(seq 1 60); do [ "$(grep -ac 'Push channel ready (mode 1)' $LOG/dproxy.log 2>/dev/null)" -ge "$P" ] && break; sleep 1; done
B=$(grep -ac 'Push channel ready (mode 1)' $LOG/dproxy.log); [ "$B" -ge "$P" ] || die "backend channels $B/$P (srv: $(timeout 10 ssh $HOST 'tail -2 /tmp/ym_bsrv.log' </dev/null 2>/dev/null | tr '\n' ' '))"
step "   $B backend listeners ready"

step "3. bench-client (P=$P M=$M, preflight 내장)"
(sleep 8; mpstat -P ALL 4 1 2>/dev/null | awk '/Average/ && $2!="CPU" {busy+=100-$NF; n++} END{printf "[DPU busy ~= %.1f/%d cores]\n", busy/100, n}') &
(sleep 9; N=$(curl -s --max-time 4 http://127.0.0.1:4991/metrics | awk '/^request_total\{direction="outbound"/{s+=$NF} END{printf "%d", s}'); echo "[mid-run outbound request_total = $N]") &
timeout 120 ssh $HOST "cd ~/bf-workspace/dmeshgo && BENCH_P=$P BENCH_M=$M BENCH_K=$P BENCH_W=$W ./bin/bench-client 2>&1 | grep -aE 'preflight|RESULT|error|failed'" </dev/null 2>&1
sleep 1

step "4. h2 종단 검증"
HTTP_N=$(curl -s --max-time 5 http://127.0.0.1:4991/metrics | awk '/^request_total\{direction="outbound"/{s+=$NF} END{printf "%d", s}')
echo "   outbound request_total 합계 = ${HTTP_N:-fail}"
step "done"
pkill -f "release/linkerd2-proxy" 2>/dev/null; pkill -f "release/mock-" 2>/dev/null
timeout 20 ssh $HOST 'pkill -f bench-ser; pkill -f "build/dpumesh"; true' </dev/null >/dev/null 2>&1
