#!/bin/bash
# True-L7 (h2-terminated) core sweep. usage: sb-l7.sh <CORES> <W> <M> [curl|load]
# H2PATH=/ok selects nginx `location /ok { return 200 "ok\n"; }` (3B body) for cross-node parity.
# M ingress pairs (port 38080+i, dst 10.0.0.(1+i)) + M backend bridges, K=M.
set -u
CORES=$1; W=$2; M=$3; MODE=${4:-load}
cd /home/youngmin/DPUMesh/linkerd2-proxy
LOG=/home/youngmin/.claude/jobs/7d781695/tmp
HOST=192.168.100.1
step(){ echo "[$(date +%H:%M:%S)] $*"; }
die(){ step "FATAL: $*"; cleanup; exit 1; }
cleanup(){
  pkill -f "release/linkerd2-proxy" 2>/dev/null; pkill -f "release/mock-" 2>/dev/null
  timeout 20 ssh $HOST 'pkill -f "build/dpu""mesh"; true' </dev/null >/dev/null 2>&1
}

step "0. clean (CORES=$CORES W=$W M=$M)"
cleanup; sleep 12
source scripts/dev-proxy-env.sh >/dev/null 2>&1
export LINKERD2_PROXY_LOG=${PROXY_LOG:-warn} MOCK_POLICY_ECHO_TARGET=1
setsid ./target/release/mock-identity    > $LOG/mock-identity.log 2>&1 </dev/null &
setsid ./target/release/mock-destination > $LOG/mock-destination.log 2>&1 </dev/null &
setsid ./target/release/mock-policy      > $LOG/mock-policy.log 2>&1 </dev/null &
for p in 8087 8088 8089; do for i in $(seq 1 50); do (echo > /dev/tcp/127.0.0.1/$p) 2>/dev/null && break; sleep 0.2; done; done

step "1. proxy (W=$W workers, $CORES cores, pinned to $((16-CORES))-15)"
SHENV=""; RT_CORES=$CORES
if [ -n "${DMESH_SHARDED:-}" ]; then SHENV="DMESH_SHARDED=1"; RT_CORES=1; step "   (sharded: W=$W pinned current_thread runtimes, main rt 1 thread)"; fi
DMESH_NUM_WORKERS=$W LINKERD2_PROXY_CORES=$RT_CORES \
    LINKERD2_PROXY_ADMIN_LISTEN_ADDR=127.0.0.1:4991 \
    LINKERD2_PROXY_INBOUND_LISTEN_ADDR=127.0.0.1:5143 \
    setsid env $SHENV taskset -c $((16-CORES))-15 ./target/release/linkerd2-proxy > $LOG/dproxy.log 2>&1 </dev/null &
for i in $(seq 1 25); do [ "$(grep -ac "Started DOCA comch server" $LOG/dproxy.log 2>/dev/null)" -ge "$W" ] && break; sleep 1; done
S=$(grep -ac "Started DOCA comch server" $LOG/dproxy.log); [ "$S" -ge "$W" ] || die "proxy servers $S/$W"
step "   $S comch servers up"

step "2. $M backend bridges"
timeout 15 ssh $HOST "rm -f /tmp/ym_h2_*.log /tmp/ym_in_*.log /tmp/ym_be_*.log" </dev/null >/dev/null 2>&1
timeout 8 ssh $HOST "cd ~/bf-workspace && for j in \$(seq 0 $((M-1))); do setsid env DMESH_BACKEND_CONNECT=127.0.0.1:8086 DMESH_DST_IP=10.0.0.\$((1+j)) DMESH_DST_PORT=8086 DMESH_SERVER_IDX=\$((j % $W)) ./build/dpumesh -p 94:00.1 -t 1 -d 1 > /tmp/ym_be_\$j.log 2>&1 </dev/null & done; exit 0" </dev/null >/dev/null 2>&1
for i in $(seq 1 30); do [ "$(grep -ac "Push channel ready (mode 1)" $LOG/dproxy.log 2>/dev/null)" -ge "$M" ] && break; sleep 1; done
B=$(grep -ac "Push channel ready (mode 1)" $LOG/dproxy.log); [ "$B" -ge "$M" ] || die "backend channels $B/$M"
step "   $B backend channels ready (nginx 60s window open)"

step "3. $M ingress bridges (listen-first; channel attaches on client connect)"
timeout 8 ssh $HOST "cd ~/bf-workspace && for i in \$(seq 0 $((M-1))); do rm -f /tmp/ym_in_\$i.log; setsid env DMESH_PUSH_BRIDGE_PORT=\$((38080+i)) DMESH_DST_IP=10.0.0.\$((1+i)) DMESH_DST_PORT=8086 DMESH_SERVER_IDX=\$((i % $W)) ./build/dpumesh -p 94:00.1 -t 1 -d 1 > /tmp/ym_in_\$i.log 2>&1 </dev/null & done; exit 0" </dev/null >/dev/null 2>&1
LIS=0; for t in $(seq 1 20); do LIS=$(timeout 15 ssh $HOST "grep -al 'push-ingress: listening' /tmp/ym_in_*.log 2>/dev/null | wc -l" </dev/null 2>/dev/null | tr -dc 0-9); [ "${LIS:-0}" -ge "$M" ] && break; sleep 2; done
[ "${LIS:-0}" -ge "$M" ] || die "ingress listening $LIS/$M"
step "   $LIS/$M listening"

if [ "$MODE" = "curl" ]; then
    step "4. preflight: 1 request (port 38080 only; sequential closes risk the teardown segfault)"
    timeout 30 ssh $HOST "echo -n '  port 38080: '; curl -s -o /dev/null -w '%{http_code} in %{time_total}s\n' --max-time 8 --http2-prior-knowledge http://127.0.0.1:38080${H2PATH:-/}" </dev/null 2>&1
    step "PREFLIGHT DONE"; cleanup; exit 0
fi

step "4. h2load x$M (timed 10s, warmup 3s, -c1 -m300)"
(sleep 8; mpstat -P ALL 4 1 2>/dev/null | awk '/Average/ && $2!="CPU" {busy+=100-$NF; n++} END{printf "[mpstat] DPU busy cores ~= %.1f / %d\n", busy/100, n}') &
timeout 120 ssh $HOST "for i in \$(seq 0 $((M-1))); do h2load --duration=10 --warm-up-time=3 -c1 -m300 http://127.0.0.1:\$((38080+i))${H2PATH:-/} > /tmp/ym_h2_\$i.log 2>&1 & done; wait; python3 -c \"
import re,glob
t=0; n=0; ok=0
for f in sorted(glob.glob('/tmp/ym_h2_*.log')):
    s=open(f).read()
    m=re.search(r'finished in [^,]+, ([0-9.]+) req/s', s)
    d=re.search(r'requests: ([0-9]+) total', s)
    if m: t+=float(m.group(1)); ok+=1
    if d: n+=int(d.group(1))
print(f'TOTAL {t:.0f} req/s over {ok} conns, {n} requests')\"; rm -f /tmp/ym_h2_*.log" </dev/null 2>&1

step "5. h2-termination check (metrics)"
HTTP_N=$(curl -s --max-time 5 http://127.0.0.1:4991/metrics | awk '/^request_total\{direction="outbound"/{s+=$NF} END{printf "%d", s}')
echo "   outbound HTTP request_total = ${HTTP_N:-scrape-failed} (h2load 총 요청수와 근사해야 h2 종단)"
step "done"; cleanup
