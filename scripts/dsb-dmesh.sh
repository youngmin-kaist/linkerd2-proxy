#!/bin/bash
# DSB hotelReservation over the DMA transport: proxy + services + preflight.
set -u
W=${1:-8}
cd /home/youngmin/DPUMesh/linkerd2-proxy
LOG=/home/youngmin/.claude/jobs/7d781695/tmp
HOST=192.168.100.1
step(){ echo "[$(date +%H:%M:%S)] $*"; }
die(){ step "FATAL: $*"; exit 1; }

step "0. clean"
pkill -f "release/linkerd2-proxy" 2>/dev/null; pkill -f "release/mock-" 2>/dev/null
timeout 20 ssh $HOST 'pkill -f "hotelres-dmesh/bin/"; true' </dev/null >/dev/null 2>&1
sleep 10

step "1. mocks + proxy (sharded W=$W)"
source scripts/dev-proxy-env.sh >/dev/null 2>&1
export LINKERD2_PROXY_LOG=warn MOCK_POLICY_ECHO_TARGET=1
setsid ./target/release/mock-identity    > $LOG/mock-identity.log 2>&1 </dev/null &
setsid ./target/release/mock-destination > $LOG/mock-destination.log 2>&1 </dev/null &
setsid ./target/release/mock-policy      > $LOG/mock-policy.log 2>&1 </dev/null &
for p in 8087 8088 8089; do for i in $(seq 1 50); do (echo > /dev/tcp/127.0.0.1/$p) 2>/dev/null && break; sleep 0.2; done; done
DMESH_NO_TEARDOWN=1 DMESH_SHARDED=1 DMESH_NUM_WORKERS=$W LINKERD2_PROXY_CORES=1 \
    LINKERD2_PROXY_ADMIN_LISTEN_ADDR=127.0.0.1:4991 \
    LINKERD2_PROXY_INBOUND_LISTEN_ADDR=127.0.0.1:5143 \
    setsid taskset -c $((16-W))-15 ./target/release/linkerd2-proxy > $LOG/dproxy.log 2>&1 </dev/null &
for i in $(seq 1 25); do [ "$(grep -ac 'Started DOCA comch server' $LOG/dproxy.log 2>/dev/null)" -ge "$W" ] && break; sleep 1; done
[ "$(grep -ac 'Started DOCA comch server' $LOG/dproxy.log)" -ge "$W" ] || die "proxy servers"
step "   proxy up"

step "2. hotelRes services (dmesh mode)"
timeout 90 ssh $HOST "bash /tmp/dsb_run_services.sh dmesh $W" </dev/null 2>&1 | tail -2
for i in $(seq 1 30); do [ "$(grep -ac 'Push channel ready (mode 1)' $LOG/dproxy.log 2>/dev/null)" -ge 9 ] && break; sleep 1; done
B=$(grep -ac 'Push channel ready (mode 1)' $LOG/dproxy.log)
step "   backend listeners: $B/9+"
[ "$B" -ge 9 ] || die "listeners"

step "3. preflight: 1 request"
timeout 30 ssh $HOST 'curl -s -o /tmp/pf2.json -w "%{http_code} in %{time_total}s\n" --max-time 15 "http://127.0.0.1:5000/hotels?inDate=2015-04-09&outDate=2015-04-10&lat=38.0235&lon=-122.095"; head -c 80 /tmp/pf2.json; echo' </dev/null 2>&1
step "ready"
