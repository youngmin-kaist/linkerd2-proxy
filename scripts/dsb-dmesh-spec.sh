#!/bin/bash
# DSB over DMA with arbitrary replica spec. usage: dsb-dmesh-spec.sh <W> "<SPEC>"
set -u
W=${1:-16}; SPEC=${2:-}
cd /home/youngmin/DPUMesh/linkerd2-proxy
LOG=/home/youngmin/.claude/jobs/7d781695/tmp; HOST=192.168.100.1
step(){ echo "[$(date +%H:%M:%S)] $*"; }; die(){ step "FATAL: $*"; exit 1; }
pkill -9 -f "release/linkerd2-pr""oxy" 2>/dev/null; pkill -9 -f "release/mo""ck-" 2>/dev/null
timeout 20 ssh $HOST 'for b in frontend geo rate profile recommendation user reservation review attractions search; do pkill -9 -f "hotelres-dmesh/bin/$b" 2>/dev/null; done; true' </dev/null >/dev/null 2>&1
sleep 20
source scripts/dev-proxy-env.sh >/dev/null 2>&1
export LINKERD2_PROXY_LOG=warn MOCK_POLICY_ECHO_TARGET=1
setsid ./target/release/mock-identity    > $LOG/mock-identity.log 2>&1 </dev/null &
setsid ./target/release/mock-destination > $LOG/mock-destination.log 2>&1 </dev/null &
setsid ./target/release/mock-policy      > $LOG/mock-policy.log 2>&1 </dev/null &
for p in 8087 8088 8089; do for i in $(seq 1 50); do (echo > /dev/tcp/127.0.0.1/$p) 2>/dev/null && break; sleep 0.2; done; done
DMESH_NO_TEARDOWN=1 DMESH_SHARDED=1 DMESH_NUM_WORKERS=$W LINKERD2_PROXY_CORES=1 \
  LINKERD2_PROXY_ADMIN_LISTEN_ADDR=127.0.0.1:4991 LINKERD2_PROXY_INBOUND_LISTEN_ADDR=127.0.0.1:5143 \
  setsid taskset -c $((16-W))-15 ./target/release/linkerd2-proxy > $LOG/dproxy.log 2>&1 </dev/null &
for i in $(seq 1 25); do [ "$(grep -ac 'Started DOCA comch server' $LOG/dproxy.log 2>/dev/null)" -ge "$W" ] && break; sleep 1; done
[ "$(grep -ac 'Started DOCA comch server' $LOG/dproxy.log)" -ge "$W" ] || die "proxy servers"
# expected backend listeners = 9 services, replicated per SPEC
EXP=$(python3 -c "
spec='$SPEC'; d={}
for kv in [x for x in spec.split(',') if x]:
    k,v=kv.split(':'); d[k]=int(v)
print(sum(d.get(s,1) for s in ['srv-geo','srv-rate','srv-search','srv-profile','srv-recommendation','srv-user','srv-reservation','srv-review','srv-attractions']))")
timeout 120 ssh $HOST "bash /tmp/dsb_run_services.sh dmesh $W '$SPEC' 4" </dev/null 2>&1 | tail -1
for i in $(seq 1 40); do [ "$(grep -ac 'Push channel ready (mode 1)' $LOG/dproxy.log 2>/dev/null)" -ge "$EXP" ] && break; sleep 1; done
B=$(grep -ac 'Push channel ready (mode 1)' $LOG/dproxy.log); step "backend listeners: $B/$EXP"; [ "$B" -ge "$EXP" ] || die "listeners"
timeout 30 ssh $HOST 'curl -s -o /dev/null -w "preflight GET: %{http_code}\n" --max-time 15 "http://127.0.0.1:5000/hotels?inDate=2015-04-09&outDate=2015-04-10&lat=38.0235&lon=-122.095"; curl -s -o /dev/null -w "preflight reservation POST: %{http_code}\n" --max-time 15 "http://127.0.0.1:5000/reservation?inDate=2015-04-09&outDate=2015-04-10&lat=38.0235&lon=-122.095&hotelId=8&customerName=t&username=Cornell_1&password=1111111111&number=1"' </dev/null 2>&1
step ready
