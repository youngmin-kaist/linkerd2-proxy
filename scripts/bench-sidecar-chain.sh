#!/bin/bash
# Setup A: 2-proxy sidecar chain benchmark on the host (run ON rapids4).
#   h2load -> proxy A (outbound :4140) -> mTLS+TransportHeader -> proxy B (inbound :4143) -> nginx :8086
#
# Core layout (session limited to 0-17):
#   nginx    0-3   (pinned via worker_cpu_affinity, done once)
#   h2load   4-7
#   proxy A  8..8+CORES_A-1
#   proxy B  17 downwards, CORES_B cpus
#
# Usage: sidecar-bench.sh <CORES_A> <CORES_B> <H2LOAD_CONNS> [N_REQUESTS]
set -u
cd ~/bf-workspace/linkerd2-proxy

CORES_A=${1:?cores for proxy A}
CORES_B=${2:?cores for proxy B}
CONNS=${3:?h2load connections}
NREQ=${4:-600000}
LOG=/tmp/ym_sidecar
mkdir -p $LOG

# CPU placement (override via env). Setting the same range for H2_CPUS and
# A_CPUS makes the client app and its sidecar SHARE that core group (pod-like);
# same idea for B_CPUS. Defaults keep the original separated layout.
H2_CPUS=${H2_CPUS:-4-7}
A_CPUS=${A_CPUS:-"8-$((8 + CORES_A - 1))"}
B_CPUS=${B_CPUS:-"$((18 - CORES_B))-17"}

pkill -f "release/linkerd2-proxy" 2>/dev/null
pkill -f "release/mock-" 2>/dev/null
sleep 1

# CPUS=<list> puts h2load and every proxy in ONE shared pool (free scheduling
# inside the mask; nothing is pinned to a single core).
if [ -n "${CPUS:-}" ]; then
    H2_CPUS=$CPUS; A_CPUS=$CPUS; B_CPUS=$CPUS
fi
export CPUS=${CPUS:-}

# --- mocks (unpinned; near-idle) -------------------------------------------
source scripts/dev-proxy-env.sh >/dev/null 2>&1
export MOCK_POLICY_TAGGED_ID="default.default.serviceaccount.identity.linkerd.cluster.local"
export MOCK_POLICY_TAGGED_PORT=4143
# dev env turns on debug logging; that costs real throughput — bench at warn.
export LINKERD2_PROXY_LOG=warn

setsid ./target/release/mock-identity    > $LOG/mock-identity.log 2>&1 </dev/null &
setsid ./target/release/mock-destination > $LOG/mock-destination.log 2>&1 </dev/null &
setsid ./target/release/mock-policy      > $LOG/mock-policy.log 2>&1 </dev/null &
for p in 8087 8088 8089; do
    for i in $(seq 1 50); do (echo > /dev/tcp/127.0.0.1/$p) 2>/dev/null && break; sleep 0.2; done
done

# --- proxy B: server sidecar (inbound :4143) --------------------------------
env LINKERD2_PROXY_INBOUND_LISTEN_ADDR=127.0.0.1:4143 \
    LINKERD2_PROXY_OUTBOUND_LISTEN_ADDR=127.0.0.1:4240 \
    LINKERD2_PROXY_ADMIN_LISTEN_ADDR=127.0.0.1:4192 \
    LINKERD2_PROXY_CORES=$CORES_B \
    setsid taskset -c $B_CPUS ./target/release/linkerd2-proxy > $LOG/proxy-b.log 2>&1 </dev/null &

# --- proxy A: client sidecar (outbound :4140) --------------------------------
# A's inbound is unused but always binds; move it off B's :4143.
env LINKERD2_PROXY_INBOUND_LISTEN_ADDR=127.0.0.1:5143 \
    LINKERD2_PROXY_ADMIN_LISTEN_ADDR=127.0.0.1:4191 \
    LINKERD2_PROXY_CORES=$CORES_A \
    setsid taskset -c $A_CPUS ./target/release/linkerd2-proxy > $LOG/proxy-a.log 2>&1 </dev/null &

for p in 4140 4143; do
    ok=0
    for i in $(seq 1 100); do (echo > /dev/tcp/127.0.0.1/$p) 2>/dev/null && { ok=1; break; }; sleep 0.2; done
    [ $ok = 1 ] || { echo "FATAL: port $p never came up"; tail -20 $LOG/proxy-*.log; exit 1; }
done
sleep 2

# --- measure -----------------------------------------------------------------
mpstat -P 0-17 1 > $LOG/mpstat.log 2>&1 &
MPID=$!
H2T=$(( CONNS < 4 ? CONNS : 4 ))
taskset -c $H2_CPUS h2load -t $H2T -c $CONNS -m 100 -n $NREQ --warm-up-time 3 \
    http://127.0.0.1:4140/1k.bin 2>&1 | tee $LOG/h2load.log | \
    grep -E "finished in|requests:|status codes|time for request|req/s"
kill $MPID 2>/dev/null

# core-seconds burned per range during the run (user+sys, averaged)
python3 - <<'EOF'
rows={}
for line in open('/tmp/ym_sidecar/mpstat.log'):
    f=line.split()
    if len(f) < 5 or f[-1] == '%idle':
        continue
    if len(f) > 1 and f[1] in ('AM','PM'):
        f = [f[0]] + f[2:]
    try:
        cpu=int(f[1]); idle=float(f[-1])
    except ValueError:
        continue
    rows.setdefault(cpu,[]).append(100.0-idle)
def rng(a,b):
    vals=[v for c in range(a,b+1) for v in rows.get(c,[])]
    return sum(vals)/len(vals)*(b-a+1)/100 if vals else 0
print(f"busy-cores nginx(0-3)={rng(0,3):.1f} h2load(4-7)={rng(4,7):.1f} proxies(8-17)={rng(8,17):.1f}")

import os
spec = os.environ.get('CPUS', '')
if spec:
    cpus=set()
    for part in spec.split(','):
        if '-' in part:
            a,b=part.split('-'); cpus.update(range(int(a),int(b)+1))
        elif part: cpus.add(int(part))
    vals=[v for c in cpus for v in rows.get(c,[])]
    if vals:
        busy=sum(vals)/len(vals)*len(cpus)/100
        print(f"pool({spec}) busy={busy:.1f}/{len(cpus)} ({busy/len(cpus)*100:.0f}%)")
EOF

pkill -f "release/linkerd2-proxy" 2>/dev/null
pkill -f "release/mock-" 2>/dev/null
echo "done A=$CORES_A B=$CORES_B conns=$CONNS"
