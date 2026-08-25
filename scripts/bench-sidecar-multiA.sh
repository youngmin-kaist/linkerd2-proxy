#!/bin/bash
# Setup A, per-pod-faithful topology: N client sidecars (A_i, 1 core each) all
# sending to ONE server sidecar (B) in front of nginx. Runs ON rapids4.
#   h2load_i -> A_i (outbound 41<50+i>) -> mTLS+TransportHeader -> B :4143 -> nginx :8086
# Usage: sidecar-multiA.sh <N_A> <CORES_B> <CONNS_PER_A> [NREQ_PER_A]
set -u
cd ~/bf-workspace/linkerd2-proxy

NA=${1:?number of client sidecars}
CORES_B=${2:?cores for B}
CONNS=${3:?h2load conns per A}
NREQ=${4:-500000}
ACORES=${5:-$NA}   # A instances share cores 8..8+ACORES-1
LOG=/tmp/ym_sidecar
mkdir -p $LOG

# CPU placement (override via env). A_CPUS set => every A shares that range
# (instead of 1-core pinning); pair with H2_CPUS on the same range for a
# pod-like shared app+sidecar group.
H2_CPUS=${H2_CPUS:-4-7}

pkill -f "release/linkerd2-proxy" 2>/dev/null
pkill -f "release/mock-" 2>/dev/null
sleep 1

source scripts/dev-proxy-env.sh >/dev/null 2>&1
export MOCK_POLICY_TAGGED_ID="default.default.serviceaccount.identity.linkerd.cluster.local"
export MOCK_POLICY_TAGGED_PORT=4143
export LINKERD2_PROXY_LOG=warn

# CPUS=<list> puts h2load and every proxy in ONE shared pool (free scheduling
# inside the mask; nothing is pinned to a single core).
if [ -n "${CPUS:-}" ]; then
    H2_CPUS=$CPUS; A_CPUS=$CPUS; B_CPUS=$CPUS
fi
export CPUS=${CPUS:-}

setsid ./target/release/mock-identity    > $LOG/mock-identity.log 2>&1 </dev/null &
setsid ./target/release/mock-destination > $LOG/mock-destination.log 2>&1 </dev/null &
setsid ./target/release/mock-policy      > $LOG/mock-policy.log 2>&1 </dev/null &
for p in 8087 8088 8089; do
    for i in $(seq 1 50); do (echo > /dev/tcp/127.0.0.1/$p) 2>/dev/null && break; sleep 0.2; done
done

# B on the top CORES_B cpus of 8-17 (env-overridable)
B_CPUS=${B_CPUS:-"$((18 - CORES_B))-17"}
env LINKERD2_PROXY_INBOUND_LISTEN_ADDR=127.0.0.1:4143 \
    LINKERD2_PROXY_OUTBOUND_LISTEN_ADDR=127.0.0.1:4240 \
    LINKERD2_PROXY_ADMIN_LISTEN_ADDR=127.0.0.1:4192 \
    LINKERD2_PROXY_CORES=$CORES_B \
    setsid taskset -c $B_CPUS ./target/release/linkerd2-proxy > $LOG/proxy-b.log 2>&1 </dev/null &

# N_A client sidecars, 1 core each, cpus 8,9,10...
for i in $(seq 0 $((NA - 1))); do
    env LINKERD2_PROXY_OUTBOUND_LISTEN_ADDR=127.0.0.1:$((4150 + i)) \
        LINKERD2_PROXY_INBOUND_LISTEN_ADDR=127.0.0.1:$((5150 + i)) \
        LINKERD2_PROXY_ADMIN_LISTEN_ADDR=127.0.0.1:$((4300 + i)) \
        LINKERD2_PROXY_CORES=1 \
        setsid taskset -c ${A_CPUS:-$((8 + i % ACORES))} ./target/release/linkerd2-proxy > $LOG/proxy-a$i.log 2>&1 </dev/null &
done

for i in $(seq 0 $((NA - 1))); do
    ok=0
    for t in $(seq 1 100); do (echo > /dev/tcp/127.0.0.1/$((4150 + i))) 2>/dev/null && { ok=1; break; }; sleep 0.2; done
    [ $ok = 1 ] || { echo "FATAL: A$i never came up"; exit 1; }
done
(echo > /dev/tcp/127.0.0.1/4143) 2>/dev/null || sleep 2
sleep 2

mpstat -P 0-17 1 > $LOG/mpstat.log 2>&1 &
MPID=$!
H2T=$(( CONNS < 4 ? CONNS : 4 ))
H2PIDS=""
for i in $(seq 0 $((NA - 1))); do
    taskset -c $H2_CPUS h2load -t $H2T -c $CONNS -m 100 -n $NREQ --warm-up-time 3 \
        http://127.0.0.1:$((4150 + i))/1k.bin > $LOG/h2load-$i.log 2>&1 &
    H2PIDS="$H2PIDS $!"
done
wait $H2PIDS 2>/dev/null
kill $MPID 2>/dev/null

python3 - "$NA" "$CORES_B" <<'EOF'
import re, sys
na = int(sys.argv[1]); nb = int(sys.argv[2])
total = 0.0; ok = True
for i in range(na):
    txt = open(f'/tmp/ym_sidecar/h2load-{i}.log').read()
    m = re.search(r'finished in [^,]+, ([\d.]+) req/s', txt)
    s = re.search(r'(\d+) succeeded', txt)
    f = re.search(r'(\d+) failed', txt)
    if not m: ok = False; continue
    total += float(m.group(1))
    if f and int(f.group(1)) > 0: ok = False
print(f"TOTAL {total:.0f} req/s across {na} client sidecars  all_ok={ok}")
rows={}
for line in open('/tmp/ym_sidecar/mpstat.log'):
    fl=line.split()
    if len(fl) < 5 or fl[-1] == '%idle': continue
    if len(fl) > 1 and fl[1] in ('AM','PM'): fl = [fl[0]] + fl[2:]
    try: cpu=int(fl[1]); idle=float(fl[-1])
    except ValueError: continue
    rows.setdefault(cpu,[]).append(100.0-idle)
def rng(a,b):
    vals=[v for c in range(a,b+1) for v in rows.get(c,[])]
    return sum(vals)/len(vals)*(b-a+1)/100 if vals else 0
print(f"busy-cores nginx(0-3)={rng(0,3):.1f} h2load(4-7)={rng(4,7):.1f} "
      f"A={rng(8,17-nb):.1f} B={rng(18-nb,17):.1f}/{nb}")

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
echo "done NA=$NA B=$CORES_B conns/A=$CONNS"
