#!/bin/bash
# Setup A, replicated-server topology: N_A client sidecars -> N_B server
# sidecars ("server pod replicas") -> nginx. Runs ON rapids4.
# A_i routes to B_(i mod N_B) via a per-B mock-policy instance whose
# MOCK_POLICY_TAGGED_PORT points at that B's inbound.
# Usage: sidecar-multiAB.sh <N_A> <N_B> <CORES_PER_B> <CONNS_PER_A> [NREQ_PER_A] [A_CORES]
set -u
cd ~/bf-workspace/linkerd2-proxy

NA=${1:?client sidecars}
NB=${2:?server sidecars}
CB=${3:?cores per server sidecar}
CONNS=${4:?h2load conns per A}
NREQ=${5:-300000}
ACORES=${6:-$NA}
LOG=/tmp/ym_sidecar
mkdir -p $LOG
A_PIDS=""; B_PIDS=""

# CPU placement (override via env). A_CPUS => all A's share that range;
# H2_CPUS on the same range co-locates the client apps with their sidecars.
H2_CPUS=${H2_CPUS:-4-7}

# Budget guard only applies to the default separated layout; custom A_CPUS/
# B_CPUS placements manage their own budget.
if [ -z "${CPUS:-}" ] && [ -z "${A_CPUS:-}" ] && [ -z "${B_CPUS:-}" ] && [ $((ACORES + NB * CB)) -gt 10 ]; then
    echo "FATAL: core budget exceeded: A=$ACORES + B=$((NB * CB)) > 10"; exit 1
fi

pkill -f "release/linkerd2-proxy" 2>/dev/null
pkill -f "release/mock-" 2>/dev/null
sleep 1

source scripts/dev-proxy-env.sh >/dev/null 2>&1
export MOCK_POLICY_TAGGED_ID="default.default.serviceaccount.identity.linkerd.cluster.local"
export LINKERD2_PROXY_LOG=warn

# CPUS=<list> puts h2load and every proxy in ONE shared pool (free scheduling
# inside the mask; nothing is pinned to a single core).
if [ -n "${CPUS:-}" ]; then
    H2_CPUS=$CPUS; A_CPUS=$CPUS; B_CPUS=$CPUS
fi
export CPUS=${CPUS:-}

setsid ./target/release/mock-identity    > $LOG/mock-identity.log 2>&1 </dev/null &
setsid ./target/release/mock-destination > $LOG/mock-destination.log 2>&1 </dev/null &
# one mock-policy per server sidecar: 8187+j -> tagged port 4143+j
for j in $(seq 0 $((NB - 1))); do
    env MOCK_POLICY_ADDR=127.0.0.1:$((8187 + j)) \
        MOCK_POLICY_TAGGED_PORT=$((4143 + j)) \
        setsid ./target/release/mock-policy > $LOG/mock-policy-$j.log 2>&1 </dev/null &
done
for p in 8088 8089 $(seq 8187 $((8187 + NB - 1))); do
    for i in $(seq 1 50); do (echo > /dev/tcp/127.0.0.1/$p) 2>/dev/null && break; sleep 0.2; done
done

# B_j: inbound 4143+j, CB cores each, packed from cpu 17 down
for j in $(seq 0 $((NB - 1))); do
    LO=$((18 - (j + 1) * CB)); HI=$((17 - j * CB))
    BJ_CPUS=${B_CPUS:-$LO-$HI}
    env LINKERD2_PROXY_INBOUND_LISTEN_ADDR=127.0.0.1:$((4143 + j)) \
        LINKERD2_PROXY_OUTBOUND_LISTEN_ADDR=127.0.0.1:$((4240 + j)) \
        LINKERD2_PROXY_ADMIN_LISTEN_ADDR=127.0.0.1:$((4400 + j)) \
        LINKERD2_PROXY_POLICY_SVC_ADDR=127.0.0.1:$((8187 + j)) \
        LINKERD2_PROXY_CORES=$CB \
        setsid taskset -c $BJ_CPUS ./target/release/linkerd2-proxy > $LOG/proxy-b$j.log 2>&1 </dev/null &
    B_PIDS="$B_PIDS $!"
done

# A_i: outbound 4150+i, policy of B_(i mod NB), sharing cpus 8..8+ACORES-1
for i in $(seq 0 $((NA - 1))); do
    env LINKERD2_PROXY_OUTBOUND_LISTEN_ADDR=127.0.0.1:$((4500 + i)) \
        LINKERD2_PROXY_INBOUND_LISTEN_ADDR=127.0.0.1:$((5150 + i)) \
        LINKERD2_PROXY_ADMIN_LISTEN_ADDR=127.0.0.1:$((4300 + i)) \
        LINKERD2_PROXY_POLICY_SVC_ADDR=127.0.0.1:$((8187 + i % NB)) \
        LINKERD2_PROXY_CORES=1 \
        setsid taskset -c ${A_CPUS:-$((8 + i % ACORES))} ./target/release/linkerd2-proxy > $LOG/proxy-a$i.log 2>&1 </dev/null &
    A_PIDS="$A_PIDS $!"
done

for i in $(seq 0 $((NA - 1))); do
    ok=0
    for t in $(seq 1 100); do (echo > /dev/tcp/127.0.0.1/$((4500 + i))) 2>/dev/null && { ok=1; break; }; sleep 0.2; done
    [ $ok = 1 ] || { echo "FATAL: A$i never came up"; tail -5 $LOG/proxy-a$i.log; exit 1; }
done
sleep 2

# per-role CPU accounting (works in shared-pool mode where per-core ranges
# cannot attribute usage): sum utime+stime deltas per process group.
sum_ticks() {
    local total=0 p rest
    for p in $*; do
        rest=$(cat /proc/$p/stat 2>/dev/null) || continue
        rest=${rest##*) }
        set -- $rest
        total=$((total + ${12} + ${13}))
    done
    echo $total
}
# reaped-children cputime of THIS shell (h2loads are the only children we
# wait() on, so the delta across the load phase is the client apps' CPU)
child_ticks() {
    local rest
    rest=$(cat /proc/$$/stat)
    rest=${rest##*) }
    set -- $rest
    echo $((${14} + ${15}))
}
NGINX_MASTER=$(pgrep -o -f "nginx: master process /usr/sbin/nginx")
NGINX_PIDS=$(pgrep -P "$NGINX_MASTER" 2>/dev/null | tr "\n" " ")
MOCK_PIDS=$(pgrep -f "release/mock-" | tr "\n" " ")

mpstat -P 0-17 1 > $LOG/mpstat.log 2>&1 &
MPID=$!
H2T=$(( CONNS < 4 ? CONNS : 4 ))
H2PIDS=""
for i in $(seq 0 $((NA - 1))); do
    taskset -c $H2_CPUS h2load -t $H2T -c $CONNS -m 100 -n $NREQ --warm-up-time 3 \
        http://127.0.0.1:$((4500 + i))/1k.bin > $LOG/h2load-$i.log 2>&1 &
    H2PIDS="$H2PIDS $!"
done
T0=$(date +%s.%N)
TK_A0=$(sum_ticks $A_PIDS); TK_B0=$(sum_ticks $B_PIDS)
TK_H0=$(child_ticks); TK_N0=$(sum_ticks $NGINX_PIDS); TK_M0=$(sum_ticks $MOCK_PIDS)
wait $H2PIDS 2>/dev/null
T1=$(date +%s.%N)
TK_A1=$(sum_ticks $A_PIDS); TK_B1=$(sum_ticks $B_PIDS)
TK_H1=$(child_ticks); TK_N1=$(sum_ticks $NGINX_PIDS); TK_M1=$(sum_ticks $MOCK_PIDS)
kill $MPID 2>/dev/null
CLK=$(getconf CLK_TCK)
awk -v h0=$TK_H0 -v h1=$TK_H1 -v a0=$TK_A0 -v a1=$TK_A1 -v b0=$TK_B0 -v b1=$TK_B1 -v n0=$TK_N0 -v n1=$TK_N1 \
    -v m0=$TK_M0 -v m1=$TK_M1 -v t0=$T0 -v t1=$T1 -v clk=$CLK 'BEGIN {
    dt = t1 - t0
    printf "role-cores (avg over %.1fs): clientApp(h2load)=%.2f sidecarA=%.2f sidecarB=%.2f nginx=%.2f mocks=%.2f\n",
        dt, (h1-h0)/clk/dt, (a1-a0)/clk/dt, (b1-b0)/clk/dt, (n1-n0)/clk/dt, (m1-m0)/clk/dt
}' 

python3 - "$NA" "$NB" "$CB" "$ACORES" <<'EOF'
import re, sys
na, nb, cb, ac = map(int, sys.argv[1:5])
total = 0.0; ok = True
for i in range(na):
    txt = open(f'/tmp/ym_sidecar/h2load-{i}.log').read()
    m = re.search(r'finished in [^,]+, ([\d.]+) req/s', txt)
    f = re.search(r'(\d+) failed', txt)
    if not m: ok = False; continue
    total += float(m.group(1))
    if f and int(f.group(1)) > 0: ok = False
print(f"TOTAL {total:.0f} req/s  NA={na} NB={nb} all_ok={ok}")
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
btot = nb*cb
print(f"busy-cores nginx(0-3)={rng(0,3):.1f} h2load(4-7)={rng(4,7):.1f} "
      f"A={rng(8,8+ac-1):.1f}/{ac} B={rng(18-btot,17):.1f}/{btot}")

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
echo "done NA=$NA NB=$NB CB=$CB"
