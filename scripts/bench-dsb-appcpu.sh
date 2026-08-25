#!/bin/bash
# Per-container CPU over a short steady window, via host-side cgroup deltas.
# usage: dsb-appcpu.sh <R> [DUR=10]   (runs ON rapids4)
set -u
R=$1; DUR=${2:-10}
NODE=linkerd-bench-control-plane
URL="http://172.18.0.2:32357"
WRK=~/DeathStarBench/wrk2/wrk
LUA=~/DeathStarBench/hotelReservation/wrk2/scripts/hotel-reservation/mixed-workload_hit1.lua
CID=$(docker inspect -f '{{.Id}}' $NODE)
BASE=/sys/fs/cgroup/system.slice/docker-$CID.scope

# container-id -> "pod container" map (hotel-res only)
docker exec $NODE crictl ps -o json 2>/dev/null | python3 -c '
import json,sys
for c in json.load(sys.stdin)["containers"]:
    l=c.get("labels",{})
    if l.get("io.kubernetes.pod.namespace")!="hotel-res": continue
    print(c["id"][:13], l["io.kubernetes.pod.name"], l["io.kubernetes.container.name"])' > /tmp/ym_cmap.txt

snap() {
    find $BASE -name "cri-containerd-*.scope" -type d 2>/dev/null | while read d; do
        id=${d##*cri-containerd-}; id=${id%.scope}
        u=$(awk "/usage_usec/{print \$2}" $d/cpu.stat 2>/dev/null)
        [ -n "$u" ] && echo "${id:0:13} $u"
    done
}

( taskset -c 0-17 $WRK -D exp -t 6 -c 768 -d $((DUR+7)) -s $LUA "$URL" -R $R > /tmp/ym_wrk_app.log 2>&1 & )
sleep 5                       # ramp
snap > /tmp/ym_snap0; T0=$(date +%s%6N)
sleep $DUR
snap > /tmp/ym_snap1; T1=$(date +%s%6N)
wait 2>/dev/null; sleep 3

grep -E "Requests/sec|^ 99.000%" /tmp/ym_wrk_app.log | tr '\n' ' '; echo
python3 - "$T0" "$T1" <<'PYEOF'
import sys, collections
t0, t1 = int(sys.argv[1]), int(sys.argv[2]); dt = (t1 - t0)
cmap = {}
for line in open("/tmp/ym_cmap.txt"):
    i, pod, cont = line.split()
    cmap[i] = (pod.split("-hotel-res")[0], cont)
def load(p):
    d = {}
    for line in open(p):
        i, u = line.split(); d[i] = int(u)
    return d
s0, s1 = load("/tmp/ym_snap0"), load("/tmp/ym_snap1")
svc_app = collections.defaultdict(float); svc_proxy = collections.defaultdict(float)
for i, (svc, cont) in cmap.items():
    if i not in s0 or i not in s1: continue
    cores = (s1[i] - s0[i]) / dt
    if cont == "linkerd-proxy": svc_proxy[svc] += cores
    else: svc_app[svc] += cores
apps = sum(svc_app.values()); proxies = sum(svc_proxy.values())
def grp(d):
    return " ".join(f"{k}={v:.2f}" for k, v in sorted(d.items(), key=lambda x: -x[1]) if v >= 0.05)
print(f"APP-TOTAL={apps:.2f} PROXY-TOTAL={proxies:.2f}")
print(f"  app: {grp(svc_app)}")
if proxies > 0.01: print(f"  proxy: {grp(svc_proxy)}")
PYEOF
