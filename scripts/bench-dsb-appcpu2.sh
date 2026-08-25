#!/bin/bash
# Per-PROCESS (utime/stime) + per-container cgroup CPU over a 10s window.
# usage: dsb-appcpu2.sh <R> [DUR=10]  (ON rapids4)
set -u
R=$1; DUR=${2:-10}
NODE=linkerd-bench-control-plane
URL="http://172.18.0.2:32357"
WRK=~/DeathStarBench/wrk2/wrk
LUA=~/DeathStarBench/hotelReservation/wrk2/scripts/hotel-reservation/mixed-workload_hit1.lua
NCID=$(docker inspect -f '{{.Id}}' $NODE)
BASE=/sys/fs/cgroup/system.slice/docker-$NCID.scope

docker exec $NODE crictl ps -o json 2>/dev/null | python3 -c '
import json,sys
for c in json.load(sys.stdin)["containers"]:
    l=c.get("labels",{})
    if l.get("io.kubernetes.pod.namespace")!="hotel-res": continue
    print(c["id"][:13], l["io.kubernetes.pod.name"].split("-hotel-res")[0], l["io.kubernetes.container.name"])' > /tmp/ym_cmap.txt

# pid -> "svc cont" via /proc/PID/cgroup
python3 - <<'PYEOF' > /tmp/ym_pidmap.txt
import os, re
cmap = {}
for line in open("/tmp/ym_cmap.txt"):
    i, svc, cont = line.split(); cmap[i] = (svc, cont)
for pid in filter(str.isdigit, os.listdir("/proc")):
    try: cg = open(f"/proc/{pid}/cgroup").read()
    except OSError: continue
    m = re.search(r"cri-containerd-([0-9a-f]{13})", cg)
    if m and m.group(1) in cmap:
        svc, cont = cmap[m.group(1)]
        print(pid, svc, cont)
PYEOF

snap_proc() { while read pid svc cont; do s=$(cat /proc/$pid/stat 2>/dev/null) || continue; rest=${s##*) }; set -- $rest; echo "$pid ${12} ${13}"; done < /tmp/ym_pidmap.txt; }
snap_cg() { find $BASE -name "cri-containerd-*.scope" -type d 2>/dev/null | while read d; do id=${d##*cri-containerd-}; id=${id%.scope}; u=$(awk "/usage_usec/{print \$2}" $d/cpu.stat 2>/dev/null); [ -n "$u" ] && echo "${id:0:13} $u"; done; }

( taskset -c 0-17 $WRK -D exp -t 6 -c 768 -d $((DUR+7)) -s $LUA "$URL" -R $R > /tmp/ym_wrk_app.log 2>&1 & )
sleep 5
snap_proc > /tmp/ym_p0; snap_cg > /tmp/ym_c0; T0=$(date +%s%6N)
sleep $DUR
snap_proc > /tmp/ym_p1; snap_cg > /tmp/ym_c1; T1=$(date +%s%6N)
wait 2>/dev/null; sleep 3
grep "Requests/sec" /tmp/ym_wrk_app.log | tr '\n' ' '; echo

python3 - "$T0" "$T1" "$(getconf CLK_TCK)" <<'PYEOF'
import sys, collections
t0, t1, clk = int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3])
dt_s = (t1 - t0) / 1e6
pmap = {}
for line in open("/tmp/ym_pidmap.txt"):
    pid, svc, cont = line.split(); pmap[pid] = (svc, cont)
def loadp(p):
    d = {}
    for line in open(p):
        pid, u, s = line.split(); d[pid] = (int(u), int(s))
    return d
p0, p1 = loadp("/tmp/ym_p0"), loadp("/tmp/ym_p1")
app_u = collections.defaultdict(float); app_s = collections.defaultdict(float)
pxy_u = pxy_s = 0.0
for pid, (svc, cont) in pmap.items():
    if pid not in p0 or pid not in p1: continue
    du = (p1[pid][0] - p0[pid][0]) / clk / dt_s
    ds = (p1[pid][1] - p0[pid][1]) / clk / dt_s
    if cont == "linkerd-proxy": pxy_u += du; pxy_s += ds
    else: app_u[svc] += du; app_s[svc] += ds
cmap = {}
for line in open("/tmp/ym_cmap.txt"):
    i, svc, cont = line.split(); cmap[i] = (svc, cont)
def loadc(p):
    d = {}
    for line in open(p):
        i, u = line.split(); d[i] = int(u)
    return d
c0, c1 = loadc("/tmp/ym_c0"), loadc("/tmp/ym_c1")
cg_app = 0.0; cg_pxy = 0.0
for i, (svc, cont) in cmap.items():
    if i not in c0 or i not in c1: continue
    cores = (c1[i] - c0[i]) / (t1 - t0)
    if cont == "linkerd-proxy": cg_pxy += cores
    else: cg_app += cores
au, as_ = sum(app_u.values()), sum(app_s.values())
print(f"APP  proc: user={au:.2f} sys={as_:.2f} total={au+as_:.2f} | cgroup={cg_app:.2f}")
print(f"PROXY proc: user={pxy_u:.2f} sys={pxy_s:.2f} total={pxy_u+pxy_s:.2f} | cgroup={cg_pxy:.2f}")
top = sorted(app_u, key=lambda k: -(app_u[k]+app_s[k]))[:5]
print("  per-svc(proc u/s): " + " ".join(f"{k}={app_u[k]:.2f}/{app_s[k]:.2f}" for k in top))
PYEOF
