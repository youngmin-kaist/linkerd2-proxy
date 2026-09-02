#!/bin/bash
# usage: tcp-mix.sh <SPEC> <c> <R>  -> warm, then peak with CPU breakdown
SPEC=$1; C=$2; R=$3
ssh 192.168.100.1 "bash /tmp/dsb_run_services.sh tcp 8 '$SPEC' 2>&1 | tail -1" </dev/null 2>&1
ssh 192.168.100.1 'W=~/DeathStarBench/wrk2/wrk; L=~/DeathStarBench/hotelReservation/wrk2/scripts/hotel-reservation/mixed-workload_type_1.lua
curl -s -o /dev/null -w "preflight: %{http_code}\n" --max-time 10 "http://127.0.0.1:5000/hotels?inDate=2015-04-09&outDate=2015-04-10&lat=38.0235&lon=-122.095"
$W -D exp -t 8 -c 128 -d 20 -L -s $L http://127.0.0.1:5000 -R 3000 >/dev/null 2>&1; $W -D exp -t 8 -c 128 -d 20 -L -s $L http://127.0.0.1:5000 -R 3000 >/dev/null 2>&1
$W -D exp -t 8 -c '"$C"' -d 30 -L -s $L http://127.0.0.1:5000 -R '"$R"' > /tmp/wrk_mix.out 2>/dev/null & WP=$!
sleep 8
S1=$(mktemp); S2=$(mktemp)
snap(){ for f in /proc/[0-9]*/stat; do read -r -a a < $f 2>/dev/null || continue; printf "%s %s %s\n" "${a[0]}" "${a[1]}" "$(( ${a[13]} + ${a[14]} ))"; done; }
snap > $S1; T1=$(date +%s%N); mpstat 10 1 2>/dev/null | tail -1 | awk '"'"'{printf "  [host busy = %.1f/36 cores, idle %.1f%%]\n", 36*(100-$NF)/100, $NF}'"'"'; snap > $S2; T2=$(date +%s%N)
python3 - "$S1" "$S2" "$T1" "$T2" <<'"'"'PY'"'"'
import sys, collections
s1={}
for l in open(sys.argv[1]):
    p=l.split()
    if len(p)==3: s1[p[0]]=(p[1],int(p[2]))
dt=(int(sys.argv[4])-int(sys.argv[3]))/1e9; agg=collections.Counter()
for l in open(sys.argv[2]):
    p=l.split()
    if len(p)!=3 or p[0] not in s1: continue
    n0,t0=s1[p[0]]; c=(int(p[2])-t0)/100.0/dt
    if c>=0.05: agg[n0.strip("()")]+=c
print("  " + ", ".join(f"{k} {v:.1f}" for k,v in agg.most_common(9)))
PY
docker stats --no-stream --format "{{.Name}} {{.CPUPerc}}" 2>/dev/null | grep -E "memcached|mongodb-(reservation|rate|profile)" | sed -E "s/hotelres-dmesh-//; s/-1 / /" | tr "\n" " " | sed "s/^/  [containers] /"; echo; rm -f $S1 $S2; wait $WP; grep -aE "Requests/sec|50.000%" /tmp/wrk_mix.out | tr "\n" " "; echo' </dev/null 2>&1
