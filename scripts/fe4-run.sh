#!/bin/bash
# usage: fe4-run.sh <tcp|dmesh> <SPEC> <c_per_inst> <R_per_inst>   (4 frontends x 4 wrk2)
MODE=$1; SPEC=$2; C=$3; R=$4; J=/home/youngmin/.claude/jobs/7d781695/tmp
if [ "$MODE" = dmesh ]; then
  sed -i 's|bash /tmp/dsb_run_services.sh dmesh $W '"'"'$SPEC'"'"'"|bash /tmp/dsb_run_services.sh dmesh $W '"'"'$SPEC'"'"' 4"|' $J/dsb-dmesh-spec.sh
  bash $J/dsb-dmesh-spec.sh 16 "$SPEC" 2>&1 | grep -aE "listeners|FATAL|ready"
else
  ssh 192.168.100.1 "bash /tmp/dsb_run_services.sh tcp 8 '$SPEC' 4 2>&1 | tail -1" </dev/null 2>&1
fi
ssh 192.168.100.1 'W=~/DeathStarBench/wrk2/wrk; L=~/DeathStarBench/hotelReservation/wrk2/scripts/hotel-reservation/mixed-workload_type_1.lua
for f in 0 1 2 3; do P=$((5000+10000*f)); curl -s -o /dev/null -w "preflight :$P %{http_code}  " --max-time 10 "http://127.0.0.1:$P/hotels?inDate=2015-04-09&outDate=2015-04-10&lat=38.0235&lon=-122.095"; done; echo
for f in 0 1 2 3; do $W -D exp -t 2 -c 64 -d 20 -L -s $L http://127.0.0.1:$((5000+10000*f)) -R 1500 >/dev/null 2>&1 & done; wait
for f in 0 1 2 3; do $W -D exp -t 2 -c 64 -d 15 -L -s $L http://127.0.0.1:$((5000+10000*f)) -R 1500 >/dev/null 2>&1 & done; wait
for f in 0 1 2 3; do $W -D exp -t 4 -c '"$C"' -d 30 -L -s $L http://127.0.0.1:$((5000+10000*f)) -R '"$R"' > /tmp/wrk_fe$f.out 2>/dev/null & done
sleep 8; S1=$(mktemp); S2=$(mktemp)
snap(){ for f in /proc/[0-9]*/stat; do read -r -a a < $f 2>/dev/null || continue; printf "%s %s %s\n" "${a[0]}" "${a[1]}" "$(( ${a[13]} + ${a[14]} ))"; done; }
snap > $S1; T1=$(date +%s%N); mpstat 10 1 2>/dev/null | tail -1 | awk '"'"'{printf "  [host busy = %.1f/36, idle %.1f%%]\n", 36*(100-$NF)/100, $NF}'"'"'; snap > $S2; T2=$(date +%s%N)
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
docker stats --no-stream --format "{{.Name}} {{.CPUPerc}}" 2>/dev/null | grep -E "memcached-reserve|memcached-rate|mongodb-reservation" | sed -E "s/hotelres-dmesh-//; s/-1 / /" | tr "\n" " " | sed "s/^/  [containers] /"; echo
rm -f $S1 $S2; wait
T=0; for f in 0 1 2 3; do v=$(grep -aoE "Requests/sec: +[0-9.]+" /tmp/wrk_fe$f.out | grep -oE "[0-9.]+$"); p=$(grep -aoE "50.000% +[0-9.]+[a-z]+" /tmp/wrk_fe$f.out | awk "{print \$2}"); echo -n "  fe$f ${v:-0} (p50 $p) "; T=$(python3 -c "print($T+${v:-0})"); done; echo; echo "  TOTAL = $T req/s"' </dev/null 2>&1
