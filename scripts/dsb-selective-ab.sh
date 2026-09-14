#!/bin/bash
# DSB hotelReservation over DMA, selective h2 OFF vs ON. W=4 (proxy cores 12-15), default spec (1 replica each).
S=/tmp/claude-1002/-home-youngmin-DPUMesh/ac490328-9512-412c-81c8-43cc15f58ed1/scratchpad; HOST=192.168.100.1
LUA='/home/youngmin/DeathStarBench/hotelReservation/wrk2/scripts/hotel-reservation/mixed-workload_type_1.lua'
for MODE in off on; do
  if [ $MODE = on ]; then export DMESH_SELECTIVE_H2=1; else unset DMESH_SELECTIVE_H2; fi
  echo "##### selective=$MODE $(date +%T)"
  timeout 240 bash $S/dsb-dmesh-jet.sh 4 "" 2>&1 | grep -E "backend listeners|preflight|ready|FATAL" | sed 's/^/  /'
  M0=$(curl -s --max-time 5 http://127.0.0.1:4991/metrics | awk '/^request_total\{direction="outbound"/{s+=$NF} END{printf "%d", s}')
  ssh -o BatchMode=yes $HOST "for i in 1 2; do /home/youngmin/dpumesh/DeathStarBench/wrk2/wrk -D exp -t 8 -c 128 -d20s -L -s $LUA http://127.0.0.1:5000 -R 3000 >/dev/null 2>&1; done" </dev/null; echo "  warmup done"
  for R in 4000 8000 12000 16000; do
    (sleep 5; mpstat -P 12-15 20 1 | awk '/Average/ && $2!="CPU" {b+=100-$NF} END{printf "  R=%s DPU proxy cores busy=%.2f/4\n", "'$R'", b/100}') &
    ssh -o BatchMode=yes $HOST "/home/youngmin/dpumesh/DeathStarBench/wrk2/wrk -D exp -t 8 -c 128 -d30s -L -s $LUA http://127.0.0.1:5000 -R $R > /tmp/wrk_ab_$R.out 2>/dev/null; grep -aoE 'Requests/sec: +[0-9.]+' /tmp/wrk_ab_$R.out | awk '{printf \"  R=$R achieved=%s\", \$2}'; grep -aE '^ +(50|99)\.000%' /tmp/wrk_ab_$R.out | awk '{printf \"  %s=%s\", \$1, \$2}'; grep -aoE 'Non-2xx or 3xx responses: [0-9]+' /tmp/wrk_ab_$R.out | awk '{printf \"  non2xx=%s\", \$NF}'; echo; mpstat 30 1 | awk '/Average/ && \$2==\"all\" {printf \"  host busy=%.1f/16\n\", (100-\$NF)*16/100}' &" </dev/null; wait
  done
  M1=$(curl -s --max-time 5 http://127.0.0.1:4991/metrics | awk '/^request_total\{direction="outbound"/{s+=$NF} END{printf "%d", s}'); echo "  proxy outbound request_total Δ=$((M1-M0)) (h2 종단 검증)"
  for p in $(pgrep -x linkerd2-proxy); do sudo -n kill $p; done; pkill -x mock-identity; pkill -x mock-policy; pkill -f "[m]ock-destinatio"
  ssh -o BatchMode=yes $HOST 'for b in frontend geo rate profile recommendation user reservation review attractions search; do pkill -9 -f "hotelres-dmesh/bin/$b"; done; true' </dev/null; sleep 45
done; echo "##### done $(date +%T)"
