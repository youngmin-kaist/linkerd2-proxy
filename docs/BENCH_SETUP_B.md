# Setup B — DMA linkerd2-proxy 멀티코어 (진행 기록, 2026-08-25)

## 구성 요소 (신규 구현)
- C: DMESH_FLOW_MODE_INGRESS_PUSH(mode 2) + listen형 push 브리지(DMESH_PUSH_BRIDGE_PORT)
  + DMESH_SERVER_IDX. 인그레스 역방향도 push(안 2) → host DPA 0개, 채널 무제한.
- Rust: DMESH_NUM_WORKERS(N-driver, 워커≤8), backend 레지스트리 Vec화(덮어쓰기 유실 수정).
- mock-policy: MOCK_POLICY_ECHO_TARGET(질의 dst를 그대로 포워드 — per-pair 샤딩용).
- 하네스: preflight(포트당 curl 1개, 별도 사이클) → 시간 기반 h2load(--duration=10
  --warm-up-time=3, M개 동시 종료로 teardown 크래시를 측정 후로 밀어냄). 런마다
  프록시 재기동 + pkill 후 8s 대기(DOCA 자원 해제).

## 핵심 결과

### 싱글 Arm 코어 (W=1, LINKERD2_PROXY_CORES=1 + taskset 1코어, 3회)
| rep | req/s | 코어 busy |
|---|---|---|
| 1/2/3 | **184,970 / 181,588 / 180,645** | 100/99/100% |

**≈183k req/s @ Arm 1코어, 전 요청 200 OK.** 경로 검증: 백엔드 브리지 DMA 카운터
1.3GB 일치, 10.0.0.1은 host에서 unreachable(TCP 폴백 불가), dmesh-lat 카운터 동작.

기존 "16.6k"와의 11배 차이의 원인 분해:
1. **측정 왜곡**: 카운트 기반 짧은 런(~1.2s)은 h2 slow-start/램프 포함 평균 — 시간 기반
   워밍업 제외로 교정. (라우터 31k/100k 등 과거 짧은-런 수치도 같은 왜곡 소지)
2. **전송 교체**: 안1의 128B fused-copy 역방향 → push 8KB 배치.
3. **단일 스레드 로컬리티**: 드라이버+h2세션+h2클라이언트가 한 코어에 동거 —
   크로스코어 웨이크업 제거(16-스레드 런타임의 채널당 ~100k보다 오히려 높음).

### W 스윕 (16-스레드 런타임, 채널쌍=W)
| W | TOTAL | DPU busy | 비고 |
|---|---|---|---|
| 2 | 205k | 여유 | 전 요청 200 |
| 4 | 193k | 4.5/16 | host 하네스 포화 시작 |
| 8 | 83k | 1.8/16 | 브리지 16개 busy-poll이 host 18코어 초과, +포트충돌(8086=nginx) |

**현 병목은 DPU가 아니라 host 하네스**(브리지 busy-poll 프로세스당 1코어).
Arm 포화 측정에는 브리지 usleep 백오프 또는 다중연결 브리지 필요.
인그레스 포트 베이스는 nginx(8086) 회피 필요(28080+i 권장).

### funnel 교훈
모든 플로우가 같은 dst면 linkerd outbound가 백엔드 h2 연결 1개로 합류(사이드카
A→B와 동일) → per-pair dst 샤딩(10.0.0.1+i)으로 해소. G3에서 2채널 16.6k(=1채널과
동일)로 발현됐었음.

## 참고 비교치 (사이드카, BENCH_SIDECAR_BASELINE.md)
host Xeon 사이드카: busy 코어당 ~20-22k req/s → **DMA proxy는 Arm 1코어에 183k =
코어당 ~8-9배** (ISA 차이 감안해도 압도적; 1-pass fused 구조 + DMA 전송의 효과).

## 11× (16.6k → 183k) 검증 (2026-08-25, sb-verify.sh)

싱글 Arm 코어(taskset -c 15), W=1, 동일 프록시 바이너리:

| transport | timed 10s | count -n20000 |
|---|---|---|
| push (안2) | **183k** | 97.1k (m100) |
| DPA-reverse (안1) | 106.2k | 97.0k (m100) / **97.9k (m300 = 구 16.6k 설정)** |

- count-기반 짧은 런의 왜곡: ~1.9× (0.2s 런은 h2 ramp가 지배)
- 안1↔안2 transport 차이: timed ~1.7×, count에서는 소멸
- 구 16.6k는 정확 재현 설정(안1+count+m300)에서도 97.9k → 재현 불가. 잔여 ~5.9×는
  측정 시점 이후의 코드 개선(write-side zero-copy, aarch64 jemalloc, no-copy 헤더)에 귀속.
- 단일스레드 locality는 구 런도 CORES=1이므로 요인 아님.

주의: backend 브리지는 기동 후 60초 내 첫 요청 필요 (nginx client_header_timeout).

### 16.6k 재현 시도 (전 요인 소거, 2026-08-26)

구 16.6k 설정(안1+count+m300+싱글코어) 고정, 버전/환경만 교체한 A/B:
현재 코드 97.9k · Rust만 구버전(40ca7626) 107.6k · host 브리지만 구버전(9ec9a6d) 90.0k ·
완전 시대재현(acdb56c C+커널+구 Rust+구 host) 98.9k · +debug 로깅 98.9k ·
EU 파티션(mlx5_1 vhca1, EU96-127) 재생성 후 95.5k.
→ **16.6k는 어떤 조합으로도 재현 불가 = 당시 testbed 상태의 이상치.** 미검증 변수는
DMESH_REV_PCI=94:00.0(타 테넌트 점유)뿐. 구버전 바이너리:
`target/release/linkerd2-proxy.{fabccf2e,40ca7626}`, host `~/dpumesh-old/`.

## ⚠️ 대정정 (2026-08-26): 위 183k/205k/97k 수치는 opaque L4 아티팩트

dmesh 채널은 attach 즉시 accept되어 protocol detection(10s)이 시작되는데, 하네스의
h2load는 attach 후 12~37초에 첫 바이트를 보냄 → detect ReadTimeout → **opaque 폴백
→ L4 바이트 파이프**로 측정됨 (admin metrics에 HTTP request_total=0, tcp_read_bytes만
증가; `linkerd_http_detect: Detected result=Ok(ReadTimeout(10s))` 로그로 확인).

**진짜 h2 종단 L7 수치 (metrics로 request_total 일치 확인, 싱글 Arm 코어, warn 로그):**
- push(안2) + timed m300: **17,406 req/s** / count -n20000 m300: **17,189 req/s**
- 구 16.6k와 일치 — "count 측정왜곡" 서사가 오히려 아티팩트였음.
- opaque 수치(~100-183k)는 DMA transport 상한 참고치로만 유효.

검증 하네스: sb-verify-h2.sh (attach 후 10초 내 발사 + step 6 metrics 판별).
안1(dpa) 셀은 reverse DPA 셋업(37s)이 listen을 막아 현 구조상 창을 못 맞춤 — C 수정 필요.
액션: 유휴 attach 채널이 opaque로 굳는 accept-시점 문제는 실 버그 (첫 바이트까지 accept 지연 필요).

## 진짜 L7 코어 스케일링 (2026-08-26, sb-l7.sh, h2 종단 metrics 검증)

accept-first 수정(host_worker.c: 인그레스 TCP accept 후 채널 attach → 감지 창 항상 충족) 후,
코어별 taskset 피닝, W 워커 × M 페어(-c1 -m300 timed 10s×M 동시):

| cores | W | M | req/s | per-core | h2 검증 |
|---|---|---|---|---|---|
| 1 | 1 | 1 | 17.4k | 17.4k | metrics ✓ |
| 2 | 2 | 2 | 22.2k | 11.1k | metrics ✓ |
| 4 | 4 | 4 | 39.1k | 9.8k | (크래시로 유실, preflight h2 ✓) |
| 8 | 8 | 8 | 69.2k | 8.6k | (〃) |
| 16 | 8 | 16 | **100.0k** | 6.2k | metrics ✓ (1,305,928) |
| 16 | 16 | 16 | 101.3k | 6.3k | W=16 이득 없음 |

- mpstat: C16에서 DPU busy ~13.6/16 코어 (타 테넌트 포함) — 사실상 코어 포화.
- 스케일링 서브리니어(16코어에서 5.7×): 워커당 드라이버 폴링 + 런타임 경합 추정, 미규명.
- 참고: 사이드카 Setup A(호스트 x86)는 304k @ 18코어 공유(≈17k/core). DPUMesh 싱글 Arm
  코어 17.4k는 x86 사이드카 per-core와 대등하나, 스케일 시 효율이 떨어짐.
- 순차 연결 종료는 teardown 세그폴트를 확률적으로 유발 → preflight는 1포트만, 측정은
  동시 종료(timed)로. 측정치는 h2load 완료 후 크래시와 무관.

### per-core 효율 저하 프로파일링 (2026-08-26)

요청당: cycles 124k→217k(+75%)인데 instructions 118k→127k(+7%)뿐 — 일은 같고 **IPC가
0.95→0.59로 붕괴**(cache miss/req +54%, ctx switch 34×, 코어 마이그레이션 0→3.9k/s).
perf 콜그래프: atomics 샘플 11%→21%, 최대 사슬은 요청당 라우트 스택 클론
(`OneshotRoute → box_clone_sync → LoadShed::clone → Vec<Arc>::clone` = Arc refcount
fetch-add 폭풍) + `Mutex::lock_contended`. 유휴 스핀은 아님(유휴 CPU ~0%).
결론: 공유 캐시라인(Arc refcount·공유 메트릭·런타임 상태) 경합 + tokio work-stealing의
지역성 파괴. 개선안: worker별 독립 current_thread 런타임+피닝, 요청당 Arc 클론 제거,
메트릭 샤딩. (8×싱글코어 프로세스 A/B는 comch 멀티프로세스 연결 hang으로 보류.)

## Sharded per-worker runtimes (DMESH_SHARDED=1, 2026-08-26)

워커별 전용 OS 스레드(코어 피닝) + current_thread 런타임에서 driver+acceptor+플로우 전부
실행 (work-stealing 제거, dpu_worker.c의 Rust 미러). 모든 수치 h2 종단 metrics 검증:

| cores | shared runtime | sharded | sharded per-core | 개선 |
|---|---|---|---|---|
| 1 | 17.4k | 18.4k | 18.4k | +6% |
| 2 | 22.2k | 35.1k | 17.6k | +58% |
| 4 | 39.1k | 66.7k | 16.7k | +71% |
| 8 | 69.2k | **121.2k** | 15.2k | +75% |
| 16 | 100.0k | **150.0k** | 9.4k | +50% |

- 스케일링 5.7×→8.2×(16코어); 8코어 효율 50%→87% — IPC-붕괴 진단이 정확했음을 확인.
- C16은 DPU busy 12.2/16으로 미포화: 호스트에 h2load 16 + busy-poll 브리지 16(>18코어)로
  클라이언트 쪽이 한계 → 150k는 하한값. 브리지 busy-poll 백오프가 다음 과제.
- 사용법: DMESH_SHARDED=1 + DMESH_NUM_WORKERS=W(=코어수), LINKERD2_PROXY_CORES=1(메인 rt).
  worker i는 코어 15-i에 피닝.

## 브리지 유휴 백오프 후 최종 곡선 (2026-08-26)

run_host_push_splice에 유휴 백오프(64회 무작업→5µs, 4096회→50µs, 작업 시 리셋;
DMESH_SPLICE_SPIN=1로 순수 스핀 복귀) — M=16에서 호스트 32 hot 프로세스 경합 해소.

| cores | req/s (sharded+backoff) | per-core | 비고 |
|---|---|---|---|
| 1 | 17.2k | 17.2k | 백오프 비용 ~-6% (solo) |
| 2 | 35.1k | 17.6k | |
| 4 | 66.7k | 16.7k | |
| 8 | 120.7k | 15.1k | 호스트 한계 아님(불변) |
| 16 | **186.9k** | 11.7k | **DPU 16.7/16 완전 포화 — 진짜 상한** |

16코어 스케일링 10.9×. 사이드카 Setup A(호스트 x86 18코어) 304k와 비교: DPU 16 Arm 코어로
그 61% 처리량을 호스트 코어 소모 없이 제공(호스트는 h2load 부하기 제외 유휴).

### MemoOneshotRoute — 요청당 라우트 스택 클론 제거 (2026-08-26)

linkerd_router에 `NewMemoOneshotRoute` 추가, outbound HTTP policy 라우터에 적용: 동일
key 연속 요청은 메모된 라우트 서비스를 직접 호출(클론 0회), key 전환·미준비 시 기존
clone+Oneshot 폴백(시맨틱 보존; 정책 변경은 key 변화/라우터 재생성으로 캐시 미스).
결과: perf에서 box_clone_sync→LoadShed→Vec 클론 체인 소멸 확인. 그러나 처리량은
C1 17.4k / C8 120.5k / C16(재측정 필요)로 오차 내 동일 — 그 체인은 사이클의 ~2%였고,
남은 atomics(~17%)는 메트릭 카운터·waker 등 다른 출처. 코드가 엄밀히 덜 일하므로 유지.
다음 후보: C16에서 전 코어에 걸쳐 바운싱하는 공유 메트릭 캐시라인 샤딩.

## gRPC 64B 에코 스루풋 — dmeshgo 트랜스포트 (2026-09-01, scripts/dm-bench.sh)

Go gRPC(unary Ping, 64B req/64B resp, raw codec) ↔ DMA 채널 ↔ DPU 프록시 L7.
P = 클라이언트 연결 수(=백엔드 리스너 수, per-pair dst 샤딩), M=64 in-flight/conn.
전 측정 preflight(연결당 에코 1회 내용검증) + mid-run request_total로 h2 종단 확인.

| 코어(W) | best P | req/s | DPU busy | per-core |
|---|---|---|---|---|
| 1 | 2 | 13.1k | 1.3 | 13.1k |
| 2 | 4 | 27.3k | 2.4 | 13.7k |
| 4 | 8 | 53.2k | 4.4 | 13.3k |
| 8 | 16 | 95.8k | 8.2 | 12.0k |
| 16 | 32 | **151.5k** | **14.5/16** | 9.5k |

- 16코어에서 DPU 포화(M=128로 올려도 145k — 서버측 병목 확인). h2load 1KB 187k 대비
  gRPC unary 오버헤드(per-RPC 프레이밍/trailer) 반영된 수치.
- P 상한 = DPA 슬롯 예산: 페어당 채널 3개(ingress+backend+spare) × 슬롯 8/워커 → P ≤ ~2.6W.
  P>2W는 preflight에서 슬롯 고갈로 실패(정상 동작).

### 재연결(teardown 후 재접속) 조사 상태
- 근본 원인 좁힘: 프록시 프로세스에서만 DPA thread destroy가 flexio에서 실패
  ("Failed to destroy thread") → comch 함수 전체 wedge → 이후 모든 client 등록이
  devx syndrome 0xe5300으로 abort. C 워커(단독 conn)는 동일 코드로 재연결 성공,
  형제 conn이 있으면 C 워커도 재연결 시 crash.
- 적용된 수정(유지): 서버측 doca_comch_server_disconnect, 클라이언트 graceful close
  (host_lib), comp/msgq/consumer 파괴를 thread destroy 전 인라인으로 복원.
- DMESH_NO_TEARDOWN 스톱갭 추가했으나 아직 발동 안 함(CLOSING 경로 진입 여부 조사 필요).
  벤치는 런당 새 프록시라 영향 없음.

## DSB hotelReservation L3 통합 — gRPC-over-DMA e2e (2026-09-02)

hotelRes의 모든 서비스 간 gRPC 홉(9 서비스)을 dmeshgo 트랜스포트로 교체
(dialer WithContextDialer + 서비스 리스너 dmesh.Listen; consul 우회, 정적 dst 키
10.0.2.x). 서비스는 호스트 bare 프로세스, 인프라(mongo/memcached/consul/jaeger)는
compose 컨테이너(브리지 IP 직결), 프록시 W=8 sharded. 패치: dmeshgo/dsb-integration.patch,
하네스: scripts/dsb-dmesh.sh + dmeshgo/dsb_*.{sh,py}.

wrk2 mixed-workload, t8 c128 d20 (전 측정 preflight 200 확인, request_total 1.44M로
프록시 L7 통과 검증):

| R | TCP-direct (baseline) | DPUMesh | 
|---|---|---|
| 2000 | p50 2.7ms / p99 15ms | p50 9.2ms / p99 27ms |
| 5000 | p50 5.8ms / p99 40ms | p50 18.5ms / p99 88ms |
| 8000 | 7.6k (붕괴 시작) | 5.9k (붕괴) |
| 12000 (포화) | **7.8k** | **6.0k** (−23%) |
| host busy @포화 | 12.8/18 | **12.0/18** |
| DPU busy | — | 4-5/16 |

- 포화 −23%는 홉당 DMA 왕복 지연(에코 warm 2.1ms와 정합; frontend→search→geo/rate
  2단 체인)이 closed-loop 처리량에 반영된 것. DPU는 4-5코어로 여유 — 병목은 여전히
  호스트 앱/DB.
- **호스트 코어가 baseline보다 오히려 낮음(12.0 vs 12.8)**: 메시 L7 비용이 DPU로
  이동. 사이드카(kind: SLO −36%, 사이드카 CPU +33% host)와 대조되는 핵심 결과.
- 공정성 주의: baseline은 메시 기능 없음; DPUMesh는 L7 라우팅+authz 통과(mTLS는
  PCIe 설계상 없음). kind 사이드카 수치는 다른 환경(참고용).

### 레플리카/포화 실험 + 중요 정정 (2026-09-02)

- **정정**: 앞 절의 "포화 6.0k(−23%)"는 wrk2 c128의 동시성 한계 아티팩트였음
  (DPUMesh는 홉당 DMA RTT만큼 요청 지연이 커서 같은 처리량에 더 큰 동시성 필요).
  캐시 웜업 표준화(40s×2) + c512에서 재측정한 공정 비교:

| 구성 (c512, warmed, R=16000) | req/s | host busy | DPU busy |
|---|---|---|---|
| TCP-direct | 7,915 | 14.2/18 | — |
| **DPUMesh N=1** | **7,860 (−0.7%)** | 13.6/18 | **3.6/16** |

- **포화 측 = 호스트** (앱+DB+wrk2 공유 풀; p50 7s대 과부하 영역), DPU는 3.6/16로
  4배+ 여유 → peak throughput은 앱 한계이며 DPUMesh 세금은 지연에만 나타남
  (동시성으로 흡수 가능).
- 레플리카(엣지당 채널 N) 확장: round_robin 분산은 정확히 50/50으로 동작하지만
  처리량 이득 없음(병목이 채널이 아님). N≥2는 장시간 러닝에서 붕괴 관측 —
  채널 하나가 죽으면 재연결 wedge(미해결)로 해당 엣지가 소실되는 실전 영향.
  당분간 N=1 권장.

### rate DB-scan 버그 수정 + keepalive 안정화 재측정 (2026-09-02)

**rate 수정**: GetRates의 캐시 미스 경로가 upstream 그대로 3중 결함이었음 —
빈 필터 `Find(bson.D{})`(미스마다 풀 컬렉션 스캔) + 전체 rate plan을 응답에 추가
(정확성 오염) + 그 전체를 해당 id 캐시값으로 저장(캐시 오염; 기존 "gets/req 1000"의
근원). `bson.M{"hotelId": id}` 필터로 수정 + mongo `hotelId` 인덱스 생성.
(다른 서비스의 `Find(bson.D{})`는 시작 시 1회 로드라 정상.)

**연속 러닝 붕괴의 진범 = gRPC keepalive**: 클라이언트 ping 주기 < 서버
EnforcementPolicy 기본 MinTime(5분) → `GOAWAY too_many_pings` → TCP는 재연결로
자가치유, dmesh는 재연결 wedge로 엣지 소실. 9개 서비스 서버 옵션에
`MinTime: 10s` 추가로 근본 차단 (수정 후 too_many_pings 0, 같은 스택 3연속 피크 안정).

최종 피크(웜업 표준화, c512 R=16000, rate+keepalive 수정 후):
- TCP-direct: 7,693–7,922 req/s (3회)
- **DPUMesh N=1: 7,257–8,059 req/s (3회 연속, 평균 ~7.6k) — 동등(오차 내)**
- 피크는 캐시 웜 상태라 rate 수정의 처리량 영향은 작고, 효과는 정확성·캐시 오염 제거·
  콜드 스타트 안정성·저부하 tail(p99 89ms@R5k)에 나타남.

### TCP-direct 피크 병목 분해 + 분모 정정 (2026-09-02)

**정정**: rapids4는 36 물리코어(6554S, SMT off). 앞 절들의 host busy "x/18"은 분모
오류 — 비율은 옳고 코어 수는 2배로 읽을 것 (예: "13.6/18" → 실제 ≈27/36).

피크(8,038 req/s, c512 R=16000, warmed) 중 10초 창 프로세스별 CPU:

| 소비자 | 코어 | 비고 |
|---|---|---|
| **reservation** | **12.8** | 단일 지배 (~46% of app), ≈1.6ms CPU/req |
| rate | 4.1 | scan 수정 후에도 2위 |
| frontend | 2.5 | HTTP+팬아웃 |
| search | 2.2 | |
| profile / geo / recommendation | 1.5 / 1.0 / 0.6 | |
| memcached-reserve (컨테이너) | 1.2 | reservation 짝 |
| mongo 8개 합 | ~0.6 | 웜 캐시라 유휴 |
| wrk2 | 0.7 | 부하기 오버헤드 미미 |
| 시스템 | usr 70% / sys 7.6% / **iowait 0** / idle 21% | **busy ≈ 28.4/36** |

**병목 = reservation 서비스의 요청당 CPU 비용**(CheckAvailability 루프). 디스크/DB
아님(iowait 0, mongo 유휴). idle 7.6코어가 남으므로 전 코어 포화가 아니라 reservation
단일 인스턴스의 실질 상한 — 더 올리려면 reservation(+memcached-reserve) replica가
레버 (kind 실험의 "replicated B" 결론과 일치).

### reservation replica×4 (2026-09-02)

병목(reservation 단일 인스턴스)을 4 replica로 확장. TCP는 포트 분리(+10000·r)+
frontend round_robin, DMA는 replica 키 4개(10.0.17.1-4) 프로세스별 리스너.

| 구성 (warmed, c512) | peak req/s | host busy |
|---|---|---|
| TCP-direct, res×1 | 7.7–7.9k | ~28/36 |
| **TCP-direct, res×4** | **11.3–11.8k (peak 12.1k@R24k)** | **~31.6/36 (88%)** |

- **reservation이 정확한 레버였음**: +55% (병목 분해가 옳았음). res×4에서 host가
  진짜 포화(88%)에 근접 — 다음 병목은 전 코어.
- DMA(DPUMesh) res×4: 4채널 모두 프록시에 등록되나 트래픽 전달 0 → 붕괴.
  원인은 frontend의 단일 grpc.ClientConn + round_robin(4 subconn) 조합이 dmesh
  ContextDialer와 물릴 때 subconn이 READY로 전이해도 전달 안 됨(통합 버그, 전송
  자체와 무관). 부수로 발견·수정한 실버그: Listener 스페어 폴링이 gRPC가 닫은
  채널을 claimed() 폴링 → cgo UAF SIGSEGV. host_lib+dmeshgo에 dead-guard 추가로
  크래시는 제거. 멀티-replica 전달은 미해결(follow-up); N=1은 정상.

### 멀티-replica 다이얼 수정 (2026-09-02)

결함 2(전달 0) 근본원인 확정 + 수정: **단일 ClientConn + round_robin(N subconn)**이
dmesh 백엔드-채널 스페어-리스너 모델과 맞물려 subconn이 전달 상태로 못 감. 결함 1(워커
aliasing)은 슬롯 고갈 미발생으로 이번 실패와 무관.

수정(dmesh/balanced.go): replica당 **독립 grpc.ClientConn(단일 채널 passthrough)** N개 +
원자적 RR picker(`balancedConn` implements ClientConnInterface). frontend
initReservation만 교체. **검증: reservation POST preflight 200(이전 무한 hang) +
초기 부하에서 replica들 전달** → 다이얼 로직은 정상 동작.

**그러나 지속 부하에서 붕괴(143→0)**: reservation 채널들이 부하 중 teardown되며
flexio thread-destroy 실패(wedge)로 엣지 소실. 이는 다이얼이 아니라 **기존 미해결
teardown/재연결 wedge**가 다채널 churn에서 더 쉽게 드러난 것 (DMESH_NO_TEARDOWN도 이
경로 미차단). → 멀티-replica 피크 측정은 teardown 근본수정이 선결과제. N=1은 안정.

### teardown/flexio 근본수정 조사 (2026-09-02)

**진짜 성과 = 워커-aliasing 회귀 수정.** 멀티-replica 패치에서 채널→워커 매핑을
`(idx*4+r)%W`로 바꾼 것이 단일 서비스들을 워커 0·4에 5개씩 몰아(1·2·3 공백) DPA 슬롯
(8/워커)을 고갈시켜 **N=1 DSB를 7k→38 req/s로 회귀**시켰음. `(idx+r)%W`로 수정 →
N=1 7,022 복구. 이 슬롯-고갈 churn이 DSB에서 관측된 flexio wedge의 주 유발원이었음
(매핑 수정 후 정상 운영 flexio 에러 0).

**thread_stop 소견(opt-in 유지)**: 격리된 C-워커 sibling-teardown 재현자에서
`doca_dpa_thread_stop`을 destroy 전에 넣으면 flexio "Failed to destroy thread" 2→0.
그러나 프록시 INGRESS_PUSH 경로에선 오히려 회귀 → 기본 OFF(`DMESH_THREAD_STOP`
opt-in). 프록시 클라이언트-채널 teardown이 C-워커 backend 경로와 다른 점은 미규명.

**남은 별개 이슈**: res×4 balancedConn 멀티채널 처리량(654→17 붕괴, flexio 0·프로세스
생존) — teardown 아님, 멀티-replica 다이얼 성능 문제로 별도 트랙. N=1은 안정(7k).

### 멀티-replica 3번째 결함 조사 (2026-09-02, 미해결)

두 하위버그 수정: ① `ServiceFromTarget`이 벌거벗은 서비스명(review/attractions는
consul URL 아닌 raw name으로 다이얼)을 못 잡아 TCP 폴백("missing port") → raw name도
인식하도록 수정(회귀 없음, 보존). ② 워커 매핑 (idx+r)%W (앞 절).

**근본 결함은 미해결**: res×4 지속 부하에서 첫 런만 부분 처리(654→1445) 후 붕괴,
모든 reservation replica가 **CONNECTION_ABORTED**. 원인 = dmesh 채널의 1회용 수명 vs
gRPC 재연결: 부하 중 h2 연결이 한 번 끊기면 subchannel이 재다이얼 → 프록시가 이미
teardown한 슬롯이라 abort → TRANSIENT_FAILURE 고착. 채널 keepalive/idle 억제 우회는
역효과(부하 0). **즉 멀티-replica 피크는 dmesh 채널의 재연결/영속성(=teardown 트랙의
근본 뿌리) 없이는 불가.** 통합 계층(balancedConn, ServiceFromTarget, 워커매핑)은 모두
정상화됐고 단일요청은 200; 남은 것은 C 데이터패스의 채널 재연결 지원 하나.

현 상태 확정 수치: DPUMesh **N=1 = 7,022 req/s(안정)**, res×4는 재연결 뿌리문제로 측정불가.
TCP-direct res×4 = 12.1k(참고).

### 근본원인 확정: 채널은 "끊기는" 게 아니라 push 흐름제어 부재로 손상/정체 (2026-09-02)

**관측 (res×4, 부하 에스컬레이션)**: c256 R12000 = **8,420 req/s 정상 완주**(TRANSIENT 0).
c512 R16000에서 붕괴 — 그런데 **TRANSIENT_FAILURE 0, 연결 유지, 단일 요청까지 TIMEOUT**
= 재연결이 아니라 **영구 hang**.

**코드 확정 (dma.c push 경로)**: DPU→host push에 흐름제어가 전혀 없음.
1. 데이터 링: `push_pos`는 DPU 자체 커서 — host 소비 위치 확인 없이 wrap하며 덮어씀.
2. desc slot 링(128): `seq%128` 슬롯에 무조건 발행 — host `expected`가 128 뒤처지면
   미소비 슬롯을 새 seq로 덮어씀 → host는 expected seq를 영원히 못 만나 **영구 stall**.
3. host 소비측은 pend(8MB) 초과 시 소비 중단 → 2번 즉시 발동. (host→DPU sndbuf도 동일
   구조적 문제.)

**"채널이 왜 끊기나"의 답**: 덮어쓰기가 h2 프레임을 어긋나게 읽히면 gRPC가 connection
error로 연결을 닫고 재다이얼(→teardown된 슬롯이라 CONNECTION_ABORTED — 이전 관측),
바이트가 조용히 소실되면 hang(오늘 관측). 동전의 양면, 뿌리는 하나 = **push 전송의
backpressure 부재** (shim.c의 기존 TODO(flow-control)와 일치). c512는 in-flight 응답
바이트가 host 소비 속도를 초과하는 임계.

**수정 방향**: forward 방향의 `dma_ring_ctrl.consumer_head` 패턴을 push에도 —
host가 소비 커서를 공유 ctrl에 기록, DPU는 (seq - host_consumed) < 128 && 데이터링
여유 있을 때만 발행, 아니면 push 보류(tx_staging에 자연 축적 = 상류 backpressure).

**중간 성과**: res×4 = **8,420 req/s @ c256** (N=1 7.0k 대비 +20%, 멀티-replica 스케일링
실증) — 흐름제어 한계 내에서는 이미 동작.

### 양방향 flow-control 구현 (2026-09-02) — c512 붕괴 해결

**Push(DPU→host)**: host가 소비 커서(`struct dmesh_push_cursor`: magic+seq+bytes)를
rcvbuf의 예약영역(offset 2048)에 기록; DPU는 배치 사이 read-DMA(TASK_PULL_CURSOR)로
당겨와 `(push_seq-consumed_seq) < N-2 && 미소비바이트+2*MAX_BATCH ≤ ring`일 때만 발행.
MAGIC 없으면 레거시 동작(구 host 호환). 발행 보류 시 tx_staging의 Rust room 체크가
상류 backpressure로 이어짐.
**Forward(host→DPU)**: DPA가 이미 갱신하는 `dma_ring_ctrl.consumer_head`를 host가
읽어 outstanding desc ≥120이면 쓰기 보류(각 ≤8064B라 sndbuf 1MB wrap 불가).

결과 (res×4, 이전 즉사 조건):
| 부하 | FC 이전 | FC 이후 |
|---|---|---|
| c256 R12000 | 8.4k | 7.8k |
| **c512 R16000** | **17 (hang)** | **10,051 / 10,293 (연속 2회)** |
| c768 R24000 | 0 | 800 후 붕괴(비회복) — 잔여 한계 |

**res×4 신기록 10.3k** (N=1 7.0k 대비 +47%, TCP res×4 12.1k의 85%). c768 한계는
후속(용의: pull 주기 vs burst, 브리지 splice drop 경로, DPU측 rcv 링).

빌드 함정 기록: shim.c의 grave 잔재(제거된 object.h 필드 참조)로 **프록시 재빌드가
조용히 실패**해 FC 없는 06:53 바이너리로 두 라운드를 헛측정함 — cargo 빌드는 성공
로그를 반드시 확인할 것.

### c768 cliff 해결: 세 번째 방향(DPA→DPU staging) backpressure (2026-09-02)

**진단 사슬**: c768 붕괴는 비회복(hang) → push gate 진단 로그 0건(push FC는 무관) →
드라이버 tick 재시도·take_staged 멱등성 확인(lost-wakeup 아님) → 프록시 로그에
`Unexpected error … endpoint 10.0.12.1: operation was canceled`(29s) 후 `dmesh connection
setup failed` 연발 = storm이 h2 연결을 깨뜨리고 재다이얼이 wedge에 걸림 → 원인은
shim.c의 마지막 `TODO(flow-control)`: **DPA가 DPU staging 링에 쓸 때 Rust 소비를 확인하지
않고 wrap** → 미소비 h2 프레임 덮어쓰기.

**수정(옵트인, 프록시만 활성)**: `dpa_thread_arg.rd_pos/rd_fc` 추가. io.rs가 세그먼트를
완전 소비할 때 워터마크 갱신 → 드라이버 tick에서 변경분만 `h2d_memcpy`로 DPA에 발행
(`dmesh_doca_conn_rx_watermark`) → 커널은 여유 < 3×8064면 desc 소비 중단(→ consumer_head
정지 → host forward gate → h2 backpressure 체인 완성). C 워커/벤치(rd_fc=0)는 기존 동작.
부수: 에러 콜백의 PULL_CURSOR 처리 + stale-pull 자가복구 + gate 진단(4096회마다 WARN).

| 부하 (res×4, warmed) | 이전 | **3-방향 FC** |
|---|---|---|
| c512 R16000 | 10.1–10.3k | 9.8–9.9k |
| **c768 R24000** | **236 → hang** | **10,904 / 10,886 (연속)** |
| 이후 c512 (회복) | 15 | **9,946 / 9,940** |

Unexpected error 0, push gate held 0. **멀티-replica 피크 ≈ 10.9k req/s** (TCP res×4 12.1k의
90%). 잔여: 부하 종료 후 유휴 전환 시 `setup failed`(재다이얼→wedge, 측정 무영향).
