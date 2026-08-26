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
