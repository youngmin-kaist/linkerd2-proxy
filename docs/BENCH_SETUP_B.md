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
