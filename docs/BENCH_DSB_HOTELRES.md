# DSB hotelReservation × Linkerd sidecar (kind-linkerd-bench, 진행 중)

합성 1KB h2 벤치(BENCH_SIDECAR_BASELINE.md)를 보완하는 현실적 마이크로서비스 워크로드.
hotelReservation은 gRPC(h2) 기반 — Linkerd가 전 구간 L7 프록시하므로 mesh tax가
다-hop 콜그래프에서 어떻게 증폭되는지 측정한다.

## 환경
- kind-linkerd-bench 클러스터 (Linkerd edge-26.6.2 설치됨), 노드 컨테이너 CPU 0-13 고정
- wrk2(DSB fork)는 host에서 taskset 14-17, frontend는 NodePort(노드 IP 172.18.0.2)
- 워크로드: DSB mixed-workload_type_1.lua (search 60%/recommend 39%/user 0.5%/reserve 0.5%)
- CPU 계정: kubectl top --containers 10s 샘플링 (metrics-server), pre-load 샘플 제외

## 배포 시 필요한 수정 (재현 노트)
1. 차트 command 낡음: 최신 이미지의 바이너리는 PATH에 있음 → charts/*/values.yaml의
   `command: ./x` → `command: x` (sed 일괄).
2. 최신 바이너리가 ConfigMap FQDN을 무시하고 compose식 평문 이름을 하드코드
   (consul:8500, mongodb-geo:27017, memcached-profile:11211 등)
   → 평문 alias Service 11개 생성 (consul, mongodb-{geo,profile,rate,recommendation,
   reservation,user}:27017, memcached-{profile,rate,reserve}:11211, jaeger).
3. consul은 인메모리 레지스트리: consul 재시작 시 서비스 등록 유실 → 앱 서비스들을
   consul 이후 재시작해야 함 (mesh 주입 rollout 시에도 동일 순서 필수).

## P1 — unmeshed 베이스라인 (2026-08-20 측정)

R 스윕 (60s/스텝, -t4 -c128, open-loop):

| R(목표) | 달성 RPS | p50 | p99 | 클러스터 총 코어(avg) |
|---|---|---|---|---|
| 500 | 499 | 0.98ms | 10.2ms | 1.1 |
| 1000 | 992 | 0.96ms | 14.3ms | 2.6 |
| **2000** | **2001** | **2.1ms** | **38-40ms** | **4.4-5.2** |
| 2200 | 2201 | 3.9ms | 73.7ms | 5.7 |
| 2600 | 2604 | 10.1ms | 128.6ms | 6.4 |
| 3000 | 2999 | 43.7ms | 384.8ms | 6.9 |
| 4000+ | ~3.2k 포화 | 수 초 | 수십 초 | ~7 (max 10.8) |

- **RPS@SLO(p99<50ms) = 2000** (3회: p99 37.8/40.2/40.3ms — 재현성 높음)
- 포화 처리량 ≈ 3.2k RPS
- **병목 = reservation 서비스 단일 레플리카** (포화 시 혼자 4.0-4.5코어; SLO점 2.1-2.7코어).
  다른 서비스: rate ~0.8, frontend ~0.5, search ~0.5, 데이터스토어 전체 ~0.3, consul/jaeger ~0.01
- 클러스터 14코어 중 포화 시에도 ~7-11코어만 사용 — 코어가 아니라 서비스 그래프의
  단일 서비스 처리 한계가 상한 (mesh 주입 시 사이드카가 이 여유 코어를 쓰게 됨)

## P2 — meshed (예정)
linkerd.io/inject=enabled + consul-이후 재시작 순서로 rollout, 동일 스윕.

## P1b — 전-코어 병목(레플리카 스케일) 구성 탐색 (10s 고속 프로브, cgroup cpu.stat 계정)

단일 레플리카에선 reservation이 단독 병목(전체 14코어 중 ~7-11만 사용). 레플리카를
CPU 소비 순으로 증설해 노드 코어가 소진되는 구성을 탐색:

**확정 구성** (`ym_dsb-fast.sh`는 wrk -c 768):
| 서비스 | replicas |
|---|---|
| reservation | 6 |
| rate / search / frontend | 4 |
| profile / geo / recommendation | 3 |
| user / 데이터스토어들 | 1 |

결과 (10s 프로브, node cgroup 코어 = k8s 네트워킹/kubelet 포함 실사용):
| R | 달성 RPS | p50 | p99 | node-cores(/14) |
|---|---|---|---|---|
| 2600 | 2440 | 4.0ms | **48.6ms** ✅ | 10.0 |
| 3000 | 2891 | 7.7ms | 80.6ms | 11.3 |
| 3300 | 3250 | 14.8ms | 119ms | 12.2 |
| 4500-8000 | **3.7-3.8k 포화** | — | — | **13.0-13.1 (94%)** |

- **포화 시 노드 94% 사용 = 전-코어 병목 달성.** 잔여 ~1코어는 스케줄링 파편.
- SLO(p99<50ms) ≈ **2600 RPS @ ~10코어** (1-replica 구성 2000 대비 +30%)
- 탐색 중 교훈: wrk2는 h1이라 연결수(-c)가 in-flight 상한 — 256→768로 올려야
  포화점이 드러남. 레플리카 증설만으론 3.4k에서 가짜 정체.
- 중간 스텝(reservation=4,rate=3,search=3,frontend=3,profile/geo/rec=2): 3.4k @12.1코어

P2(meshed)는 이 구성을 기준으로 비교. 사이드카 ~28개 추가가 같은 14코어 예산에서
경쟁하므로 RPS@SLO 하락 + 코어 재분배(app vs sidecar)가 관전 포인트.

### CPU 병목 검증 (전 서비스 추가 증설 실험)

확정 구성(위)이 정말 CPU 병목인지 두 방향으로 검증:
1. **전 서비스 레플리카 추가 증설** (reservation 8, rate/search/frontend 6, profile/geo/rec 5,
   user 3 — 총 55 pod): 포화 3.5-3.8k @ 13.1-13.3코어 — **변화 없음**
2. **wrk 연결 2배** (-c 1536): 3.5k @ 12.9코어 — **변화 없음** (클라이언트 상한 아님)

→ 포화 ~3.8k RPS / node ~13.1/14코어(94%)는 **노드 CPU 병목**으로 확정.
레플리카는 동일 CPU를 재분배할 뿐. 표준 구성은 세트 A(39 pod)로 유지
(같은 포화점에 pod 수가 적어 meshed 실험에서 사이드카 수도 최소화).

## P1c — 공유 풀 구성 (wrk+클러스터 분리 없이 18코어 전체, 최종 기준)

노드 cpuset을 0-17로 확장, wrk2도 같은 18코어에서 경쟁 (스레드 6, -c 768).
측정 3중화: 클러스터 cgroup 델타 + wrk cputime + mpstat 풀 사용률.

| R | 달성 RPS | p50 | p99 | 클러스터 코어 | pool(0-17) |
|---|---|---|---|---|---|
| 2800 | 2714 | 4.0ms | **50.6ms** (SLO 경계) | 10.7 | 11.4 |
| 3200 | 3063 | 6.5ms | 92ms | 11.8 | 12.6 |
| 3600 | 3542 | 13.0ms | 203ms | 13.2 | 14.0 |
| 6000 | **4489 (포화)** | — | — | 15.2 | **16.1** |
| 9000 | 4447 | — | — | 15.3 | 16.2 |

- **포화 ≈ 4.5k RPS, 풀 사용률 16.2/18 (90%)** — 14코어 구성(3.8k) 대비 +18%
- SLO(p99<50ms) ≈ **2700-2800 RPS**
- wrk2 자체 비용은 ~0.2코어로 미미 (분리가 사치였음이 확인됨)
- **잔여 ~1.8코어는 채워지지 않음**: 레플리카 추가(세트 B, 55pod → 4.2k로 오히려 하락),
  연결 2배(c1536)·스레드 8 모두 무효. RPC 요청/응답 체인의 스케줄러 웨이크업 갭 +
  단일 데이터스토어(memcached-reserve 등) 직렬화가 원인 — RPC 앱의 실질적 100%는
  ~90% 수용률이 상한. 이 상태를 "전-코어 CPU 병목"의 기준으로 삼는다.
- P2(meshed)는 이 구성(공유 18코어, 세트 A 39pod)과 비교한다.

## P2 — meshed (Linkerd edge-26.6.2 주입, 2026-08-20 측정)

전 pod(39개, 데이터스토어 포함) 2/2 주입. 재기동 순서: 인프라(consul 포함) → 앱.
동일 공유 18코어 풀, 동일 wrk2 설정.

| R | 달성 RPS | p50 | p99 | 클러스터 코어 | pool |
|---|---|---|---|---|---|
| 1200 | 1158 | 3.4ms | **32.7ms** ✅ | 7.6 | 8.3 |
| 1600 | 1524 | 6.2ms | 64.5ms ❌ | 9.9 | 10.6 |
| 2000 | 1932 | 6.8ms | 147ms | 9.9 | 10.7 |
| 2800 | 2672 | 62ms | 577ms | 13.8 | 14.6 |
| 4500-9000 | **2.8-2.9k 포화** | — | — | 14.9 | 15.7-15.8 |

코어 분해 (R=2400, 30s, kubectl top --containers):
**앱 컨테이너 6.7코어 vs linkerd-proxy 사이드카 2.2코어** (+ pod 밖 k8s ~3코어)

### Mesh tax (unmeshed P1c 대비, 동일 조건)

| 지표 | unmeshed | meshed | tax |
|---|---|---|---|
| SLO(p99<50ms) RPS | ~2800 | **~1300-1400** | **-50%** |
| 포화 RPS | ~4.5k | **~2.9k** | **-36%** |
| p99 @R=2000 | ~40ms | 147ms | 3.7× |
| 사이드카 CPU | — | 요청당 ~0.96 core-ms (앱 2.9의 +33%) | |

- 요청당 사이드카 통과 ~6-9회(콜그래프 hop×2) → 사이드카 pass당 ~7-9k req/s/core —
  합성 1KB 마이크로벤치의 pass당 ~40k/core보다 훨씬 무거움 (gRPC 페이로드, hop당
  mTLS, 프록시 큐잉). 다-hop 증폭이 tax의 본질: CPU +33%가 throughput -36~50%로 나타남.
- R=6000 과부하에서 5xx 82건 (포화 초과 영역, SLO 비교엔 무관).

## P3 — cache hit ratio = 1 조건 (meshed, 2026-08-20)

### hit=1 만들기 (Go 재빌드 없이)
1. 정상상태 실측: profile 99.99%, rate 99.92%, **reserve 95.0%** (누적 72.5%는 재시작 워밍업 포함).
2. reserve의 잔여 5% miss 근본 원인 = **DSB 코드 버그**: CheckAvailability의 GetMulti는
   부분 miss 시 err를 안 돌려주는데 코드는 `err==ErrCacheMiss`일 때만 miss 키를 mongo에서
   채움 → 부분 miss 키는 영원히 Set되지 않음 (증거: cmd_set 누적 4.6k vs cmd_get 2.35억).
   부재 키는 조용히 count=0 취급됨.
3. 조치: ① wrk lua 날짜 고정(mixed-workload_hit1.lua: inDate=04-10, outDate=04-12 —
   search·reserve 블록 모두) → 키공간 상수화, ② 부재 키를 memcached `add`로 시딩
   (있으면 NOT_STORED로 보존; 29개만 신규). → **delta hit ratio = 1.00000, miss 0**.
4. 부수 발견: 요청 1건이 같은 reserve 키를 3-4회 반복 조회 (gets/req ~490-855).

### meshed 성능, hit=95% vs hit=1 (동일 조건)
| 지표 | meshed(랜덤 날짜, hit 95%) | meshed(hit=1) | 개선 |
|---|---|---|---|
| SLO(p99<50ms) | ~1300-1400 | **~2200** (46ms@2200) | +60% |
| 포화 | ~2.9k | **~3.7k** @15.4/18 | +28% |
| p99@1600 | 64.5ms | 24.5ms | 2.6× |

miss→mongo 경로 제거로 reservation의 mongo 조회가 사라져 임계 경로가 짧아진 효과.
주의: **unmeshed hit=1 베이스라인은 미측정** (주입 해제 필요) — mesh tax를 hit=1에서
대칭 비교하려면 어노테이션 제거 + 재기동 후 동일 사다리 1회(~10분).
재현: /tmp/ym_dsb-fast2.sh는 hit1 lua를 쓰도록 변경된 상태. 시딩은 memcached 재시작 시 재실행 필요.

### unmeshed hit=1 베이스라인 (주입 해제 후 재측정, 2026-08-20)

재시딩 노트: memcached 재시작 후 date 키는 add "0" 시딩으로 충분하나 **_cap 키 9개가
부분-miss 버그로 영구 부재**했음 — cap은 값이 실수용량이어야 하므로 mongo(number 컬렉션,
hotelId/numberOfRoom, 총 480 호텔)에서 실값을 읽어 add. 이후 delta miss=0 확인.

| R | 달성 | p50 | p99 | pool |
|---|---|---|---|---|
| 2800 | 2754 | 2.5ms | 26.7ms | 10.4 |
| 3200 | 3117 | 3.9ms | **44.5ms** ✅ | 11.6 |
| 3600 | 3540 | 6.8ms | 62.9ms ❌ | 12.7 |
| 6000-9000 | **~5.1k 포화** | — | — | 15.6-15.8 |

### 최종 mesh tax 표 (공유 18코어, 동일 조건 쌍별 비교)

| 조건 | unmeshed | meshed | mesh tax |
|---|---|---|---|
| **hit=1**: SLO(p99<50ms) | **~3200** | **~2200** | **-31%** |
| **hit=1**: 포화 | **~5.1k** | **~3.7k** | **-27%** |
| hit≈95%(랜덤 날짜): SLO | ~2800 | ~1300-1400 | -50% |
| hit≈95%: 포화 | ~4.5k | ~2.9k | -36% |

관찰: **hit=1에서 mesh tax의 상대 크기가 줄어든다** (-50%→-31%). miss→mongo 왕복이
사라져 임계 경로가 짧아지면 SLO 예산에 프록시 hop latency를 흡수할 여유가 생기기 때문.
반대로 캐시 미스가 있는 현실 워크로드일수록 sidecar mesh의 tail-latency 증폭이
치명적이라는 뜻이기도 하다.

현재 클러스터 상태: unmeshed + hit=1 (fixed-date lua + 시딩 완료). meshed 재현은
어노테이션 + 순서 재기동 + 재시딩.

## P4 — 동일 R에서 순수 앱 CPU: unmeshed vs meshed (2026-08-24)

방법: 컨테이너별 cgroup cpu.stat 델타(호스트에서 kind 노드 내부 cri-containerd
cgroup 직접 읽음 — metrics-server 불필요, 10s 정상상태 창, 5s 램프 후 측정).
crictl로 containerID→(pod,container) 매핑, linkerd-proxy 컨테이너 분리 집계.
조건: hit=1, 세트 A, 3회 반복(편차 ±1% 미만).

| R | app(unmeshed) | app(meshed) | **Δapp** | proxy | total(meshed) | 순증가 |
|---|---|---|---|---|---|---|
| 1000 | 4.16 | 3.70 | **-0.46 (-11%)** | 1.69 | 5.39 | +1.23 (+30%) |
| 1600 | 6.11 | 5.47 | **-0.64 (-10%)** | 2.53 | 8.00 | +1.89 (+31%) |
| 2000 | 7.28 | 6.86 | **-0.42 (-6%)** | 3.03 | 9.89 | +2.61 (+36%) |

**발견: 같은 부하에서 사이드카 주입 시 앱 자체 CPU가 6-11% 감소한다.**
서비스별 감소(R=1600): frontend -26%, search -21%, rate -7%, reservation -3%,
memcached ~0% — 네트워크 팬아웃이 큰 서비스일수록 크게 감소.

해석: 앱의 원격 통신이 전부 localhost(사이드카행)로 바뀌면서 커널 네트워크 비용
(veth/DNAT 경로, per-connection 처리, softirq 상당분)이 앱 cgroup에서 프록시
cgroup으로 이전 + 프록시 간 h2-upgrade 멀티플렉싱으로 원격 커넥션이 통합되는 효과.
즉 **프록시 코어(1.7-3.0)의 일부는 순수 오버헤드가 아니라 앱에서 이전된 네트워크
처리**이고, 매칭 부하 기준 mesh의 순 CPU 세금은 +30-36%.

DPUMesh 시사점: proxy를 DPU로 내리면 (a) 프록시 코어 전부가 host에서 사라지고
(b) 이 실험의 "앱 relief"(-6~11%)도 DMA 경로에선 유사하게 발생 가능 — host 앱
CPU 절감이 프록시 코어 수보다 클 수 있음.

### P4b — 프로세스 수준(utime/stime) 분해 (2026-08-24)

/proc/PID/stat 기반(스레드 합산, PID→서비스는 /proc/PID/cgroup의 cri-containerd id로
매핑). 검증: 모든 구성에서 **proc 합계 == cgroup 합계** (오차 ≤0.01코어) — 즉 P4의
앱 relief는 계정 왜곡이 아닌 실제 프로세스 CPU 감소이며, softirq는 양쪽 다
컨테이너 밖(root cgroup)에 있음.

앱 프로세스 CPU (user/sys 코어):
| R | unmeshed | meshed | Δuser | Δsys |
|---|---|---|---|---|
| 1000 | 3.38 / 0.76 | 3.17 / 0.56 | -6% | **-27%** |
| 1600 | 5.05 / 1.12 | 4.70 / 0.77 | -7% | **-31%** |
| 2000 | 6.11 / 1.26 | 5.90 / 0.90 | -3% | **-29%** |

- **relief의 절반은 sys(-27~31%)**: 원격 TCP(veth/DNAT 경로) syscall이 localhost
  전송으로 바뀐 직접 효과. frontend가 최대(sys 0.29→0.17, -41%).
- **나머지 절반은 user(-3~7%)**: localhost RTT 감소 → in-flight/고루틴 상태 축소,
  gRPC 커넥션 이벤트 처리 감소 (Go 런타임 스케줄링은 user에 계상).
- 보존 확인: R=1600 sys 총량은 unmeshed 1.12 → meshed 0.77(앱)+0.85(프록시)=1.62
  — hop당 소켓 2개 추가로 시스템 전체 네트워킹 일은 늘었고(순세금 +31%의 일부),
  다만 그 부담의 위치가 앱 프로세스 밖으로 이동했다는 것이 핵심.
- 프록시 프로세스는 sys 비중이 높음(u1.68/s0.85) — 원격 네트워킹을 대신 수행.

현재 클러스터: unmeshed + hit=1. 스크립트: scripts/bench-dsb-appcpu2.sh
