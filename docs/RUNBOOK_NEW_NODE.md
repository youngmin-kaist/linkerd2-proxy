# 다른 노드에서 DPUMesh 실행하기 (host + BlueField-3 한 쌍)

## 0. 전제
- **하드웨어/SW**: BlueField-3 DPU + x86 host, 양쪽 DOCA 3.1(`/opt/mellanox/doca`), host DOCA에 host PF가
  보일 것. 테스트베드 외에서는 빌드/실행 모두 의미 없음(DOCA 필수).
- DPU: meson/ninja, `dpacc`(DOCA 동봉), Rust stable(cargo). host: Go ≥1.26, docker compose, wrk2 빌드.
- 머신별로 바뀌는 값 4개: **DPU DOCA dev/rep PCI**(예 `03:00.1`/`94:00.1`), **host PF PCI**(예 `94:00.1`),
  **host/DPU IP**, **host 홈 경로**.

## 1. 소스 받기 (양쪽 동일 커밋)
```bash
git clone git@github.com:youngmin-kaist/DPUMesh.git && cd DPUMesh
git submodule update --init --recursive      # linkerd2-proxy = 부모가 가리키는 SHA (브랜치 dpumesh)
```
host에도 같은 클론을 두고(관례상 `~/bf-workspace`) 동일 커밋으로 맞춘다.

## 2. 빌드 순서 (반드시 이 순서)
**DPU**
```bash
cd DPUMesh && meson setup build && ninja -C build        # C 데이터패스 + dpacc(device/dpa_kernel.a)
cd ../linkerd2-proxy && cargo build --release -p linkerd2-proxy   # doca 기본 feature; shim이 ../DPUMesh/*.c 직접 컴파일
```
- 프록시는 `DPUMesh/build/device/dpa_kernel.a`를 정적 링크 → ninja가 먼저.
- **cargo 출력에서 `Finished`를 눈으로 확인**할 것. C 헤더 불일치 등으로 조용히 실패하면 옛
  바이너리로 측정하게 됨(실제로 두 라운드를 날린 함정). `ls -l target/release/linkerd2-proxy` 시각 확인.

**host**
```bash
cd ~/bf-workspace/DPUMesh && meson setup build && ninja -C build   # libdmesh_host.so + dpumesh(브리지)
```
dmeshgo(Go)는 cgo로 `${SRCDIR}/../build/libdmesh_host.so`를 rpath로 찾는다 → 리포 배치를 바꾸지 말 것.

## 3. 머신별 설정
- 프록시: `linkerd2-proxy/scripts/dev-proxy-env.sh`가 `LINKERD2_PROXY_DOCA_DEV_PCI_ADDR`(`03:00.1`),
  `LINKERD2_PROXY_DOCA_REP_PCI_ADDR`(`94:00.1`) 등을 export — 새 노드 PCI로 수정.
- host 라이브러리: `DMESH_PCI_ADDR`(기본 `94:00.1`) = host PF. 리버스 DPA는 `DMESH_REV_PCI`(브리지만 사용).
- 하네스 스크립트(`scripts/*.sh`)에 host IP `192.168.100.1`, 경로 `~/bf-workspace`, `~/hotelres-dmesh`가
  하드코딩 → sed로 치환.
- `pkill` 패턴 함정: 러너 cmdline에 타깃 문자열이 들어가면 자기 자신을 죽인다(exit 144). 스크립트는
  파일로 실행하고, 서비스는 **절대경로**로 띄운다(상대경로면 pkill이 못 잡아 옛 스택이 살아남음).

## 4. 검증 순서 (규칙: 모든 실험은 단일 요청 preflight 통과 후)
1. `scripts/dm-echo.sh` — gRPC 에코 1건이 프록시를 통과(200 + `request_total` 증가)하는지.
2. `scripts/dm-bench.sh <cores> <P>` — 64B 에코 스루풋(참고치: 16코어 ~150k).
3. DSB(선택, 아래).

## 5. DSB hotelReservation (선택)
```bash
cp -r DeathStarBench/hotelReservation ~/hotelres-dmesh
cd ~/hotelres-dmesh && patch -p1 < <DPUMesh>/dmeshgo/dsb-integration.patch
```
- 패치 안 `go.mod`의 `replace dmeshgo => /home/youngmin/bf-workspace/dmeshgo` 는 **절대경로** → 새 노드
  경로로 수정. `vendor/`는 삭제(cgo SRCDIR을 깨뜨림). `go build -buildvcs=false -o bin/<svc> ./cmd/<svc>`.
- 인프라: `docker compose up -d consul jaeger mongodb-* memcached-*` 후 `dmeshgo/dsb_host_setup.py`가
  컨테이너 브리지 IP로 `config.json` 생성. 서비스는 bare 프로세스(`dmeshgo/dsb_run_services.sh`).
- 실행: 베이스라인 `dsb_run_services.sh tcp`, DMA는 DPU에서 `scripts/dsb-dmesh-spec.sh <W> "<replica spec>"`
  (프록시+mock 기동 → 서비스 → preflight GET/POST). 부하는 `scripts/fe4-run.sh` / `tcp-mix.sh`.
- 벤치 전 **캐시 웜업 2회(c128 R3000)** 필수. 재측정 시 프록시부터 새로 띄울 것(채널 재연결 wedge).

## 6. 알려진 제약
- 채널은 1회용: 프로세스가 죽거나 gRPC가 재다이얼하면 그 엣지는 wedge(재연결 abort). 장수 연결 전제.
- DPA 슬롯 워커당 16(`DPA_THREAD_POOL_SIZE`/`DMESH_MAX_CONNECTIONS`/`MAX_CONNS` 세 곳 동기).
- 두 host 프로세스가 같은 host DPA를 못 씀(flexio 프로세스당 1 함수) — 브리지만 해당, dmeshgo는 무관.
