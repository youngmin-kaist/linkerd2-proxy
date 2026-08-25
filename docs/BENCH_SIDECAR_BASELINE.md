# Setup A — sidecar-Linkerd baseline on the host (measured 2026-08-15)

Host-side half of the "sidecar mesh on host CPUs vs DPUMesh on DPU Arm cores"
comparison. This measures a **2-proxy chain** of stock (TCP, no DMA)
linkerd2-proxy on rapids4, the same hop count a real per-pod sidecar mesh
imposes:

```
h2load ─► proxy A (outbound :4140) ─ mTLS + TransportHeader{8086} ─► proxy B (inbound :4143, direct stack) ─► nginx :8086
```

## Environment

- rapids4, Xeon Gold 6554S; benchmark confined to CPUs 0-17 (18-35 belong to
  another tenant). Pinning: nginx 0-3 (4 workers, `worker_cpu_affinity`),
  h2load 4-7, **proxies 8-17 (10 cores)** — so "all cores" for the mesh means
  10 proxy cores, identically reserved in the future DPUMesh run.
- Workload: h2c GET `/1k.bin` (exactly 1024 B), h2load closed-loop, `-m 100`
  per connection, ≥20 s steady state, `LINKERD2_PROXY_LOG=warn`.
- Backend headroom verified: h2load→nginx direct = **273k req/s** (6 conns) /
  **344k** (20 conns) — ≥3.5× above every chain result, so nginx never limits.
- Stock proxy build: `cargo build --release -p linkerd2-proxy
  --no-default-features --features allow-loopback` (doca off).
- Chain plumbing: `mock-policy` was extended (`MOCK_POLICY_TAGGED_ID` +
  `MOCK_POLICY_TAGGED_PORT`) to mark the endpoint as a meshed peer, so proxy A
  dials B's inbound with mesh TLS + a tagged-transport header — without this
  the hop is plaintext and B's direct stack refuses it. Verified: B's log shows
  the l5d transport header on every connection, and a plaintext probe to :4143
  is rejected ("direct connections must be mutually authenticated").

## Results

### Topology 1 — one client sidecar, symmetric cores (A=N, B=N)

| A+B cores | conns | req/s | proxy busy-cores |
|---|---|---|---|
| 1+1 | 4 | 52,212 | 2.2 |
| 2+2 | 8 | 64,581 | 3.8 |
| 3+3 | 12 | **70,290** (repeats 69,919 / 69,571) | 5.2 |
| 4+4 | 16 | 70,191 | 6.3 |
| 5+5 | 20 | 67,888 | 7.0 |

Plateaus at **~70k req/s** from 3+3 despite idle allocated cores: with a single
client sidecar and a single backend endpoint, A funnels every stream into ONE
upstream connection, and that per-connection pipeline (one task on A, one on B)
caps each pipe at ~15-22k req/s.

### Topology 2 — per-pod-faithful: N client sidecars → one server sidecar

Each A is its own process (1 core, sharing allowed), all traffic converges on
one B — exactly the shape of N client pods calling one server pod.

| N_A (cores) | B cores | req/s | busy A / B |
|---|---|---|---|
| 4 (4c) | 4 | 90,227 | 2.0 / 3.3 |
| 6 (6c) | 4 | 92,096 | 2.3 / 3.4 |
| 8 (5c) | 5 | 96,266 | 2.4 / 4.0 |
| 16 (4c) | 6 | **98,464** (repeats 99,658 / 98,705) | 3.0 / 4.8 |

**Peak: ~99k req/s** with 10 cores available to proxies (~7.8 actually busy).
With a single B, the server-side sidecar is the structural bottleneck — every
request passes through it, its per-core capacity is ~23-25k req/s (TLS
termination + tagged forward), and per-connection tasks leave cores partially
idle.

### Topology 3 — replicated server sidecars (server pod replicas)

N_B server sidecars, each with its own mock-policy instance so A_i routes to
B_(i mod N_B) — the shape of a server Deployment with replicas=N_B. This
removes the single fan-in:

| N_A (cores) | N_B × cores | req/s | busy A / B |
|---|---|---|---|
| 16 (4c) | 2 × 3c | 163,887 | 3.8/4 / 5.3/6 |
| 16 (4c) | 3 × 2c | 183,975 (repeats 185,598 / 170,776) | 4.0/4 / 5.0/6 |
| 20 (5c) | 5 × 1c | 203,679 (repeat 172,831 — noisy) | 4.2/5 / 4.0/5 |
| **25 (5c)** | **5 × 1c** | **212,767** | 4.8/5 / 4.7/5 |

The finer the sidecar granularity (many 1-core processes — the most
pod-faithful shape), the better it scales: at 25A+5B both sides run ~95% busy
and the 10 proxy cores deliver ~213k req/s. Run-to-run variance is ±10-15%
(shared host; another tenant owns CPUs 18-35).

### Numbers to carry into the comparison

- **Sidecar mesh, host, 10 proxy cores: ≈ 210k req/s peak** (replicated
  server sidecars, best case); ≈ 99k with a single server sidecar; ≈ 70k in
  the single-client-sidecar shape.
- End-to-end efficiency at peak: **≈ 20-22k req/s per busy proxy core** (each
  request crosses two proxies, so a single proxy pass is ~40-45k req/s-core on
  this Xeon — 1KB h2 + mTLS).
- Scaling is granularity-bound, not core-bound: the same 10 cores yield
  70k→213k purely by re-slicing them into more single-connection-friendly
  processes. The mesh tax is per-connection pipeline serialization as much as
  raw per-request CPU.
- The mTLS + h2 work per request is the dominant cost; nginx and h2load
  overheads are fully isolated on their own cores.

## Reproduce

Scripts (run ON rapids4, from `~/bf-workspace/linkerd2-proxy` — copies land in
`/tmp/ym_sidecar*`):

```bash
# symmetric chain:  <cores_A> <cores_B> <conns> [n_requests]
bash scripts/bench-sidecar-chain.sh 3 3 12 1500000
# per-pod multi-A:  <N_A> <cores_B> <conns_per_A> [n_req_per_A] [A_core_count]
bash scripts/bench-sidecar-multiA.sh 16 6 4 150000 4
# replicated-B:     <N_A> <N_B> <cores_per_B> <conns_per_A> [n_req_per_A] [A_cores]
bash scripts/bench-sidecar-multiAB.sh 25 5 1 4 100000 5
```

Both print h2load results plus per-range busy-core accounting from mpstat.
One-time host prep already applied: `/usr/share/nginx/html/1k.bin`,
`worker_processes 4` + `worker_cpu_affinity` (CPUs 0-3), `listen 8086 http2
reuseport`.

## Still open (Setup B, not run yet)

The DPUMesh side needs the push-mode ingress bridge + N-driver proxy support
(see the plan in the repo history / CLAUDE.md gotchas) before the Arm cores can
be saturated; its current h2-bridge ingress is limited to 2 channels by the
one-flexio-process-per-host-function constraint.
