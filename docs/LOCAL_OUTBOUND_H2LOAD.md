# Run linkerd2-proxy Locally With h2load

This guide describes how to run a local outbound `linkerd2-proxy` flow without a
Kubernetes control plane. It starts local mock control-plane services, sends
traffic to the outbound listener, and forwards the request to a local backend.

The tested path is:

```text
h2load -> linkerd2-proxy outbound listener :4140 -> backend :8086
```

## Requirements

- Rust toolchain from `rust-toolchain.toml`
- `h2load`
- A local HTTP/2 cleartext backend listening on `127.0.0.1:8086`
- Optional, for DOCA initialization: NVIDIA DOCA libraries discoverable by
  `pkg-config`

The helper scripts assume the backend is already running on `127.0.0.1:8086`.
For example, start an h2c-capable nginx or any other h2c test server on that
address before running the proxy.

## One-command local run

From the repository root:

```bash
scripts/run-local-outbound-h2load.sh
```

The script starts:

- `mock-identity` on `127.0.0.1:8088`
- `mock-destination` on `127.0.0.1:8089`
- `mock-policy` on `127.0.0.1:8087`
- `linkerd2-proxy` outbound listener on `127.0.0.1:4140`
- `h2load` against `http://localhost:4140/`

By default, the script runs:

```bash
h2load -c100 -n10000 http://localhost:4140/
```

Use `H2LOAD_ARGS` to override the request:

```bash
H2LOAD_ARGS='-c1 -n1 http://localhost:4140/' scripts/run-local-outbound-h2load.sh
```

Use `H2LOAD_TIMEOUT` to override the benchmark timeout:

```bash
H2LOAD_TIMEOUT=60s scripts/run-local-outbound-h2load.sh
```

## Enable DOCA initialization

The proxy has an optional `doca` feature. Enable it together with
`allow-loopback`:

```bash
PROXY_FEATURES='doca allow-loopback' \
H2LOAD_ARGS='-c1 -n1 http://localhost:4140/' \
scripts/run-local-outbound-h2load.sh
```

Expected proxy log line:

```text
DOCA compile=... runtime=... devices=... first_device=... dma=... aes_gcm=... sha=... dpa=...
```

The DOCA probe currently initializes the DOCA runtime, opens the first detected
DOCA device, and probes DMA, AES-GCM, SHA, and DPA context creation. It does not
yet submit real DMA, crypto, or DPA work.

## Logs

The script writes logs under:

```text
target/local-outbound-h2load/
```

Useful files:

- `linkerd2-proxy.log`
- `mock-identity.log`
- `mock-destination.log`
- `mock-policy.log`
- `h2load.log`

`h2load` output is shown on stdout and also saved to `h2load.log`. The mock
services and proxy redirect stdout and stderr into their log files.

## Manual run

Use this flow when debugging one component at a time.

Terminal 1:

```bash
source scripts/dev-proxy-env.sh
cargo run -p linkerd-app-integration --bin mock-identity
```

Terminal 2:

```bash
source scripts/dev-proxy-env.sh
MOCK_DESTINATION_ADDR=127.0.0.1:8089 \
MOCK_DESTINATION_BACKEND=127.0.0.1:8086 \
cargo run -p linkerd-app-integration --bin mock-destination
```

Terminal 3:

```bash
source scripts/dev-proxy-env.sh
MOCK_POLICY_ADDR=127.0.0.1:8087 \
MOCK_POLICY_BACKEND=127.0.0.1:8086 \
cargo run -p linkerd-app-integration --bin mock-policy
```

Terminal 4:

```bash
source scripts/dev-proxy-env.sh
cargo run -p linkerd2-proxy --features allow-loopback
```

For DOCA:

```bash
source scripts/dev-proxy-env.sh
cargo run -p linkerd2-proxy --features 'doca allow-loopback'
```

Terminal 5:

```bash
h2load -c1 -n1 http://localhost:4140/
```

## Important environment variables

| Variable | Default | Purpose |
| --- | --- | --- |
| `LINKERD2_PROXY_OUTBOUND_LISTEN_ADDR` | `127.0.0.1:4140` | Local outbound listener |
| `LINKERD2_PROXY_IDENTITY_SVC_ADDR` | `127.0.0.1:8088` | Mock identity service |
| `LINKERD2_PROXY_DESTINATION_SVC_ADDR` | `127.0.0.1:8089` | Mock destination service |
| `LINKERD2_PROXY_POLICY_SVC_ADDR` | `127.0.0.1:8087` | Mock policy service |
| `MOCK_DESTINATION_BACKEND` | `127.0.0.1:8086` | Backend endpoint returned by mock destination |
| `MOCK_POLICY_BACKEND` | `127.0.0.1:8086` | Backend endpoint returned by mock policy |
| `PROXY_FEATURES` | `allow-loopback` | Features passed to `linkerd2-proxy` by the helper script |
| `H2LOAD_ARGS` | `-c100 -n10000 http://localhost:4140/` | Arguments passed to `h2load` |

The one-command script overwrites the control-plane addresses so stale shell
environment variables do not accidentally point the destination service at the
backend port.

## Troubleshooting

### Backend is not listening

If the script prints:

```text
Backend nginx is not listening on 127.0.0.1:8086
```

start the h2c backend before running the script.

### h2load hangs or returns no successful responses

Check the effective addresses and recent logs printed by the script. The most
common issue is accidentally pointing `LINKERD2_PROXY_DESTINATION_SVC_ADDR` at
the backend port (`127.0.0.1:8086`) instead of the mock destination service
(`127.0.0.1:8089`). The helper script forces the correct addresses.

### Loopback connection is rejected

Use the `allow-loopback` feature:

```bash
cargo run -p linkerd2-proxy --features allow-loopback
```

Without this feature, the outbound proxy prevents loopback backend connections.

### DOCA build fails

Confirm that the DOCA libraries are visible through `pkg-config`:

```bash
pkg-config --modversion doca-common doca-dma doca-aes-gcm doca-sha doca-dpa
```

If this fails, install or source the DOCA environment before building with
`--features doca`.

