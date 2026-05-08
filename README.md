# dynamo-kvsrv-pbs

Dynamo-style replicated key–value service in Go with **empirical Probabilistically Bounded Staleness (PBS)** evaluation: Δ-p and k-p curves from read/write traces under simulated networks and replica failures.

## Features

- Consistent hashing, sloppy quorum **(N, W, R)**, vector-clock versioning  
- Read repair, Merkle-tree **anti-entropy**, **hinted handoff**, membership gossip  
- **PBS collector & plotting** (`kvsrv_eval`): traces, Δ-p / k-p sweeps, poster-style combined figures  
- Course-style **simulated RPC** (`labrpc`) plus **daemon tester** (`tester`) for process-isolated runs  

## Requirements

- **Go** 1.24+ (see `src/go.mod`)

## Repository layout

Go module **`dynamo-kvsrv`**; all packages live under **`src/`**.

| Path | Role |
|------|------|
| `src/kvsrv/` | KV server, client, PBS demo driver|
| `src/kvsrv_eval/` | PBS collector, observer, plotting |
| `src/labrpc/` | Simulated network RPC (**MIT 6.5840**) |
| `src/labgob/` | Gob wrapper with field-name checks used by `labrpc` (**MIT 6.5840**) |
| `src/tester/` | Daemon launcher, Unix-socket RPC, test config (**MIT 6.5840**) |
| `src/cmd/kvsrv1d/` | Replica daemon binary (tests and PBS runs) |
| `src/cmd/plotdeltakp/` | CLI: reads `delta_p.csv` / `k_p.csv`, writes poster PNG |

## Quick start

```bash
cd src

# Daemon binary used by tests (writes src/bin/kvsrv1d)
make daemon

# Tests (many expect bin/kvsrv1d present first)
make test
```

Build CLIs explicitly:

```bash
cd src
go build -o bin/kvsrv1d ./cmd/kvsrv1d
go build -o bin/plotdeltakp ./cmd/plotdeltakp
```

### Poster-style figure from existing CSVs

With `delta_p.csv` and `k_p.csv` in one directory:

```bash
cd src
go run ./cmd/plotdeltakp -dir path/to/run -cols 4 -o path/to/poster.png
```

`-cols 2` is two panels only. See `go run ./cmd/plotdeltakp -help`.

## Third-party / academic honesty

The following are **adapted from MIT 6.5840** lab infrastructure: **`labrpc`**, **`labgob`**, and **`tester`**. The Dynamo semantics, PBS instrumentation, and plotting pipeline in **`kvsrv`**, **`kvsrv_eval`** and **`cmd`** are **this project’s** code.


