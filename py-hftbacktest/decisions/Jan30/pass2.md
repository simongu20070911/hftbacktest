# Jan 30 Upgrade Decisions (pass2)

This file records decisions made while implementing `py-hftbacktest/upgradespecjan30.md`.

## D1: L2 build path — keep `build_asset!` macro (for now) vs rewrite onto `L2AssetBuilder`

**Where:** `py-hftbacktest/src/lib.rs` (`build_hashmap_backtest`, `build_roivec_backtest`)

**Why this came up:** The Python extension must (a) stop using the macro for **L3** assets and (b) switch backtest
construction to the Rust `Backtest::builder()` path so `use_seq_tie_break` is propagated. At the same time, the repo
was already in a broken state where `build_asset!` constructed `Asset { local, exch, reader }` but the engine’s `Asset`
now requires `use_seq_tie_break`.

### Options

1) **Keep `build_asset!` for L2 assets**, patch the macro to include `use_seq_tie_break: false`, and implement a
   separate L3 build path using the real engine `L3AssetBuilder`.

2) **Rewrite L2 asset construction** to use the real engine `L2AssetBuilder` (and add snapshot support either by
   extending the engine builders or by wrapping the depth builder to apply snapshots), removing the macro entirely.

3) **Fork/duplicate the macro logic inside `py-hftbacktest`**, but updated to match current engine types (including
   `use_seq_tie_break`) and using `Backtest::builder()`.

### Grading (0–10)

| Option | Maintainability | Performance | Flexibility | Notes |
|---|---:|---:|---:|---|
| 1 | 7 | 9 | 6 | Minimal change for L2; restores compilation; avoids policy logic in Python; L2 remains on a macro path. |
| 2 | 9 | 9 | 9 | Best long-term alignment (single source of truth in engine); but requires a larger refactor and careful snapshot handling. |
| 3 | 3 | 8 | 4 | Duplicates engine wiring logic in the extension; likely to drift again and reintroduce mismatch failures. |

### Decision

**Pick Option 1** for pass2: patch `hftbacktest-derive` to include `use_seq_tie_break: false` (restoring compilation),
keep `build_asset!` for L2 temporarily, and implement L3 via the real engine `L3AssetBuilder`. This is the best total
score under the constraint that the roadmap requires immediate L3 correctness and seq tie-break propagation, while
keeping changes targeted and reducing risk of breaking existing L2 workflows.

Follow-up (planned): migrate L2 onto `L2AssetBuilder` once `initial_snapshot` behavior is cleanly supported via a depth
builder wrapper or a first-class engine builder feature (this would further improve maintainability/flexibility).

## D2: `L2Asset`/`L3Asset` implementation — Python wrappers vs new Rust pyclasses

**Where:** `py-hftbacktest/hftbacktest/builder/__init__.py`

**Why this came up:** The roadmap requires explicit `L2Asset()` / `L3Asset()` builders in Python. The Rust extension
currently exposes a single `BacktestAsset` PyO3 class. Introducing distinct Rust asset-spec pyclasses would be the
cleanest mirror of the engine, but also increases surface area and migration risk.

### Options

1) **Python-level wrappers/subclasses over the existing Rust `BacktestAsset`**, enforcing valid method sets (L2 vs L3)
   and setting correct defaults (e.g., `L3Asset` defaults to FIFO L3 queue model; disallow `partial_fill_exchange()` and
   require `cme_databento_mbo(True)`).

2) **Add new Rust PyO3 classes** `L2Asset` and `L3Asset` that each map 1:1 to the engine’s `L2AssetBuilder` /
   `L3AssetBuilder`, and update Python to use those types directly.

### Grading (0–10)

| Option | Maintainability | Performance | Flexibility | Notes |
|---|---:|---:|---:|---|
| 1 | 8 | 9 | 8 | Small ABI change; keeps build-time wiring in Rust; enforces the new Python UX without widening the Rust API surface immediately. |
| 2 | 9 | 9 | 9 | Best semantic mirror long-term; but more code paths/FFI surface and more migration points to maintain. |

### Decision

**Pick Option 1** for pass2: implement `L2Asset`/`L3Asset` as Python subclasses of the existing Rust `BacktestAsset`,
with strict method restrictions and CME MBO “one switch” behavior. This delivers the required user-facing API now while
keeping the Rust extension surface small. Revisit Option 2 once the API stabilizes and once we have CME MBO golden tests
in place to prevent regressions.

## D3: De-duplicating L3 build wiring (HashMap vs ROIVec)

**Where:** `py-hftbacktest/src/lib.rs`

**Why this came up:** L3 assets are built via engine `L3AssetBuilder`, but the wiring currently exists twice (once for
HashMap depth, once for ROIVector depth). This is correct but increases maintenance cost and the chance of mismatch when
adding knobs (CME mode flags, future queue models, additional fee/latency variants).

### Options

1) **Keep duplication** and accept the maintenance overhead.

2) **Introduce a small Rust macro** inside `py-hftbacktest/src/lib.rs` that generates the `match` arms for both depth
   types (HashMap and ROIVec) from a single source.

3) **Attempt a generic function** over `MD` and “depth constructor” (traits + closures). This tends to become complex in
   Rust here because the builder type is monomorphized on multiple generic parameters and the depth constructor differs
   (ROI bounds only exist for ROIVec).

### Grading (0–10)

| Option | Maintainability | Performance | Flexibility | Notes |
|---|---:|---:|---:|---|
| 1 | 4 | 9 | 6 | Easy now, but drift risk is real when adding new knobs or variants. |
| 2 | 8 | 9 | 8 | Keeps monomorphized performance; centralizes wiring; moderate implementation effort; avoids trait gymnastics. |
| 3 | 6 | 9 | 9 | Flexible in principle, but complexity/trait bounds likely reduce maintainability in practice. |

### Decision

**Pick Option 2 (macro)** as the best long-term maintainability/performance/flexibility tradeoff.

**Status (implemented):** 2026-01-30 — refactored `py-hftbacktest/src/lib.rs` so both HashMap and ROIVec L3 asset
construction use the same macro-generated `match` arms (see `build_l3_asset_for_depth!` and
`l3_asset_builder_finish!`). This removes the drift risk where a new L3 knob/variant could be added to one depth type
and accidentally omitted from the other.

## D4: Python `BacktestBuilder` façade vs exposing a Rust builder 1:1

**Where:** `py-hftbacktest/hftbacktest/builder/__init__.py`, `py-hftbacktest/src/lib.rs`

**Why this came up:** The spec asks for a stable Python `BacktestBuilder` surface. Today it is a Python façade that
calls Rust `build_*_backtest(...)` with policy args, which already uses engine `Backtest::builder()` internally.

### Options

1) **Keep Python façade** (current): Python holds a small amount of configuration state, then calls a single Rust build
   function to do the real work.

2) **Expose Rust `BacktestBuilder` as a PyO3 class** and mirror the fluent builder chain across the FFI boundary.

### Grading (0–10)

| Option | Maintainability | Performance | Flexibility | Notes |
|---|---:|---:|---:|---|
| 1 | 8 | 9 | 8 | Small FFI surface; easy to extend by adding Rust build args; keeps physics in Rust. |
| 2 | 7 | 9 | 9 | Very “native”, but increases FFI surface and lifetimes/state handling across PyO3; more breakage surface. |

### Decision

**Pick Option 1** unless/until we need significantly more builder knobs and want them discoverable/typed in Python.
Current approach already satisfies the north-star principle (Rust is source of truth); the incremental value of a full
Rust builder exposure is mostly ergonomics, not correctness/perf.
