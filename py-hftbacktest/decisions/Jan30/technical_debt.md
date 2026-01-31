# Jan 30 Upgrade — Technical Debt (pass2)

This file tracks known technical debt after implementing `py-hftbacktest/upgradespecjan30.md` (pass2).

Date: 2026-01-30

## Fixed in pass2

- **D3 (L3 wiring drift risk):** de-duplicated L3 HashMap vs ROIVec wiring in `py-hftbacktest/src/lib.rs` via local macros
  (`build_l3_asset_for_depth!`, `l3_asset_builder_finish!`). This removes the “update one branch, forget the other”
  future-bug risk.

- **D2 guardrails coverage:** added `py-hftbacktest/tests/test_asset_typing.py` to explicitly assert L2/L3 Python
  guardrails (e.g., calling L2-only APIs on `L3Asset` raises).

## Still outstanding / deliberate (tracked)

- **D1 (L2 build path still uses `build_asset!`):**
  - **Where:** `py-hftbacktest/src/lib.rs` (L2 asset build arms).
  - **Why debt:** L2 wiring remains on a macro path rather than the engine’s `L2AssetBuilder`, so it can drift as engine
    types evolve.
  - **Mitigation in pass2:** `hftbacktest-derive` macro patched to include `use_seq_tie_break: false`; TODO added near the
    macro call sites in `py-hftbacktest/src/lib.rs` to track migration.

- **Asset typing is Python-enforced (not Rust-type enforced):**
  - **Where:** `py-hftbacktest/hftbacktest/builder/__init__.py`.
  - **Why debt:** `L2Asset`/`L3Asset` are Python wrappers over a shared Rust `BacktestAsset` pyclass, so misuse is caught
    by guardrails rather than the Rust type system.
  - **Reason for keeping:** smaller ABI surface while the new API stabilizes; guardrails + tests reduce risk.

- **Python `BacktestBuilder` is a façade (not a 1:1 Rust builder exposure):**
  - **Where:** `py-hftbacktest/hftbacktest/builder/__init__.py`, `py-hftbacktest/src/lib.rs`.
  - **Why debt:** builder state/config lives in Python and ultimately calls `build_hashmap_backtest` /
    `build_roivec_backtest` (now with explicit equal-ts policy args) rather than mirroring the engine builder chain.
  - **Reason for keeping:** maintains a small, stable FFI surface while preserving the north-star principle (physics in
    Rust, wiring in Python).

- **Run artifact contract not yet implemented (orders/trades logs):**
  - **Where:** spec section “Artifact + experimentation spec”.
  - **Why debt:** there is no standardized `config.json` / `orders.*` / `trades.*` writer yet; current `Recorder`
    only captures time-series state (mid + position/balance/fees/trade counters).

