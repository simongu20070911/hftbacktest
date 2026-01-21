# Bug Report — Engine Review Batch 6 (2026-01-21)

Source batch (read-only review): `~/ParallelHassnes/runs/batch_20260121T040233Z_e9fcc902/`

Reviewed repo: `/Users/simongu/AAAHFT-Futures-Alpha-seek/hftbacktest` (commit under review: `660e60a`)

Goal: convert the 6-agent read-only engine review into a concrete, test-first bugfix plan (regression → fail → fix → pass).

---

## Executive Summary (ranked)

### Critical / High (fix first)

1) **UB / memory-safety risks in `Data` / `Reader` / `AlignedArray` (multiple confirmed unsound patterns)**
   - `Data<D>` is `Clone` over `Rc<DataPtr>` but exposes safe `IndexMut` returning `&mut D` → aliasing UB risk.
     - Evidence: `hftbacktest/src/backtest/data/mod.rs:36`, `hftbacktest/src/backtest/data/mod.rs:166`
   - `unsafe impl Send for DataSend<D>` moves `Data<D>` (contains `Rc`) across threads → thread-safety UB risk, especially because user preprocessors can clone `Data`.
     - Evidence: `hftbacktest/src/backtest/data/reader.rs:165`, `hftbacktest/src/backtest/data/reader.rs:180`
   - `AlignedArray::new` allocates uninitialized memory but safe `Deref`/`as_slice` expose reads → UB risk; also `len * size_of::<T>()` arithmetic is unchecked.
     - Evidence: `hftbacktest/src/utils/aligned.rs:38`, `hftbacktest/src/utils/aligned.rs:143`
   - `DataPtr::default()` uses a null slice fat pointer; `Index` derefs `&*self.ptr` → UB if indexed.
     - Evidence: `hftbacktest/src/backtest/data/mod.rs:227`, `hftbacktest/src/backtest/data/mod.rs:243`

2) **L2 modify quantity bug: `leaves_qty` can become stale when modify routes through cancel+new**
   - Local modify updates `qty` but does not update leaves before sending the request.
     - Evidence: `hftbacktest/src/backtest/proc/local.rs:187`
   - L2 exchange `ack_modify` cancel+new path calls `ack_new(order, ..)` without resetting `order.leaves_qty = order.qty` first.
     - Evidence: `hftbacktest/src/backtest/proc/nopartialfillexchange.rs:544`, `hftbacktest/src/backtest/proc/nopartialfillexchange.rs:550`
     - Evidence: `hftbacktest/src/backtest/proc/partialfillexchange.rs:765`, `hftbacktest/src/backtest/proc/partialfillexchange.rs:771`
   - Impact: wrong resting size / wrong fills after modify; can systematically bias backtests.

3) **Cancel ACK corrupts `last_order_latency`: request `local_timestamp` is overwritten**
   - Exchange `ack_cancel` uses `mem::replace(order, exch_order)` overwriting request-side timestamps; Local computes `last_order_latency` from response `order.local_timestamp`.
     - Evidence: `hftbacktest/src/backtest/proc/nopartialfillexchange.rs:475`
     - Evidence: `hftbacktest/src/backtest/proc/local.rs:90`
   - Impact: latency metrics drift; affects any logic that uses reported latency (analysis/monitoring; possibly strategy logic).

4) **Depth UB / OOB risk in `ROIVectorMarketDepth` scans**
   - `depth_below` / `depth_above` convert possibly-negative values to `usize` and use `get_unchecked`.
     - Evidence: `hftbacktest/src/depth/roivectormarketdepth.rs:38`, `hftbacktest/src/depth/roivectormarketdepth.rs:50`
   - Impact: potential memory corruption when ROI is narrow and `best_*_tick` drifts outside ROI (plausible via L3 add/modify paths).

### Medium

5) **`FusedHashMapMarketDepth::clear_depth` off-by-one recompute**
   - After clearing bids, recompute uses `depth_below(..., clear_upto - 1, ..)` which skips `clear_upto - 1` (and similarly ask side skips `clear_upto + 1`).
     - Evidence: `hftbacktest/src/depth/fuse.rs:307`
   - Impact: best bid/ask can be wrong immediately after clear events.

6) **`IntpOrderLatency` robustness: empty dataset panic + duplicate timestamp divide-by-zero**
   - `build()` can return `Data::empty()`; `entry()` and `response()` index `[0]`.
     - Evidence: `hftbacktest/src/backtest/models/latency.rs:128`, `hftbacktest/src/backtest/models/latency.rs:175`
   - `intp()` divides by `(x2 - x1)` without guarding `x2 == x1`.
     - Evidence: `hftbacktest/src/backtest/models/latency.rs:152`
   - Impact: crashes or nonsensical latency values on malformed inputs.

### Needs-spec / design choice (confirm before “fix”)

7) **Event tie-break is asset-major (not a global kind-priority)**
   - `EventSet::next` keeps first equal timestamp winner due to `<` not `<=`; slot layout yields asset-major ordering.
     - Evidence: `hftbacktest/src/backtest/evs.rs:49`
   - This may be fine (assets independent), but document/specify it explicitly.

8) **Feed “look-ahead” prevention depends on data contract**
   - Validation that `local_ts > exch_ts` only runs in the `latency_offset != 0` adjustment path; clarify whether `latency_offset==0` should still validate input.
   - If input data can be unsorted or mismatched, there is a real look-ahead risk; needs a contract decision.

---

## Proposed Bugfix Batch (test-first workflow)

Batch style:
- isolated workdirs (git worktrees) for safety and parallelism
- shared canonical Rust cache: `/Users/simongu/AAAHFT-Futures-Alpha-seek/hftbacktest/target`
- each job must: regression test first → show failure → minimal fix → show pass (targeted test) → optionally full `cargo test -p hftbacktest --lib`

Job breakdown (8 jobs total):
1) `fix_01_alignedarray_soundness` — address uninit exposure + checked size math (`hftbacktest/src/utils/aligned.rs`)
2) `fix_02_data_rc_indexmut_soundness` — remove/replace unsound `IndexMut` over `Rc` + fix `DataPtr::default` null fat pointer (`hftbacktest/src/backtest/data/mod.rs`)
3) `fix_03_reader_thread_send_and_hang` — remove `unsafe impl Send` approach (send owned bytes/ptr instead) + ensure loader panic doesn’t hang `next_data` (`hftbacktest/src/backtest/data/reader.rs`)
4) `fix_04_l2_modify_leaves_qty` — ensure modify paths reset `leaves_qty` correctly; regression test exercising cancel+new modify (`hftbacktest/src/backtest/proc/local.rs`, `.../nopartialfillexchange.rs`, `.../partialfillexchange.rs`)
5) `fix_05_cancel_ack_preserve_local_timestamp` — preserve request `local_timestamp` through cancel ack; regression test on `last_order_latency` correctness (`hftbacktest/src/backtest/proc/*`)
6) `fix_06_roivector_depth_clamp_oob` — make `depth_below/above` clamp safely (no negative-to-usize); regression test using out-of-ROI ticks (`hftbacktest/src/depth/roivectormarketdepth.rs`)
7) `fix_07_fused_clear_depth_off_by_one` — correct recompute bounds; regression test (`hftbacktest/src/depth/fuse.rs`)
8) `fix_08_intporderlatency_empty_and_duplicate_ts` — reject empty / handle duplicates safely; regression tests (`hftbacktest/src/backtest/models/latency.rs`)

---

## Source Review Artifacts

- Agent 1 (event loop / ties): `~/ParallelHassnes/runs/batch_20260121T040233Z_e9fcc902/review_01_event_loop_and_tie_breaks/steps/run/attempts/*/final.json`
- Agent 2 (timestamp/sentinels): `~/ParallelHassnes/runs/batch_20260121T040233Z_e9fcc902/review_02_timestamp_axis_overflow_sentinels/steps/run/attempts/*/final.json`
- Agent 3 (order bus): `~/ParallelHassnes/runs/batch_20260121T040233Z_e9fcc902/review_03_order_bus_latency_scheduling/steps/run/attempts/*/final.json`
- Agent 4 (processors/state): `~/ParallelHassnes/runs/batch_20260121T040233Z_e9fcc902/review_04_exchange_local_processors_state_invariants/steps/run/attempts/*/final.json`
- Agent 5 (depth): `~/ParallelHassnes/runs/batch_20260121T040233Z_e9fcc902/review_05_depth_models_invariants_and_hotloops/steps/run/attempts/*/final.json`
- Agent 6 (data ingest/UB): `~/ParallelHassnes/runs/batch_20260121T040233Z_e9fcc902/review_06_data_ingest_pod_alignment_ub_risks/steps/run/attempts/*/final.json`

