# HFTBacktest Backtest/Research Reality Audit (verbatim)
#
- Generated at (UTC): 2026-01-20T10:57:37Z
- ParallelHassnes batch_id: `batch_20260120T091958Z_4dc3d953`
- Source runs dir: `/Users/simongu/ParallelHassnes/runs/batch_20260120T091958Z_4dc3d953`
- Launch table: `/Users/simongu/ParallelHassnes/launch_tables/launch_table_hftbacktest_backtest_reality_audit_5_20260120T090847Z.json`
#
This document captures the *verbatim* `summary` text emitted by each agent run in the batch above.
#
---
#
## Completed (2026-01-20)

Batch: `batch_20260120T121548Z_1d74ae4b` (hftbacktest commit `13c7fd1`)

- [x] fix_01_depth_clear_bugs
- [x] fix_02_btree_depth_contracts
- [x] fix_03_partialfill_accounting_and_rejects
- [x] fix_04_npy_reader_robustness
- [x] fix_05_timestamp_overflow_checks

## Completed (2026-01-21)

Batch: `batch_20260121T024937Z_f8f60086` (10 parallel isolated agents; shared Rust target cache)

- [x] fix_06_event_tie_policy_and_lookahead (ExchOrder before ExchData at same timestamp + regression tests)
- [x] fix_07_goto_end_endofdata_flush (cur_ts end-of-data drift + flush pending order responses + regression test)
- [x] fix_08_orderbus_reordering_and_jitter_knob (keep FIFO default; add opt-in bounded timestamp reordering + reserved i64::MAX scheduling guard)
- [x] fix_09_depth_roi_nan_and_out_of_roi_semantics (avoid NaN poisoning for out-of-ROI qty queries + regression tests)
- [x] fix_10_snapshot_roundtrip_btree_and_roi (implement `ApplySnapshot::snapshot()` for BTree/ROI depth + round-trip tests)
- [x] fix_11_npy_schema_strictness (strict dtype/field validation by default; explicit opt-in permissive mode + tests)
- [x] fix_12_npy_truncation_hang_and_bounds (extra regression coverage: oversized input + corrupted `.npz` size mismatch)
- [x] audit_01_fill_queue_realism_discernment (correctness: prevent impossible overfills from multiple active L2 orders at same (side, price_tick) + tests)
- [x] verify_02_timestamp_axis_and_overflow_still_fixed (nonnegative duration/timeout; checked timestamp arithmetic; reject reserved `i64::MAX` timestamps)

## agent_01_event_ordering_and_lookahead (status: needs_attention)
#
- final.json: `/Users/simongu/ParallelHassnes/runs/batch_20260120T091958Z_4dc3d953/agent_01_event_ordering_and_lookahead/steps/run/attempts/20260120T102818Z_run_20260120T102818Z_333a388f/final.json`
#
~~~~text
Event selection / tie rules:
- Global loop is `Backtest::goto` (`hftbacktest/src/backtest/mod.rs:755`), which repeatedly calls `EventSet::next`.
- `EventSet::next` (`hftbacktest/src/backtest/evs.rs:43`) picks the minimum timestamp across `num_assets*4` slots; ties use the first slot encountered because comparison is strict `<`.
- Slot index encodes `(asset_no, kind)` as `4*asset_no + kind`, where kind priority is `LocalData(0) < LocalOrder(1) < ExchData(2) < ExchOrder(3)` (`hftbacktest/src/backtest/evs.rs:17`). So equal timestamps are ordered by `asset_no` ascending first, then that kind order.
- Within a processor’s feed stream, equal-timestamp events are processed in underlying `Data<Event>` row order via `BacktestProcessorState::advance` (`hftbacktest/src/backtest/mod.rs:655`). Order buses drain FIFO for a given receipt timestamp and enforce nondecreasing timestamps via clamping (`hftbacktest/src/backtest/order.rs:34`).

Look-ahead / mis-ordering risks:
- Same-timestamp exchange data vs order receipt: because `ExchData` precedes `ExchOrder`, an order received at exchange time `t` is evaluated after all exchange feed events with `exch_ts == t`. This can make fills/acks depend on the post-update book for `t` (e.g., `ack_new` reads `self.depth.best_*` in `hftbacktest/src/backtest/proc/nopartialfillexchange.rs:314`), which is a within-timestamp look-ahead if you intended “order before some updates at t”.
- Same-timestamp local data vs local order response: `LocalData` precedes `LocalOrder`, fixing an arrival order for simultaneous local feed + order responses.
- The loop compares `local_ts` and `exch_ts` directly (see `Processor::event_seen_timestamp` contract in `hftbacktest/src/backtest/proc/mod.rs:129`). If those fields aren’t on a single comparable time axis (or if any `local_ts <= exch_ts`), the engine can process local observations “before” exchange events, which is true look-ahead.

Timestamp handling notes:
- Time units must match between feed timestamps and latency models; docs recommend nanoseconds (`hftbacktest/src/backtest/models/latency.rs:36`).
- Feed preprocessing enforces `local_ts > exch_ts` only when a latency offset is applied (`hftbacktest/src/backtest/data/reader.rs:466`); otherwise equality/violations are not rejected. No monotonicity checks exist for per-processor seen timestamps.
- `goto_end()` can leave `cur_ts` unchanged because the `EndOfData` return path doesn’t update it (`hftbacktest/src/backtest/mod.rs:740`, `hftbacktest/src/backtest/mod.rs:858`).

Mitigations (concrete):
- Make an explicit, configurable same-timestamp policy (at minimum: choose between `ExchData↔ExchOrder` ordering; optionally also `LocalData↔LocalOrder`, and cross-asset ordering). Consider phase-based processing per timestamp.
- Add optional validation hooks: assert `local_ts > exch_ts` where appropriate; assert nondecreasing processed timestamps per processor/global; log tie frequency and resolved order.
- Add unit tests with synthetic data + deterministic latencies to lock down ordering and catch look-ahead: (1) same-timestamp trade/update + order receipt, (2) response latency 0 generating LocalOrder at same timestamp, (3) multi-asset tie ordering invariance.

Optional check:
- Ran `CARGO_TARGET_DIR=.ph_target/agent_01 cargo test -p hftbacktest --lib`: 22/22 tests passed (warnings in live module and npy parser).
~~~~
#
## agent_02_fill_and_queue_realism (status: needs_attention)
#
- final.json: `/Users/simongu/ParallelHassnes/runs/batch_20260120T091958Z_4dc3d953/agent_02_fill_and_queue_realism/steps/run/attempts/20260120T102818Z_run_20260120T102818Z_91d9b870/final.json`
#
~~~~text
Scope: Audited backtest-only fill/queue path in `hftbacktest/src/backtest/**`, `hftbacktest/src/depth/**`, and event/types definitions.
Note: an initial symbol search matched strings in `hftbacktest/src/live/**` and `hftbacktest/examples/**`; no deeper review of those paths was performed.

1) Queue/Fill Mechanics (what the code actually does)
- L2 local: `Local` maintains its own depth view at `local_ts`, buffers last trades, and sends order requests via `LocalToExch` (latency model decides entry/response). (`hftbacktest/src/backtest/proc/local.rs:27`, `hftbacktest/src/backtest/order.rs:114`)
- L2 exchange: `NoPartialFillExchange` and `PartialFillExchange` maintain an exchange-side depth at `exch_ts`, store active orders in `HashMap<OrderId, Order>` plus per-price `HashSet<OrderId>`, and use a `QueueModel` per order for queue position. (`hftbacktest/src/backtest/proc/nopartialfillexchange.rs:64`, `hftbacktest/src/backtest/proc/partialfillexchange.rs:78`)
  - Resting limit accept: initialize queue position from displayed qty at that tick (`QueueModel::new_order`). (`hftbacktest/src/backtest/models/queue.rs:67`, `hftbacktest/src/backtest/proc/nopartialfillexchange.rs:347`, `hftbacktest/src/backtest/proc/partialfillexchange.rs:479`)
  - Fills on trades: for same-price trades, decrement queue-ahead via `QueueModel::trade` then fill if `QueueModel::is_filled()>0`; for better-than-trade price, fill immediately. (`hftbacktest/src/backtest/proc/nopartialfillexchange.rs:114`, `hftbacktest/src/backtest/proc/partialfillexchange.rs:128`)
  - Queue updates on depth qty changes at the same tick via `QueueModel::depth`. (`hftbacktest/src/backtest/proc/nopartialfillexchange.rs:218`, `hftbacktest/src/backtest/proc/partialfillexchange.rs:267`)
  - Crossing fills: when the *best* moves (bid up / ask down), fill all opposite-side orders that become marketable by price alone. (`hftbacktest/src/backtest/proc/nopartialfillexchange.rs:242`, `hftbacktest/src/backtest/proc/partialfillexchange.rs:291`)
  - Cancel: remove immediately at exchange receipt time if found, else reject. (`hftbacktest/src/backtest/proc/nopartialfillexchange.rs:436`, `hftbacktest/src/backtest/proc/partialfillexchange.rs:654`)
  - Modify: if price changed or qty increased, do cancel+new (queue reset); else update qty/leaves in place (queue retained). (`hftbacktest/src/backtest/proc/nopartialfillexchange.rs:468`, `hftbacktest/src/backtest/proc/partialfillexchange.rs:686`)
- L2 queue models:
  - `RiskAdverseQueueModel`: stores `front_q_qty` as displayed qty at submission, decremented only by trades; `is_filled` converts negative `front_q_qty` to integer-lot fills via `.round()`. (`hftbacktest/src/backtest/models/queue.rs:42`, `hftbacktest/src/backtest/models/queue.rs:86`)
  - `ProbQueueModel`: also advances on depth decreases using a probability function; subtracts `cum_trade_qty` to avoid double counting; also uses `.round()` in `is_filled`. (`hftbacktest/src/backtest/models/queue.rs:130`, `hftbacktest/src/backtest/models/queue.rs:181`, `hftbacktest/src/backtest/models/queue.rs:207`)
- L3 exchange: `L3NoPartialFillExchange` + `L3FIFOQueueModel` maintain explicit FIFO queues per price from MBO add/modify/cancel events; backtest orders fill (fully) when (a) best crosses them or (b) a market-feed order behind them has a fill event. (`hftbacktest/src/backtest/proc/l3_nopartialfillexchange.rs:42`, `hftbacktest/src/backtest/models/queue.rs:512`, `hftbacktest/src/backtest/models/queue.rs:974`)
- Order transport realism: `OrderBus::append()` enforces non-decreasing timestamps (no request/response reordering), explicitly “assumed to be in order”. (`hftbacktest/src/backtest/order.rs:25`)

2) Materially Optimistic Assumptions (will systematically overestimate edge)
- “Touch/cross fills” ignore queue priority at best: L2 crossing handlers fill *all* your orders at prices that become marketable when best bid/ask moves, regardless of whether you’re at the front at that tick (and without requiring trade/volume evidence). This can heavily overstate maker fill probability around spread compression. (`hftbacktest/src/backtest/proc/nopartialfillexchange.rs:242`, `hftbacktest/src/backtest/proc/partialfillexchange.rs:339`)
- Liquidity-taking fills ignore self-impact / liquidity constraints:
  - `NoPartialFillExchange` always fully fills marketable orders at best regardless of book qty. (`hftbacktest/src/backtest/proc/nopartialfillexchange.rs:314`)
  - `PartialFillExchange` executes taker legs using book qty but does not consume the book; for GTC marketable limits it force-fills any remainder at the limit price (effectively assuming liquidity exists even if not displayed). (`hftbacktest/src/backtest/proc/partialfillexchange.rs:466`, `hftbacktest/src/backtest/proc/partialfillexchange.rs:594`)
- Multiple orders at the same price are not FIFO’d (and can double-count volume): active orders at a tick are tracked as a `HashSet<OrderId>`, and each order’s queue position is decremented by the full trade qty / depth change, independently. If a strategy can place multiple orders at one tick, it can get unrealistically many fills from the same market volume. (`hftbacktest/src/backtest/proc/partialfillexchange.rs:89`, `hftbacktest/src/backtest/proc/nopartialfillexchange.rs:75`)
- Queue fill threshold uses `round()` on fractional queue positions: both queue models convert `-front_q_qty/lot_size` via `.round()`; with probabilistic updates producing fractional `front_q_qty`, this can create early fills (crossing slightly negative). `ProbQueueModel` also only guards `prob.is_infinite()`, not NaN. (`hftbacktest/src/backtest/models/queue.rs:88`, `hftbacktest/src/backtest/models/queue.rs:198`)
- L3 fill semantics assume “fill event ≠ delete” (and no partials): `L3NoPartialFillExchange` calls `fill_market_feed_order::<false>` only; it does not update depth on fill events and assumes later cancel/modify events reconcile. Feeds where fill implies delete (or partial qty reduction via fill) can leave stale depth/queue and distort inferred fills. (`hftbacktest/src/backtest/proc/l3_nopartialfillexchange.rs:393`, `hftbacktest/src/backtest/models/queue.rs:974`)
- No adverse selection / picked-off modeling: maker fills occur at your limit price with no systematic penalty tied to subsequent price movement; this commonly overstates passive HFT. (structural gap, not a single line)

3) Invariants & Edge Cases (can break tracking or bias results)
- L2 depth clears reset the book but do not expire/reinit active orders; queue state can become stale across clears, and new orders placed when depth is empty get `front_q_qty=0` (instant front). L3 explicitly expires backtest orders on clear, so L2 vs L3 behavior diverges. (`hftbacktest/src/backtest/proc/nopartialfillexchange.rs:529`, `hftbacktest/src/backtest/models/queue.rs:67`, `hftbacktest/src/backtest/models/queue.rs:1079`)
- L3 FIFO queue model relies on internal map↔queue consistency and uses `unwrap()`/`unreachable!()` in cancel/modify/fill paths; messy or inconsistent MBO feeds (duplicate cancels, missing deletes, unexpected fill semantics) can panic rather than returning recoverable errors. (`hftbacktest/src/backtest/models/queue.rs:703`, `hftbacktest/src/backtest/models/queue.rs:1008`)
- Depth implementation auto-repairs crossed best quotes by shifting the opposite best tick (without removing crossed levels); this can mask inconsistent states and interact oddly with “crossing fill” logic. (`hftbacktest/src/depth/hashmapmarketdepth.rs:124`)

4) Realism Knobs (parameterize to reduce optimism)
- `crossing_fill_mode`: disabled | trade-confirmed | probabilistic; avoid unconditional fills on best-tick updates.
- `self_impact_mode`: consume book for taker orders (and optionally for maker fills) to prevent reusing the same displayed liquidity; optionally add slippage/impact curve.
- `maker_adverse_selection_penalty`: ticks/bps (possibly conditional on short-horizon mid move) to reflect being picked off.
- `queue_priority_model`: choose `RiskAdverse` vs calibrated probabilistic vs L3 FIFO; add `queue_position_error`/`queue_jump_prob` for L2 uncertainty/hidden liquidity.
- `latency_jitter` (+ optional limited reordering): current `OrderBus` enforces in-order delivery. (`hftbacktest/src/backtest/order.rs:25`)
- L3 feed semantics knobs: `fill_event_implies_delete` and optional partial-fill-from-event-qty support.

5) Minimal Tests to Validate Realism
- L2 “double-count” regression: two backtest orders at same tick + one trade => at most one order can fill given available volume.
- L2 depth-clear behavior: clear book with active orders => deterministic policy (expire orders OR reset queue positions); assert no NaNs in `order.q`.
- QueueModel properties: `is_filled()` never negative/NaN and never exceeds cumulative executed volume; avoid `.round()`-based early fills.
- L3 sequencing: add→fill→cancel consistency; and a separate test for “fill implies delete” feeds when enabled.

Bottom line: Useful for coarse research, but current defaults can materially overestimate HFT edge (especially top-of-book maker fills and aggressive taker logic) unless you add conservative knobs (disable unconditional crossing fills, add self-impact, and calibrate queue + adverse selection).
~~~~
#
## agent_03_latency_fee_pnl_accounting (status: needs_attention)
#
- final.json: `/Users/simongu/ParallelHassnes/runs/batch_20260120T091958Z_4dc3d953/agent_03_latency_fee_pnl_accounting/steps/run/attempts/20260120T102818Z_run_20260120T102818Z_191e81b4/final.json`
#
~~~~text
Latency modeling (RESEARCH/BACKTEST):
- Feed events carry both `exch_ts` and `local_ts`; processors see them at different times via `Processor::event_seen_timestamp` (local uses `local_ts` when `LOCAL_EVENT`, exchange uses `exch_ts` when `EXCH_EVENT`). The backtest event loop (`EventSet` in `Backtest::goto`) interleaves `LocalData/LocalOrder/ExchData/ExchOrder` strictly by their chosen “seen” timestamps.
- Order latency is modeled via `LatencyModel::{entry,response}` and the order buses: `LocalToExch::request` schedules exchange receipt at `order.local_timestamp + entry_latency`; `ExchToLocal::respond` schedules local receipt at `order.exch_timestamp + response_latency`.
- `OrderBus::append` enforces non-decreasing timestamps (FIFO/in-order assumption). This can mask real-world request/response reordering (could be optimistic or pessimistic depending on strategy/cancel/replace patterns).
- Latency offsets: feed uses `FeedLatencyAdjustment` (adds `latency_offset` to `local_ts` only; rejects if `local_ts <= exch_ts`). Order-latency history uses `OrderLatencyAdjustment` (adds `latency_offset` to `exch_ts` and `2*latency_offset` to `resp_ts`, leaving `req_ts` unchanged), consistent with adding one-way delay in both directions.

Fee modeling:
- Fee is applied only on fills via `State::apply_fill` using `fee_model.amount(order, amount)`. Implementations are simple linear models: `TradingValueFeeModel`, `TradingQtyFeeModel`, and `FlatPerTradeFeeModel` (maker/taker via `order.maker`; optional directional buyer/seller add-ons).
- No rounding, minimum fee, fee currency conversion, or per-fill vs per-order aggregation logic beyond “each `apply_fill` increments fee/volume/trades”. This can be optimistic vs venues with rounding/min-fees.

PnL / state accounting consistency findings:
1) **Severe accounting mismatch for `PartialFillExchange`: local state only updates on `Status::Filled`.**
   - `PartialFillExchange::fill` calls `self.state.apply_fill(order)` for every fill (including partials) and may emit responses with `Status::PartiallyFilled` (or `Expired` after partial IOC). But `Local::process_recv_order_` (and `L3Local`) only call `apply_fill` when `order.status == Status::Filled`.
   - Consequences in backtest results (local is the authoritative state exposed via `Bot::state_values`):
     - Partial fills with `Status::PartiallyFilled` are not reflected in `position/balance/fee/num_trades/trading_*`.
     - IOC partial-fill-then-expire: exchange applies fills, but local sees final `Status::Expired` and applies **no** fills.
     - Multi-level taker executions in `ack_new` use `fill::<false>` repeatedly (no per-fill response), then send only one final response; local can only apply a single fill (and only for the last `exec_qty/exec_price_tick`), losing earlier executions.
   - This is internally inconsistent and can be subtly optimistic (understated fees/exposure; strategy logic sees incorrect `position()` during the run).

2) **Rejected modify rollback bug (local/exchange state divergence).**
   - For request rejections generated by negative entry latency (handled in `LocalToExch::request`), the rejection response can carry reverted fields (e.g., `modify`’s reject closure restores old `price_tick/qty`).
   - However, `Local::process_recv_order_` and `L3Local::process_recv_order` skip `local_order.update(&order)` when `order.req == Status::Rejected` and only clear `req`/maybe set `Expired` for `New` orders. For a rejected **modify**, the local order keeps the modified `price_tick/qty` even though the modification never reached the exchange.
   - This breaks “local orders reflect exchange reality” and can bias strategy behavior.

3) Timestamp semantics/diagnostics caveat:
- `Local::{last_order_latency, order_latency()}` is updated for any received order with `exch_timestamp > 0` (including fills), but stores `(order.local_timestamp, order.exch_timestamp, receipt_ts)`. For fills, `order.local_timestamp` is the last request time, not a “fill request time”, so this tuple can be misleading if strategies interpret it as an order-RTT metric for the fill itself.

Suggested tests to catch drift/inconsistency (no source changes made):
- Partial-fill accounting:
  - Synthetic data + `PartialFillExchange` where a single order is filled in 2+ chunks (including an IOC that partially fills then expires). Assert local `StateValues.position/balance/fee/num_trades` equal the sum of executions.
  - Market/limit-cross execution across multiple price levels: assert local balance equals Σ(price_i * qty_i * contract_size * side_sign) and fee equals Σ(fee_model(amount_i)).
- Local-vs-exchange state convergence:
  - In a unit test inside `backtest::mod` (like existing tests), run `goto_end()` and assert local `state_values` equals exchange processor’s internal `StateValues` (they should match after all events/responses are processed) for both exchange kinds.
- Rejected modify rollback:
  - Use `ConstantLatency` with negative `entry_latency` to force local-side rejection. Place a live order, call `modify`, advance until rejection is received, and assert local order’s `price_tick/qty` reverted and `req==None`.
- Equity invariants:
  - Linear asset, zero price movement: after a buy then sell at same price, equity change equals `-fees` (no money creation). For maker rebates (negative maker fee), equity increases only by the rebate amount.
- End-of-data flush:
  - Construct feed ending at T but with an order response scheduled at T+Δ via response latency; call `goto_end()` and assert the response is processed and state updates occur.
~~~~
#
## agent_04_depth_models_invariants (status: needs_attention)
#
- final.json: `/Users/simongu/ParallelHassnes/runs/batch_20260120T091958Z_4dc3d953/agent_04_depth_models_invariants/steps/run/attempts/20260120T102818Z_run_20260120T102818Z_2b1d3cd1/final.json`
#
~~~~text
Scope: research/backtest only (did not open `examples/`, `collector/`, `connector/`, `py-hftbacktest/`, or `hftbacktest/src/live/**`).

Depth implementations relevant to backtests:
- `HashMapMarketDepth` (L2+L3; `ApplySnapshot` + `snapshot()` implemented)
- `BTreeMarketDepth` (L2+L3; `ApplySnapshot::snapshot()` is `todo!()`)
- `ROIVectorMarketDepth` (L2+L3 within ROI; `ApplySnapshot::snapshot()` is `unimplemented!()`)
- `FusedHashMapMarketDepth` (implements `MarketDepth` + `ApplySnapshot`, but *cannot* be used as a backtest depth type because it does not implement `L2MarketDepth` or `L3MarketDepth`)

Assumed invariants / semantics in the backtest paths:
- `best_bid_tick == INVALID_MIN` means “no bid”; `best_ask_tick == INVALID_MAX` means “no ask”.
- Feed `px`/`qty` are finite; `qty >= 0`.
- Tick/lot mapping uses rounding: `price_tick = round(px/tick_size)`, `qty_lot = round(qty/lot_size)`; level is removed when `qty_lot == 0`.
- L2 depth updates are treated as absolute qty-at-level (not delta updates).
- Backtest processors treat `*_DEPTH_SNAPSHOT_EVENT` exactly like `*_DEPTH_EVENT`; “snapshot replaces book” semantics require an explicit clear event stream.
- Trade events *do not mutate depth* in processors; they only trigger “check if my order filled” logic, so realism requires depth updates consistent with trades.
- Strategy-sensitive quantities (mid/spread, marketability, crossing fills, FOK/IOC depth-walk) depend directly on `best_bid_tick()` / `best_ask_tick()` and `qty_at_tick()` behaving sanely.

Realism / invariant violations that can bias mid/spread/BBO stability and fills:
1) CRITICAL: `HashMapMarketDepth::clear_depth` off-by-one best recomputation for finite clears.
   - buy side: `depth_below(..., clear_upto - 1, ...)` skips the valid `clear_upto-1` candidate.
   - sell side: `depth_above(..., clear_upto + 1, ...)` skips the valid `clear_upto+1` candidate.
   This can set BBO to `INVALID_MIN/INVALID_MAX` even when depth remains (e.g., clearing only the best level), corrupting spread/mid and order marketability when `DEPTH_CLEAR` events exist. (`hftbacktest/src/depth/hashmapmarketdepth.rs:196`)
2) CRITICAL: `ROIVectorMarketDepth::clear_depth` sell-side range uses `roi_ub` in index math (`clear_upto + 1 - roi_ub`) and also has the same off-by-one best recomputation pattern. This likely makes ask clears no-op / incorrect, leaving stale asks and wrong best ask. (`hftbacktest/src/depth/roivectormarketdepth.rs:314`)
3) CRITICAL: `BTreeMarketDepth` L2 updates violate the `L2MarketDepth` contract: `prev_qty` returned is the previous BEST level’s qty, not the previous qty at `price_tick`. Exchange processors pass that `prev_qty` into queue models (`on_bid_qty_chg/on_ask_qty_chg`), biasing queue position and fills. (`hftbacktest/src/depth/btreemarketdepth.rs:65`)
4) HIGH: `BTreeMarketDepth` L3 `delete_order` recomputes best bid with `.keys().next()` (min) when deleting the best bid, instead of `.last()` (max). With multiple bid levels, deleting the best can make best bid jump to the *lowest* remaining tick, distorting mid/spread and marketability. (`hftbacktest/src/depth/btreemarketdepth.rs:311`)
5) HIGH: `BTreeMarketDepth` does not prevent crossed books (explicitly warned in its doc comment). If best bid >= best ask occurs, backtest marketability/crossing-fill logic can become unrealistically generous and mid/spread can become nonsensical.
6) MEDIUM: `ROIVectorMarketDepth` ignores updates outside ROI and returns `NaN` for qty queries outside ROI (and `best_ask_qty()` returns `NaN` when BBO is outside ROI). These NaNs can poison queue models and depth-walk logic (often resulting in “never fills” or spurious expiries), and ignoring out-of-ROI updates can make BBO become `INVALID_*` unless ROI always contains top-of-book.
7) MEDIUM: `ApplySnapshot::snapshot()` is not implemented for `BTreeMarketDepth` and `ROIVectorMarketDepth` (panics if called), so snapshot round-trips/exporting aren’t safe.
8) NOTE: Backtest processors don’t handle `DEPTH_BBO_EVENT`; feeds must encode BBO changes as depth events.

Minimal regression tests to lock invariants (per depth type):
- `HashMapMarketDepth`:
  - L2 clear buy inclusive: bids at ticks 100 and 99, call `clear_depth(Buy, 100*tick_size)`, expect `best_bid_tick()==99` and qty@99 preserved.
  - L2 clear sell inclusive: asks at ticks 100 and 101, call `clear_depth(Sell, 100*tick_size)`, expect `best_ask_tick()==101` and qty@101 preserved.
- `BTreeMarketDepth`:
  - L2 `prev_qty` semantics: update bid at 100 then update bid at 99; second call should return `prev_qty==0` (qty previously at 99), not qty at best.
  - L3 best-bid recompute: add buy orders at ticks 100/99/98, delete the 100 order; expect `best_bid_tick()==99`.
- `ROIVectorMarketDepth`:
  - Sell clear indexing with non-zero ROI lower bound: choose ROI e.g. 50..150, set asks at ticks 100 & 101, clear upto 100; expect best ask 101 and level 100 cleared (catches the `roi_ub`-index bug + off-by-one).
  - Decide + lock out-of-ROI query semantics (prefer finite `0.0` over `NaN` if using with queue/fill models).
- `ApplySnapshot` (where supported):
  - apply_snapshot -> snapshot -> apply_snapshot round-trip preserves BBO and per-level qty on a small synthetic snapshot.

(Existing unit tests in depth files primarily cover L3 add/delete/modify; they do not cover L2 `clear_depth` or L2 `prev_qty` semantics, which is why the issues above can slip through.)
~~~~
#
## agent_05_data_ingest_and_timestamp_semantics (status: needs_attention)
#
- final.json: `/Users/simongu/ParallelHassnes/runs/batch_20260120T091958Z_4dc3d953/agent_05_data_ingest_and_timestamp_semantics/steps/run/attempts/20260120T102818Z_run_20260120T102818Z_95c64ec1/final.json`
#
~~~~text
Audited `.npy/.npz` ingestion (`read_npy`, `read_npy_file`, `read_npz_file`), header parsing, and `Data/DataPtr` casting, plus timestamp adjustment codepaths used in backtesting.

Key findings (panic/hang risk)
- Truncated/malformed inputs can panic due to unchecked slicing and `unwrap()`s in `read_npy` (`hftbacktest/src/backtest/data/npy/mod.rs:241-262`). In backtest, `Reader` loads in a spawned thread; a panic there can deadlock the run because the cache entry never becomes “ready” and `rx.recv()` blocks forever.
- `read_npy` read loop can spin forever if `Read::read()` returns `0` before `size` bytes are read (`hftbacktest/src/backtest/data/npy/mod.rs:241-244`). This affects both plain `.npy` and `.npz` (truncated zip entry).
- Empty files (`size == 0`) will panic at allocation time (`DataPtr::new(0)` -> `AlignedArray::new` panics): `hftbacktest/src/backtest/data/mod.rs:145-151`, `hftbacktest/src/utils/aligned.rs:34-43`.
- Additional backtest-mode panics: generated `build_asset!` code uses `.unwrap()` for building `Reader` and loading initial snapshots (`hftbacktest-derive/src/lib.rs:262-306`); `IntpOrderLatency` calls `next_data().unwrap()` inside its runtime loop (`hftbacktest/src/backtest/models/latency.rs:266`).

Key findings (silent corruption / misinterpretation)
- Field-name mismatch is only a `println!("Warning")` and ingestion proceeds (`hftbacktest/src/backtest/data/npy/mod.rs:270-278`). If columns are swapped/renamed but retain types, semantics silently change.
- Field-count mismatches are ignored: `check_field_consistency()` uses `zip()` and never checks lengths (`hftbacktest/src/backtest/data/npy/mod.rs:148-168`). Missing/extra fields (including padding-like fields) can be accepted.
- Missing `descr` in the header is effectively accepted: `NpyHeader::from_header` defaults `descr = []` if absent (`hftbacktest/src/backtest/data/npy/mod.rs:62-120`), and the `zip()` behavior above can produce an “OK diff,” leading to reinterpretation of arbitrary payload bytes as `D`.
- `shape[0]` is not validated against payload length. Actual row count is inferred from `(file_size - offset) / size_of::<D>()` (`hftbacktest/src/backtest/data/mod.rs:42-45`), so a lying `shape` doesn’t error. Trailing non-multiple bytes are silently ignored; trailing full-record garbage becomes extra rows.
- `Data::is_empty()` checks raw buffer length, not logical record count (`hftbacktest/src/backtest/data/mod.rs:49-51`). A valid `.npy` with `shape=(0,)` and no payload will be treated as “non-empty,” which can cause downstream panics when code assumes `data[0]` exists (notably in `IntpOrderLatency`).

Key findings (UB/safety hazards beyond “just robustness”)
- `.npy` I/O relies on an unsafe `POD` marker that does not guarantee “any bit-pattern valid” or “no padding/uninitialized bytes.” Yet ingestion creates `&D` by casting raw bytes (`hftbacktest/src/backtest/data/mod.rs:95-121`) and writing reads raw bytes from `T` (`hftbacktest/src/backtest/data/npy/mod.rs:360-418`). If a `POD` type contains `bool`, enums, pointers/references, or padding that can be uninitialized when constructed in Rust, this becomes UB.
- Alignment: the reader enforces `(10 + header_len) % 64 == 0` but does not assert `align_of::<D>() <= 64` or `data_offset % align_of::<D>() == 0` (`hftbacktest/src/backtest/data/npy/mod.rs:285-293`). A type `D` with alignment > 64 would be UB.
- Header parser string restrictions (`hftbacktest/src/backtest/data/npy/parser.rs:89-95`) accept only `[A-Za-z0-9_<>]` inside strings; valid NumPy dtype strings/field names that include other characters will fail parsing, and `read_npy` currently unwraps parse results -> panic.

Timestamp semantics / overflow & wrap
- `types::Event` defines `exch_ts` = exchange occurrence time and `local_ts` = local receipt time (`hftbacktest/src/types.rs:313-333`). Local processors act on `local_ts` for `LOCAL_EVENT` rows; exchange processors act on `exch_ts` for `EXCH_EVENT` rows (`hftbacktest/src/backtest/proc/local.rs:284-286`, `hftbacktest/src/backtest/proc/partialfillexchange.rs:743-745`).
- Unit assumption is documented (“nanoseconds recommended; must match dataset unit”) but not enforced; mixed units silently distort backtest timing (`hftbacktest/src/backtest/models/latency.rs:34-38`, `hftbacktest/src/types.rs:930-946`).
- Multiple unchecked timestamp additions can wrap in release builds:
  - Feed latency adjustment: `local_ts += latency_offset` (`hftbacktest/src/backtest/data/reader.rs:482-484`). A large negative offset can underflow and wrap to a huge positive, potentially passing the `local_ts > exch_ts` check and silently corrupting time.
  - Order latency adjustment: `exch_ts += offset; resp_ts += 2*offset` (`hftbacktest/src/backtest/models/latency.rs:289-292`) with no overflow checks or invariant revalidation.
  - Backtest time progression: `cur_ts + timeout/duration` (`hftbacktest/src/backtest/mod.rs:1099-1119`) without checked arithmetic.

Recommended strict validation rules (to prevent silent corruption; pipeline-friendly)
- I/O correctness: reject `size == 0`; require `size >= 10`; use `read_exact` (or loop treating `read()==0` as `UnexpectedEof`) to avoid infinite loops; add configurable maximum file/zip-entry size to prevent OOM/zip-bomb scenarios.
- Header bounds & parsing: require `10 + header_len <= size`; require header ends with `\n`; reject non-UTF8 headers; replace all `unwrap()`s in `read_npy` with `?` returning `InvalidData` containing filepath/context.
- Header completeness: require `descr`, `fortran_order`, and `shape` keys; treat missing/empty `descr` as hard error; for forward compatibility, consider ignoring unknown keys rather than erroring.
- DType/schema matching: in strict mode require exact match of field *count*, *order*, *names*, and *types*. Never accept name mismatches unless an explicit, user-supplied mapping is provided. Always check `expected_types.len() == found_types.len()` (no `zip()` truncation).
- Shape/payload matching: require 1-D; compute `payload_len = size - (10 + header_len)`; require `payload_len % size_of::<D>() == 0`; require `shape[0] == payload_len / size_of::<D>()`. If you want to permit trailing padding, require it be all-zero and ensure it never contributes to row count.
- Alignment: require `align_of::<D>() <= CACHE_LINE_SIZE` and `data_offset % align_of::<D>() == 0`. For compatibility with standard NumPy `.npy` (often 16-byte header padding), either (a) accept 16-aligned headers but copy payload into a fresh, properly aligned buffer before casting, or (b) clearly document/enforce the 64-byte alignment requirement as part of the data contract.
- Type safety: replace the custom `POD` bound for `.npy` I/O with a stricter trait that guarantees no padding + any-bit-pattern-valid, or explicitly forbid `bool`/enums/pointers and validate values before casting.
- Timestamp validation: validate monotonicity on processor-relevant timestamps, validate `local_ts >= exch_ts` where both are meaningful, and use `checked_add/checked_sub` for all timestamp arithmetic (offsets, `cur_ts + duration/timeout`), returning `InvalidData` on overflow instead of wrapping.
~~~~
