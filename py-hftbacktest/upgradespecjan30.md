
---

The north-star principle

Python should be a “configuration + orchestration + analysis” layer, and Rust should be the single source of truth for market/exchange physics. The only things Python should do that affect fills are (a) selecting which engine features are enabled (L2 vs L3, queue model, exchange model, CME policies), and (b) selecting which dataset format is being replayed (Databento MBO etc). Everything else (partial fill budgeting, top-of-book liquidity caps, trigger activation semantics, order not found policies, sequencing) must live in the Rust engine so results are reproducible and consistent across Python/Rust frontends.

In other words: Python glue is allowed to be “smart about wiring”, but never “smart about matching”.

---

What it should feel like for a user

The dream is: building a CME MBO backtest in Python feels as “native” as the Rust builder, and the “CME MBO mode” is a single explicit switch that turns on the correct set of engine policies. The strategy loop remains Numba-friendly, so you don’t lose speed.

A canonical example that should work cleanly:

```python
import hftbacktest as hbt
from hftbacktest.data.utils.databento import convert

# 1) Convert Databento MBO -> hftbacktest event format
convert(
    input_file="ESM4.mbo.dbn.zst",
    symbol="ESM4",
    output_filename="ESM4_2024-05-01.npz",
    base_latency=0,
    file_type="mbo",
)

# 2) Build an L3 CME MBO asset
asset = (
    hbt.L3Asset()
      .symbol("ESM4")
      .data("ESM4_2024-05-01.npz")
      .tick_size(0.25)
      .lot_size(1.0)
      .linear_asset(contract_size=50.0)
      .l3_fifo_queue_model()
      .cme_databento_mbo(True)          # the “one switch” for CME MBO physics
      .constant_latency(entry_latency=2000, resp_latency=2000)
      .trading_value_fee_model(maker_fee=-0.01, taker_fee=0.02)
      .last_trades_capacity(2000)
)

# 3) Backtest builder (this is where seq tie-break becomes real)
bt = (
    hbt.BacktestBuilder.hashmap()
      .add_asset(asset)
      .exch_order_equal_ts_policy(hbt.EXCH_EQUAL_TS_AFTER_DATA)  # optional but explicit
      .build()
)

# 4) Numba strategy loop, now with CME trigger orders available
from numba import njit

@njit
def run(bt):
    oid = 1
    while bt.elapse(1_000_000) == 0:
        d = bt.depth(0)
        mid = 0.5 * (d.best_bid + d.best_ask)

        # submit a server-side stop-market buy above mid
        bt.submit_stop_market(
            asset_no=0,
            order_id=oid,
            side=hbt.BUY,
            trigger_price=mid + 2.0,
            qty=1.0,
            time_in_force=hbt.GTC,
            wait=False
        )
        oid += 1

run(bt)
```

That one snippet encodes the core of your request: builder correctness, seq tie-break correctness, and order type exposure correctness.

---

Layering and package layout spec

Keep the Python distribution as one installable package, but internally make the layers explicit so teams don’t step on each other:

1. `hftbacktest._hftbacktest` (Rust extension, PyO3/maturin)
   This layer owns:

* Building assets/backtests (L2 & L3) by calling the real Rust builders.
* Exposing a stable C-ABI surface for Numba jitclasses (like you already do in `src/backtest.rs`, `src/depth.rs`, etc).
* Exposing “capabilities” and small query functions (e.g., order trigger params).

2. `hftbacktest.binding` (Numba jitclass wrappers)
   This layer owns:

* `HashMapMarketDepthBacktest_`, `ROIVectorMarketDepthBacktest_`
* `HashMapMarketDepth_`, `ROIVectorMarketDepth_`
* `Order_`, `OrderDict_`, `StateValues_`
* Thin wrappers around C functions; no policy logic.

3. `hftbacktest.builder` (Python-facing ergonomic builders)
   This layer owns:

* Friendly Python builder objects `L2Asset()`, `L3Asset()`, `BacktestBuilder.hashmap()`, `BacktestBuilder.roivec()`.
* Input validation and “nice errors”.
* Compatibility shims for the old `BacktestAsset()` API (deprecate slowly).

4. `hftbacktest.data` (conversion + validation)
   This layer owns:

* Databento MBO conversion (already exists and already sets `Event.ival = sequence` and uses nanosecond-safe epoch conversion).
* Validation utilities (already exists: `correct_event_order`, `validate_event_order`, etc).
* Optional: dataset metadata helpers (symbol, tick size, etc).

5. `hftbacktest.recorder` + `hftbacktest.stats`
   This layer owns:

* Time-series recording, equity computation, resampling, plotting, etc (already exists).

This separation matches the domain-vs-infrastructure split you’re trying to enforce more broadly. 

---

Core object model and API spec

A. Asset builders (Python)

You should have two explicit asset builders, mirroring Rust: `L2Asset` and `L3Asset`. No “magic L3 detection” based on queue model in Python; instead, the asset type determines which Rust builder is invoked.

Common methods (both L2 and L3):

* `.symbol(str)` (optional but extremely useful for debugging / metadata)
* `.data(path | np.ndarray | list[path] | list[np.ndarray])`
* `.initial_snapshot(path | np.ndarray)` (optional)
* `.tick_size(float)`, `.lot_size(float)`
* `.linear_asset(contract_size)`, `.inverse_asset(contract_size)`
* `.constant_latency(entry_latency, resp_latency)` / `.intp_order_latency(...)`
* `.latency_offset(ns)`
* `.parallel_load(bool)`
* `.trading_value_fee_model(maker_fee, taker_fee)` / `.trading_qty_fee_model(...)` / `.flat_per_trade_fee_model(...)`
* `.last_trades_capacity(int)`

L2-only methods:

* `.queue_model.<risk_adverse | log_prob | log_prob2 | power_prob | power_prob2 | power_prob3>(...)`
* `.exchange.<no_partial_fill | partial_fill>()`
* (Optional) `.roi_lb()/.roi_ub()` only if building ROIVector depth

L3-only methods:

* `.l3_fifo_queue_model()` (and in the future, other L3 queue models)
* `.exchange.<l3_no_partial_fill | l3_partial_fill>()` but with a hard rule: L3 partial fill is only enabled through CME MBO mode (see below)
* `.order_bus_max_timestamp_reordering(ns)` (expose the Rust knob; default 0)
* `.cme_mbo_order_not_found_reject_marks_inactive(bool)` (default false unless CME mode)
* `.cme_databento_mbo(bool)` convenience switch

B. The CME MBO “one switch” semantics

This is key: `asset.cme_databento_mbo(True)` must be declarative and must fully define the physics policy bundle.

When enabled, it must:

* Force `L3PartialFillExchange` as the exchange model.
* Force seq tie-break usage for that asset (so the backtest builder turns it on globally).
* Enable “reject marks inactive” policy for cancel/modify order-not-found rejects.
* Enable “partial fill budgeting” that uses `EXCH_FILL_EVENT` liquidity budgets and caps taker execution by top-of-book liquidity (the Rust side already documents this intent in the L3 asset builder).

This bundle must be the only supported entrypoint for L3PartialFillExchange from Python. Anything else should error loudly with a message like: “L3 PartialFillExchange requires cme_databento_mbo(True)”.

C. Backtest builder (Python)

Create a real Python `BacktestBuilder` with a stable surface:

* `BacktestBuilder.hashmap()` and `BacktestBuilder.roivec(roi_lb=None, roi_ub=None)` constructors.
* `.add_asset(asset)` (multiple allowed)
* `.exch_order_equal_ts_policy(policy)` where `policy` is one of:

  * `EXCH_EQUAL_TS_BEFORE_DATA` (default conservative)
  * `EXCH_EQUAL_TS_AFTER_DATA`
  * `EXCH_EQUAL_TS_RANDOM_SEEDED(seed)`
* `.build()` returns a Numba-jittable backtest instance (same as today: a jitclass wrapper around a pointer).

The “seq tie-break” rule is global: if any asset requires it (CME MBO L3 does), the engine must build with `use_seq_tie_break = True` so EventSet compares `(timestamp, seq)` where `seq = Event.ival`. That is exactly the behavior you need for Databento `sequence`.

Also: the builder must not call `Backtest::new(...)` anymore; it must call the Rust `Backtest::builder()` path so `use_seq_tie_break` is propagated correctly.

---

The three concrete integration requirements you asked for

1. Builder correctness (L3 CME MBO)

Rust side (py-hftbacktest extension):

* Stop using the `build_asset!` derive macro for L3 assets entirely. The macro currently hardwires L3 to `L3NoPartialFillExchange` and blocks L3PartialFillExchange (you even have an explicit “unsupported” guard), which is exactly what’s preventing CME MBO from working.
* Instead, for L3 assets, directly invoke `hftbacktest::backtest::L3AssetBuilder` and wire through:

  * queue model (L3 FIFO)
  * depth builder (HashMap depth or ROI vector depth)
  * latency model
  * fee model
  * CME MBO switches
  * order_bus_max_timestamp_reordering

Python side:

* Provide `L3Asset()` builder that maps 1:1 onto the Rust L3AssetBuilder semantics.

2. Seq tie-break correctness

Engine spec:

* Seq tie-break is enabled at Backtest construction time, not per-step.
* When enabled, the engine must compare events by `(timestamp, seq)` where `seq = Event.ival` (already how EventSet works).
* Databento MBO conversion must keep writing `sequence` into `Event.ival` (you already do this), and conversion must preserve nanosecond precision (you already use Polars epoch ns conversion to avoid Python datetime truncation).

Python usability spec:

* Provide a visible “mode” for seq tie-break:

  * `bt.uses_seq_tie_break -> bool` (read-only property, queried via FFI)
  * `asset.requires_seq_tie_break -> bool` (debug only)
    This makes it obvious to the user when they’re running a mode that depends on ival.

Validation spec (optional but valuable):

* `hftbacktest.data.validation.validate_seq_monotonic(data)` for CME MBO: inside each `(exch_ts)` group, sequence must be non-decreasing (or at least stable), and you should warn if too many equal seq keys exist.

3. Order types exposure (CME L3)

The engine already has server-side trigger order support for L3 (Stop-Market, Stop-Limit, MIT) via LocalProcessor defaults overridden by L3Local. The missing part is Python exposure.

Rust FFI surface additions:

* Add C-ABI functions analogous to `hashmapbt_submit_buy_order`, but for triggers:

  * `*_submit_stop_market(hbt_ptr, asset_no, order_id, side, trigger_price, qty, tif, wait)`
  * `*_submit_stop_limit(hbt_ptr, asset_no, order_id, side, trigger_price, limit_price, qty, tif, wait)`
  * `*_submit_mit(hbt_ptr, asset_no, order_id, side, trigger_price, qty, tif, wait)`
  * `*_modify_stop_limit(hbt_ptr, asset_no, order_id, trigger_price, limit_price, qty, wait)`
* Keep return code semantics identical to existing submit/modify/cancel calls.

Numba wrapper additions (`binding.py`):

* Add methods on both HashMap and ROIVec backtest jitclasses:

  * `submit_stop_market(...)`
  * `submit_stop_limit(...)`
  * `submit_mit(...)`
  * `modify_stop_limit(...)`
* Add module-level constants for trigger kinds (Numba-friendly ints), even if the API uses explicit methods. This helps logging and analysis.

Order inspection exposure:

* Add an FFI query function that can downcast the Rust `Order.q` to trigger params when present:

  * `order_trigger_params(order_ptr, out_kind_u8, out_trigger_tick_i64) -> bool`
* Then add Python-side properties:

  * `Order.is_trigger -> bool`
  * `Order.trigger_kind -> uint8`  (STOP_MARKET / STOP_LIMIT / MIT)
  * `Order.trigger_price -> float` (computed from trigger_tick * tick_size)
    For Stop-Limit, `Order.price` remains the limit price, and `Order.trigger_price` is exposed separately.

This matches the “OrderView includes exec_qty, exec_price, req, price_tick” idea you already converged on for maintainability and correctness, except adapted to the Python binding layer. 

---

Determinism and ordering spec (make it explicit)

You want this written down because CME MBO correctness lives or dies here.

1. Event timestamps

* `Event.exch_ts` and `Event.local_ts` must be non-decreasing within their own event streams after conversion+correction (you already validate this for EXCH_EVENT / LOCAL_EVENT subsets).
* `Event.ival` is the sequence tie-break key when seq tie-break is enabled.

2. What seq tie-break means

* When multiple event sources have equal timestamps (local feed, exchange feed, local order bus, exchange order bus), and seq tie-break is enabled, the next event is the one with the smallest `Event.ival` among tied timestamps.
* If seq tie-break is disabled, ties are resolved by the engine’s stable scanning order (less desirable for CME MBO).

3. Exchange order vs exchange data at equal timestamps

* This is not a “tie-break”; it’s a policy decision (BeforeExchData vs AfterExchData vs RandomSeeded) and must be configured (default conservative).
* Expose this policy knob in Python builder so research can test sensitivity.

4. CME MBO “batch/packet” boundary

* Databento `sequence` should represent a vendor packet/batch boundary (you already store it).
* Trigger activation logic (stop/MIT) must respect the batch boundary; Python doesn’t implement this, but Python must ensure the engine is in seq tie-break mode so the engine sees the intended ordering.

---

Artifact + experimentation spec (so the stack supports “experimenting + bookkeeping”)

You said the crate is intended for experimenting/bookkeeping, and you’re open to moving glue/reporting into Python. Here’s the dream experiment output contract for Python runs, which avoids duplicating logic across repos:

Every run writes a directory with:

* `config.json` (full resolved config: asset params, builder policy, git SHA, dataset hashes)
* `equity_<asset>.npz` (Recorder output per asset)
* `orders.parquet` or `orders.jsonl` (order lifecycle log: submit/ack/fill/cancel/reject, with trigger fields)
* `trades.parquet` or `trades.jsonl` (fills)
* `summary.json` (key metrics: SR/Sortino/MaxDD/Return, plus CME-mode knobs)

And then the analysis layer (`hftbacktest.stats`) reads this without needing any engine context.

This keeps the lab bookkeeping where it belongs (Python), and keeps the engine where it belongs (Rust).

---

Backwards compatibility plan (so you don’t break everyone overnight)

You can do this without a big-bang migration:

Phase 1: Keep `BacktestAsset()` but re-implement build paths

* Internally, `BacktestAsset` becomes a thin compatibility wrapper that produces either an L2Asset or L3Asset spec.
* Building uses BacktestBuilder (Rust side) always, so seq tie-break works.
* Existing L2 workflows keep working.

Phase 2: Introduce explicit `L2Asset()` / `L3Asset()` and document them as preferred

* Mark `BacktestAsset` as “legacy, will be removed in v3”.

Phase 3: Remove the macro-based L3 build path entirely

* No L3 behavior hidden behind queue model string matching; everything goes through the real Rust builders.

---

Parallelizable work packages (so multiple coders can move fast without collisions)

If you want to split this across people cleanly, here’s the natural split:

1. Rust extension builder work

* Implement L3AssetBuilder wiring and BacktestBuilder wiring
* Expose new builder knobs (CME MBO, seq tie-break propagation, exch_equal_ts_policy)
* Add FFI query `bt_uses_seq_tie_break()`

2. Rust FFI for trigger orders

* Add C ABI submit/modify methods for stop/MIT family
* Add order trigger introspection function

3. Python binding layer (Numba)

* Add jitclass methods
* Add constants / enums (Numba-friendly)
* Add `Order.trigger_*` properties

4. Python builder ergonomics

* Add `L2Asset`, `L3Asset`, `BacktestBuilder`
* Keep `BacktestAsset` as wrapper/deprecation shim

5. Tests + golden replays

* A deterministic CME MBO mini-fixture (tiny synthetic dataset) that asserts:

  * seq tie-break is enabled
  * stop/MIT triggers do not activate mid-batch
  * partial fill budgeting behaves as expected
* A Python test that proves the builder no longer errors on L3 FIFO + CME MBO partial fill exchange.

