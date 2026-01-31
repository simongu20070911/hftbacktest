use std::{ffi::c_void, mem::size_of, ptr::slice_from_raw_parts_mut};

pub use backtest::*;
pub use depth::*;
pub use fuse::*;
#[cfg(feature = "live")]
use hftbacktest::live::{Instrument, LiveBotBuilder};
use hftbacktest::{
    backtest::{
        Asset,
        Backtest,
        DataSource,
        assettype::{InverseAsset, LinearAsset},
        data::{Data, DataPtr, FeedLatencyAdjustment, Reader, read_npz_file},
        ExchangeKind as CoreExchangeKind,
        ExchOrderEqualTsPolicy,
        L3AssetBuilder,
        models::{
            CommonFees,
            ConstantLatency,
            FlatPerTradeFeeModel,
            IntpOrderLatency,
            L3FIFOQueueModel,
            LogProbQueueFunc,
            LogProbQueueFunc2,
            OrderLatencyRow,
            PowerProbQueueFunc,
            PowerProbQueueFunc2,
            PowerProbQueueFunc3,
            ProbQueueModel,
            RiskAdverseQueueModel,
            TradingQtyFeeModel,
            TradingValueFeeModel,
        },
        order::order_bus,
        proc::{
            L3Local,
            L3NoPartialFillExchange,
            Local,
            LocalProcessor,
            NoPartialFillExchange,
            PartialFillExchange,
            Processor,
        },
        state::State,
    },
    prelude::{ApplySnapshot, Event, HashMapMarketDepth, ROIVectorMarketDepth},
    types::BuildError,
};
use hftbacktest_derive::build_asset;
pub use order::*;
use pyo3::{
    PyTypeInfo,
    exceptions::{PyDeprecationWarning, PyValueError},
    ffi::c_str,
    prelude::*,
};

#[cfg(feature = "live")]
use crate::live::{HashMapMarketDepthLiveBot, ROIVectorMarketDepthLiveBot};

mod backtest;
mod depth;
mod fuse;
#[cfg(feature = "live")]
mod live;
mod order;

#[derive(Clone)]
pub enum AssetType {
    LinearAsset { contract_size: f64 },
    InverseAsset { contract_size: f64 },
}

#[derive(Clone)]
pub enum LatencyModel {
    ConstantLatency {
        entry_latency: i64,
        resp_latency: i64,
    },
    IntpOrderLatency {
        data: Vec<DataSource<OrderLatencyRow>>,
        latency_offset: i64,
    },
}

#[derive(Clone)]
pub enum QueueModel {
    RiskAdverseQueueModel {},
    PowerProbQueueModel { n: f64 },
    LogProbQueueModel {},
    LogProbQueueModel2 {},
    PowerProbQueueModel2 { n: f64 },
    PowerProbQueueModel3 { n: f64 },
    L3FIFOQueueModel {},
}

#[derive(Clone)]
pub enum ExchangeKind {
    NoPartialFillExchange {},
    PartialFillExchange {},
}

#[derive(Clone)]
pub enum FeeModel {
    TradingValueFeeModel { fees: CommonFees },
    TradingQtyFeeModel { fees: CommonFees },
    FlatPerTradeFeeModel { fees: CommonFees },
}

/// Builds a backtesting asset.
#[pyclass(subclass)]
pub struct BacktestAsset {
    data: Vec<DataSource<Event>>,
    asset_type: AssetType,
    latency_model: LatencyModel,
    queue_model: QueueModel,
    exch_kind: ExchangeKind,
    tick_size: f64,
    lot_size: f64,
    last_trades_cap: usize,
    roi_lb: f64,
    roi_ub: f64,
    initial_snapshot: Option<DataSource<Event>>,
    fee_model: FeeModel,
    latency_offset: i64,
    order_bus_max_timestamp_reordering: i64,
    parallel_load: bool,
    cme_mbo_order_not_found_reject_marks_inactive: bool,
    cme_databento_mbo: bool,
}

unsafe impl Send for BacktestAsset {}
unsafe impl Sync for BacktestAsset {}

#[pymethods]
impl BacktestAsset {
    /// Constructs an instance of `BacktestAsset`.
    #[allow(clippy::new_without_default)]
    #[new]
    pub fn new() -> Self {
        Self {
            data: Vec::new(),
            latency_model: LatencyModel::ConstantLatency {
                entry_latency: 0,
                resp_latency: 0,
            },
            asset_type: AssetType::LinearAsset { contract_size: 1.0 },
            queue_model: QueueModel::LogProbQueueModel2 {},
            tick_size: 0.0,
            lot_size: 0.0,
            exch_kind: ExchangeKind::NoPartialFillExchange {},
            last_trades_cap: 0,
            roi_lb: 0.0,
            roi_ub: 0.0,
            initial_snapshot: None,
            fee_model: FeeModel::TradingValueFeeModel {
                fees: CommonFees::new(0.0, 0.0),
            },
            latency_offset: 0,
            order_bus_max_timestamp_reordering: 0,
            parallel_load: true,
            cme_mbo_order_not_found_reject_marks_inactive: false,
            cme_databento_mbo: false,
        }
    }

    /// Sets whether to load the next data in parallel with backtesting. This can speed up the
    /// backtest by reducing data loading time, but it also increases memory usage.
    ///
    /// Args:
    ///     preload: whether to preload the next data in parallel with backtesting.
    ///              The default value is `True`.
    pub fn parallel_load(mut slf: PyRefMut<Self>, parallel_load: bool) -> PyRefMut<Self> {
        slf.parallel_load = parallel_load;
        slf
    }

    /// Sets the latency offset to adjust the feed latency by the specified amount. This is
    /// particularly useful in cross-exchange backtesting, where the feed data is collected from a
    /// different site than the one where the strategy is intended to run.
    ///
    /// Args:
    ///     latency_offset: offset to adjust the feed latency by the specified amount.
    ///                     The default value is `0`.
    pub fn latency_offset(mut slf: PyRefMut<Self>, latency_offset: i64) -> PyRefMut<Self> {
        slf.latency_offset = latency_offset;
        slf
    }

    /// Sets the maximum timestamp reordering window for the internal order buses.
    ///
    /// A value of `0` keeps the strict FIFO/clamp behavior (default). A positive value allows
    /// order requests/responses to be reordered by timestamp within the specified window.
    ///
    /// Args:
    ///     max_timestamp_reordering: maximum allowed reordering window in timestamp units.
    pub fn order_bus_max_timestamp_reordering(
        mut slf: PyRefMut<Self>,
        max_timestamp_reordering: i64,
    ) -> PyRefMut<Self> {
        slf.order_bus_max_timestamp_reordering = max_timestamp_reordering.max(0);
        slf
    }

    /// Sets the lower bound price of the `ROIVectorMarketDepth <https://docs.rs/hftbacktest/latest/hftbacktest/depth/struct.ROIVectorMarketDepth.html>`_.
    /// Only valid if `ROIVectorMarketDepthBacktest` is built.
    ///
    /// Args:
    ///     roi_lb: the lower bound price of the range of interest.
    pub fn roi_lb(mut slf: PyRefMut<Self>, roi_lb: f64) -> PyRefMut<Self> {
        slf.roi_lb = roi_lb;
        slf
    }

    /// Sets the upper bound price of the `ROIVectorMarketDepth <https://docs.rs/hftbacktest/latest/hftbacktest/depth/struct.ROIVectorMarketDepth.html>`_.
    /// Only valid if `ROIVectorMarketDepthBacktest` is built.
    ///
    /// Args:
    ///     roi_ub: the upper bound price of the range of interest.
    pub fn roi_ub(mut slf: PyRefMut<Self>, roi_ub: f64) -> PyRefMut<Self> {
        slf.roi_ub = roi_ub;
        slf
    }

    pub fn add_file(mut slf: PyRefMut<Self>, data: String) -> PyRefMut<Self> {
        slf.data.push(DataSource::File(data));
        slf
    }

    pub fn _add_data_ndarray(mut slf: PyRefMut<Self>, data: usize, len: usize) -> PyRefMut<Self> {
        let arr = slice_from_raw_parts_mut(data as *mut u8, len * size_of::<Event>());
        let data = unsafe { Data::<Event>::from_data_ptr(DataPtr::from_ptr(arr), 0) };
        slf.data.push(DataSource::Data(data));
        slf
    }

    /// Sets the asset as a `LinearAsset <https://docs.rs/hftbacktest/latest/hftbacktest/backtest/assettype/struct.LinearAsset.html>`_.
    ///
    /// Args:
    ///     contract_size: contract size of the asset.
    pub fn linear_asset(mut slf: PyRefMut<Self>, contract_size: f64) -> PyRefMut<Self> {
        slf.asset_type = AssetType::LinearAsset { contract_size };
        slf
    }

    /// Sets the asset as a `InverseAsset <https://docs.rs/hftbacktest/latest/hftbacktest/backtest/assettype/struct.InverseAsset.html>`_.
    ///
    /// Args:
    ///     contract_size: contract size of the asset.
    pub fn inverse_asset(mut slf: PyRefMut<Self>, contract_size: f64) -> PyRefMut<Self> {
        slf.asset_type = AssetType::InverseAsset { contract_size };
        slf
    }

    /// DEPRECATED: Use `constant_order_latency` instead.
    ///
    /// Uses `ConstantLatency <https://docs.rs/hftbacktest/latest/hftbacktest/backtest/models/struct.ConstantLatency.html>`_
    /// for the order latency model.
    /// The units of the arguments should match the timestamp units of your data. Nanoseconds are
    /// typically used in HftBacktest.
    ///
    /// Args:
    ///     entry_latency: order entry latency.
    ///     resp_latency: order response latency.
    pub fn constant_latency(
        mut slf: PyRefMut<Self>,
        entry_latency: i64,
        resp_latency: i64,
    ) -> PyRefMut<Self> {
        Python::attach(|py| {
            PyErr::warn(
                py,
                &PyDeprecationWarning::type_object(py),
                c_str!("constant_latency() is deprecated; use constant_order_latency()."),
                1,
            )
        })
        .unwrap();

        slf.latency_model = LatencyModel::ConstantLatency {
            entry_latency,
            resp_latency,
        };
        slf
    }

    /// Uses `ConstantLatency <https://docs.rs/hftbacktest/latest/hftbacktest/backtest/models/struct.ConstantLatency.html>`_
    /// for the order latency model.
    /// The units of the arguments should match the timestamp units of your data. Nanoseconds are
    /// typically used in HftBacktest.
    ///
    /// Args:
    ///     entry_latency: order entry latency.
    ///     resp_latency: order response latency.
    pub fn constant_order_latency(
        mut slf: PyRefMut<Self>,
        entry_latency: i64,
        resp_latency: i64,
    ) -> PyRefMut<Self> {
        slf.latency_model = LatencyModel::ConstantLatency {
            entry_latency,
            resp_latency,
        };
        slf
    }

    /// Uses `IntpOrderLatency <https://docs.rs/hftbacktest/latest/hftbacktest/backtest/models/struct.IntpOrderLatency.html>`_
    /// for the order latency model.
    /// Please see the data format.
    /// The units of the historical latencies should match the timestamp units of your data.
    /// Nanoseconds are typically used in HftBacktest.
    ///
    /// Args:
    ///     data: a list of file paths for the historical order latency data in `npz`.
    ///     latency_offset: the latency offset to adjust the order entry and response latency by the
    ///                     specified amount. This is particularly useful in cross-exchange
    ///                     backtesting, where the feed data is collected from a different site than
    ///                     the one where the strategy is intended to run.
    pub fn intp_order_latency(
        mut slf: PyRefMut<Self>,
        data: Vec<String>,
        latency_offset: i64,
    ) -> PyRefMut<Self> {
        slf.latency_model = LatencyModel::IntpOrderLatency {
            data: data
                .iter()
                .map(|file| DataSource::File(file.to_string()))
                .collect(),
            latency_offset,
        };
        slf
    }

    pub fn _intp_order_latency_ndarray(
        mut slf: PyRefMut<Self>,
        data: usize,
        len: usize,
        latency_offset: i64,
    ) -> PyRefMut<Self> {
        let arr = slice_from_raw_parts_mut(data as *mut u8, len * size_of::<OrderLatencyRow>());
        let data = unsafe { Data::<OrderLatencyRow>::from_data_ptr(DataPtr::from_ptr(arr), 0) };
        slf.latency_model = LatencyModel::IntpOrderLatency {
            data: vec![DataSource::Data(data)],
            latency_offset,
        };
        slf
    }

    /// Uses the `RiskAdverseQueueModel <https://docs.rs/hftbacktest/latest/hftbacktest/backtest/models/struct.RiskAdverseQueueModel.html>`_
    /// for the queue position model.
    ///
    /// * `Order Fill - RiskAdverseQueueModel <https://hftbacktest.readthedocs.io/en/latest/order_fill.html#riskaversequeuemodel>`_
    pub fn risk_adverse_queue_model(mut slf: PyRefMut<Self>) -> PyRefMut<Self> {
        slf.queue_model = QueueModel::RiskAdverseQueueModel {};
        slf
    }

    /// Uses the `LogProbQueueModel` for the queue position model.
    ///
    /// Please find the details below.
    ///
    /// * `Order Fill - ProbQueueModel <https://hftbacktest.readthedocs.io/en/latest/order_fill.html#probqueuemodel>`_
    /// * `ProbQueueModel <https://docs.rs/hftbacktest/latest/hftbacktest/backtest/models/struct.ProbQueueModel.html>`_
    /// * `LogProbQueueFunc <https://docs.rs/hftbacktest/latest/hftbacktest/backtest/models/struct.LogProbQueueFunc.html>`_
    pub fn log_prob_queue_model(mut slf: PyRefMut<Self>) -> PyRefMut<Self> {
        slf.queue_model = QueueModel::LogProbQueueModel {};
        slf
    }

    /// Uses the `LogProbQueueModel2` for the queue position model.
    ///
    /// Please find the details below.
    ///
    /// * `Order Fill - ProbQueueModel <https://hftbacktest.readthedocs.io/en/latest/order_fill.html#probqueuemodel>`_
    /// * `ProbQueueModel <https://docs.rs/hftbacktest/latest/hftbacktest/backtest/models/struct.ProbQueueModel.html>`_
    /// * `LogProbQueueFunc2 <https://docs.rs/hftbacktest/latest/hftbacktest/backtest/models/struct.LogProbQueueFunc2.html>`_
    pub fn log_prob_queue_model2(mut slf: PyRefMut<Self>) -> PyRefMut<Self> {
        slf.queue_model = QueueModel::LogProbQueueModel2 {};
        slf
    }

    /// Uses the `PowerProbQueueModel` for the queue position model.
    ///
    /// Please find the details below.
    ///
    /// * `Order Fill - ProbQueueModel <https://hftbacktest.readthedocs.io/en/latest/order_fill.html#probqueuemodel>`_
    /// * `ProbQueueModel <https://docs.rs/hftbacktest/latest/hftbacktest/backtest/models/struct.ProbQueueModel.html>`_
    /// * `PowerProbQueueFunc <https://docs.rs/hftbacktest/latest/hftbacktest/backtest/models/struct.PowerProbQueueFunc.html>`_
    pub fn power_prob_queue_model(mut slf: PyRefMut<Self>, n: f64) -> PyRefMut<Self> {
        slf.queue_model = QueueModel::PowerProbQueueModel { n };
        slf
    }

    /// Uses the `PowerProbQueueModel2` for the queue position model.
    ///
    /// Please find the details below.
    ///
    /// * `Order Fill - ProbQueueModel <https://hftbacktest.readthedocs.io/en/latest/order_fill.html#probqueuemodel>`_
    /// * `ProbQueueModel <https://docs.rs/hftbacktest/latest/hftbacktest/backtest/models/struct.ProbQueueModel.html>`_
    /// * `PowerProbQueueFunc2 <https://docs.rs/hftbacktest/latest/hftbacktest/backtest/models/struct.PowerProbQueueFunc2.html>`_
    pub fn power_prob_queue_model2(mut slf: PyRefMut<Self>, n: f64) -> PyRefMut<Self> {
        slf.queue_model = QueueModel::PowerProbQueueModel2 { n };
        slf
    }

    /// Uses the `PowerProbQueueModel3` for the queue position model.
    ///
    /// Please find the details below.
    ///
    /// * `Order Fill - ProbQueueModel <https://hftbacktest.readthedocs.io/en/latest/order_fill.html#probqueuemodel>`_
    /// * `ProbQueueModel <https://docs.rs/hftbacktest/latest/hftbacktest/backtest/models/struct.ProbQueueModel.html>`_
    /// * `PowerProbQueueFunc3 <https://docs.rs/hftbacktest/latest/hftbacktest/backtest/models/struct.PowerProbQueueFunc3.html>`_
    pub fn power_prob_queue_model3(mut slf: PyRefMut<Self>, n: f64) -> PyRefMut<Self> {
        slf.queue_model = QueueModel::PowerProbQueueModel3 { n };
        slf
    }

    /// Uses the `L3FIFOQueueModel` for the queue position model.
    ///
    /// Please find the details below.
    ///
    /// * `Order Fill <https://hftbacktest.readthedocs.io/en/latest/order_fill.html>`_
    /// * `L3FIFOQueueModel <https://docs.rs/hftbacktest/latest/hftbacktest/backtest/models/struct.L3FIFOQueueModel.html>`_
    pub fn l3_fifo_queue_model(mut slf: PyRefMut<Self>) -> PyRefMut<Self> {
        slf.queue_model = QueueModel::L3FIFOQueueModel {};
        slf
    }

    /// Sets the initial snapshot.
    pub fn initial_snapshot(mut slf: PyRefMut<Self>, file: String) -> PyRefMut<Self> {
        slf.initial_snapshot = Some(DataSource::File(file));
        slf
    }

    pub fn _initial_snapshot_ndarray(
        mut slf: PyRefMut<Self>,
        data: usize,
        len: usize,
    ) -> PyRefMut<Self> {
        let arr = slice_from_raw_parts_mut(data as *mut u8, len * size_of::<Event>());
        let data = unsafe { Data::<Event>::from_data_ptr(DataPtr::from_ptr(arr), 0) };
        slf.initial_snapshot = Some(DataSource::Data(data));
        slf
    }

    /// Sets the tick size of the asset.
    pub fn tick_size(mut slf: PyRefMut<Self>, tick_size: f64) -> PyRefMut<Self> {
        slf.tick_size = tick_size;
        slf
    }

    /// Sets the lot size of the asset.
    pub fn lot_size(mut slf: PyRefMut<Self>, lot_size: f64) -> PyRefMut<Self> {
        slf.lot_size = lot_size;
        slf
    }

    /// Uses the `NoPartiallFillExchange <https://docs.rs/hftbacktest/latest/hftbacktest/backtest/proc/struct.NoPartialFillExchange.html>`_
    /// for the exchange model.
    pub fn no_partial_fill_exchange(mut slf: PyRefMut<Self>) -> PyRefMut<Self> {
        slf.exch_kind = ExchangeKind::NoPartialFillExchange {};
        slf
    }

    /// Uses the `PartiallFillExchange <https://docs.rs/hftbacktest/latest/hftbacktest/backtest/proc/struct.PartialFillExchange.html>`_
    /// for the exchange model.
    pub fn partial_fill_exchange(mut slf: PyRefMut<Self>) -> PyRefMut<Self> {
        slf.exch_kind = ExchangeKind::PartialFillExchange {};
        slf
    }

    /// CME/Databento MBO policy: treat cancel/modify `OrderNotFound` rejects as "order not active",
    /// so the reject response marks the order inactive immediately.
    ///
    /// Default: `false` (keep legacy behavior).
    pub fn cme_mbo_order_not_found_reject_marks_inactive(
        mut slf: PyRefMut<Self>,
        enabled: bool,
    ) -> PyRefMut<Self> {
        slf.cme_mbo_order_not_found_reject_marks_inactive = enabled;
        slf
    }

    /// Convenience switch for CME via Databento MBO backtests.
    ///
    /// When enabled, this forces the L3 partial fill exchange model and enables CME/MBO-specific
    /// physics policies in the Rust engine (including seq tie-break).
    ///
    /// This is the only supported entrypoint for L3 PartialFillExchange from Python.
    pub fn cme_databento_mbo(mut slf: PyRefMut<Self>, enabled: bool) -> PyRefMut<Self> {
        if enabled {
            slf.cme_databento_mbo = true;
            slf.exch_kind = ExchangeKind::PartialFillExchange {};
            slf.cme_mbo_order_not_found_reject_marks_inactive = true;
        }
        slf
    }

    /// Sets the initial capacity of the vector storing the last market trades.
    /// The default value is `0`, indicating that no last trades are stored.
    pub fn last_trades_capacity(mut slf: PyRefMut<Self>, capacity: usize) -> PyRefMut<Self> {
        slf.last_trades_cap = capacity;
        slf
    }

    /// Uses `TradingValueFeeModel <https://docs.rs/hftbacktest/latest/hftbacktest/backtest/models/struct.TradingValueFeeModel.html>`_.
    /// A negative fee represents rebates.
    pub fn trading_value_fee_model(
        mut slf: PyRefMut<Self>,
        maker_fee: f64,
        taker_fee: f64,
    ) -> PyRefMut<Self> {
        slf.fee_model = FeeModel::TradingValueFeeModel {
            fees: CommonFees::new(maker_fee, taker_fee),
        };
        slf
    }

    /// Uses `TradingQtyFeeModel <https://docs.rs/hftbacktest/latest/hftbacktest/backtest/models/struct.TradingQtyFeeModel.html>`_.
    /// A negative fee represents rebates.
    pub fn trading_qty_fee_model(
        mut slf: PyRefMut<Self>,
        maker_fee: f64,
        taker_fee: f64,
    ) -> PyRefMut<Self> {
        slf.fee_model = FeeModel::TradingQtyFeeModel {
            fees: CommonFees::new(maker_fee, taker_fee),
        };
        slf
    }

    /// Uses `FlatPerTradeFeeModel <https://docs.rs/hftbacktest/latest/hftbacktest/backtest/models/struct.FlatPerTradeFeeModel.html>`_.
    /// A negative fee represents rebates.
    pub fn flat_per_trade_fee_model(
        mut slf: PyRefMut<Self>,
        maker_fee: f64,
        taker_fee: f64,
    ) -> PyRefMut<Self> {
        slf.fee_model = FeeModel::FlatPerTradeFeeModel {
            fees: CommonFees::new(maker_fee, taker_fee),
        };
        slf
    }
}

#[pymodule]
fn _hftbacktest(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(build_hashmap_backtest, m)?)?;
    m.add_function(wrap_pyfunction!(build_roivec_backtest, m)?)?;
    #[cfg(feature = "live")]
    m.add_function(wrap_pyfunction!(build_hashmap_livebot, m)?)?;
    #[cfg(feature = "live")]
    m.add_function(wrap_pyfunction!(build_roivec_livebot, m)?)?;
    m.add_class::<BacktestAsset>()?;
    m.add_class::<LiveInstrument>()?;
    Ok(())
}

type LogProbQueueModelFunc = LogProbQueueFunc;
type LogProbQueueModel2Func = LogProbQueueFunc2;
type PowerProbQueueModelFunc = PowerProbQueueFunc;
type PowerProbQueueModel2Func = PowerProbQueueFunc2;
type PowerProbQueueModel3Func = PowerProbQueueFunc3;

fn build_error_to_py(err: BuildError) -> PyErr {
    PyErr::new::<PyValueError, _>(err.to_string())
}

fn backtest_error_to_py<E: std::fmt::Debug>(err: E) -> PyErr {
    PyErr::new::<PyValueError, _>(format!("{err:?}"))
}

fn load_initial_snapshot(snapshot: &Option<DataSource<Event>>) -> PyResult<Option<Data<Event>>> {
    match snapshot {
        None => Ok(None),
        Some(DataSource::Data(data)) => Ok(Some(data.clone())),
        Some(DataSource::File(path)) => read_npz_file(path, "data").map(Some).map_err(|err| {
            PyErr::new::<PyValueError, _>(format!("failed to read snapshot `{path}`: {err:?}"))
        }),
    }
}

macro_rules! l3_asset_builder_finish {
    ($builder:expr, $asset:ident, $depth_builder:ident, $exch_kind:ident) => {{
        $builder
            .data($asset.data.clone())
            .parallel_load($asset.parallel_load)
            .latency_offset($asset.latency_offset)
            .order_bus_max_timestamp_reordering($asset.order_bus_max_timestamp_reordering)
            .depth($depth_builder)
            .exchange($exch_kind)
            .last_trades_capacity($asset.last_trades_cap)
            .cme_mbo_order_not_found_reject_marks_inactive(
                $asset.cme_mbo_order_not_found_reject_marks_inactive,
            )
            .cme_databento_mbo($asset.cme_databento_mbo)
            .build()
            .map_err(build_error_to_py)
    }};
}

macro_rules! build_l3_asset_for_depth {
    ($asset:ident, $depth_ty:ty, $depth_builder:ident, $exch_kind:ident) => {{
        match (
            &$asset.asset_type,
            &$asset.latency_model,
            &$asset.fee_model,
            &$asset.queue_model,
        ) {
            (
                AssetType::LinearAsset { contract_size },
                LatencyModel::ConstantLatency {
                    entry_latency,
                    resp_latency,
                },
                FeeModel::TradingValueFeeModel { fees },
                QueueModel::L3FIFOQueueModel {},
            ) => l3_asset_builder_finish!(
                L3AssetBuilder::<
                    ConstantLatency,
                    LinearAsset,
                    L3FIFOQueueModel,
                    $depth_ty,
                    TradingValueFeeModel<CommonFees>,
                >::new()
                .latency_model(ConstantLatency::new(*entry_latency, *resp_latency))
                .asset_type(LinearAsset::new(*contract_size))
                .fee_model(TradingValueFeeModel::new(fees.clone()))
                .queue_model(L3FIFOQueueModel::new()),
                $asset,
                $depth_builder,
                $exch_kind
            ),
            (
                AssetType::LinearAsset { contract_size },
                LatencyModel::ConstantLatency {
                    entry_latency,
                    resp_latency,
                },
                FeeModel::TradingQtyFeeModel { fees },
                QueueModel::L3FIFOQueueModel {},
            ) => l3_asset_builder_finish!(
                L3AssetBuilder::<
                    ConstantLatency,
                    LinearAsset,
                    L3FIFOQueueModel,
                    $depth_ty,
                    TradingQtyFeeModel<CommonFees>,
                >::new()
                .latency_model(ConstantLatency::new(*entry_latency, *resp_latency))
                .asset_type(LinearAsset::new(*contract_size))
                .fee_model(TradingQtyFeeModel::new(fees.clone()))
                .queue_model(L3FIFOQueueModel::new()),
                $asset,
                $depth_builder,
                $exch_kind
            ),
            (
                AssetType::LinearAsset { contract_size },
                LatencyModel::ConstantLatency {
                    entry_latency,
                    resp_latency,
                },
                FeeModel::FlatPerTradeFeeModel { fees },
                QueueModel::L3FIFOQueueModel {},
            ) => l3_asset_builder_finish!(
                L3AssetBuilder::<
                    ConstantLatency,
                    LinearAsset,
                    L3FIFOQueueModel,
                    $depth_ty,
                    FlatPerTradeFeeModel<CommonFees>,
                >::new()
                .latency_model(ConstantLatency::new(*entry_latency, *resp_latency))
                .asset_type(LinearAsset::new(*contract_size))
                .fee_model(FlatPerTradeFeeModel::new(fees.clone()))
                .queue_model(L3FIFOQueueModel::new()),
                $asset,
                $depth_builder,
                $exch_kind
            ),
            (
                AssetType::InverseAsset { contract_size },
                LatencyModel::ConstantLatency {
                    entry_latency,
                    resp_latency,
                },
                FeeModel::TradingValueFeeModel { fees },
                QueueModel::L3FIFOQueueModel {},
            ) => l3_asset_builder_finish!(
                L3AssetBuilder::<
                    ConstantLatency,
                    InverseAsset,
                    L3FIFOQueueModel,
                    $depth_ty,
                    TradingValueFeeModel<CommonFees>,
                >::new()
                .latency_model(ConstantLatency::new(*entry_latency, *resp_latency))
                .asset_type(InverseAsset::new(*contract_size))
                .fee_model(TradingValueFeeModel::new(fees.clone()))
                .queue_model(L3FIFOQueueModel::new()),
                $asset,
                $depth_builder,
                $exch_kind
            ),
            (
                AssetType::InverseAsset { contract_size },
                LatencyModel::ConstantLatency {
                    entry_latency,
                    resp_latency,
                },
                FeeModel::TradingQtyFeeModel { fees },
                QueueModel::L3FIFOQueueModel {},
            ) => l3_asset_builder_finish!(
                L3AssetBuilder::<
                    ConstantLatency,
                    InverseAsset,
                    L3FIFOQueueModel,
                    $depth_ty,
                    TradingQtyFeeModel<CommonFees>,
                >::new()
                .latency_model(ConstantLatency::new(*entry_latency, *resp_latency))
                .asset_type(InverseAsset::new(*contract_size))
                .fee_model(TradingQtyFeeModel::new(fees.clone()))
                .queue_model(L3FIFOQueueModel::new()),
                $asset,
                $depth_builder,
                $exch_kind
            ),
            (
                AssetType::InverseAsset { contract_size },
                LatencyModel::ConstantLatency {
                    entry_latency,
                    resp_latency,
                },
                FeeModel::FlatPerTradeFeeModel { fees },
                QueueModel::L3FIFOQueueModel {},
            ) => l3_asset_builder_finish!(
                L3AssetBuilder::<
                    ConstantLatency,
                    InverseAsset,
                    L3FIFOQueueModel,
                    $depth_ty,
                    FlatPerTradeFeeModel<CommonFees>,
                >::new()
                .latency_model(ConstantLatency::new(*entry_latency, *resp_latency))
                .asset_type(InverseAsset::new(*contract_size))
                .fee_model(FlatPerTradeFeeModel::new(fees.clone()))
                .queue_model(L3FIFOQueueModel::new()),
                $asset,
                $depth_builder,
                $exch_kind
            ),
            (
                AssetType::LinearAsset { contract_size },
                LatencyModel::IntpOrderLatency {
                    data,
                    latency_offset,
                },
                FeeModel::TradingValueFeeModel { fees },
                QueueModel::L3FIFOQueueModel {},
            ) => {
                let latency_model = IntpOrderLatency::build(
                    data.clone(),
                    $asset.parallel_load,
                    *latency_offset,
                )
                .map_err(backtest_error_to_py)?;
                l3_asset_builder_finish!(
                    L3AssetBuilder::<
                        IntpOrderLatency,
                        LinearAsset,
                        L3FIFOQueueModel,
                        $depth_ty,
                        TradingValueFeeModel<CommonFees>,
                    >::new()
                    .latency_model(latency_model)
                    .asset_type(LinearAsset::new(*contract_size))
                    .fee_model(TradingValueFeeModel::new(fees.clone()))
                    .queue_model(L3FIFOQueueModel::new()),
                    $asset,
                    $depth_builder,
                    $exch_kind
                )
            }
            (
                AssetType::LinearAsset { contract_size },
                LatencyModel::IntpOrderLatency {
                    data,
                    latency_offset,
                },
                FeeModel::TradingQtyFeeModel { fees },
                QueueModel::L3FIFOQueueModel {},
            ) => {
                let latency_model = IntpOrderLatency::build(
                    data.clone(),
                    $asset.parallel_load,
                    *latency_offset,
                )
                .map_err(backtest_error_to_py)?;
                l3_asset_builder_finish!(
                    L3AssetBuilder::<
                        IntpOrderLatency,
                        LinearAsset,
                        L3FIFOQueueModel,
                        $depth_ty,
                        TradingQtyFeeModel<CommonFees>,
                    >::new()
                    .latency_model(latency_model)
                    .asset_type(LinearAsset::new(*contract_size))
                    .fee_model(TradingQtyFeeModel::new(fees.clone()))
                    .queue_model(L3FIFOQueueModel::new()),
                    $asset,
                    $depth_builder,
                    $exch_kind
                )
            }
            (
                AssetType::LinearAsset { contract_size },
                LatencyModel::IntpOrderLatency {
                    data,
                    latency_offset,
                },
                FeeModel::FlatPerTradeFeeModel { fees },
                QueueModel::L3FIFOQueueModel {},
            ) => {
                let latency_model = IntpOrderLatency::build(
                    data.clone(),
                    $asset.parallel_load,
                    *latency_offset,
                )
                .map_err(backtest_error_to_py)?;
                l3_asset_builder_finish!(
                    L3AssetBuilder::<
                        IntpOrderLatency,
                        LinearAsset,
                        L3FIFOQueueModel,
                        $depth_ty,
                        FlatPerTradeFeeModel<CommonFees>,
                    >::new()
                    .latency_model(latency_model)
                    .asset_type(LinearAsset::new(*contract_size))
                    .fee_model(FlatPerTradeFeeModel::new(fees.clone()))
                    .queue_model(L3FIFOQueueModel::new()),
                    $asset,
                    $depth_builder,
                    $exch_kind
                )
            }
            (
                AssetType::InverseAsset { contract_size },
                LatencyModel::IntpOrderLatency {
                    data,
                    latency_offset,
                },
                FeeModel::TradingValueFeeModel { fees },
                QueueModel::L3FIFOQueueModel {},
            ) => {
                let latency_model = IntpOrderLatency::build(
                    data.clone(),
                    $asset.parallel_load,
                    *latency_offset,
                )
                .map_err(backtest_error_to_py)?;
                l3_asset_builder_finish!(
                    L3AssetBuilder::<
                        IntpOrderLatency,
                        InverseAsset,
                        L3FIFOQueueModel,
                        $depth_ty,
                        TradingValueFeeModel<CommonFees>,
                    >::new()
                    .latency_model(latency_model)
                    .asset_type(InverseAsset::new(*contract_size))
                    .fee_model(TradingValueFeeModel::new(fees.clone()))
                    .queue_model(L3FIFOQueueModel::new()),
                    $asset,
                    $depth_builder,
                    $exch_kind
                )
            }
            (
                AssetType::InverseAsset { contract_size },
                LatencyModel::IntpOrderLatency {
                    data,
                    latency_offset,
                },
                FeeModel::TradingQtyFeeModel { fees },
                QueueModel::L3FIFOQueueModel {},
            ) => {
                let latency_model = IntpOrderLatency::build(
                    data.clone(),
                    $asset.parallel_load,
                    *latency_offset,
                )
                .map_err(backtest_error_to_py)?;
                l3_asset_builder_finish!(
                    L3AssetBuilder::<
                        IntpOrderLatency,
                        InverseAsset,
                        L3FIFOQueueModel,
                        $depth_ty,
                        TradingQtyFeeModel<CommonFees>,
                    >::new()
                    .latency_model(latency_model)
                    .asset_type(InverseAsset::new(*contract_size))
                    .fee_model(TradingQtyFeeModel::new(fees.clone()))
                    .queue_model(L3FIFOQueueModel::new()),
                    $asset,
                    $depth_builder,
                    $exch_kind
                )
            }
            (
                AssetType::InverseAsset { contract_size },
                LatencyModel::IntpOrderLatency {
                    data,
                    latency_offset,
                },
                FeeModel::FlatPerTradeFeeModel { fees },
                QueueModel::L3FIFOQueueModel {},
            ) => {
                let latency_model = IntpOrderLatency::build(
                    data.clone(),
                    $asset.parallel_load,
                    *latency_offset,
                )
                .map_err(backtest_error_to_py)?;
                l3_asset_builder_finish!(
                    L3AssetBuilder::<
                        IntpOrderLatency,
                        InverseAsset,
                        L3FIFOQueueModel,
                        $depth_ty,
                        FlatPerTradeFeeModel<CommonFees>,
                    >::new()
                    .latency_model(latency_model)
                    .asset_type(InverseAsset::new(*contract_size))
                    .fee_model(FlatPerTradeFeeModel::new(fees.clone()))
                    .queue_model(L3FIFOQueueModel::new()),
                    $asset,
                    $depth_builder,
                    $exch_kind
                )
            }
            _ => Err(PyErr::new::<PyValueError, _>(
                "invalid L3 asset configuration",
            )),
        }
    }};
}

#[pyfunction]
#[pyo3(signature=(assets, exch_order_equal_ts_policy_kind=0, exch_order_equal_ts_seed=0))]
pub fn build_hashmap_backtest(
    assets: Vec<PyRefMut<BacktestAsset>>,
    exch_order_equal_ts_policy_kind: u8,
    exch_order_equal_ts_seed: u64,
) -> PyResult<usize> {
    fn build_l3_asset(
        asset: &BacktestAsset,
    ) -> PyResult<Asset<dyn LocalProcessor<HashMapMarketDepth>, dyn Processor, Event>> {
        let tick_size = asset.tick_size;
        let lot_size = asset.lot_size;
        let snapshot = load_initial_snapshot(&asset.initial_snapshot)?;
        let depth_builder = move || {
            let mut depth = HashMapMarketDepth::new(tick_size, lot_size);
            if let Some(snapshot) = snapshot.as_ref() {
                depth.apply_snapshot(snapshot);
            }
            depth
        };

        let exch_kind = match asset.exch_kind {
            ExchangeKind::NoPartialFillExchange {} => CoreExchangeKind::NoPartialFillExchange,
            ExchangeKind::PartialFillExchange {} => CoreExchangeKind::PartialFillExchange,
        };

        build_l3_asset_for_depth!(asset, HashMapMarketDepth, depth_builder, exch_kind)
    }

    let policy = match exch_order_equal_ts_policy_kind {
        0 => ExchOrderEqualTsPolicy::BeforeExchData,
        1 => ExchOrderEqualTsPolicy::AfterExchData,
        2 => ExchOrderEqualTsPolicy::RandomSeeded {
            seed: exch_order_equal_ts_seed,
        },
        _ => {
            return PyResult::Err(PyErr::new::<PyValueError, _>(
                "invalid exch_order_equal_ts_policy_kind",
            ));
        }
    };

    let mut builder = Backtest::<HashMapMarketDepth>::builder().exch_order_equal_ts_policy(policy);
    for asset in assets {
        let asst = match &asset.queue_model {
            QueueModel::L3FIFOQueueModel {} => build_l3_asset(&asset)?,
            // TODO(D1): Migrate L2 asset construction off `build_asset!` and onto the engine's
            // `L2AssetBuilder` for a single-source-of-truth wiring path (and to reduce macro drift).
            _ => build_asset!(
                asset,
                HashMapMarketDepth,
                [
                    LinearAsset { contract_size },
                    InverseAsset { contract_size }
                ],
                [
                    ConstantLatency {
                        entry_latency,
                        resp_latency
                    },
                    IntpOrderLatency {
                        data,
                        latency_offset
                    }
                ],
                [
                    RiskAdverseQueueModel {},
                    LogProbQueueModel {},
                    LogProbQueueModel2 {},
                    PowerProbQueueModel { n },
                    PowerProbQueueModel2 { n },
                    PowerProbQueueModel3 { n },
                    L3FIFOQueueModel {}
                ],
                [NoPartialFillExchange {}, PartialFillExchange {}],
                [
                    TradingValueFeeModel { fees },
                    TradingQtyFeeModel { fees },
                    FlatPerTradeFeeModel { fees },
                ]
            ),
        };
        builder = builder.add_asset(asst);
    }

    let hbt = builder.build().map_err(build_error_to_py)?;
    Ok(Box::into_raw(Box::new(hbt)) as *mut c_void as usize)
}

#[pyfunction]
#[pyo3(signature=(assets, exch_order_equal_ts_policy_kind=0, exch_order_equal_ts_seed=0))]
pub fn build_roivec_backtest(
    assets: Vec<PyRefMut<BacktestAsset>>,
    exch_order_equal_ts_policy_kind: u8,
    exch_order_equal_ts_seed: u64,
) -> PyResult<usize> {
    fn build_l3_asset(
        asset: &BacktestAsset,
    ) -> PyResult<Asset<dyn LocalProcessor<ROIVectorMarketDepth>, dyn Processor, Event>> {
        let tick_size = asset.tick_size;
        let lot_size = asset.lot_size;
        let roi_lb = asset.roi_lb;
        let roi_ub = asset.roi_ub;
        let snapshot = load_initial_snapshot(&asset.initial_snapshot)?;
        let depth_builder = move || {
            let mut depth = ROIVectorMarketDepth::new(tick_size, lot_size, roi_lb, roi_ub);
            if let Some(snapshot) = snapshot.as_ref() {
                depth.apply_snapshot(snapshot);
            }
            depth
        };

        let exch_kind = match asset.exch_kind {
            ExchangeKind::NoPartialFillExchange {} => CoreExchangeKind::NoPartialFillExchange,
            ExchangeKind::PartialFillExchange {} => CoreExchangeKind::PartialFillExchange,
        };

        build_l3_asset_for_depth!(asset, ROIVectorMarketDepth, depth_builder, exch_kind)
    }

    let policy = match exch_order_equal_ts_policy_kind {
        0 => ExchOrderEqualTsPolicy::BeforeExchData,
        1 => ExchOrderEqualTsPolicy::AfterExchData,
        2 => ExchOrderEqualTsPolicy::RandomSeeded {
            seed: exch_order_equal_ts_seed,
        },
        _ => {
            return PyResult::Err(PyErr::new::<PyValueError, _>(
                "invalid exch_order_equal_ts_policy_kind",
            ));
        }
    };

    let mut builder =
        Backtest::<ROIVectorMarketDepth>::builder().exch_order_equal_ts_policy(policy);

    for asset in assets {
        let asst = match &asset.queue_model {
            QueueModel::L3FIFOQueueModel {} => build_l3_asset(&asset)?,
            // TODO(D1): Migrate L2 asset construction off `build_asset!` and onto the engine's
            // `L2AssetBuilder` for a single-source-of-truth wiring path (and to reduce macro drift).
            _ => build_asset!(
                asset,
                ROIVectorMarketDepth,
                [
                    LinearAsset { contract_size },
                    InverseAsset { contract_size }
                ],
                [
                    ConstantLatency {
                        entry_latency,
                        resp_latency
                    },
                    IntpOrderLatency {
                        data,
                        latency_offset
                    }
                ],
                [
                    RiskAdverseQueueModel {},
                    LogProbQueueModel {},
                    LogProbQueueModel2 {},
                    PowerProbQueueModel { n },
                    PowerProbQueueModel2 { n },
                    PowerProbQueueModel3 { n },
                    L3FIFOQueueModel {}
                ],
                [NoPartialFillExchange {}, PartialFillExchange {}],
                [
                    TradingValueFeeModel { fees },
                    TradingQtyFeeModel { fees },
                    FlatPerTradeFeeModel { fees },
                ]
            ),
        };
        builder = builder.add_asset(asst);
    }

    let hbt = builder.build().map_err(build_error_to_py)?;
    Ok(Box::into_raw(Box::new(hbt)) as *mut c_void as usize)
}

/// Builds a live trading instrument.
#[pyclass]
pub struct LiveInstrument {
    connector_name: String,
    symbol: String,
    tick_size: f64,
    lot_size: f64,
    last_trades_cap: usize,
    roi_lb: f64,
    roi_ub: f64,
}

unsafe impl Send for LiveInstrument {}

#[pymethods]
impl LiveInstrument {
    /// Constructs an instance of `LiveInstrument`.
    #[allow(clippy::new_without_default)]
    #[new]
    pub fn new() -> Self {
        Self {
            connector_name: String::new(),
            symbol: String::new(),
            tick_size: 0.0,
            lot_size: 0.0,
            last_trades_cap: 0,
            roi_lb: 0.0,
            roi_ub: 0.0,
        }
    }

    /// Sets a connector name.
    pub fn connector(mut slf: PyRefMut<Self>, name: String) -> PyRefMut<Self> {
        slf.connector_name = name;
        slf
    }

    /// Sets a symbol.
    pub fn symbol(mut slf: PyRefMut<Self>, symbol: String) -> PyRefMut<Self> {
        slf.symbol = symbol;
        slf
    }

    /// Sets the tick size of the asset.
    pub fn tick_size(mut slf: PyRefMut<Self>, tick_size: f64) -> PyRefMut<Self> {
        slf.tick_size = tick_size;
        slf
    }

    /// Sets the lot size of the asset.
    pub fn lot_size(mut slf: PyRefMut<Self>, lot_size: f64) -> PyRefMut<Self> {
        slf.lot_size = lot_size;
        slf
    }

    /// Sets the initial capacity of the vector storing the last market trades.
    /// The default value is `0`, indicating that no last trades are stored.
    pub fn last_trades_capacity(mut slf: PyRefMut<Self>, capacity: usize) -> PyRefMut<Self> {
        slf.last_trades_cap = capacity;
        slf
    }

    /// Sets the lower bound price of the `ROIVectorMarketDepth <https://docs.rs/hftbacktest/latest/hftbacktest/depth/struct.ROIVectorMarketDepth.html>`_.
    /// Only valid if `ROIVectorMarketDepthLiveBot` is built.
    ///
    /// Args:
    ///     roi_lb: the lower bound price of the range of interest.
    pub fn roi_lb(mut slf: PyRefMut<Self>, roi_lb: f64) -> PyRefMut<Self> {
        slf.roi_lb = roi_lb;
        slf
    }

    /// Sets the upper bound price of the `ROIVectorMarketDepth <https://docs.rs/hftbacktest/latest/hftbacktest/depth/struct.ROIVectorMarketDepth.html>`_.
    /// Only valid if `ROIVectorMarketDepthLiveBot` is built.
    ///
    /// Args:
    ///     roi_ub: the upper bound price of the range of interest.
    pub fn roi_ub(mut slf: PyRefMut<Self>, roi_ub: f64) -> PyRefMut<Self> {
        slf.roi_ub = roi_ub;
        slf
    }
}

#[cfg(feature = "live")]
#[pyfunction]
pub fn build_hashmap_livebot(instruments: Vec<PyRefMut<LiveInstrument>>) -> PyResult<usize> {
    let mut builder = LiveBotBuilder::new();
    for instrument in instruments {
        builder = builder.register(Instrument::new(
            &instrument.connector_name,
            &instrument.symbol,
            instrument.tick_size,
            instrument.lot_size,
            HashMapMarketDepth::new(instrument.tick_size, instrument.lot_size),
            instrument.last_trades_cap,
        ));
    }
    let hbt: HashMapMarketDepthLiveBot = builder
        .error_handler(|_error| Ok(()))
        .order_recv_hook(|_prev, _new| Ok(()))
        .build()
        .unwrap();

    Ok(Box::into_raw(Box::new(hbt)) as *mut c_void as usize)
}

#[cfg(feature = "live")]
#[pyfunction]
pub fn build_roivec_livebot(instruments: Vec<PyRefMut<LiveInstrument>>) -> PyResult<usize> {
    let mut builder = LiveBotBuilder::new();
    for instrument in instruments {
        builder = builder.register(Instrument::new(
            &instrument.connector_name,
            &instrument.symbol,
            instrument.tick_size,
            instrument.lot_size,
            ROIVectorMarketDepth::new(
                instrument.tick_size,
                instrument.lot_size,
                instrument.roi_lb,
                instrument.roi_ub,
            ),
            instrument.last_trades_cap,
        ));
    }
    let hbt: ROIVectorMarketDepthLiveBot = builder
        .error_handler(|_error| Ok(()))
        .order_recv_hook(|_prev, _new| Ok(()))
        .build()
        .unwrap();

    Ok(Box::into_raw(Box::new(hbt)) as *mut c_void as usize)
}
