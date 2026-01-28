use std::{
    collections::HashMap,
    io::{Error as IoError, ErrorKind},
    ops::{Deref, DerefMut},
};

pub use data::DataSource;
use data::Reader;
use models::FeeModel;
use thiserror::Error;

pub use crate::backtest::{
    models::L3QueueModel,
    proc::{L3Local, L3NoPartialFillExchange, L3PartialFillExchange},
};
use crate::{
    backtest::{
        assettype::AssetType,
        data::{Data, FeedLatencyAdjustment, NpyDTyped},
        evs::{EventIntentKind, EventSet},
        models::{LatencyModel, QueueModel},
        order::{order_bus, order_bus_with_max_timestamp_reordering},
        proc::{
            Local,
            LocalProcessor,
            NoPartialFillExchange,
            PartialFillExchange,
            Processor,
        },
        state::State,
    },
    depth::{L2MarketDepth, L3MarketDepth, MarketDepth},
    prelude::{
        Bot,
        OrdType,
        Order,
        OrderId,
        OrderRequest,
        Side,
        StateValues,
        TimeInForce,
        UNTIL_END_OF_DATA,
        WaitOrderResponse,
    },
    types::{BuildError, ElapseResult, Event},
};

/// Provides asset types.
pub mod assettype;

pub mod models;

/// OrderBus implementation
pub mod order;

/// Local and exchange models
pub mod proc;

/// Trading state.
pub mod state;

/// Recorder for a bot's trading statistics.
pub mod recorder;

pub mod data;
mod evs;

/// Policy for ordering exchange order-receipt events (ExchOrder) vs exchange market-data events
/// (ExchData) when their timestamps are equal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExchOrderEqualTsPolicy {
    /// Process ExchOrder before ExchData when timestamps are equal (conservative anti-lookahead
    /// policy; this matches the historical default behavior).
    BeforeExchData,
    /// Process ExchOrder after ExchData when timestamps are equal (pessimistic; avoids stale-book
    /// optimistic fills when the feed contains same-timestamp cancels/modifies).
    AfterExchData,
    /// Randomly choose Before/After for each equal-timestamp tie, using a deterministic seed.
    RandomSeeded { seed: u64 },
}

#[derive(Clone, Copy)]
struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        // SplitMix64: simple, fast, deterministic; good enough for tie-breaking.
        self.state = self.state.wrapping_add(0x9e3779b97f4a7c15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
        z ^ (z >> 31)
    }
}

/// Errors that can occur during backtesting.
#[derive(Error, Debug)]
pub enum BacktestError {
    #[error("Order related to a given order id already exists")]
    OrderIdExist,
    #[error("Order request is in process")]
    OrderRequestInProcess,
    #[error("Order not found")]
    OrderNotFound,
    #[error("order request is invalid")]
    InvalidOrderRequest,
    #[error("order status is invalid to proceed the request")]
    InvalidOrderStatus,
    #[error("end of data")]
    EndOfData,
    #[error("data error: {0:?}")]
    DataError(#[from] IoError),
}

/// Backtesting Asset
pub struct Asset<L: ?Sized, E: ?Sized, D: NpyDTyped + Clone /* todo: ugly bounds */> {
    pub local: Box<L>,
    pub exch: Box<E>,
    pub reader: Reader<D>,
    /// Whether the scheduler should use `(timestamp, seq)` tie-breaking for this asset.
    ///
    /// This is intended for CME via Databento MBO, where `Event.ival` is populated with Databento
    /// `sequence` values.
    pub use_seq_tie_break: bool,
}

impl<L, E, D: NpyDTyped + Clone> Asset<L, E, D> {
    /// Constructs an instance of `Asset`. Use this method if a custom local processor or an
    /// exchange processor is needed.
    pub fn new(local: L, exch: E, reader: Reader<D>) -> Self {
        Self {
            local: Box::new(local),
            exch: Box::new(exch),
            reader,
            use_seq_tie_break: false,
        }
    }

    /// Returns an `L2AssetBuilder`.
    pub fn l2_builder<LM, AT, QM, MD, FM>() -> L2AssetBuilder<LM, AT, QM, MD, FM>
    where
        AT: AssetType + Clone + 'static,
        MD: MarketDepth + L2MarketDepth + 'static,
        QM: QueueModel<MD> + 'static,
        LM: LatencyModel + Clone + 'static,
        FM: FeeModel + Clone + 'static,
    {
        L2AssetBuilder::new()
    }

    /// Returns an `L3AssetBuilder`.
    pub fn l3_builder<LM, AT, QM, MD, FM>() -> L3AssetBuilder<LM, AT, QM, MD, FM>
    where
        AT: AssetType + Clone + 'static,
        MD: MarketDepth + L3MarketDepth + 'static,
        QM: L3QueueModel<MD> + 'static,
        LM: LatencyModel + Clone + 'static,
        FM: FeeModel + Clone + 'static,
        BacktestError: From<<MD as L3MarketDepth>::Error>,
    {
        L3AssetBuilder::new()
    }
}

/// Exchange model kind.
pub enum ExchangeKind {
    /// Uses [NoPartialFillExchange](`NoPartialFillExchange`).
    NoPartialFillExchange,
    /// Uses [PartialFillExchange](`PartialFillExchange`).
    PartialFillExchange,
}

/// A level-2 asset builder.
pub struct L2AssetBuilder<LM, AT, QM, MD, FM> {
    latency_model: Option<LM>,
    asset_type: Option<AT>,
    data: Vec<DataSource<Event>>,
    parallel_load: bool,
    latency_offset: i64,
    order_bus_max_timestamp_reordering: i64,
    fee_model: Option<FM>,
    exch_kind: ExchangeKind,
    last_trades_cap: usize,
    queue_model: Option<QM>,
    depth_builder: Option<Box<dyn Fn() -> MD>>,
}

impl<LM, AT, QM, MD, FM> L2AssetBuilder<LM, AT, QM, MD, FM>
where
    AT: AssetType + Clone + 'static,
    MD: MarketDepth + L2MarketDepth + 'static,
    QM: QueueModel<MD> + 'static,
    LM: LatencyModel + Clone + 'static,
    FM: FeeModel + Clone + 'static,
{
    /// Constructs an `L2AssetBuilder`.
    pub fn new() -> Self {
        Self {
            latency_model: None,
            asset_type: None,
            data: vec![],
            parallel_load: false,
            latency_offset: 0,
            order_bus_max_timestamp_reordering: 0,
            fee_model: None,
            exch_kind: ExchangeKind::NoPartialFillExchange,
            last_trades_cap: 0,
            queue_model: None,
            depth_builder: None,
        }
    }

    /// Sets the feed data.
    pub fn data(self, data: Vec<DataSource<Event>>) -> Self {
        Self { data, ..self }
    }

    /// Sets whether to load the next data in parallel with backtesting. This can speed up the
    /// backtest by reducing data loading time, but it also increases memory usage.
    /// The default value is `true`.
    pub fn parallel_load(self, parallel_load: bool) -> Self {
        Self {
            parallel_load,
            ..self
        }
    }

    /// Sets the latency offset to adjust the feed latency by the specified amount. This is
    /// particularly useful in cross-exchange backtesting, where the feed data is collected from a
    /// different site than the one where the strategy is intended to run.
    pub fn latency_offset(self, latency_offset: i64) -> Self {
        Self {
            latency_offset,
            ..self
        }
    }

    /// Sets the maximum timestamp reordering window for the internal order buses.
    ///
    /// A value of `0` keeps the strict FIFO/clamp behavior (default). A positive value allows
    /// order requests/responses to be reordered by timestamp within the specified window.
    pub fn order_bus_max_timestamp_reordering(self, max_timestamp_reordering: i64) -> Self {
        Self {
            order_bus_max_timestamp_reordering: max_timestamp_reordering.max(0),
            ..self
        }
    }

    /// Sets a latency model.
    pub fn latency_model(self, latency_model: LM) -> Self {
        Self {
            latency_model: Some(latency_model),
            ..self
        }
    }

    /// Sets an asset type.
    pub fn asset_type(self, asset_type: AT) -> Self {
        Self {
            asset_type: Some(asset_type),
            ..self
        }
    }

    /// Sets a fee model.
    pub fn fee_model(self, fee_model: FM) -> Self {
        Self {
            fee_model: Some(fee_model),
            ..self
        }
    }

    /// Sets an exchange model. The default value is [`NoPartialFillExchange`].
    pub fn exchange(self, exch_kind: ExchangeKind) -> Self {
        Self { exch_kind, ..self }
    }

    /// Sets the initial capacity of the vector storing the last market trades.
    /// The default value is `0`, indicating that no last trades are stored.
    pub fn last_trades_capacity(self, capacity: usize) -> Self {
        Self {
            last_trades_cap: capacity,
            ..self
        }
    }

    /// Sets a queue model.
    pub fn queue_model(self, queue_model: QM) -> Self {
        Self {
            queue_model: Some(queue_model),
            ..self
        }
    }

    /// Sets a market depth builder.
    pub fn depth<Builder>(self, builder: Builder) -> Self
    where
        Builder: Fn() -> MD + 'static,
    {
        Self {
            depth_builder: Some(Box::new(builder)),
            ..self
        }
    }

    /// Builds an `Asset`.
    pub fn build(self) -> Result<Asset<dyn LocalProcessor<MD>, dyn Processor, Event>, BuildError> {
        let reader = if self.latency_offset == 0 {
            Reader::builder()
                .parallel_load(self.parallel_load)
                .data(self.data)
                .build()
                .map_err(|err| BuildError::Error(err.into()))?
        } else {
            Reader::builder()
                .parallel_load(self.parallel_load)
                .data(self.data)
                .preprocessor(FeedLatencyAdjustment::new(self.latency_offset))
                .build()
                .map_err(|err| BuildError::Error(err.into()))?
        };

        let create_depth = self
            .depth_builder
            .as_ref()
            .ok_or(BuildError::BuilderIncomplete("depth"))?;
        let order_latency = self
            .latency_model
            .clone()
            .ok_or(BuildError::BuilderIncomplete("order_latency"))?;
        let asset_type = self
            .asset_type
            .clone()
            .ok_or(BuildError::BuilderIncomplete("asset_type"))?;
        let fee_model = self
            .fee_model
            .clone()
            .ok_or(BuildError::BuilderIncomplete("fee_model"))?;

        let (order_e2l, order_l2e) = if self.order_bus_max_timestamp_reordering > 0 {
            order_bus_with_max_timestamp_reordering(
                order_latency,
                self.order_bus_max_timestamp_reordering,
            )
        } else {
            order_bus(order_latency)
        };

        let local = Local::new(
            create_depth(),
            State::new(asset_type, fee_model),
            self.last_trades_cap,
            order_l2e,
        );

        let queue_model = self
            .queue_model
            .ok_or(BuildError::BuilderIncomplete("queue_model"))?;
        let asset_type = self
            .asset_type
            .clone()
            .ok_or(BuildError::BuilderIncomplete("asset_type"))?;
        let fee_model = self
            .fee_model
            .clone()
            .ok_or(BuildError::BuilderIncomplete("fee_model"))?;

        match self.exch_kind {
            ExchangeKind::NoPartialFillExchange => {
                let exch = NoPartialFillExchange::new(
                    create_depth(),
                    State::new(asset_type, fee_model),
                    queue_model,
                    order_e2l,
                );

                Ok(Asset {
                    local: Box::new(local),
                    exch: Box::new(exch),
                    reader,
                    use_seq_tie_break: false,
                })
            }
            ExchangeKind::PartialFillExchange => {
                let exch = PartialFillExchange::new(
                    create_depth(),
                    State::new(asset_type, fee_model),
                    queue_model,
                    order_e2l,
                );

                Ok(Asset {
                    local: Box::new(local),
                    exch: Box::new(exch),
                    reader,
                    use_seq_tie_break: false,
                })
            }
        }
    }
}

impl<LM, AT, QM, MD, FM> Default for L2AssetBuilder<LM, AT, QM, MD, FM>
where
    AT: AssetType + Clone + 'static,
    MD: MarketDepth + L2MarketDepth + 'static,
    QM: QueueModel<MD> + 'static,
    LM: LatencyModel + Clone + 'static,
    FM: FeeModel + Clone + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

/// A level-3 asset builder.
pub struct L3AssetBuilder<LM, AT, QM, MD, FM> {
    latency_model: Option<LM>,
    asset_type: Option<AT>,
    data: Vec<DataSource<Event>>,
    parallel_load: bool,
    latency_offset: i64,
    order_bus_max_timestamp_reordering: i64,
    fee_model: Option<FM>,
    exch_kind: ExchangeKind,
    last_trades_cap: usize,
    queue_model: Option<QM>,
    depth_builder: Option<Box<dyn Fn() -> MD>>,
    cme_mbo_order_not_found_reject_marks_inactive: bool,
    cme_databento_mbo: bool,
}

impl<LM, AT, QM, MD, FM> L3AssetBuilder<LM, AT, QM, MD, FM>
where
    AT: AssetType + Clone + 'static,
    MD: MarketDepth + L3MarketDepth + 'static,
    QM: L3QueueModel<MD> + 'static,
    LM: LatencyModel + Clone + 'static,
    FM: FeeModel + Clone + 'static,
    BacktestError: From<<MD as L3MarketDepth>::Error>,
{
    /// Constructs an `L3AssetBuilder`.
    pub fn new() -> Self {
        Self {
            latency_model: None,
            asset_type: None,
            data: vec![],
            parallel_load: false,
            latency_offset: 0,
            order_bus_max_timestamp_reordering: 0,
            fee_model: None,
            exch_kind: ExchangeKind::NoPartialFillExchange,
            last_trades_cap: 0,
            queue_model: None,
            depth_builder: None,
            cme_mbo_order_not_found_reject_marks_inactive: false,
            cme_databento_mbo: false,
        }
    }

    /// Sets the feed data.
    pub fn data(self, data: Vec<DataSource<Event>>) -> Self {
        Self { data, ..self }
    }

    /// Sets whether to load the next data in parallel with backtesting. This can speed up the
    /// backtest by reducing data loading time, but it also increases memory usage.
    /// The default value is `true`.
    pub fn parallel_load(self, parallel_load: bool) -> Self {
        Self {
            parallel_load,
            ..self
        }
    }

    /// Sets the latency offset to adjust the feed latency by the specified amount. This is
    /// particularly useful in cross-exchange backtesting, where the feed data is collected from a
    /// different site than the one where the strategy is intended to run.
    pub fn latency_offset(self, latency_offset: i64) -> Self {
        Self {
            latency_offset,
            ..self
        }
    }

    /// Sets the maximum timestamp reordering window for the internal order buses.
    ///
    /// A value of `0` keeps the strict FIFO/clamp behavior (default). A positive value allows
    /// order requests/responses to be reordered by timestamp within the specified window.
    pub fn order_bus_max_timestamp_reordering(self, max_timestamp_reordering: i64) -> Self {
        Self {
            order_bus_max_timestamp_reordering: max_timestamp_reordering.max(0),
            ..self
        }
    }

    /// Sets a latency model.
    pub fn latency_model(self, latency_model: LM) -> Self {
        Self {
            latency_model: Some(latency_model),
            ..self
        }
    }

    /// Sets an asset type.
    pub fn asset_type(self, asset_type: AT) -> Self {
        Self {
            asset_type: Some(asset_type),
            ..self
        }
    }

    /// Sets a fee model.
    pub fn fee_model(self, fee_model: FM) -> Self {
        Self {
            fee_model: Some(fee_model),
            ..self
        }
    }

    /// Sets an exchange model. The default value is [`NoPartialFillExchange`].
    pub fn exchange(self, exch_kind: ExchangeKind) -> Self {
        Self { exch_kind, ..self }
    }

    /// CME/Databento MBO policy: treat cancel/modify `OrderNotFound` rejects as "order not active",
    /// so the reject response marks the order inactive immediately (avoids transient ghost working
    /// orders in race windows).
    ///
    /// Default: `false` (keep legacy behavior).
    pub fn cme_mbo_order_not_found_reject_marks_inactive(self, enabled: bool) -> Self {
        Self {
            cme_mbo_order_not_found_reject_marks_inactive: enabled,
            ..self
        }
    }

    /// Convenience switch for CME via Databento MBO backtests.
    ///
    /// Enables CME/MBO-specific physics policies:
    /// - `EXCH_FILL_EVENT` quantity budgeting (partial fills).
    /// - Taker execution capped by top-of-book liquidity (no infinite-liquidity fills).
    /// - Cancel/modify `OrderNotFound` rejects mark orders inactive (no ghost working orders).
    pub fn cme_databento_mbo(self, enabled: bool) -> Self {
        if enabled {
            Self {
                cme_databento_mbo: true,
                exch_kind: ExchangeKind::PartialFillExchange,
                cme_mbo_order_not_found_reject_marks_inactive: true,
                ..self
            }
        } else {
            self
        }
    }

    /// Sets the initial capacity of the vector storing the last market trades.
    /// The default value is `0`, indicating that no last trades are stored.
    pub fn last_trades_capacity(self, capacity: usize) -> Self {
        Self {
            last_trades_cap: capacity,
            ..self
        }
    }

    /// Sets a queue model.
    pub fn queue_model(self, queue_model: QM) -> Self {
        Self {
            queue_model: Some(queue_model),
            ..self
        }
    }

    /// Sets a market depth builder.
    pub fn depth<Builder>(self, builder: Builder) -> Self
    where
        Builder: Fn() -> MD + 'static,
    {
        Self {
            depth_builder: Some(Box::new(builder)),
            ..self
        }
    }

    /// Builds an `Asset`.
    pub fn build(self) -> Result<Asset<dyn LocalProcessor<MD>, dyn Processor, Event>, BuildError> {
        let reader = if self.latency_offset == 0 {
            Reader::builder()
                .parallel_load(self.parallel_load)
                .data(self.data)
                .build()
                .map_err(|err| BuildError::Error(err.into()))?
        } else {
            Reader::builder()
                .parallel_load(self.parallel_load)
                .data(self.data)
                .preprocessor(FeedLatencyAdjustment::new(self.latency_offset))
                .build()
                .map_err(|err| BuildError::Error(err.into()))?
        };

        let create_depth = self
            .depth_builder
            .as_ref()
            .ok_or(BuildError::BuilderIncomplete("depth"))?;
        let order_latency = self
            .latency_model
            .clone()
            .ok_or(BuildError::BuilderIncomplete("order_latency"))?;
        let asset_type = self
            .asset_type
            .clone()
            .ok_or(BuildError::BuilderIncomplete("asset_type"))?;
        let fee_model = self
            .fee_model
            .clone()
            .ok_or(BuildError::BuilderIncomplete("fee_model"))?;

        let (order_e2l, order_l2e) = if self.order_bus_max_timestamp_reordering > 0 {
            order_bus_with_max_timestamp_reordering(
                order_latency,
                self.order_bus_max_timestamp_reordering,
            )
        } else {
            order_bus(order_latency)
        };

        let local = L3Local::new(
            create_depth(),
            State::new(asset_type, fee_model),
            self.last_trades_cap,
            order_l2e,
        );

        let queue_model = self
            .queue_model
            .ok_or(BuildError::BuilderIncomplete("queue_model"))?;
        let asset_type = self
            .asset_type
            .clone()
            .ok_or(BuildError::BuilderIncomplete("asset_type"))?;
        let fee_model = self
            .fee_model
            .clone()
            .ok_or(BuildError::BuilderIncomplete("fee_model"))?;

        match self.exch_kind {
            ExchangeKind::NoPartialFillExchange => {
                let exch = L3NoPartialFillExchange::new(
                    create_depth(),
                    State::new(asset_type, fee_model),
                    queue_model,
                    order_e2l,
                )
                .with_order_not_found_reject_marks_inactive(
                    self.cme_mbo_order_not_found_reject_marks_inactive,
                );

                Ok(Asset {
                    local: Box::new(local),
                    exch: Box::new(exch),
                    reader,
                    use_seq_tie_break: false,
                })
            }
            ExchangeKind::PartialFillExchange => {
                if !self.cme_databento_mbo {
                    return Err(BuildError::InvalidArgument(
                        "L3 PartialFillExchange is only supported for CME Databento MBO via `cme_databento_mbo(true)`",
                    ));
                }

                let exch = L3PartialFillExchange::new(
                    create_depth(),
                    State::new(asset_type, fee_model),
                    queue_model,
                    order_e2l,
                )
                .with_order_not_found_reject_marks_inactive(
                    self.cme_mbo_order_not_found_reject_marks_inactive,
                );

                Ok(Asset {
                    local: Box::new(local),
                    exch: Box::new(exch),
                    reader,
                    use_seq_tie_break: true,
                })
            }
        }
    }
}

impl<LM, AT, QM, MD, FM> Default for L3AssetBuilder<LM, AT, QM, MD, FM>
where
    AT: AssetType + Clone + 'static,
    MD: MarketDepth + L3MarketDepth + 'static,
    QM: L3QueueModel<MD> + 'static,
    LM: LatencyModel + Clone + 'static,
    FM: FeeModel + Clone + 'static,
    BacktestError: From<<MD as L3MarketDepth>::Error>,
{
    fn default() -> Self {
        Self::new()
    }
}

/// [`Backtest`] builder.
pub struct BacktestBuilder<MD> {
    local: Vec<BacktestProcessorState<Box<dyn LocalProcessor<MD>>>>,
    exch: Vec<BacktestProcessorState<Box<dyn Processor>>>,
    exch_order_equal_ts_policy: ExchOrderEqualTsPolicy,
    use_seq_tie_break: bool,
}

impl<MD> BacktestBuilder<MD> {
    /// Adds [`Asset`], which will undergo simulation within the backtester.
    pub fn add_asset(self, asset: Asset<dyn LocalProcessor<MD>, dyn Processor, Event>) -> Self {
        let mut self_ = Self { ..self };
        self_.use_seq_tie_break |= asset.use_seq_tie_break;
        self_.local.push(BacktestProcessorState::new(
            asset.local,
            asset.reader.clone(),
        ));
        self_
            .exch
            .push(BacktestProcessorState::new(asset.exch, asset.reader));
        self_
    }

    /// Sets the ordering policy for exchange order receipts vs exchange market data at equal
    /// timestamps.
    pub fn exch_order_equal_ts_policy(self, policy: ExchOrderEqualTsPolicy) -> Self {
        let mut self_ = Self { ..self };
        self_.exch_order_equal_ts_policy = policy;
        self_
    }

    /// Builds [`Backtest`].
    pub fn build(self) -> Result<Backtest<MD>, BuildError> {
        let num_assets = self.local.len();
        if self.local.len() != num_assets || self.exch.len() != num_assets {
            panic!();
        }
        Ok(Backtest {
            cur_ts: i64::MAX,
            evs: EventSet::new(num_assets, self.use_seq_tie_break),
            local: self.local,
            exch: self.exch,
            exch_order_equal_ts_policy: self.exch_order_equal_ts_policy,
            exch_order_equal_ts_rng: match self.exch_order_equal_ts_policy {
                ExchOrderEqualTsPolicy::RandomSeeded { seed } => Some(SplitMix64::new(seed)),
                _ => None,
            },
            exch_order_equal_ts_random_tie_ts: vec![i64::MIN; num_assets],
            exch_order_equal_ts_random_tie_seq: vec![i64::MIN; num_assets],
        })
    }
}

/// This backtester provides multi-asset and multi-exchange model backtesting, allowing you to
/// configure different setups such as queue models or asset types for each asset. However, this may
/// result in slightly slower performance compared to [`Backtest`].
pub struct Backtest<MD> {
    cur_ts: i64,
    evs: EventSet,
    local: Vec<BacktestProcessorState<Box<dyn LocalProcessor<MD>>>>,
    exch: Vec<BacktestProcessorState<Box<dyn Processor>>>,
    exch_order_equal_ts_policy: ExchOrderEqualTsPolicy,
    exch_order_equal_ts_rng: Option<SplitMix64>,
    exch_order_equal_ts_random_tie_ts: Vec<i64>,
    exch_order_equal_ts_random_tie_seq: Vec<i64>,
}

impl<P: Processor> Deref for BacktestProcessorState<P> {
    type Target = P;

    fn deref(&self) -> &Self::Target {
        &self.processor
    }
}

impl<P: Processor> DerefMut for BacktestProcessorState<P> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.processor
    }
}

/// Per asset backtesting state used internally to advance event buffers.
pub struct BacktestProcessorState<P: Processor> {
    data: Data<Event>,
    processor: P,
    reader: Reader<Event>,
    row: Option<usize>,
}

impl<P: Processor> BacktestProcessorState<P> {
    fn new(processor: P, reader: Reader<Event>) -> BacktestProcessorState<P> {
        Self {
            data: Data::empty(),
            processor,
            reader,
            row: None,
        }
    }

    /// Get the index of the next available row, only advancing the reader if there's no
    /// row currently available.
    fn next_row(&mut self) -> Result<usize, BacktestError> {
        if self.row.is_none() {
            let _ = self.advance()?;
        }

        self.row.ok_or(BacktestError::EndOfData)
    }

    /// Advance the state of this processor to the next available event and return the
    /// timestamp it occurred at, if any.
    fn advance(&mut self) -> Result<i64, BacktestError> {
        loop {
            let start = self.row.map(|rn| rn + 1).unwrap_or(0);

            for rn in start..self.data.len() {
                if let Some(ts) = self.processor.event_seen_timestamp(&self.data[rn]) {
                    if ts == i64::MAX {
                        return Err(BacktestError::DataError(IoError::new(
                            ErrorKind::InvalidData,
                            "timestamp `i64::MAX` is reserved for `UNTIL_END_OF_DATA`",
                        )));
                    }
                    self.row = Some(rn);
                    return Ok(ts);
                }
            }

            let next = self.reader.next_data()?;

            self.reader.release(std::mem::replace(&mut self.data, next));
            self.row = None;
        }
    }

    #[inline]
    fn current_event_seq(&self) -> i64 {
        match self.row {
            Some(rn) => self.data[rn].ival,
            None => i64::MAX,
        }
    }
}

impl<MD> Backtest<MD>
where
    MD: MarketDepth,
{
    pub fn builder() -> BacktestBuilder<MD> {
        BacktestBuilder {
            local: vec![],
            exch: vec![],
            exch_order_equal_ts_policy: ExchOrderEqualTsPolicy::BeforeExchData,
            use_seq_tie_break: false,
        }
    }

    pub fn new(
        local: Vec<Box<dyn LocalProcessor<MD>>>,
        exch: Vec<Box<dyn Processor>>,
        reader: Vec<Reader<Event>>,
    ) -> Self {
        let num_assets = local.len();
        if local.len() != num_assets || exch.len() != num_assets || reader.len() != num_assets {
            panic!();
        }

        let local = local
            .into_iter()
            .zip(reader.iter())
            .map(|(proc, reader)| BacktestProcessorState::new(proc, reader.clone()))
            .collect();
        let exch = exch
            .into_iter()
            .zip(reader.iter())
            .map(|(proc, reader)| BacktestProcessorState::new(proc, reader.clone()))
            .collect();

        Self {
            local,
            exch,
            cur_ts: i64::MAX,
            evs: EventSet::new(num_assets, false),
            exch_order_equal_ts_policy: ExchOrderEqualTsPolicy::BeforeExchData,
            exch_order_equal_ts_rng: None,
            exch_order_equal_ts_random_tie_ts: vec![i64::MIN; num_assets],
            exch_order_equal_ts_random_tie_seq: vec![i64::MIN; num_assets],
        }
    }

    #[inline]
    fn compute_exch_order_seq(
        policy: ExchOrderEqualTsPolicy,
        rng: &mut Option<SplitMix64>,
        random_tie_ts: &mut [i64],
        random_tie_seq: &mut [i64],
        asset_no: usize,
        order_ts: i64,
        exch_data_ts: i64,
    ) -> i64 {
        if order_ts == i64::MAX {
            if matches!(policy, ExchOrderEqualTsPolicy::RandomSeeded { .. }) {
                random_tie_ts[asset_no] = i64::MIN;
            }
            return i64::MAX;
        }
        match policy {
            ExchOrderEqualTsPolicy::BeforeExchData => i64::MIN,
            ExchOrderEqualTsPolicy::AfterExchData => i64::MAX,
            ExchOrderEqualTsPolicy::RandomSeeded { .. } => {
                if order_ts != exch_data_ts {
                    random_tie_ts[asset_no] = i64::MIN;
                    i64::MIN
                } else {
                    if random_tie_ts[asset_no] == order_ts {
                        return random_tie_seq[asset_no];
                    }
                    let rng = rng.as_mut().expect("RandomSeeded policy requires rng");
                    let seq = if (rng.next_u64() & 1) == 0 { i64::MIN } else { i64::MAX };
                    random_tie_ts[asset_no] = order_ts;
                    random_tie_seq[asset_no] = seq;
                    seq
                }
            }
        }
    }

    fn sync_exch_order_seq_for_asset(&mut self, asset_no: usize) {
        if !matches!(
            self.exch_order_equal_ts_policy,
            ExchOrderEqualTsPolicy::RandomSeeded { .. }
        ) {
            return;
        }

        let order_ts = self.evs.peek_exch_order_timestamp(asset_no);
        let exch_data_ts = self.evs.peek_exch_data_timestamp(asset_no);
        let seq = Self::compute_exch_order_seq(
            self.exch_order_equal_ts_policy,
            &mut self.exch_order_equal_ts_rng,
            &mut self.exch_order_equal_ts_random_tie_ts,
            &mut self.exch_order_equal_ts_random_tie_seq,
            asset_no,
            order_ts,
            exch_data_ts,
        );
        self.evs.update_exch_order(asset_no, order_ts, seq);
    }

    fn initialize_evs(&mut self) -> Result<(), BacktestError> {
        for (asset_no, local) in self.local.iter_mut().enumerate() {
            match local.advance() {
                Ok(ts) => self
                    .evs
                    .update_local_data(asset_no, ts, local.current_event_seq()),
                Err(BacktestError::EndOfData) => {
                    self.evs.invalidate_local_data(asset_no);
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }
        for (asset_no, exch) in self.exch.iter_mut().enumerate() {
            match exch.advance() {
                Ok(ts) => self
                    .evs
                    .update_exch_data(asset_no, ts, exch.current_event_seq()),
                Err(BacktestError::EndOfData) => {
                    self.evs.invalidate_exch_data(asset_no);
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }
        Ok(())
    }

    pub fn goto_end(&mut self) -> Result<ElapseResult, BacktestError> {
        if self.cur_ts == i64::MAX {
            self.initialize_evs()?;
            match self.evs.next() {
                Some(ev) => {
                    self.cur_ts = ev.timestamp;
                }
                None => {
                    return Ok(ElapseResult::EndOfData);
                }
            }
        }
        self.goto::<false>(UNTIL_END_OF_DATA, WaitOrderResponse::None)
    }

    fn goto<const WAIT_NEXT_FEED: bool>(
        &mut self,
        timestamp: i64,
        wait_order_response: WaitOrderResponse,
    ) -> Result<ElapseResult, BacktestError> {
        let mut result = ElapseResult::Ok;
        let mut timestamp = timestamp;
        let mut last_event_timestamp: Option<i64> = None;
        for asset_no in 0..self.local.len() {
            let exch_data_ts = self.evs.peek_exch_data_timestamp(asset_no);
            let local_send_ts = self.local[asset_no].earliest_send_order_timestamp();
            let local_recv_ts = self.local[asset_no].earliest_recv_order_timestamp();
            let exch_order_seq = Self::compute_exch_order_seq(
                self.exch_order_equal_ts_policy,
                &mut self.exch_order_equal_ts_rng,
                &mut self.exch_order_equal_ts_random_tie_ts,
                &mut self.exch_order_equal_ts_random_tie_seq,
                asset_no,
                local_send_ts,
                exch_data_ts,
            );

            self.evs
                .update_exch_order(asset_no, local_send_ts, exch_order_seq);
            self.evs
                .update_local_order(asset_no, local_recv_ts, i64::MAX);
        }
        loop {
            match self.evs.next() {
                Some(ev) => {
                    if ev.timestamp > timestamp {
                        self.cur_ts = timestamp;
                        return Ok(result);
                    }
                    last_event_timestamp = Some(ev.timestamp);
                    match ev.kind {
                        EventIntentKind::LocalData => {
                            let local = unsafe { self.local.get_unchecked_mut(ev.asset_no) };
                            let next = local.next_row().and_then(|row| {
                                local.processor.process(&local.data[row])?;
                                local.advance()
                            });

                            match next {
                                Ok(next_ts) => {
                                    self.evs.update_local_data(
                                        ev.asset_no,
                                        next_ts,
                                        local.current_event_seq(),
                                    );
                                }
                                Err(BacktestError::EndOfData) => {
                                    self.evs.invalidate_local_data(ev.asset_no);
                                }
                                Err(e) => {
                                    return Err(e);
                                }
                            }
                            if WAIT_NEXT_FEED {
                                timestamp = ev.timestamp;
                                result = ElapseResult::MarketFeed;
                            }
                        }
                        EventIntentKind::LocalOrder => {
                            let local = unsafe { self.local.get_unchecked_mut(ev.asset_no) };
                            let wait_order_resp_id = match wait_order_response {
                                WaitOrderResponse::Specified {
                                    asset_no: wait_order_asset_no,
                                    order_id: wait_order_id,
                                } if ev.asset_no == wait_order_asset_no => Some(wait_order_id),
                                _ => None,
                            };
                            if local.process_recv_order(ev.timestamp, wait_order_resp_id)?
                                || wait_order_response == WaitOrderResponse::Any
                            {
                                timestamp = ev.timestamp;
                                if WAIT_NEXT_FEED {
                                    result = ElapseResult::OrderResponse;
                                }
                            }
                            self.evs.update_local_order(
                                ev.asset_no,
                                local.earliest_recv_order_timestamp(),
                                i64::MAX,
                            );
                        }
                        EventIntentKind::ExchData => {
                            let (next, next_seq, local_order_ts) = {
                                let exch = unsafe { self.exch.get_unchecked_mut(ev.asset_no) };
                                let next = exch.next_row().and_then(|row| {
                                    exch.processor.process(&exch.data[row])?;
                                    exch.advance()
                                });
                                (next, exch.current_event_seq(), exch.earliest_send_order_timestamp())
                            };

                            match next {
                                Ok(next_ts) => {
                                    self.evs.update_exch_data(ev.asset_no, next_ts, next_seq);
                                }
                                Err(BacktestError::EndOfData) => {
                                    self.evs.invalidate_exch_data(ev.asset_no);
                                }
                                Err(e) => {
                                    return Err(e);
                                }
                            }
                            // Under RandomSeeded, the ExchOrder vs ExchData decision must be made
                            // when the timestamps actually tie, which may happen only after ExchData
                            // advances.
                            self.sync_exch_order_seq_for_asset(ev.asset_no);

                            self.evs
                                .update_local_order(ev.asset_no, local_order_ts, i64::MAX);
                        }
                        EventIntentKind::ExchOrder => {
                            let (exch_order_ts, local_order_ts) = {
                                let exch = unsafe { self.exch.get_unchecked_mut(ev.asset_no) };
                                let _ = exch.process_recv_order(ev.timestamp, None)?;
                                (exch.earliest_recv_order_timestamp(), exch.earliest_send_order_timestamp())
                            };
                            let exch_data_ts = self.evs.peek_exch_data_timestamp(ev.asset_no);
                            let exch_order_seq = Self::compute_exch_order_seq(
                                self.exch_order_equal_ts_policy,
                                &mut self.exch_order_equal_ts_rng,
                                &mut self.exch_order_equal_ts_random_tie_ts,
                                &mut self.exch_order_equal_ts_random_tie_seq,
                                ev.asset_no,
                                exch_order_ts,
                                exch_data_ts,
                            );
                            self.evs.update_exch_order(ev.asset_no, exch_order_ts, exch_order_seq);
                            self.evs
                                .update_local_order(ev.asset_no, local_order_ts, i64::MAX);
                        }
                    }
                }
                None => {
                    if let Some(last_event_timestamp) = last_event_timestamp {
                        self.cur_ts = last_event_timestamp;
                    }
                    return Ok(ElapseResult::EndOfData);
                }
            }
        }
    }
}

impl<MD> Bot<MD> for Backtest<MD>
where
    MD: MarketDepth,
{
    type Error = BacktestError;

    #[inline]
    fn current_timestamp(&self) -> i64 {
        self.cur_ts
    }

    #[inline]
    fn num_assets(&self) -> usize {
        self.local.len()
    }

    #[inline]
    fn position(&self, asset_no: usize) -> f64 {
        self.local.get(asset_no).unwrap().position()
    }

    #[inline]
    fn state_values(&self, asset_no: usize) -> &StateValues {
        self.local.get(asset_no).unwrap().state_values()
    }

    fn depth(&self, asset_no: usize) -> &MD {
        self.local.get(asset_no).unwrap().depth()
    }

    fn last_trades(&self, asset_no: usize) -> &[Event] {
        self.local.get(asset_no).unwrap().last_trades()
    }

    #[inline]
    fn clear_last_trades(&mut self, asset_no: Option<usize>) {
        match asset_no {
            Some(an) => {
                let local = self.local.get_mut(an).unwrap();
                local.clear_last_trades();
            }
            None => {
                for local in self.local.iter_mut() {
                    local.clear_last_trades();
                }
            }
        }
    }

    #[inline]
    fn orders(&self, asset_no: usize) -> &HashMap<u64, Order> {
        self.local.get(asset_no).unwrap().orders()
    }

    #[inline]
    fn submit_buy_order(
        &mut self,
        asset_no: usize,
        order_id: OrderId,
        price: f64,
        qty: f64,
        time_in_force: TimeInForce,
        order_type: OrdType,
        wait: bool,
    ) -> Result<ElapseResult, Self::Error> {
        let local = self.local.get_mut(asset_no).unwrap();
        local.submit_order(
            order_id,
            Side::Buy,
            price,
            qty,
            order_type,
            time_in_force,
            self.cur_ts,
        )?;

        if wait {
            return self.goto::<false>(
                UNTIL_END_OF_DATA,
                WaitOrderResponse::Specified { asset_no, order_id },
            );
        }
        Ok(ElapseResult::Ok)
    }

    #[inline]
    fn submit_sell_order(
        &mut self,
        asset_no: usize,
        order_id: OrderId,
        price: f64,
        qty: f64,
        time_in_force: TimeInForce,
        order_type: OrdType,
        wait: bool,
    ) -> Result<ElapseResult, Self::Error> {
        let local = self.local.get_mut(asset_no).unwrap();
        local.submit_order(
            order_id,
            Side::Sell,
            price,
            qty,
            order_type,
            time_in_force,
            self.cur_ts,
        )?;

        if wait {
            return self.goto::<false>(
                UNTIL_END_OF_DATA,
                WaitOrderResponse::Specified { asset_no, order_id },
            );
        }
        Ok(ElapseResult::Ok)
    }

    fn submit_order(
        &mut self,
        asset_no: usize,
        order: OrderRequest,
        wait: bool,
    ) -> Result<ElapseResult, Self::Error> {
        let local = self.local.get_mut(asset_no).unwrap();
        local.submit_order(
            order.order_id,
            order.side,
            order.price,
            order.qty,
            order.order_type,
            order.time_in_force,
            self.cur_ts,
        )?;

        if wait {
            return self.goto::<false>(
                UNTIL_END_OF_DATA,
                WaitOrderResponse::Specified {
                    asset_no,
                    order_id: order.order_id,
                },
            );
        }
        Ok(ElapseResult::Ok)
    }

    #[inline]
    fn modify(
        &mut self,
        asset_no: usize,
        order_id: OrderId,
        price: f64,
        qty: f64,
        wait: bool,
    ) -> Result<ElapseResult, Self::Error> {
        let local = self.local.get_mut(asset_no).unwrap();
        local.modify(order_id, price, qty, self.cur_ts)?;

        if wait {
            return self.goto::<false>(
                UNTIL_END_OF_DATA,
                WaitOrderResponse::Specified { asset_no, order_id },
            );
        }
        Ok(ElapseResult::Ok)
    }

    #[inline]
    fn cancel(
        &mut self,
        asset_no: usize,
        order_id: OrderId,
        wait: bool,
    ) -> Result<ElapseResult, Self::Error> {
        let local = self.local.get_mut(asset_no).unwrap();
        local.cancel(order_id, self.cur_ts)?;

        if wait {
            return self.goto::<false>(
                UNTIL_END_OF_DATA,
                WaitOrderResponse::Specified { asset_no, order_id },
            );
        }
        Ok(ElapseResult::Ok)
    }

    #[inline]
    fn clear_inactive_orders(&mut self, asset_no: Option<usize>) {
        match asset_no {
            Some(asset_no) => {
                self.local
                    .get_mut(asset_no)
                    .unwrap()
                    .clear_inactive_orders();
            }
            None => {
                for local in self.local.iter_mut() {
                    local.clear_inactive_orders();
                }
            }
        }
    }

    #[inline]
    fn wait_order_response(
        &mut self,
        asset_no: usize,
        order_id: OrderId,
        timeout: i64,
    ) -> Result<ElapseResult, BacktestError> {
        if timeout < 0 {
            return Err(BacktestError::DataError(IoError::new(
                ErrorKind::InvalidInput,
                "`timeout` must be non-negative",
            )));
        }
        if self.cur_ts == i64::MAX {
            self.initialize_evs()?;
            match self.evs.next() {
                Some(ev) => {
                    self.cur_ts = ev.timestamp;
                }
                None => {
                    return Ok(ElapseResult::EndOfData);
                }
            }
        }
        self.goto::<false>(
            self.cur_ts.checked_add(timeout).unwrap_or(UNTIL_END_OF_DATA),
            WaitOrderResponse::Specified { asset_no, order_id },
        )
    }

    #[inline]
    fn wait_next_feed(
        &mut self,
        include_order_resp: bool,
        timeout: i64,
    ) -> Result<ElapseResult, Self::Error> {
        if timeout < 0 {
            return Err(BacktestError::DataError(IoError::new(
                ErrorKind::InvalidInput,
                "`timeout` must be non-negative",
            )));
        }
        if self.cur_ts == i64::MAX {
            self.initialize_evs()?;
            match self.evs.next() {
                Some(ev) => {
                    self.cur_ts = ev.timestamp;
                }
                None => {
                    return Ok(ElapseResult::EndOfData);
                }
            }
        }
        let target = self.cur_ts.checked_add(timeout).unwrap_or(UNTIL_END_OF_DATA);
        if include_order_resp {
            self.goto::<true>(target, WaitOrderResponse::Any)
        } else {
            self.goto::<true>(target, WaitOrderResponse::None)
        }
    }

    #[inline]
    fn elapse(&mut self, duration: i64) -> Result<ElapseResult, Self::Error> {
        if duration < 0 {
            return Err(BacktestError::DataError(IoError::new(
                ErrorKind::InvalidInput,
                "`duration` must be non-negative",
            )));
        }
        if self.cur_ts == i64::MAX {
            self.initialize_evs()?;
            match self.evs.next() {
                Some(ev) => {
                    self.cur_ts = ev.timestamp;
                }
                None => {
                    return Ok(ElapseResult::EndOfData);
                }
            }
        }
        self.goto::<false>(
            self.cur_ts.checked_add(duration).unwrap_or(UNTIL_END_OF_DATA),
            WaitOrderResponse::None,
        )
    }

    #[inline]
    fn elapse_bt(&mut self, duration: i64) -> Result<ElapseResult, Self::Error> {
        self.elapse(duration)
    }

    #[inline]
    fn close(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }

    #[inline]
    fn feed_latency(&self, asset_no: usize) -> Option<(i64, i64)> {
        self.local.get(asset_no).unwrap().feed_latency()
    }

    #[inline]
    fn order_latency(&self, asset_no: usize) -> Option<(i64, i64, i64)> {
        self.local.get(asset_no).unwrap().order_latency()
    }
}

#[cfg(test)]
mod test {
    use std::error::Error;
    use std::io::ErrorKind;

    use super::{ExchOrderEqualTsPolicy, evs::{EventIntentKind, EventSet}};
    use crate::{
        backtest::{
            Backtest,
            BacktestError,
            DataSource,
            ExchangeKind::NoPartialFillExchange,
            ExchangeKind::PartialFillExchange,
            L2AssetBuilder,
            assettype::LinearAsset,
            data::Data,
            models::{
                CommonFees,
                ConstantLatency,
                LatencyModel,
                PowerProbQueueFunc3,
                ProbQueueModel,
                RiskAdverseQueueModel,
                TradingValueFeeModel,
            },
        },
        depth::HashMapMarketDepth,
        prelude::{Bot, Event},
        types::{
            ElapseResult,
            EXCH_ASK_DEPTH_SNAPSHOT_EVENT,
            EXCH_EVENT,
            LOCAL_EVENT,
            OrdType,
            Order,
            Status,
            TimeInForce,
        },
    };

    #[derive(Clone)]
    struct RejectModifyLatency;

    impl LatencyModel for RejectModifyLatency {
        fn entry(&mut self, _timestamp: i64, order: &Order) -> i64 {
            if order.req == Status::Replaced {
                -1
            } else {
                0
            }
        }

        fn response(&mut self, _timestamp: i64, _order: &Order) -> i64 {
            0
        }
    }

    #[test]
    fn skips_unseen_events() -> Result<(), Box<dyn Error>> {
        let data = Data::from_data(&[
            Event {
                ev: EXCH_EVENT | LOCAL_EVENT,
                exch_ts: 0,
                local_ts: 0,
                px: 0.0,
                qty: 0.0,
                order_id: 0,
                ival: 0,
                fval: 0.0,
            },
            Event {
                ev: LOCAL_EVENT | EXCH_EVENT,
                exch_ts: 1,
                local_ts: 1,
                px: 0.0,
                qty: 0.0,
                order_id: 0,
                ival: 0,
                fval: 0.0,
            },
            Event {
                ev: EXCH_EVENT,
                exch_ts: 3,
                local_ts: 4,
                px: 0.0,
                qty: 0.0,
                order_id: 0,
                ival: 0,
                fval: 0.0,
            },
            Event {
                ev: LOCAL_EVENT,
                exch_ts: 3,
                local_ts: 4,
                px: 0.0,
                qty: 0.0,
                order_id: 0,
                ival: 0,
                fval: 0.0,
            },
        ]);

        let mut backtester = Backtest::builder()
            .add_asset(
                L2AssetBuilder::default()
                    .data(vec![DataSource::Data(data)])
                    .latency_model(ConstantLatency::new(50, 50))
                    .asset_type(LinearAsset::new(1.0))
                    .fee_model(TradingValueFeeModel::new(CommonFees::new(0.0, 0.0)))
                    .queue_model(ProbQueueModel::new(PowerProbQueueFunc3::new(3.0)))
                    .exchange(NoPartialFillExchange)
                    .depth(|| HashMapMarketDepth::new(0.01, 1.0))
                    .build()?,
            )
            .build()?;

        // Process first events and advance a single timestep
        backtester.elapse_bt(1)?;
        assert_eq!(1, backtester.cur_ts);

        // Check that we correctly skip past events that aren't seen by a given processor
        backtester.elapse_bt(1)?;
        assert_eq!(2, backtester.cur_ts);
        assert_eq!(Some(3), backtester.local[0].row);
        assert_eq!(Some(2), backtester.exch[0].row);

        backtester.elapse_bt(1)?;
        assert_eq!(3, backtester.cur_ts);

        Ok(())
    }

    #[test]
    fn partialfillexchange_multilevel_fill_on_accept_is_accounted() -> Result<(), Box<dyn Error>> {
        let data = Data::from_data(&[
            Event {
                ev: EXCH_ASK_DEPTH_SNAPSHOT_EVENT,
                exch_ts: 0,
                local_ts: 0,
                px: 100.0,
                qty: 1.0,
                order_id: 0,
                ival: 0,
                fval: 0.0,
            },
            Event {
                ev: EXCH_ASK_DEPTH_SNAPSHOT_EVENT,
                exch_ts: 0,
                local_ts: 0,
                px: 101.0,
                qty: 2.0,
                order_id: 0,
                ival: 0,
                fval: 0.0,
            },
        ]);

        let mut backtester = Backtest::builder()
            .add_asset(
                L2AssetBuilder::default()
                    .data(vec![DataSource::Data(data)])
                    .latency_model(ConstantLatency::new(0, 0))
                    .asset_type(LinearAsset::new(1.0))
                    .fee_model(TradingValueFeeModel::new(CommonFees::new(0.0, 0.0)))
                    .queue_model(RiskAdverseQueueModel::new())
                    .exchange(PartialFillExchange)
                    .depth(|| HashMapMarketDepth::new(1.0, 1.0))
                    .build()?,
            )
            .build()?;

        // Processes initial depth events at timestamp 0.
        let _ = backtester.elapse_bt(0)?;

        backtester.submit_buy_order(
            0,
            1,
            101.0,
            3.0,
            TimeInForce::IOC,
            OrdType::Limit,
            true,
        )?;

        let values = backtester.state_values(0);
        assert_eq!(3.0, values.position);
        assert_eq!(-302.0, values.balance);
        Ok(())
    }

    #[test]
    fn rejected_modify_rolls_back_local_order_fields() -> Result<(), Box<dyn Error>> {
        let data = Data::from_data(&[Event {
            ev: EXCH_EVENT,
            exch_ts: 0,
            local_ts: 0,
            px: 0.0,
            qty: 0.0,
            order_id: 0,
            ival: 0,
            fval: 0.0,
        }]);

        let mut backtester = Backtest::builder()
            .add_asset(
                L2AssetBuilder::default()
                    .data(vec![DataSource::Data(data)])
                    .latency_model(RejectModifyLatency)
                    .asset_type(LinearAsset::new(1.0))
                    .fee_model(TradingValueFeeModel::new(CommonFees::new(0.0, 0.0)))
                    .queue_model(RiskAdverseQueueModel::new())
                    .exchange(PartialFillExchange)
                    .depth(|| HashMapMarketDepth::new(1.0, 1.0))
                    .build()?,
            )
            .build()?;

        let _ = backtester.elapse_bt(0)?;

        backtester.submit_buy_order(
            0,
            1,
            10.0,
            1.0,
            TimeInForce::GTC,
            OrdType::Limit,
            true,
        )?;

        backtester.modify(0, 1, 12.0, 2.0, true)?;

        let order = backtester.orders(0).get(&1).unwrap();
        assert_eq!(10, order.price_tick);
        assert_eq!(1.0, order.qty);
        Ok(())
    }

    #[test]
    fn goto_end_advances_timestamp_and_flushes_pending_order_responses() -> Result<(), Box<dyn Error>>
    {
        let data = Data::from_data(&[Event {
            ev: EXCH_ASK_DEPTH_SNAPSHOT_EVENT,
            exch_ts: 0,
            local_ts: 0,
            px: 100.0,
            qty: 1.0,
            order_id: 0,
            ival: 0,
            fval: 0.0,
        }]);

        let mut backtester = Backtest::builder()
            .add_asset(
                L2AssetBuilder::default()
                    .data(vec![DataSource::Data(data)])
                    .latency_model(ConstantLatency::new(0, 10))
                    .asset_type(LinearAsset::new(1.0))
                    .fee_model(TradingValueFeeModel::new(CommonFees::new(0.0, 0.0)))
                    .queue_model(RiskAdverseQueueModel::new())
                    .exchange(PartialFillExchange)
                    .depth(|| HashMapMarketDepth::new(1.0, 1.0))
                    .build()?,
            )
            .build()?;

        let _ = backtester.elapse_bt(0)?;

        backtester.submit_buy_order(0, 1, 100.0, 1.0, TimeInForce::IOC, OrdType::Limit, false)?;

        let result = backtester.goto_end()?;
        assert_eq!(ElapseResult::EndOfData, result);

        assert_eq!(1.0, backtester.position(0));

        assert_eq!(10, backtester.current_timestamp());
        Ok(())
    }

    #[test]
    fn l2_partialfillexchange_rejects_second_order_at_same_tick_and_prevents_overfill(
    ) -> Result<(), Box<dyn Error>> {
        let data = Data::from_data(&[
            Event {
                ev: EXCH_EVENT,
                exch_ts: 0,
                local_ts: 0,
                px: 0.0,
                qty: 0.0,
                order_id: 0,
                ival: 0,
                fval: 0.0,
            },
            Event {
                ev: crate::types::EXCH_BUY_TRADE_EVENT,
                exch_ts: 1,
                local_ts: 0,
                px: 100.0,
                qty: 1.0,
                order_id: 0,
                ival: 0,
                fval: 0.0,
            },
        ]);

        let mut backtester = Backtest::builder()
            .add_asset(
                L2AssetBuilder::default()
                    .data(vec![DataSource::Data(data)])
                    .latency_model(ConstantLatency::new(0, 0))
                    .asset_type(LinearAsset::new(1.0))
                    .fee_model(TradingValueFeeModel::new(CommonFees::new(0.0, 0.0)))
                    .queue_model(RiskAdverseQueueModel::new())
                    .exchange(PartialFillExchange)
                    .depth(|| HashMapMarketDepth::new(1.0, 1.0))
                    .build()?,
            )
            .build()?;

        let _ = backtester.elapse_bt(0)?;

        backtester.submit_sell_order(
            0,
            1,
            100.0,
            1.0,
            TimeInForce::GTC,
            OrdType::Limit,
            true,
        )?;
        backtester.submit_sell_order(
            0,
            2,
            100.0,
            1.0,
            TimeInForce::GTC,
            OrdType::Limit,
            true,
        )?;

        backtester.elapse_bt(1)?;

        assert_eq!(-1.0, backtester.state_values(0).position);

        let order_2 = backtester.orders(0).get(&2).unwrap();
        assert_eq!(Status::Expired, order_2.status);

        Ok(())
    }

    #[test]
    fn l2_partialfillexchange_rejects_modify_to_existing_tick_without_canceling_original(
    ) -> Result<(), Box<dyn Error>> {
        let data = Data::from_data(&[Event {
            ev: EXCH_EVENT,
            exch_ts: 0,
            local_ts: 0,
            px: 0.0,
            qty: 0.0,
            order_id: 0,
            ival: 0,
            fval: 0.0,
        }]);

        let mut backtester = Backtest::builder()
            .add_asset(
                L2AssetBuilder::default()
                    .data(vec![DataSource::Data(data)])
                    .latency_model(ConstantLatency::new(0, 0))
                    .asset_type(LinearAsset::new(1.0))
                    .fee_model(TradingValueFeeModel::new(CommonFees::new(0.0, 0.0)))
                    .queue_model(RiskAdverseQueueModel::new())
                    .exchange(PartialFillExchange)
                    .depth(|| HashMapMarketDepth::new(1.0, 1.0))
                    .build()?,
            )
            .build()?;

        let _ = backtester.elapse_bt(0)?;

        backtester.submit_sell_order(
            0,
            1,
            101.0,
            1.0,
            TimeInForce::GTC,
            OrdType::Limit,
            true,
        )?;
        backtester.submit_sell_order(
            0,
            2,
            102.0,
            1.0,
            TimeInForce::GTC,
            OrdType::Limit,
            true,
        )?;

        backtester.modify(0, 1, 102.0, 1.0, true)?;

        let order_1 = backtester.orders(0).get(&1).unwrap();
        assert_eq!(101, order_1.price_tick);
        assert_eq!(Status::New, order_1.status);

        let order_2 = backtester.orders(0).get(&2).unwrap();
        assert_eq!(102, order_2.price_tick);
        assert_eq!(Status::New, order_2.status);

        Ok(())
    }

    #[test]
    fn eventset_tie_breaks_by_seq_within_timestamp() {
        let mut evs = EventSet::new(1, false);
        evs.update_exch_data(0, 1, 10);
        evs.update_exch_order(0, 1, 0);

        let ev = evs.next().expect("expected an event");
        assert_eq!(ev.timestamp, 1);
        assert_eq!(ev.asset_no, 0);
        assert!(matches!(ev.kind, EventIntentKind::ExchOrder));
    }

    #[test]
    fn exch_order_is_processed_before_exch_data_at_same_timestamp() -> Result<(), Box<dyn Error>> {
        // If exchange market data (book update) is processed before an order receipt event at the
        // same exchange timestamp, order acceptance/fills can read a post-update book state at that
        // timestamp (within-timestamp look-ahead). This regression test asserts a conservative
        // ordering: ExchOrder precedes ExchData when timestamps are equal (the default policy).
        let data = Data::from_data(&[
            Event {
                ev: EXCH_ASK_DEPTH_SNAPSHOT_EVENT,
                exch_ts: 0,
                local_ts: 0,
                px: 101.0,
                qty: 1.0,
                order_id: 0,
                ival: 0,
                fval: 0.0,
            },
            Event {
                ev: EXCH_ASK_DEPTH_SNAPSHOT_EVENT,
                exch_ts: 1,
                local_ts: 1,
                px: 99.0,
                qty: 1.0,
                order_id: 0,
                ival: 0,
                fval: 0.0,
            },
        ]);

        let mut backtester = Backtest::builder()
            .add_asset(
                L2AssetBuilder::default()
                    .data(vec![DataSource::Data(data)])
                    // Order sent at local_ts=0 arrives at exch_ts=1.
                    .latency_model(ConstantLatency::new(1, 0))
                    .asset_type(LinearAsset::new(1.0))
                    .fee_model(TradingValueFeeModel::new(CommonFees::new(0.0, 0.0)))
                    .queue_model(RiskAdverseQueueModel::new())
                    .exchange(NoPartialFillExchange)
                    .depth(|| HashMapMarketDepth::new(1.0, 1.0))
                    .build()?,
            )
            .build()?;

        // Seed exchange depth at exch_ts=0 with best ask=101.
        backtester.elapse_bt(0)?;

        // A buy at 100 should not see the post-update best ask=99 when it reaches the exchange at
        // exch_ts=1. With the conservative ordering, it becomes maker (resting) first, then gets
        // filled as the book moves through it (exec at 100 as maker).
        backtester.submit_buy_order(
            0,
            1,
            100.0,
            1.0,
            TimeInForce::GTC,
            OrdType::Limit,
            true,
        )?;

        let order = backtester.orders(0).get(&1).unwrap();
        assert_eq!(order.status, Status::Filled);
        assert!(order.maker, "expected maker fill (no same-ts look-ahead)");
        assert_eq!(order.exec_price_tick, 100);
        Ok(())
    }

    #[test]
    fn exch_order_is_processed_after_exch_data_at_same_timestamp() -> Result<(), Box<dyn Error>> {
        // With `AfterExchData`, an order arriving exactly at the same exchange timestamp as a book
        // update will be processed after the update (pessimistic tie policy).
        let data = Data::from_data(&[
            Event {
                ev: EXCH_ASK_DEPTH_SNAPSHOT_EVENT,
                exch_ts: 0,
                local_ts: 0,
                px: 101.0,
                qty: 1.0,
                order_id: 0,
                ival: 0,
                fval: 0.0,
            },
            Event {
                ev: EXCH_ASK_DEPTH_SNAPSHOT_EVENT,
                exch_ts: 1,
                local_ts: 1,
                px: 99.0,
                qty: 1.0,
                order_id: 0,
                ival: 0,
                fval: 0.0,
            },
        ]);

        let mut backtester = Backtest::builder()
            .add_asset({
                let mut asset = L2AssetBuilder::default()
                    .data(vec![DataSource::Data(data)])
                    // Order sent at local_ts=0 arrives at exch_ts=1.
                    .latency_model(ConstantLatency::new(1, 0))
                    .asset_type(LinearAsset::new(1.0))
                    .fee_model(TradingValueFeeModel::new(CommonFees::new(0.0, 0.0)))
                    .queue_model(RiskAdverseQueueModel::new())
                    .exchange(NoPartialFillExchange)
                    .depth(|| HashMapMarketDepth::new(1.0, 1.0))
                    .build()?;
                // Enable `(timestamp, seq)` tie-breaking so `ExchOrderEqualTsPolicy` is active.
                asset.use_seq_tie_break = true;
                asset
            })
            .exch_order_equal_ts_policy(ExchOrderEqualTsPolicy::AfterExchData)
            .build()?;

        // Seed exchange depth at exch_ts=0 with best ask=101.
        backtester.elapse_bt(0)?;

        // With the pessimistic ordering, the order sees the post-update ask=99 when it arrives at
        // exch_ts=1 and becomes taker at 99.
        backtester.submit_buy_order(
            0,
            1,
            100.0,
            1.0,
            TimeInForce::GTC,
            OrdType::Limit,
            true,
        )?;

        let order = backtester.orders(0).get(&1).unwrap();
        assert_eq!(order.status, Status::Filled);
        assert!(!order.maker, "expected taker fill under AfterExchData");
        assert_eq!(order.exec_price_tick, 99);
        Ok(())
    }

    #[test]
    fn exch_order_equal_ts_policy_random_seeded_is_deterministic(
    ) -> Result<(), Box<dyn Error>> {
        // Choose a seed whose first SplitMix64 output selects the AfterExchData branch, so this
        // test fails if RandomSeeded silently degenerates to BeforeExchData.
        let seed = 1;
        let mut rng = super::SplitMix64::new(seed);
        let before = (rng.next_u64() & 1) == 0;

        let data = Data::from_data(&[
            Event {
                ev: EXCH_ASK_DEPTH_SNAPSHOT_EVENT,
                exch_ts: 0,
                local_ts: 0,
                px: 101.0,
                qty: 1.0,
                order_id: 0,
                ival: 0,
                fval: 0.0,
            },
            Event {
                ev: EXCH_ASK_DEPTH_SNAPSHOT_EVENT,
                exch_ts: 1,
                local_ts: 1,
                px: 99.0,
                qty: 1.0,
                order_id: 0,
                ival: 0,
                fval: 0.0,
            },
        ]);

        let mut backtester = Backtest::builder()
            .add_asset({
                let mut asset = L2AssetBuilder::default()
                    .data(vec![DataSource::Data(data)])
                    // Order sent at local_ts=0 arrives at exch_ts=1.
                    .latency_model(ConstantLatency::new(1, 0))
                    .asset_type(LinearAsset::new(1.0))
                    .fee_model(TradingValueFeeModel::new(CommonFees::new(0.0, 0.0)))
                    .queue_model(RiskAdverseQueueModel::new())
                    .exchange(NoPartialFillExchange)
                    .depth(|| HashMapMarketDepth::new(1.0, 1.0))
                    .build()?;
                // Enable `(timestamp, seq)` tie-breaking so `ExchOrderEqualTsPolicy` is active.
                asset.use_seq_tie_break = true;
                asset
            })
            .exch_order_equal_ts_policy(ExchOrderEqualTsPolicy::RandomSeeded { seed })
            .build()?;

        backtester.elapse_bt(0)?;

        backtester.submit_buy_order(
            0,
            1,
            100.0,
            1.0,
            TimeInForce::GTC,
            OrdType::Limit,
            true,
        )?;

        let order = backtester.orders(0).get(&1).unwrap();
        assert_eq!(order.status, Status::Filled);
        if before {
            assert!(
                order.maker,
                "expected maker fill under RandomSeeded (BeforeExchData branch)"
            );
            assert_eq!(order.exec_price_tick, 100);
        } else {
            assert!(
                !order.maker,
                "expected taker fill under RandomSeeded (AfterExchData branch)"
            );
            assert_eq!(order.exec_price_tick, 99);
        }
        Ok(())
    }

    #[test]
    #[ignore = "debug-only trace dump; run manually with `cargo test -- --ignored --nocapture`"]
    fn exch_order_equal_ts_policy_tie_break_trace_dump() {
        fn kind_str(kind: EventIntentKind) -> &'static str {
            match kind {
                EventIntentKind::LocalData => "LocalData",
                EventIntentKind::LocalOrder => "LocalOrder",
                EventIntentKind::ExchOrder => "ExchOrder",
                EventIntentKind::ExchData => "ExchData",
            }
        }

        fn dump(label: &str, evs: &EventSet) {
            let (ld_ts, ld_seq) = evs.debug_slot(0, EventIntentKind::LocalData);
            let (lo_ts, lo_seq) = evs.debug_slot(0, EventIntentKind::LocalOrder);
            let (eo_ts, eo_seq) = evs.debug_slot(0, EventIntentKind::ExchOrder);
            let (ed_ts, ed_seq) = evs.debug_slot(0, EventIntentKind::ExchData);
            println!(
                "{label}: LD=({ld_ts},{ld_seq}) LO=({lo_ts},{lo_seq}) EO=({eo_ts},{eo_seq}) ED=({ed_ts},{ed_seq})"
            );
        }

        fn run_case(label: &str, policy: ExchOrderEqualTsPolicy) {
            let mut evs = EventSet::new(1, true);
            // Disable other streams.
            evs.update_local_data(0, i64::MAX, i64::MAX);
            evs.update_local_order(0, i64::MAX, i64::MAX);

            // A deterministic "order arrives at 100, feed currently at 50 then advances to 100"
            // shape. This is the case where RandomSeeded used to silently degenerate to
            // BeforeExchData (because the tie only appears after ExchData advances).
            let order_ts = 100;
            let mut rng = match policy {
                ExchOrderEqualTsPolicy::RandomSeeded { seed } => Some(super::SplitMix64::new(seed)),
                _ => None,
            };
            let mut random_tie_ts = vec![i64::MIN; 1];
            let mut random_tie_seq = vec![i64::MIN; 1];

            // Stage 1: ExchData is at 50, so there is no tie yet.
            evs.update_exch_data(0, 50, 1);
            let eo_seq = Backtest::<HashMapMarketDepth>::compute_exch_order_seq(
                policy,
                &mut rng,
                &mut random_tie_ts,
                &mut random_tie_seq,
                0,
                order_ts,
                50,
            );
            evs.update_exch_order(0, order_ts, eo_seq);

            dump(&format!("{label}/stage1"), &evs);
            let next = evs.next().expect("expected a next event");
            println!("{label}/stage1 next = {} @ {}", kind_str(next.kind), next.timestamp);

            // Stage 2: ExchData advances to 100, which creates the exact-ns tie.
            evs.update_exch_data(0, 100, 2);
            let eo_seq = Backtest::<HashMapMarketDepth>::compute_exch_order_seq(
                policy,
                &mut rng,
                &mut random_tie_ts,
                &mut random_tie_seq,
                0,
                order_ts,
                100,
            );
            evs.update_exch_order(0, order_ts, eo_seq);

            dump(&format!("{label}/stage2"), &evs);
            let next = evs.next().expect("expected a next event");
            println!("{label}/stage2 next = {} @ {}", kind_str(next.kind), next.timestamp);
        }

        run_case("BeforeExchData", ExchOrderEqualTsPolicy::BeforeExchData);
        run_case("AfterExchData", ExchOrderEqualTsPolicy::AfterExchData);
        // Seed chosen so the first SplitMix64 sample selects the AfterExchData branch.
        run_case(
            "RandomSeeded(seed=1)",
            ExchOrderEqualTsPolicy::RandomSeeded { seed: 1 },
        );
    }

    #[test]
    fn elapse_rejects_negative_duration() -> Result<(), Box<dyn Error>> {
        let data = Data::from_data(&[Event {
            ev: EXCH_EVENT,
            exch_ts: 0,
            local_ts: 0,
            px: 0.0,
            qty: 0.0,
            order_id: 0,
            ival: 0,
            fval: 0.0,
        }]);

        let mut backtester = Backtest::builder()
            .add_asset(
                L2AssetBuilder::default()
                    .data(vec![DataSource::Data(data)])
                    .latency_model(ConstantLatency::new(0, 0))
                    .asset_type(LinearAsset::new(1.0))
                    .fee_model(TradingValueFeeModel::new(CommonFees::new(0.0, 0.0)))
                    .queue_model(RiskAdverseQueueModel::new())
                    .exchange(NoPartialFillExchange)
                    .depth(|| HashMapMarketDepth::new(1.0, 1.0))
                    .build()?,
            )
            .build()?;

        let err = backtester.elapse_bt(-1).unwrap_err();
        match err {
            BacktestError::DataError(io_err) => assert_eq!(io_err.kind(), ErrorKind::InvalidInput),
            _ => panic!("expected invalid input error, got: {err:?}"),
        }
        Ok(())
    }

    #[test]
    fn elapse_does_not_overflow_target_timestamp() -> Result<(), Box<dyn Error>> {
        let data = Data::from_data(&[Event {
            ev: EXCH_EVENT,
            exch_ts: i64::MAX - 1,
            local_ts: i64::MAX - 1,
            px: 0.0,
            qty: 0.0,
            order_id: 0,
            ival: 0,
            fval: 0.0,
        }]);

        let mut backtester = Backtest::builder()
            .add_asset(
                L2AssetBuilder::default()
                    .data(vec![DataSource::Data(data)])
                    .latency_model(ConstantLatency::new(0, 0))
                    .asset_type(LinearAsset::new(1.0))
                    .fee_model(TradingValueFeeModel::new(CommonFees::new(0.0, 0.0)))
                    .queue_model(RiskAdverseQueueModel::new())
                    .exchange(NoPartialFillExchange)
                    .depth(|| HashMapMarketDepth::new(1.0, 1.0))
                    .build()?,
            )
            .build()?;

        let result = backtester.elapse_bt(10)?;
        assert_eq!(result, ElapseResult::EndOfData);
        Ok(())
    }

    #[test]
    fn rejects_reserved_i64_max_event_timestamp() -> Result<(), Box<dyn Error>> {
        let data = Data::from_data(&[Event {
            ev: EXCH_EVENT,
            exch_ts: i64::MAX,
            local_ts: i64::MAX,
            px: 0.0,
            qty: 0.0,
            order_id: 0,
            ival: 0,
            fval: 0.0,
        }]);

        let mut backtester = Backtest::builder()
            .add_asset(
                L2AssetBuilder::default()
                    .data(vec![DataSource::Data(data)])
                    .latency_model(ConstantLatency::new(0, 0))
                    .asset_type(LinearAsset::new(1.0))
                    .fee_model(TradingValueFeeModel::new(CommonFees::new(0.0, 0.0)))
                    .queue_model(RiskAdverseQueueModel::new())
                    .exchange(NoPartialFillExchange)
                    .depth(|| HashMapMarketDepth::new(1.0, 1.0))
                    .build()?,
            )
            .build()?;

        let err = backtester.elapse_bt(0).unwrap_err();
        match err {
            BacktestError::DataError(io_err) => assert_eq!(io_err.kind(), ErrorKind::InvalidData),
            _ => panic!("expected invalid data error, got: {err:?}"),
        }
        Ok(())
    }
}
