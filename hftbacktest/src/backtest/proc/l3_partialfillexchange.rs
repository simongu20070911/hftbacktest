use std::collections::HashMap;

use crate::{
    backtest::{
        BacktestError,
        assettype::AssetType,
        models::{FeeModel, L3QueueModel, LatencyModel},
        order::ExchToLocal,
        proc::{Processor, TriggerOrderKind, TriggerOrderParams},
        state::State,
    },
    depth::L3MarketDepth,
    prelude::OrdType,
    types::{
        BUY_EVENT,
        EXCH_ASK_ADD_ORDER_EVENT,
        EXCH_ASK_DEPTH_CLEAR_EVENT,
        EXCH_BID_ADD_ORDER_EVENT,
        EXCH_BID_DEPTH_CLEAR_EVENT,
        EXCH_CANCEL_ORDER_EVENT,
        EXCH_DEPTH_CLEAR_EVENT,
        EXCH_EVENT,
        EXCH_FILL_EVENT,
        EXCH_MODIFY_ORDER_EVENT,
        EXCH_TRADE_EVENT,
        Event,
        Order,
        OrderId,
        SELL_EVENT,
        Side,
        Status,
        TimeInForce,
    },
};

/// The exchange model with partial fills for L3 (Market-By-Order).
///
/// This is intended for CME via Databento MBO backtesting:
/// - Uses `EXCH_FILL_EVENT` quantity as an execution budget (partial fills).
/// - Caps taker fills by visible top-of-book liquidity (no infinite liquidity).
/// - Does not fill maker orders solely due to best-price crossing; executions are driven by fill
///   events.
#[derive(Clone)]
struct HeldTriggerOrder {
    order: Order,
    params: TriggerOrderParams,
}

struct PendingTriggerGroup {
    exch_ts: i64,
    local_ts: i64,
    orders: Vec<HeldTriggerOrder>,
}

pub struct L3PartialFillExchange<AT, LM, QM, MD, FM>
where
    AT: AssetType,
    LM: LatencyModel,
    QM: L3QueueModel<MD>,
    MD: L3MarketDepth,
    FM: FeeModel,
{
    depth: MD,
    state: State<AT, FM>,
    queue_model: QM,
    order_e2l: ExchToLocal<LM>,
    order_not_found_reject_marks_inactive: bool,
    trigger_orders: HashMap<OrderId, HeldTriggerOrder>,
    pending_trigger_group: Option<PendingTriggerGroup>,
}

impl<AT, LM, QM, MD, FM> L3PartialFillExchange<AT, LM, QM, MD, FM>
where
    AT: AssetType,
    LM: LatencyModel,
    QM: L3QueueModel<MD>,
    MD: L3MarketDepth,
    FM: FeeModel,
    BacktestError: From<<MD as L3MarketDepth>::Error>,
{
    pub fn new(
        depth: MD,
        state: State<AT, FM>,
        queue_model: QM,
        order_e2l: ExchToLocal<LM>,
    ) -> Self {
        Self {
            depth,
            state,
            queue_model,
            order_e2l,
            order_not_found_reject_marks_inactive: false,
            trigger_orders: HashMap::new(),
            pending_trigger_group: None,
        }
    }

    pub fn with_order_not_found_reject_marks_inactive(mut self, enabled: bool) -> Self {
        self.order_not_found_reject_marks_inactive = enabled;
        self
    }

    fn reject_order_not_found(&self, order: &mut Order, timestamp: i64) {
        order.req = Status::Rejected;
        order.exch_timestamp = timestamp;
        if self.order_not_found_reject_marks_inactive {
            order.exec_qty = 0.0;
            order.leaves_qty = 0.0;
            order.status = Status::Expired;
        }
    }

    fn expired(&mut self, mut order: Order, timestamp: i64) -> Result<(), BacktestError> {
        order.exec_qty = 0.0;
        order.leaves_qty = 0.0;
        order.status = Status::Expired;
        order.exch_timestamp = timestamp;

        self.order_e2l.respond(order);
        Ok(())
    }

    fn try_extract_trigger_params(order: &Order) -> Option<TriggerOrderParams> {
        order
            .q
            .as_any()
            .downcast_ref::<TriggerOrderParams>()
            .cloned()
    }

    fn trade_triggers(held: &HeldTriggerOrder, trade_tick: i64) -> bool {
        let trigger_tick = held.params.trigger_tick;
        match held.params.kind {
            TriggerOrderKind::StopMarket | TriggerOrderKind::StopLimit => match held.order.side {
                Side::Buy => trade_tick >= trigger_tick,
                Side::Sell => trade_tick <= trigger_tick,
                Side::None | Side::Unsupported => false,
            },
            TriggerOrderKind::Mit => match held.order.side {
                Side::Buy => trade_tick <= trigger_tick,
                Side::Sell => trade_tick >= trigger_tick,
                Side::None | Side::Unsupported => false,
            },
        }
    }

    fn flush_pending_trigger_group(&mut self) -> Result<(), BacktestError> {
        let Some(pending) = self.pending_trigger_group.take() else {
            return Ok(());
        };

        for held in pending.orders {
            // Use a fresh request instance for the activated child order.
            let mut child = held.order.clone();
            child.q = Box::new(());
            child.req = Status::None;

            match held.params.kind {
                TriggerOrderKind::StopLimit => child.order_type = OrdType::Limit,
                TriggerOrderKind::StopMarket | TriggerOrderKind::Mit => child.order_type = OrdType::Market,
            }

            self.ack_new(&mut child, pending.exch_ts)?;

            self.order_e2l.respond(child);
        }

        Ok(())
    }

    fn fill_exec<const MAKE_RESPONSE: bool>(
        &mut self,
        order: &mut Order,
        timestamp: i64,
        maker: bool,
        exec_price_tick: i64,
        exec_qty: f64,
    ) -> Result<(), BacktestError> {
        if order.status == Status::Expired
            || order.status == Status::Canceled
            || order.status == Status::Filled
        {
            return Err(BacktestError::InvalidOrderStatus);
        }

        let exec_qty = exec_qty.max(0.0);
        if exec_qty == 0.0 {
            return Ok(());
        }

        order.maker = maker;
        order.exec_price_tick = if maker { order.price_tick } else { exec_price_tick };

        order.exec_qty = exec_qty;
        order.leaves_qty -= exec_qty;
        if (order.leaves_qty / self.depth.lot_size()).round() > 0.0 {
            order.status = Status::PartiallyFilled;
        } else {
            order.leaves_qty = 0.0;
            order.status = Status::Filled;
        }
        order.exch_timestamp = timestamp;

        self.state.apply_fill(order);

        if MAKE_RESPONSE {
            self.order_e2l.respond(order.clone());
            // Reset transient fields so subsequent acks don't leak exec info.
            order.exec_qty = 0.0;
            order.exec_price_tick = 0;
            order.maker = false;
        }
        Ok(())
    }

    fn taker_fill_at_best<const KEEP_REMAINDER: bool>(
        &mut self,
        order: &mut Order,
        timestamp: i64,
        best_tick: i64,
        best_qty: f64,
    ) -> Result<(), BacktestError> {
        let fill_qty = order.leaves_qty.min(best_qty.max(0.0));

        // FOK is all-or-nothing at our modeled liquidity.
        if order.time_in_force == TimeInForce::FOK && fill_qty < order.leaves_qty {
            order.exec_qty = 0.0;
            order.leaves_qty = 0.0;
            order.status = Status::Expired;
            order.exch_timestamp = timestamp;
            return Ok(());
        }

        if fill_qty > 0.0 {
            self.fill_exec::<false>(order, timestamp, false, best_tick, fill_qty)?;
        }

        if KEEP_REMAINDER && order.status == Status::PartiallyFilled {
            // Keep remaining quantity resting.
            let mut resting = order.clone();
            resting.exec_qty = 0.0;
            resting.exec_price_tick = 0;
            resting.maker = false;
            resting.status = Status::New;
            resting.req = Status::None;
            self.queue_model.add_backtest_order(resting, &self.depth)?;
            return Ok(());
        }

        // Otherwise, cancel the remainder (IOC/Market semantics) or expire if nothing filled.
        if order.status == Status::PartiallyFilled {
            order.exec_qty = fill_qty;
            order.leaves_qty = 0.0;
            order.status = Status::Canceled;
            order.exch_timestamp = timestamp;
        } else if order.status != Status::Filled {
            order.exec_qty = 0.0;
            order.leaves_qty = 0.0;
            order.status = Status::Expired;
            order.exch_timestamp = timestamp;
        }
        Ok(())
    }

    fn ack_new(&mut self, order: &mut Order, timestamp: i64) -> Result<(), BacktestError> {
        if self.queue_model.contains_backtest_order(order.order_id) {
            return Err(BacktestError::OrderIdExist);
        }

        // Server-side trigger orders (Stop-Market / Stop-Limit / MIT) are accepted and held by the
        // exchange until a trade triggers them.
        if let Some(params) = Self::try_extract_trigger_params(order) {
            order.status = Status::New;
            order.exch_timestamp = timestamp;
            self.trigger_orders.insert(
                order.order_id,
                HeldTriggerOrder {
                    order: order.clone(),
                    params,
                },
            );
            return Ok(());
        }

        match order.side {
            Side::Buy => match order.order_type {
                OrdType::Limit => {
                    if order.price_tick >= self.depth.best_ask_tick() {
                        match order.time_in_force {
                            TimeInForce::GTX => {
                                order.status = Status::Expired;
                                order.leaves_qty = 0.0;
                                order.exch_timestamp = timestamp;
                                Ok(())
                            }
                            TimeInForce::GTC => self.taker_fill_at_best::<true>(
                                order,
                                timestamp,
                                self.depth.best_ask_tick(),
                                self.depth.best_ask_qty(),
                            ),
                            TimeInForce::IOC | TimeInForce::FOK => self.taker_fill_at_best::<false>(
                                order,
                                timestamp,
                                self.depth.best_ask_tick(),
                                self.depth.best_ask_qty(),
                            ),
                            TimeInForce::Unsupported => Err(BacktestError::InvalidOrderRequest),
                        }
                    } else {
                        match order.time_in_force {
                            TimeInForce::GTC | TimeInForce::GTX => {
                                order.status = Status::New;
                                order.exch_timestamp = timestamp;
                                self.queue_model.add_backtest_order(order.clone(), &self.depth)?;
                                Ok(())
                            }
                            TimeInForce::FOK | TimeInForce::IOC => {
                                order.status = Status::Expired;
                                order.leaves_qty = 0.0;
                                order.exch_timestamp = timestamp;
                                Ok(())
                            }
                            TimeInForce::Unsupported => Err(BacktestError::InvalidOrderRequest),
                        }
                    }
                }
                OrdType::Market => self.taker_fill_at_best::<false>(
                    order,
                    timestamp,
                    self.depth.best_ask_tick(),
                    self.depth.best_ask_qty(),
                ),
                OrdType::Unsupported => Err(BacktestError::InvalidOrderRequest),
            },
            Side::Sell => match order.order_type {
                OrdType::Limit => {
                    if order.price_tick <= self.depth.best_bid_tick() {
                        match order.time_in_force {
                            TimeInForce::GTX => {
                                order.status = Status::Expired;
                                order.leaves_qty = 0.0;
                                order.exch_timestamp = timestamp;
                                Ok(())
                            }
                            TimeInForce::GTC => self.taker_fill_at_best::<true>(
                                order,
                                timestamp,
                                self.depth.best_bid_tick(),
                                self.depth.best_bid_qty(),
                            ),
                            TimeInForce::IOC | TimeInForce::FOK => self.taker_fill_at_best::<false>(
                                order,
                                timestamp,
                                self.depth.best_bid_tick(),
                                self.depth.best_bid_qty(),
                            ),
                            TimeInForce::Unsupported => Err(BacktestError::InvalidOrderRequest),
                        }
                    } else {
                        match order.time_in_force {
                            TimeInForce::GTC | TimeInForce::GTX => {
                                order.status = Status::New;
                                order.exch_timestamp = timestamp;
                                self.queue_model.add_backtest_order(order.clone(), &self.depth)?;
                                Ok(())
                            }
                            TimeInForce::FOK | TimeInForce::IOC => {
                                order.status = Status::Expired;
                                order.leaves_qty = 0.0;
                                order.exch_timestamp = timestamp;
                                Ok(())
                            }
                            TimeInForce::Unsupported => Err(BacktestError::InvalidOrderRequest),
                        }
                    }
                }
                OrdType::Market => self.taker_fill_at_best::<false>(
                    order,
                    timestamp,
                    self.depth.best_bid_tick(),
                    self.depth.best_bid_qty(),
                ),
                OrdType::Unsupported => Err(BacktestError::InvalidOrderRequest),
            },
            Side::None | Side::Unsupported => unreachable!(),
        }
    }

    fn ack_cancel(&mut self, order: &mut Order, timestamp: i64) -> Result<(), BacktestError> {
        let req_local_timestamp = order.local_timestamp;

        if self.trigger_orders.remove(&order.order_id).is_some() {
            order.local_timestamp = req_local_timestamp;
            order.exec_qty = 0.0;
            order.exec_price_tick = 0;
            order.maker = false;
            order.leaves_qty = 0.0;
            order.status = Status::Canceled;
            order.exch_timestamp = timestamp;
            return Ok(());
        }

        match self
            .queue_model
            .cancel_backtest_order(order.order_id, &self.depth)
        {
            Ok(exch_order) => {
                let _ = std::mem::replace(order, exch_order);

                order.local_timestamp = req_local_timestamp;
                order.exec_qty = 0.0;
                order.exec_price_tick = 0;
                order.maker = false;
                order.leaves_qty = 0.0;
                order.status = Status::Canceled;
                order.exch_timestamp = timestamp;
                Ok(())
            }
            Err(BacktestError::OrderNotFound) => {
                self.reject_order_not_found(order, timestamp);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    fn ack_modify<const RESET_QUEUE_POS: bool>(
        &mut self,
        order: &mut Order,
        timestamp: i64,
    ) -> Result<(), BacktestError> {
        if let Some(held) = self.trigger_orders.get_mut(&order.order_id) {
            let Some(params) = Self::try_extract_trigger_params(order) else {
                return Err(BacktestError::InvalidOrderRequest);
            };
            if params.kind != held.params.kind {
                return Err(BacktestError::InvalidOrderRequest);
            }

            held.params = params;
            held.order.update(order);

            order.leaves_qty = order.qty;
            order.exch_timestamp = timestamp;
            order.status = Status::New;
            return Ok(());
        }

        // Replace that becomes marketable should be executed (or GTX-expired) immediately.
        // Process it as cancel+new so it uses the same marketability checks as `ack_new`.
        let marketable_after_replace = match (order.side, order.order_type) {
            (Side::Buy, OrdType::Limit) => order.price_tick >= self.depth.best_ask_tick(),
            (Side::Sell, OrdType::Limit) => order.price_tick <= self.depth.best_bid_tick(),
            (Side::Buy, OrdType::Market) | (Side::Sell, OrdType::Market) => true,
            _ => false,
        };
        if marketable_after_replace {
            match self
                .queue_model
                .cancel_backtest_order(order.order_id, &self.depth)
            {
                Ok(_) => {
                    order.leaves_qty = order.qty;
                    return self.ack_new(order, timestamp);
                }
                Err(BacktestError::OrderNotFound) => {
                    self.reject_order_not_found(order, timestamp);
                    return Ok(());
                }
                Err(e) => return Err(e),
            }
        }

        match self
            .queue_model
            .modify_backtest_order(order.order_id, order, &self.depth)
        {
            Ok(()) => {
                order.leaves_qty = order.qty;
                order.exch_timestamp = timestamp;
                order.status = Status::New;
                Ok(())
            }
            Err(BacktestError::OrderNotFound) => {
                self.reject_order_not_found(order, timestamp);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }
}

impl<AT, LM, QM, MD, FM> Processor for L3PartialFillExchange<AT, LM, QM, MD, FM>
where
    AT: AssetType,
    LM: LatencyModel,
    QM: L3QueueModel<MD>,
    MD: L3MarketDepth,
    FM: FeeModel,
    BacktestError: From<<MD as L3MarketDepth>::Error>,
{
    fn event_seen_timestamp(&self, event: &Event) -> Option<i64> {
        event.is(EXCH_EVENT).then_some(event.exch_ts)
    }

    fn process(&mut self, event: &Event) -> Result<(), BacktestError> {
        // If we deferred trigger activation waiting for a paired `EXCH_FILL_EVENT` at the same
        // (exch_ts, local_ts), flush as soon as we advance to a different timestamp-group.
        if let Some(pending) = &self.pending_trigger_group
            && (pending.exch_ts != event.exch_ts || pending.local_ts != event.local_ts)
        {
            self.flush_pending_trigger_group()?;
        }

        if event.is(EXCH_BID_DEPTH_CLEAR_EVENT) {
            self.depth.clear_orders(Side::Buy);
            let expired = self.queue_model.clear_orders(Side::Buy);
            for order in expired {
                self.expired(order, event.exch_ts)?;
            }
        } else if event.is(EXCH_ASK_DEPTH_CLEAR_EVENT) {
            self.depth.clear_orders(Side::Sell);
            let expired = self.queue_model.clear_orders(Side::Sell);
            for order in expired {
                self.expired(order, event.exch_ts)?;
            }
        } else if event.is(EXCH_DEPTH_CLEAR_EVENT) {
            self.depth.clear_orders(Side::None);
            let expired = self.queue_model.clear_orders(Side::None);
            for order in expired {
                self.expired(order, event.exch_ts)?;
            }
        } else if event.is(EXCH_BID_ADD_ORDER_EVENT) {
            self.depth
                .add_buy_order(event.order_id, event.px, event.qty, event.exch_ts)?;
            self.queue_model.add_market_feed_order(event, &self.depth)?;
        } else if event.is(EXCH_ASK_ADD_ORDER_EVENT) {
            self.depth
                .add_sell_order(event.order_id, event.px, event.qty, event.exch_ts)?;
            self.queue_model.add_market_feed_order(event, &self.depth)?;
        } else if event.is(EXCH_MODIFY_ORDER_EVENT) {
            self.depth
                .modify_order(event.order_id, event.px, event.qty, event.exch_ts)?;
            self.queue_model
                .modify_market_feed_order(event.order_id, event, &self.depth)?;
        } else if event.is(EXCH_CANCEL_ORDER_EVENT) {
            let order_id = event.order_id;
            self.depth.delete_order(order_id, event.exch_ts)?;
            self.queue_model
                .cancel_market_feed_order(event.order_id, &self.depth)?;
        } else if event.is(EXCH_TRADE_EVENT) {
            // Trade-triggered server-side orders (Stop / MIT).
            let trade_tick = (event.px / self.depth.tick_size()).round() as i64;

            let mut triggered_ids: Vec<OrderId> = self
                .trigger_orders
                .iter()
                .filter_map(|(&order_id, held)| {
                    Self::trade_triggers(held, trade_tick).then_some(order_id)
                })
                .collect();
            triggered_ids.sort_by_key(|order_id| {
                let held = &self.trigger_orders[order_id];
                (held.order.exch_timestamp, *order_id)
            });

            if !triggered_ids.is_empty() {
                let pending = self
                    .pending_trigger_group
                    .get_or_insert(PendingTriggerGroup {
                        exch_ts: event.exch_ts,
                        local_ts: event.local_ts,
                        orders: Vec::new(),
                    });
                debug_assert_eq!(pending.exch_ts, event.exch_ts);
                debug_assert_eq!(pending.local_ts, event.local_ts);
                for order_id in triggered_ids {
                    let held = self.trigger_orders.remove(&order_id).unwrap();
                    pending.orders.push(held);
                }
            }
        } else if event.is(EXCH_FILL_EVENT) {
            if event.is(BUY_EVENT) || event.is(SELL_EVENT) {
                let filled = self.queue_model.fill_market_feed_order_budgeted::<false>(
                    event.order_id,
                    event,
                    &self.depth,
                    event.qty,
                )?;
                for (mut order, exec_qty) in filled {
                    let price_tick = order.price_tick;
                    self.fill_exec::<true>(&mut order, event.exch_ts, true, price_tick, exec_qty)?;
                }
            }

            if let Some(pending) = &self.pending_trigger_group
                && pending.exch_ts == event.exch_ts
                && pending.local_ts == event.local_ts
            {
                // Activate after the paired fill event so newly-triggered orders cannot be filled
                // by the same execution record.
                self.flush_pending_trigger_group()?;
            }
        }

        Ok(())
    }

    fn process_recv_order(
        &mut self,
        timestamp: i64,
        _wait_resp_order_id: Option<OrderId>,
    ) -> Result<bool, BacktestError> {
        while let Some(mut order) = self.order_e2l.receive(timestamp) {
            if order.req == Status::New {
                order.req = Status::None;
                self.ack_new(&mut order, timestamp)?;
            } else if order.req == Status::Canceled {
                order.req = Status::None;
                self.ack_cancel(&mut order, timestamp)?;
            } else if order.req == Status::Replaced {
                order.req = Status::None;
                self.ack_modify::<false>(&mut order, timestamp)?;
            } else {
                return Err(BacktestError::InvalidOrderRequest);
            }
            self.order_e2l.respond(order);
        }
        Ok(false)
    }

    fn earliest_recv_order_timestamp(&self) -> i64 {
        self.order_e2l
            .earliest_recv_order_timestamp()
            .unwrap_or(i64::MAX)
    }

    fn earliest_send_order_timestamp(&self) -> i64 {
        self.order_e2l
            .earliest_send_order_timestamp()
            .unwrap_or(i64::MAX)
    }
}

#[cfg(test)]
mod tests {
    use super::L3PartialFillExchange;
    use crate::{
        backtest::{
            assettype::LinearAsset,
            models::{CommonFees, ConstantLatency, L3FIFOQueueModel, TradingValueFeeModel},
            order::order_bus,
            proc::{L3Local, LocalProcessor, Processor},
            state::State,
            L3QueueModel,
        },
        depth::HashMapMarketDepth,
        types::{
            BUY_EVENT,
            EXCH_ASK_ADD_ORDER_EVENT,
            EXCH_FILL_EVENT,
            EXCH_TRADE_EVENT,
            Event,
            SELL_EVENT,
            Side,
            Status,
            TimeInForce,
        },
    };

    fn make_state() -> State<LinearAsset, TradingValueFeeModel<CommonFees>> {
        State::new(
            LinearAsset::new(1.0),
            TradingValueFeeModel::new(CommonFees::new(0.0, 0.0)),
        )
    }

    fn make_depth() -> HashMapMarketDepth {
        HashMapMarketDepth::new(/* tick_size */ 1.0, /* lot_size */ 1.0)
    }

    #[test]
    fn stop_market_triggers_after_paired_fill_event() {
        let (order_e2l, order_l2e) = order_bus(ConstantLatency::new(/* entry */ 0, /* resp */ 0));

        let mut local = L3Local::new(make_depth(), make_state(), 0, order_l2e);
        let mut exch = L3PartialFillExchange::new(
            make_depth(),
            make_state(),
            L3FIFOQueueModel::new(),
            order_e2l,
        );

        // Best ask = 101, qty = 2 (visible top-of-book liquidity).
        let mkt_ask_order_id = 10;
        exch.process(&Event {
            ev: EXCH_ASK_ADD_ORDER_EVENT,
            exch_ts: 1,
            local_ts: 101,
            px: 101.0,
            qty: 2.0,
            order_id: mkt_ask_order_id,
            ival: 0,
            fval: 0.0,
        })
        .unwrap();

        // Submit a buy Stop-Market that triggers on trade >= 104.
        let order_id = 1;
        local
            .submit_stop_market(
                order_id,
                Side::Buy,
                /* trigger */ 104.0,
                /* qty */ 1.0,
                TimeInForce::IOC,
                /* now */ 0,
            )
            .unwrap();
        exch.process_recv_order(0, None).unwrap();
        local.process_recv_order(0, None).unwrap();
        assert_eq!(local.orders().get(&order_id).unwrap().status, Status::New);

        // Trade triggers the stop, but activation must be deferred until the paired fill at the
        // same (exch_ts, local_ts) has been processed.
        exch.process(&Event {
            ev: EXCH_TRADE_EVENT | BUY_EVENT,
            exch_ts: 10,
            local_ts: 100,
            px: 105.0,
            qty: 1.0,
            order_id: mkt_ask_order_id,
            ival: 0,
            fval: 0.0,
        })
        .unwrap();
        local.process_recv_order(10, None).unwrap();
        assert_eq!(local.orders().get(&order_id).unwrap().exec_qty, 0.0);

        // Paired fill at the exact same timestamps.
        exch.process(&Event {
            ev: EXCH_FILL_EVENT | SELL_EVENT,
            exch_ts: 10,
            local_ts: 100,
            px: 101.0,
            qty: 1.0,
            order_id: mkt_ask_order_id,
            ival: 0,
            fval: 0.0,
        })
        .unwrap();

        // Stop-Market activates as a normal Market order at ts=10 and fills at the current best ask.
        local.process_recv_order(10, None).unwrap();
        let ord = local.orders().get(&order_id).unwrap();
        assert_eq!(ord.status, Status::Filled);
        assert_eq!(ord.exec_price_tick, 101);
        assert_eq!(ord.exec_qty, 1.0);
        assert_eq!(ord.leaves_qty, 0.0);
    }

    #[test]
    fn mit_triggers_after_paired_fill_event() {
        let (order_e2l, order_l2e) = order_bus(ConstantLatency::new(/* entry */ 0, /* resp */ 0));

        let mut local = L3Local::new(make_depth(), make_state(), 0, order_l2e);
        let mut exch = L3PartialFillExchange::new(
            make_depth(),
            make_state(),
            L3FIFOQueueModel::new(),
            order_e2l,
        );

        // Best ask = 101, qty = 2.
        let mkt_ask_order_id = 10;
        exch.process(&Event {
            ev: EXCH_ASK_ADD_ORDER_EVENT,
            exch_ts: 1,
            local_ts: 101,
            px: 101.0,
            qty: 2.0,
            order_id: mkt_ask_order_id,
            ival: 0,
            fval: 0.0,
        })
        .unwrap();

        // Buy MIT triggers on trade <= 96.
        let order_id = 1;
        local
            .submit_mit(
                order_id,
                Side::Buy,
                /* trigger */ 96.0,
                /* qty */ 1.0,
                TimeInForce::IOC,
                /* now */ 0,
            )
            .unwrap();
        exch.process_recv_order(0, None).unwrap();
        local.process_recv_order(0, None).unwrap();

        // Trade <= trigger; still defer activation until paired fill.
        exch.process(&Event {
            ev: EXCH_TRADE_EVENT | SELL_EVENT,
            exch_ts: 10,
            local_ts: 100,
            px: 95.0,
            qty: 1.0,
            order_id: mkt_ask_order_id,
            ival: 0,
            fval: 0.0,
        })
        .unwrap();
        local.process_recv_order(10, None).unwrap();
        assert_eq!(local.orders().get(&order_id).unwrap().exec_qty, 0.0);

        exch.process(&Event {
            ev: EXCH_FILL_EVENT | SELL_EVENT,
            exch_ts: 10,
            local_ts: 100,
            px: 101.0,
            qty: 1.0,
            order_id: mkt_ask_order_id,
            ival: 0,
            fval: 0.0,
        })
        .unwrap();

        local.process_recv_order(10, None).unwrap();
        let ord = local.orders().get(&order_id).unwrap();
        assert_eq!(ord.status, Status::Filled);
        assert_eq!(ord.exec_price_tick, 101);
        assert_eq!(ord.exec_qty, 1.0);
    }

    #[test]
    fn stop_limit_triggers_after_paired_fill_and_rests() {
        let (order_e2l, order_l2e) = order_bus(ConstantLatency::new(/* entry */ 0, /* resp */ 0));

        let mut local = L3Local::new(make_depth(), make_state(), 0, order_l2e);
        let mut exch = L3PartialFillExchange::new(
            make_depth(),
            make_state(),
            L3FIFOQueueModel::new(),
            order_e2l,
        );

        // Best ask = 101, qty = 2.
        let mkt_ask_order_id = 10;
        exch.process(&Event {
            ev: EXCH_ASK_ADD_ORDER_EVENT,
            exch_ts: 1,
            local_ts: 101,
            px: 101.0,
            qty: 2.0,
            order_id: mkt_ask_order_id,
            ival: 0,
            fval: 0.0,
        })
        .unwrap();

        // Buy Stop-Limit triggers on trade >= 104, then places a LIMIT at 99 (non-marketable).
        let order_id = 1;
        local
            .submit_stop_limit(
                order_id,
                Side::Buy,
                /* trigger */ 104.0,
                /* limit */ 99.0,
                /* qty */ 1.0,
                TimeInForce::GTC,
                /* now */ 0,
            )
            .unwrap();
        exch.process_recv_order(0, None).unwrap();
        local.process_recv_order(0, None).unwrap();
        assert!(
            !<L3FIFOQueueModel as L3QueueModel<HashMapMarketDepth>>::contains_backtest_order(
                &exch.queue_model,
                order_id
            )
        );

        // Trigger trade; still no activation until fill.
        exch.process(&Event {
            ev: EXCH_TRADE_EVENT | BUY_EVENT,
            exch_ts: 10,
            local_ts: 100,
            px: 105.0,
            qty: 1.0,
            order_id: mkt_ask_order_id,
            ival: 0,
            fval: 0.0,
        })
        .unwrap();
        assert!(
            !<L3FIFOQueueModel as L3QueueModel<HashMapMarketDepth>>::contains_backtest_order(
                &exch.queue_model,
                order_id
            )
        );

        exch.process(&Event {
            ev: EXCH_FILL_EVENT | SELL_EVENT,
            exch_ts: 10,
            local_ts: 100,
            px: 101.0,
            qty: 1.0,
            order_id: mkt_ask_order_id,
            ival: 0,
            fval: 0.0,
        })
        .unwrap();

        // Activated: should now be a resting limit order on the exchange.
        assert!(
            <L3FIFOQueueModel as L3QueueModel<HashMapMarketDepth>>::contains_backtest_order(
                &exch.queue_model,
                order_id
            )
        );
        local.process_recv_order(10, None).unwrap();
        let ord = local.orders().get(&order_id).unwrap();
        assert_eq!(ord.status, Status::New);
        assert_eq!(ord.exec_qty, 0.0);
    }
}
