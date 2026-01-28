use std::collections::{HashMap, hash_map::Entry};

use crate::{
    backtest::{
        BacktestError,
        assettype::AssetType,
        models::{FeeModel, LatencyModel},
        order::LocalToExch,
        proc::{LocalProcessor, Processor, TriggerOrderKind, TriggerOrderParams},
        state::State,
    },
    depth::L3MarketDepth,
    types::{
        Event, LOCAL_ASK_ADD_ORDER_EVENT, LOCAL_ASK_DEPTH_CLEAR_EVENT, LOCAL_BID_ADD_ORDER_EVENT,
        LOCAL_BID_DEPTH_CLEAR_EVENT, LOCAL_CANCEL_ORDER_EVENT, LOCAL_DEPTH_CLEAR_EVENT,
        LOCAL_EVENT, LOCAL_MODIFY_ORDER_EVENT, LOCAL_TRADE_EVENT, OrdType, Order, OrderId, Side,
        StateValues, Status, TimeInForce,
    },
};

#[derive(Clone, Debug)]
struct PendingReplaceOrig {
    price_tick: i64,
    qty: f64,
    leaves_qty: f64,
    trigger_params: Option<TriggerOrderParams>,
}

/// The Level3 Market-By-Order local model.
pub struct L3Local<AT, LM, MD, FM>
where
    AT: AssetType,
    LM: LatencyModel,
    MD: L3MarketDepth,
    FM: FeeModel,
{
    orders: HashMap<OrderId, Order>,
    /// Original order-field snapshot for an in-flight replace request.
    ///
    /// L3Local::modify() pre-mutates the local order to model the local's optimistic view.
    /// If the exchange later rejects the replace (e.g., OrderNotFound), we must restore the
    /// original fields to keep the local order state consistent with the exchange.
    pending_replace_orig: HashMap<OrderId, PendingReplaceOrig>,
    order_l2e: LocalToExch<LM>,
    depth: MD,
    state: State<AT, FM>,
    trades: Vec<Event>,
    last_feed_latency: Option<(i64, i64)>,
    last_order_latency: Option<(i64, i64, i64)>,
}

impl<AT, LM, MD, FM> L3Local<AT, LM, MD, FM>
where
    AT: AssetType,
    LM: LatencyModel,
    MD: L3MarketDepth,
    FM: FeeModel,
{
    /// Constructs an instance of `L3Local`.
    pub fn new(
        depth: MD,
        state: State<AT, FM>,
        trade_len: usize,
        order_l2e: LocalToExch<LM>,
    ) -> Self {
        Self {
            orders: Default::default(),
            pending_replace_orig: Default::default(),
            order_l2e,
            depth,
            state,
            trades: Vec::with_capacity(trade_len),
            last_feed_latency: None,
            last_order_latency: None,
        }
    }

    fn submit_common(
        &mut self,
        mut order: Order,
        current_timestamp: i64,
    ) -> Result<(), BacktestError> {
        if self.orders.contains_key(&order.order_id) {
            return Err(BacktestError::OrderIdExist);
        }

        order.req = Status::New;
        order.local_timestamp = current_timestamp;
        self.orders.insert(order.order_id, order.clone());

        order.exec_qty = 0.0;
        order.exec_price_tick = 0;
        order.maker = false;
        self.order_l2e.request(order, |order| {
            order.req = Status::Rejected;
            order.exec_qty = 0.0;
            order.exec_price_tick = 0;
            order.maker = false;
        });

        Ok(())
    }
}

impl<AT, LM, MD, FM> LocalProcessor<MD> for L3Local<AT, LM, MD, FM>
where
    AT: AssetType,
    LM: LatencyModel,
    MD: L3MarketDepth,
    FM: FeeModel,
    BacktestError: From<<MD as L3MarketDepth>::Error>,
{
    fn submit_order(
        &mut self,
        order_id: OrderId,
        side: Side,
        price: f64,
        qty: f64,
        order_type: OrdType,
        time_in_force: TimeInForce,
        current_timestamp: i64,
    ) -> Result<(), BacktestError> {
        let price_tick = (price / self.depth.tick_size()).round() as i64;
        let order = Order::new(
            order_id,
            price_tick,
            self.depth.tick_size(),
            qty,
            side,
            order_type,
            time_in_force,
        );
        self.submit_common(order, current_timestamp)
    }

    fn submit_stop_market(
        &mut self,
        order_id: OrderId,
        side: Side,
        trigger_price: f64,
        qty: f64,
        time_in_force: TimeInForce,
        current_timestamp: i64,
    ) -> Result<(), BacktestError> {
        let trigger_tick = (trigger_price / self.depth.tick_size()).round() as i64;
        let mut order = Order::new(
            order_id,
            /* price_tick */ trigger_tick,
            self.depth.tick_size(),
            qty,
            side,
            OrdType::Market,
            time_in_force,
        );
        order.q = Box::new(TriggerOrderParams {
            kind: TriggerOrderKind::StopMarket,
            trigger_tick,
        });
        self.submit_common(order, current_timestamp)
    }

    fn submit_mit(
        &mut self,
        order_id: OrderId,
        side: Side,
        trigger_price: f64,
        qty: f64,
        time_in_force: TimeInForce,
        current_timestamp: i64,
    ) -> Result<(), BacktestError> {
        let trigger_tick = (trigger_price / self.depth.tick_size()).round() as i64;
        let mut order = Order::new(
            order_id,
            /* price_tick */ trigger_tick,
            self.depth.tick_size(),
            qty,
            side,
            OrdType::Market,
            time_in_force,
        );
        order.q = Box::new(TriggerOrderParams {
            kind: TriggerOrderKind::Mit,
            trigger_tick,
        });
        self.submit_common(order, current_timestamp)
    }

    fn submit_stop_limit(
        &mut self,
        order_id: OrderId,
        side: Side,
        trigger_price: f64,
        limit_price: f64,
        qty: f64,
        time_in_force: TimeInForce,
        current_timestamp: i64,
    ) -> Result<(), BacktestError> {
        let trigger_tick = (trigger_price / self.depth.tick_size()).round() as i64;
        let limit_tick = (limit_price / self.depth.tick_size()).round() as i64;
        let mut order = Order::new(
            order_id,
            /* price_tick */ limit_tick,
            self.depth.tick_size(),
            qty,
            side,
            OrdType::Limit,
            time_in_force,
        );
        order.q = Box::new(TriggerOrderParams {
            kind: TriggerOrderKind::StopLimit,
            trigger_tick,
        });
        self.submit_common(order, current_timestamp)
    }

    fn modify_stop_limit(
        &mut self,
        order_id: OrderId,
        trigger_price: f64,
        limit_price: f64,
        qty: f64,
        current_timestamp: i64,
    ) -> Result<(), BacktestError> {
        let order = self
            .orders
            .get_mut(&order_id)
            .ok_or(BacktestError::OrderNotFound)?;

        if order.req != Status::None {
            return Err(BacktestError::OrderRequestInProcess);
        }

        let params = order
            .q
            .as_any()
            .downcast_ref::<TriggerOrderParams>()
            .ok_or(BacktestError::InvalidOrderRequest)?;
        if params.kind != TriggerOrderKind::StopLimit {
            return Err(BacktestError::InvalidOrderRequest);
        }

        let orig_params = params.clone();
        let orig = PendingReplaceOrig {
            price_tick: order.price_tick,
            qty: order.qty,
            leaves_qty: order.leaves_qty,
            trigger_params: Some(orig_params),
        };
        self.pending_replace_orig.insert(order_id, orig);

        let trigger_tick = (trigger_price / self.depth.tick_size()).round() as i64;
        let limit_tick = (limit_price / self.depth.tick_size()).round() as i64;

        order.price_tick = limit_tick;
        order.qty = qty;
        order.leaves_qty = qty;
        if let Some(params) = order.q.as_any_mut().downcast_mut::<TriggerOrderParams>() {
            params.trigger_tick = trigger_tick;
        }

        order.req = Status::Replaced;
        order.local_timestamp = current_timestamp;

        let mut req_order = order.clone();
        req_order.exec_qty = 0.0;
        req_order.exec_price_tick = 0;
        req_order.maker = false;
        self.order_l2e.request(req_order, |rej| {
            rej.req = Status::Rejected;
            // restore core fields
            rej.exec_qty = 0.0;
            rej.exec_price_tick = 0;
            rej.maker = false;
        });

        Ok(())
    }

    fn modify(
        &mut self,
        order_id: OrderId,
        price: f64,
        qty: f64,
        current_timestamp: i64,
    ) -> Result<(), BacktestError> {
        let order = self
            .orders
            .get_mut(&order_id)
            .ok_or(BacktestError::OrderNotFound)?;

        if order.req != Status::None {
            return Err(BacktestError::OrderRequestInProcess);
        }

        // Stop-Limit has two distinct prices (trigger + post-trigger limit), so the generic
        // `modify(order_id, price, qty)` API is ambiguous. Force callers to use
        // `modify_stop_limit(order_id, trigger_price, limit_price, qty)` instead.
        if let Some(params) = order.q.as_any().downcast_ref::<TriggerOrderParams>()
            && params.kind == TriggerOrderKind::StopLimit
        {
            return Err(BacktestError::InvalidOrderRequest);
        }

        // NOTE: The local model optimistically applies the requested price/qty immediately.
        // If the exchange later rejects the replace (e.g., OrderNotFound in a race window),
        // process_recv_order() must restore the original fields from pending_replace_orig.
        let orig_params = order
            .q
            .as_any()
            .downcast_ref::<TriggerOrderParams>()
            .cloned();
        let orig = PendingReplaceOrig {
            price_tick: order.price_tick,
            qty: order.qty,
            leaves_qty: order.leaves_qty,
            trigger_params: orig_params,
        };
        self.pending_replace_orig.insert(order_id, orig);

        let price_tick = (price / self.depth.tick_size()).round() as i64;
        order.price_tick = price_tick;
        order.qty = qty;
        order.leaves_qty = qty;
        if let Some(params) = order.q.as_any_mut().downcast_mut::<TriggerOrderParams>() {
            match params.kind {
                TriggerOrderKind::StopMarket | TriggerOrderKind::Mit => {
                    // For Stop-Market and MIT, `modify(price, qty)` means modifying the trigger.
                    params.trigger_tick = price_tick;
                }
                TriggerOrderKind::StopLimit => {}
            }
        }

        order.req = Status::Replaced;
        order.local_timestamp = current_timestamp;

        let mut req_order = order.clone();
        req_order.exec_qty = 0.0;
        req_order.exec_price_tick = 0;
        req_order.maker = false;
        self.order_l2e.request(req_order, |order| {
            order.req = Status::Rejected;
            order.exec_qty = 0.0;
            order.exec_price_tick = 0;
            order.maker = false;
        });

        Ok(())
    }

    fn cancel(&mut self, order_id: OrderId, current_timestamp: i64) -> Result<(), BacktestError> {
        let order = self
            .orders
            .get_mut(&order_id)
            .ok_or(BacktestError::OrderNotFound)?;

        if order.req != Status::None {
            return Err(BacktestError::OrderRequestInProcess);
        }

        order.req = Status::Canceled;
        order.local_timestamp = current_timestamp;

        let mut req_order = order.clone();
        req_order.exec_qty = 0.0;
        req_order.exec_price_tick = 0;
        req_order.maker = false;
        self.order_l2e.request(req_order, |order| {
            order.req = Status::Rejected;
            order.exec_qty = 0.0;
            order.exec_price_tick = 0;
            order.maker = false;
        });

        Ok(())
    }

    fn clear_inactive_orders(&mut self) {
        self.orders.retain(|_, order| {
            order.status != Status::Expired
                && order.status != Status::Filled
                && order.status != Status::Canceled
        });
        // Keep per-order in-flight snapshots bounded to existing orders.
        // (This also covers late exchange rejects after an order is removed.)
        self.pending_replace_orig
            .retain(|order_id, _| self.orders.contains_key(order_id));
    }

    fn position(&self) -> f64 {
        self.state_values().position
    }

    fn state_values(&self) -> &StateValues {
        self.state.values()
    }

    fn depth(&self) -> &MD {
        &self.depth
    }

    fn orders(&self) -> &HashMap<OrderId, Order> {
        &self.orders
    }

    fn last_trades(&self) -> &[Event] {
        self.trades.as_slice()
    }

    fn clear_last_trades(&mut self) {
        self.trades.clear();
    }

    fn feed_latency(&self) -> Option<(i64, i64)> {
        self.last_feed_latency
    }

    fn order_latency(&self) -> Option<(i64, i64, i64)> {
        self.last_order_latency
    }
}

impl<AT, LM, MD, FM> Processor for L3Local<AT, LM, MD, FM>
where
    AT: AssetType,
    LM: LatencyModel,
    MD: L3MarketDepth,
    FM: FeeModel,
    BacktestError: From<<MD as L3MarketDepth>::Error>,
{
    fn event_seen_timestamp(&self, event: &Event) -> Option<i64> {
        event.is(LOCAL_EVENT).then_some(event.local_ts)
    }

    fn process(&mut self, ev: &Event) -> Result<(), BacktestError> {
        // Processes a depth event
        if ev.is(LOCAL_BID_DEPTH_CLEAR_EVENT) {
            self.depth.clear_orders(Side::Buy);
        } else if ev.is(LOCAL_ASK_DEPTH_CLEAR_EVENT) {
            self.depth.clear_orders(Side::Sell);
        } else if ev.is(LOCAL_DEPTH_CLEAR_EVENT) {
            self.depth.clear_orders(Side::None);
        } else if ev.is(LOCAL_BID_ADD_ORDER_EVENT) {
            self.depth
                .add_buy_order(ev.order_id, ev.px, ev.qty, ev.local_ts)?;
        } else if ev.is(LOCAL_ASK_ADD_ORDER_EVENT) {
            self.depth
                .add_sell_order(ev.order_id, ev.px, ev.qty, ev.local_ts)?;
        } else if ev.is(LOCAL_MODIFY_ORDER_EVENT) {
            self.depth
                .modify_order(ev.order_id, ev.px, ev.qty, ev.local_ts)?;
        } else if ev.is(LOCAL_CANCEL_ORDER_EVENT) {
            self.depth.delete_order(ev.order_id, ev.local_ts)?;
        }
        // Processes a trade event
        else if ev.is(LOCAL_TRADE_EVENT) && self.trades.capacity() > 0 {
            self.trades.push(ev.clone());
        }

        // Stores the current feed latency
        self.last_feed_latency = Some((ev.exch_ts, ev.local_ts));

        Ok(())
    }

    fn process_recv_order(
        &mut self,
        timestamp: i64,
        wait_resp_order_id: Option<OrderId>,
    ) -> Result<bool, BacktestError> {
        // Processes the order part.
        let mut wait_resp_order_received = false;
        while let Some(order) = self.order_l2e.receive(timestamp) {
            // Updates the order latency only if it has a valid exchange timestamp. When the
            // order is rejected before it reaches the matching engine, it has no exchange
            // timestamp. This situation occurs in crypto exchanges.
            if order.exch_timestamp > 0 {
                self.last_order_latency =
                    Some((order.local_timestamp, order.exch_timestamp, timestamp));
            }

            if let Some(wait_resp_order_id) = wait_resp_order_id
                && order.order_id == wait_resp_order_id
            {
                wait_resp_order_received = true;
            }

            // Processes receiving order response.
            if order.exec_qty > 0.0 {
                self.state.apply_fill(&order);
            }
            // Applies the received order response to the local orders.
            match self.orders.entry(order.order_id) {
                Entry::Occupied(mut entry) => {
                    let local_order = entry.get_mut();
                    if order.req == Status::Rejected {
                        if order.local_timestamp == local_order.local_timestamp {
                            // A late reject (e.g., cancel/modify OrderNotFound) must not overwrite
                            // a terminal local order state back to working.
                            if local_order.status == Status::Expired
                                || local_order.status == Status::Filled
                                || local_order.status == Status::Canceled
                            {
                                local_order.req = Status::None;
                                self.pending_replace_orig.remove(&order.order_id);
                            } else {
                                let prev_req = local_order.req;
                                local_order.update(&order);
                                local_order.req = Status::None;
                                if prev_req == Status::New {
                                    local_order.status = Status::Expired;
                                } else if prev_req == Status::Replaced {
                                    // Exchange-side replace rejects must not apply the attempted
                                    // price/qty to the local order (L3LOCAL-001).
                                    if let Some(orig) =
                                        self.pending_replace_orig.remove(&order.order_id)
                                    {
                                        local_order.price_tick = orig.price_tick;
                                        local_order.qty = orig.qty;
                                        // If the exchange marks the order inactive on reject
                                        // (CME/Databento MBO policy), keep leaves_qty as provided
                                        // by the response (typically 0).
                                        if local_order.status != Status::Expired
                                            && local_order.status != Status::Filled
                                            && local_order.status != Status::Canceled
                                        {
                                            local_order.leaves_qty = orig.leaves_qty;
                                        }
                                        if let Some(params) = orig.trigger_params {
                                            local_order.q = Box::new(params);
                                        }
                                    }
                                }
                            }
                        }
                    } else {
                        let prev_req = local_order.req;
                        local_order.update(&order);
                        if prev_req == Status::Replaced
                            && order.local_timestamp == local_order.local_timestamp
                        {
                            self.pending_replace_orig.remove(&order.order_id);
                        }
                    }
                }
                Entry::Vacant(entry) => {
                    if order.req != Status::Rejected {
                        entry.insert(order);
                    }
                }
            }
        }
        Ok(wait_resp_order_received)
    }

    fn earliest_recv_order_timestamp(&self) -> i64 {
        self.order_l2e
            .earliest_recv_order_timestamp()
            .unwrap_or(i64::MAX)
    }

    fn earliest_send_order_timestamp(&self) -> i64 {
        self.order_l2e
            .earliest_send_order_timestamp()
            .unwrap_or(i64::MAX)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        backtest::{
            BacktestError,
            assettype::LinearAsset,
            models::{
                CommonFees, ConstantLatency, L3FIFOQueueModel, LatencyModel, TradingValueFeeModel,
            },
            order::{order_bus, order_bus_with_max_timestamp_reordering},
            proc::{L3PartialFillExchange, LocalProcessor, Processor, TriggerOrderParams},
            state::State,
        },
        depth::HashMapMarketDepth,
        types::{
            EXCH_BID_ADD_ORDER_EVENT, EXCH_FILL_EVENT, Event, OrdType, Order, SELL_EVENT, Side,
            Status, TimeInForce,
        },
    };

    fn make_state() -> State<LinearAsset, TradingValueFeeModel<CommonFees>> {
        State::new(
            LinearAsset::new(1.0),
            TradingValueFeeModel::new(CommonFees::new(0.0, 0.0)),
        )
    }

    fn make_depth() -> HashMapMarketDepth {
        HashMapMarketDepth::new(1.0, 1.0)
    }

    #[derive(Clone)]
    struct ResponseLatencyByOrderKind {
        entry_latency: i64,
        ack_response_latency: i64,
        fill_response_latency: i64,
        reject_response_latency: i64,
    }

    impl LatencyModel for ResponseLatencyByOrderKind {
        fn entry(&mut self, _timestamp: i64, _order: &Order) -> i64 {
            self.entry_latency
        }

        fn response(&mut self, _timestamp: i64, order: &Order) -> i64 {
            if order.req == Status::Rejected {
                return self.reject_response_latency;
            }
            if order.status == Status::Filled || order.status == Status::PartiallyFilled {
                return self.fill_response_latency;
            }
            self.ack_response_latency
        }
    }

    #[test]
    fn late_cancel_reject_does_not_resurrect_filled_order() {
        let entry_latency = 0;
        let resp_latency = 10;
        let (mut order_e2l, order_l2e) =
            order_bus(ConstantLatency::new(entry_latency, resp_latency));

        let mut local = super::L3Local::new(make_depth(), make_state(), 0, order_l2e);

        let order_id = 1;
        let t_new_local = 0;
        let t_new_exch = t_new_local + entry_latency;

        // Exchange fill happens before the local submits cancel, but the fill is observed locally
        // after a response latency.
        let t_fill_exch = 100;
        let t_cancel_local = 105;
        let t_cancel_exch = t_cancel_local + entry_latency;

        local
            .submit_order(
                order_id,
                Side::Buy,
                100.0,
                1.0,
                OrdType::Limit,
                TimeInForce::GTC,
                t_new_local,
            )
            .unwrap();

        // Ack the new order so it becomes cancelable locally.
        let mut ack = Order::new(
            order_id,
            100,
            1.0,
            1.0,
            Side::Buy,
            OrdType::Limit,
            TimeInForce::GTC,
        );
        ack.status = Status::New;
        ack.req = Status::None;
        ack.exch_timestamp = t_new_exch;
        ack.local_timestamp = t_new_local;
        order_e2l.respond(ack);

        local
            .process_recv_order(t_new_exch + resp_latency, None)
            .unwrap();

        // Local submits cancel after the exchange fill occurred, but before the fill is observed.
        local.cancel(order_id, t_cancel_local).unwrap();

        // Deliver the fill response first.
        let mut fill = Order::new(
            order_id,
            100,
            1.0,
            1.0,
            Side::Buy,
            OrdType::Limit,
            TimeInForce::GTC,
        );
        fill.status = Status::Filled;
        fill.req = Status::None;
        fill.leaves_qty = 0.0;
        fill.exec_qty = 1.0;
        fill.exec_price_tick = 100;
        fill.maker = true;
        fill.exch_timestamp = t_fill_exch;
        order_e2l.respond(fill);

        // And then a late cancel reject (OrderNotFound -> req=Rejected) response.
        let mut reject = Order::new(
            order_id,
            100,
            1.0,
            1.0,
            Side::Buy,
            OrdType::Limit,
            TimeInForce::GTC,
        );
        reject.status = Status::New;
        reject.req = Status::Rejected;
        reject.exch_timestamp = t_cancel_exch;
        reject.local_timestamp = t_cancel_local;
        order_e2l.respond(reject);

        local
            .process_recv_order(t_fill_exch + resp_latency, None)
            .unwrap();
        let after_fill = local.orders().get(&order_id).unwrap().status;
        assert_eq!(after_fill, Status::Filled);

        local
            .process_recv_order(t_cancel_exch + resp_latency, None)
            .unwrap();
        let after_reject = local.orders().get(&order_id).unwrap().status;

        // Monotonic state invariant: terminal status must not regress to working due to a late reject.
        assert_eq!(after_reject, Status::Filled);
        assert_eq!(local.orders().get(&order_id).unwrap().req, Status::None);
    }

    #[test]
    fn replace_reject_restores_original_price_and_qty() {
        let entry_latency = 0;
        let resp_latency = 0;
        let (mut order_e2l, order_l2e) =
            order_bus(ConstantLatency::new(entry_latency, resp_latency));

        let mut local = super::L3Local::new(make_depth(), make_state(), 0, order_l2e);

        let order_id = 1;
        let t_new_local = 1;
        let t_new_exch = t_new_local + entry_latency;

        local
            .submit_order(
                order_id,
                Side::Buy,
                100.0,
                1.0,
                OrdType::Limit,
                TimeInForce::GTC,
                t_new_local,
            )
            .unwrap();

        // Ack the new order so it becomes modifiable locally.
        let mut ack = Order::new(
            order_id,
            100,
            1.0,
            1.0,
            Side::Buy,
            OrdType::Limit,
            TimeInForce::GTC,
        );
        ack.status = Status::New;
        ack.req = Status::None;
        ack.exch_timestamp = t_new_exch;
        ack.local_timestamp = t_new_local;
        order_e2l.respond(ack);
        local
            .process_recv_order(t_new_exch + resp_latency, None)
            .unwrap();

        // Submit a replace locally (optimistically updates price/qty).
        let t_replace_local = 2;
        local.modify(order_id, 101.0, 2.0, t_replace_local).unwrap();
        let before_resp = local.orders().get(&order_id).unwrap();
        assert_eq!(before_resp.price_tick, 101);
        assert_eq!(before_resp.qty, 2.0);

        // Exchange rejects the replace (e.g., OrderNotFound); it may echo the attempted price/qty.
        let t_replace_exch = t_replace_local + entry_latency;
        let mut reject = Order::new(
            order_id,
            101,
            1.0,
            2.0,
            Side::Buy,
            OrdType::Limit,
            TimeInForce::GTC,
        );
        reject.status = Status::New;
        reject.req = Status::Rejected;
        reject.leaves_qty = 2.0;
        reject.exch_timestamp = t_replace_exch;
        reject.local_timestamp = t_replace_local;
        order_e2l.respond(reject);

        local
            .process_recv_order(t_replace_exch + resp_latency, None)
            .unwrap();

        // Local must revert price/qty/leaves_qty to the pre-replace state.
        let after_reject = local.orders().get(&order_id).unwrap();
        assert_eq!(after_reject.req, Status::None);
        assert_eq!(after_reject.status, Status::New);
        assert_eq!(after_reject.price_tick, 100);
        assert_eq!(after_reject.qty, 1.0);
        assert_eq!(after_reject.leaves_qty, 1.0);
    }

    #[test]
    fn stop_limit_modify_via_generic_modify_is_rejected() {
        let entry_latency = 0;
        let resp_latency = 0;
        let (order_e2l, order_l2e) =
            order_bus(ConstantLatency::new(entry_latency, resp_latency));

        let mut local = super::L3Local::new(make_depth(), make_state(), 0, order_l2e);
        let mut exch = L3PartialFillExchange::new(
            make_depth(),
            make_state(),
            L3FIFOQueueModel::new(),
            order_e2l,
        );

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

        let res = local.modify(order_id, /* price */ 100.0, /* qty */ 2.0, /* now */ 1);
        assert!(matches!(res, Err(BacktestError::InvalidOrderRequest)));

        let ord = local.orders().get(&order_id).unwrap();
        assert_eq!(ord.price_tick, 99);
        assert_eq!(ord.qty, 1.0);
        let params = ord.q.as_any().downcast_ref::<TriggerOrderParams>().unwrap();
        assert_eq!(params.trigger_tick, 104);
    }

    #[test]
    fn replace_reject_marks_inactive_keeps_leaves_qty_zero() {
        let entry_latency = 0;
        let resp_latency = 0;
        let (mut order_e2l, order_l2e) =
            order_bus(ConstantLatency::new(entry_latency, resp_latency));

        let mut local = super::L3Local::new(make_depth(), make_state(), 0, order_l2e);

        let order_id = 1;
        let t_new_local = 1;
        let t_new_exch = t_new_local + entry_latency;

        local
            .submit_order(
                order_id,
                Side::Buy,
                100.0,
                1.0,
                OrdType::Limit,
                TimeInForce::GTC,
                t_new_local,
            )
            .unwrap();

        // Ack the new order so it becomes modifiable locally.
        let mut ack = Order::new(
            order_id,
            100,
            1.0,
            1.0,
            Side::Buy,
            OrdType::Limit,
            TimeInForce::GTC,
        );
        ack.status = Status::New;
        ack.req = Status::None;
        ack.exch_timestamp = t_new_exch;
        ack.local_timestamp = t_new_local;
        order_e2l.respond(ack);
        local
            .process_recv_order(t_new_exch + resp_latency, None)
            .unwrap();

        // Submit a replace locally (optimistically updates price/qty).
        let t_replace_local = 2;
        local.modify(order_id, 101.0, 2.0, t_replace_local).unwrap();

        // Exchange rejects the replace and marks the order inactive (CME/Databento MBO policy).
        let t_replace_exch = t_replace_local + entry_latency;
        let mut reject = Order::new(
            order_id,
            101,
            1.0,
            2.0,
            Side::Buy,
            OrdType::Limit,
            TimeInForce::GTC,
        );
        reject.status = Status::Expired;
        reject.req = Status::Rejected;
        reject.leaves_qty = 0.0;
        reject.exch_timestamp = t_replace_exch;
        reject.local_timestamp = t_replace_local;
        order_e2l.respond(reject);

        local
            .process_recv_order(t_replace_exch + resp_latency, None)
            .unwrap();

        let after_reject = local.orders().get(&order_id).unwrap();
        assert_eq!(after_reject.req, Status::None);
        assert_eq!(after_reject.status, Status::Expired);
        assert_eq!(after_reject.price_tick, 100);
        assert_eq!(after_reject.qty, 1.0);
        assert_eq!(after_reject.leaves_qty, 0.0);
    }

    #[test]
    fn cme_mbo_replace_after_exchange_fill_emits_reject_and_rolls_back_local_fields() {
        // This is a reachability-gated integration repro:
        // - Exchange-side OrderNotFound reject is produced by the exchange processor (not injected).
        // - A fill removes the backtest order on the exchange before the replace arrives.
        // - Response latencies are configured so the replace-reject can arrive before the fill
        //   notification, which is a CME/MBO race-window scenario.
        let latency_model = ResponseLatencyByOrderKind {
            entry_latency: 0,
            ack_response_latency: 0,
            fill_response_latency: 100,
            reject_response_latency: 0,
        };
        // Allow order response timestamps to be reordered so the replace-reject can be observed
        // before the (earlier exchange-time) fill notification.
        let max_timestamp_reordering = 1_000;
        let (order_e2l, order_l2e) =
            order_bus_with_max_timestamp_reordering(latency_model, max_timestamp_reordering);

        let mut local = super::L3Local::new(make_depth(), make_state(), 0, order_l2e);
        let mut exch = L3PartialFillExchange::new(
            make_depth(),
            make_state(),
            L3FIFOQueueModel::new(),
            order_e2l,
        )
        .with_order_not_found_reject_marks_inactive(true);

        let order_id = 1;
        let px0 = 100.0;
        let qty0 = 1.0;

        // 1) Submit + ack so the local can send a replace.
        local
            .submit_order(
                order_id,
                Side::Buy,
                px0,
                qty0,
                OrdType::Limit,
                TimeInForce::GTC,
                0,
            )
            .unwrap();
        exch.process_recv_order(0, None).unwrap();
        local.process_recv_order(0, None).unwrap();

        // 2) Add a market-feed bid order at the same price behind the backtest order.
        // When that market-feed order is filled, FIFO implies all earlier orders at that price
        // (including our backtest order) are filled first.
        let mkt_bid_order_id = 2;
        exch.process(&Event {
            ev: EXCH_BID_ADD_ORDER_EVENT,
            exch_ts: 1,
            local_ts: 0,
            px: px0,
            qty: 1.0,
            order_id: mkt_bid_order_id,
            ival: 0,
            fval: 0.0,
        })
        .unwrap();

        // 3) Exchange fill removes the backtest order before the replace arrives.
        // (Fill notification to local is delayed by fill_response_latency.)
        exch.process(&Event {
            ev: EXCH_FILL_EVENT | SELL_EVENT,
            exch_ts: 2,
            local_ts: 0,
            px: px0,
            qty: 1.0,
            order_id: mkt_bid_order_id,
            ival: 0,
            fval: 0.0,
        })
        .unwrap();

        // 4) Local sends a replace after the order is already gone on the exchange.
        // This pre-mutates local price/qty optimistically.
        local.modify(order_id, 101.0, 2.0, 3).unwrap();
        assert_eq!(local.orders().get(&order_id).unwrap().price_tick, 101);
        assert_eq!(local.orders().get(&order_id).unwrap().qty, 2.0);

        // 5) Exchange processes replace: OrderNotFound => reject (CME/MBO policy marks inactive).
        exch.process_recv_order(3, None).unwrap();
        local.process_recv_order(3, None).unwrap();

        // Local must roll back attempted price/qty, and must not resurrect leaves_qty if the
        // reject marked the order inactive.
        let after_reject = local.orders().get(&order_id).unwrap();
        assert_eq!(after_reject.req, Status::None);
        assert_eq!(after_reject.status, Status::Expired);
        assert_eq!(after_reject.price_tick, 100);
        assert_eq!(after_reject.qty, 1.0);
        assert_eq!(after_reject.leaves_qty, 0.0);

        // 6) Later, the delayed fill notification arrives; it should not reintroduce the attempted
        // replace fields.
        local.process_recv_order(102, None).unwrap();
        let after_fill = local.orders().get(&order_id).unwrap();
        assert_eq!(after_fill.price_tick, 100);
        assert_eq!(after_fill.qty, 1.0);
    }
}
