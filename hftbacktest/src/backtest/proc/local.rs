use std::collections::{HashMap, hash_map::Entry};

use crate::{
    backtest::{
        BacktestError,
        assettype::AssetType,
        models::{FeeModel, LatencyModel},
        order::LocalToExch,
        proc::{LocalProcessor, Processor},
        state::State,
    },
    depth::{L2MarketDepth, MarketDepth},
    types::{
        Event,
        LOCAL_ASK_DEPTH_CLEAR_EVENT,
        LOCAL_ASK_DEPTH_EVENT,
        LOCAL_ASK_DEPTH_SNAPSHOT_EVENT,
        LOCAL_BID_DEPTH_CLEAR_EVENT,
        LOCAL_BID_DEPTH_EVENT,
        LOCAL_BID_DEPTH_SNAPSHOT_EVENT,
        LOCAL_DEPTH_CLEAR_EVENT,
        LOCAL_EVENT,
        LOCAL_TRADE_EVENT,
        OrdType,
        Order,
        OrderId,
        Side,
        StateValues,
        Status,
        TimeInForce,
    },
};

/// The local model.
pub struct Local<AT, LM, MD, FM>
where
    AT: AssetType,
    LM: LatencyModel,
    MD: MarketDepth,
    FM: FeeModel,
{
    orders: HashMap<OrderId, Order>,
    order_l2e: LocalToExch<LM>,
    depth: MD,
    state: State<AT, FM>,
    trades: Vec<Event>,
    last_feed_latency: Option<(i64, i64)>,
    last_order_latency: Option<(i64, i64, i64)>,
}

impl<AT, LM, MD, FM> Local<AT, LM, MD, FM>
where
    AT: AssetType,
    LM: LatencyModel,
    MD: MarketDepth,
    FM: FeeModel,
{
    /// Constructs an instance of `Local`.
    pub fn new(
        depth: MD,
        state: State<AT, FM>,
        last_trades_cap: usize,
        order_l2e: LocalToExch<LM>,
    ) -> Self {
        Self {
            orders: Default::default(),
            order_l2e,
            depth,
            state,
            trades: Vec::with_capacity(last_trades_cap),
            last_feed_latency: None,
            last_order_latency: None,
        }
    }

    pub fn process_recv_order_<const USE_HANDLER: bool, Handler>(
        &mut self,
        timestamp: i64,
        wait_resp_order_id: Option<OrderId>,
        mut handler: Handler,
    ) -> Result<bool, BacktestError>
    where
        Handler: FnMut(&Order),
    {
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
                            let prev_req = local_order.req;
                            local_order.update(&order);
                            local_order.req = Status::None;
                            if prev_req == Status::New {
                                local_order.status = Status::Expired;
                            }
                        }
                    } else {
                        local_order.update(&order);
                    }
                    if USE_HANDLER {
                        handler(&order);
                    }
                }
                Entry::Vacant(entry) => {
                    if order.req != Status::Rejected {
                        let order_ = entry.insert(order);
                        if USE_HANDLER {
                            handler(order_);
                        }
                    }
                }
            }
        }
        Ok(wait_resp_order_received)
    }
}

impl<AT, LM, MD, FM> LocalProcessor<MD> for Local<AT, LM, MD, FM>
where
    AT: AssetType,
    LM: LatencyModel,
    MD: MarketDepth + L2MarketDepth,
    FM: FeeModel,
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
        if self.orders.contains_key(&order_id) {
            return Err(BacktestError::OrderIdExist);
        }

        let price_tick = (price / self.depth.tick_size()).round() as i64;
        let mut order = Order::new(
            order_id,
            price_tick,
            self.depth.tick_size(),
            qty,
            side,
            order_type,
            time_in_force,
        );
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

        let orig_price_tick = order.price_tick;
        let orig_qty = order.qty;

        let price_tick = (price / self.depth.tick_size()).round() as i64;
        order.price_tick = price_tick;
        order.qty = qty;

        order.req = Status::Replaced;
        order.local_timestamp = current_timestamp;

        let mut req_order = order.clone();
        req_order.exec_qty = 0.0;
        req_order.exec_price_tick = 0;
        req_order.maker = false;
        self.order_l2e.request(req_order, |order| {
            order.req = Status::Rejected;
            order.price_tick = orig_price_tick;
            order.qty = orig_qty;
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
        })
    }

    fn position(&self) -> f64 {
        self.state.values().position
    }

    fn state_values(&self) -> &StateValues {
        self.state.values()
    }

    fn depth(&self) -> &MD {
        &self.depth
    }

    fn orders(&self) -> &HashMap<u64, Order> {
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

impl<AT, LM, MD, FM> Processor for Local<AT, LM, MD, FM>
where
    AT: AssetType,
    LM: LatencyModel,
    MD: MarketDepth + L2MarketDepth,
    FM: FeeModel,
{
    fn event_seen_timestamp(&self, event: &Event) -> Option<i64> {
        event.is(LOCAL_EVENT).then_some(event.local_ts)
    }

    fn process(&mut self, ev: &Event) -> Result<(), BacktestError> {
        // Processes a depth event
        if ev.is(LOCAL_BID_DEPTH_CLEAR_EVENT) {
            self.depth.clear_depth(Side::Buy, ev.px);
        } else if ev.is(LOCAL_ASK_DEPTH_CLEAR_EVENT) {
            self.depth.clear_depth(Side::Sell, ev.px);
        } else if ev.is(LOCAL_DEPTH_CLEAR_EVENT) {
            self.depth.clear_depth(Side::None, 0.0);
        } else if ev.is(LOCAL_BID_DEPTH_EVENT) || ev.is(LOCAL_BID_DEPTH_SNAPSHOT_EVENT) {
            self.depth.update_bid_depth(ev.px, ev.qty, ev.local_ts);
        } else if ev.is(LOCAL_ASK_DEPTH_EVENT) || ev.is(LOCAL_ASK_DEPTH_SNAPSHOT_EVENT) {
            self.depth.update_ask_depth(ev.px, ev.qty, ev.local_ts);
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
        self.process_recv_order_::<false, _>(timestamp, wait_resp_order_id, |_| {})
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
            assettype::LinearAsset,
            models::{
                CommonFees,
                ConstantLatency,
                L3FIFOQueueModel,
                RiskAdverseQueueModel,
                TradingValueFeeModel,
            },
            order::order_bus,
            proc::{
                L3Local,
                L3NoPartialFillExchange,
                LocalProcessor,
                NoPartialFillExchange,
                PartialFillExchange,
                Processor,
            },
            state::State,
        },
        depth::HashMapMarketDepth,
        types::{OrdType, Side, TimeInForce},
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

    #[test]
    fn cancel_ack_preserves_request_local_timestamp_for_latency_nopartialfill_exchange() {
        let entry_latency = 3;
        let resp_latency = 4;
        let (order_e2l, order_l2e) = order_bus(ConstantLatency::new(entry_latency, resp_latency));

        let mut local = super::Local::new(make_depth(), make_state(), 0, order_l2e);
        let mut exch = NoPartialFillExchange::new(
            make_depth(),
            make_state(),
            RiskAdverseQueueModel::new(),
            order_e2l,
        );

        let order_id = 1;
        let t_new = 10;
        let t_cancel = 40;

        local
            .submit_order(
                order_id,
                Side::Buy,
                100.0,
                1.0,
                OrdType::Limit,
                TimeInForce::GTC,
                t_new,
            )
            .unwrap();

        let t_new_exch = exch.earliest_recv_order_timestamp();
        assert_eq!(t_new_exch, t_new + entry_latency);
        exch.process_recv_order(t_new_exch, None).unwrap();

        let t_new_resp = local.earliest_recv_order_timestamp();
        assert_eq!(t_new_resp, t_new_exch + resp_latency);
        local.process_recv_order(t_new_resp, None).unwrap();

        local.cancel(order_id, t_cancel).unwrap();

        let t_cancel_exch = exch.earliest_recv_order_timestamp();
        assert_eq!(t_cancel_exch, t_cancel + entry_latency);
        exch.process_recv_order(t_cancel_exch, None).unwrap();

        let t_cancel_resp = local.earliest_recv_order_timestamp();
        assert_eq!(t_cancel_resp, t_cancel_exch + resp_latency);
        local.process_recv_order(t_cancel_resp, None).unwrap();

        assert_eq!(
            local.order_latency(),
            Some((t_cancel, t_cancel_exch, t_cancel_resp))
        );
    }

    #[test]
    fn cancel_ack_preserves_request_local_timestamp_for_latency_partialfillexchange() {
        let entry_latency = 3;
        let resp_latency = 4;
        let (order_e2l, order_l2e) = order_bus(ConstantLatency::new(entry_latency, resp_latency));

        let mut local = super::Local::new(make_depth(), make_state(), 0, order_l2e);
        let mut exch = PartialFillExchange::new(
            make_depth(),
            make_state(),
            RiskAdverseQueueModel::new(),
            order_e2l,
        );

        let order_id = 1;
        let t_new = 10;
        let t_cancel = 40;

        local
            .submit_order(
                order_id,
                Side::Buy,
                100.0,
                1.0,
                OrdType::Limit,
                TimeInForce::GTC,
                t_new,
            )
            .unwrap();

        let t_new_exch = exch.earliest_recv_order_timestamp();
        assert_eq!(t_new_exch, t_new + entry_latency);
        exch.process_recv_order(t_new_exch, None).unwrap();

        let t_new_resp = local.earliest_recv_order_timestamp();
        assert_eq!(t_new_resp, t_new_exch + resp_latency);
        local.process_recv_order(t_new_resp, None).unwrap();

        local.cancel(order_id, t_cancel).unwrap();

        let t_cancel_exch = exch.earliest_recv_order_timestamp();
        assert_eq!(t_cancel_exch, t_cancel + entry_latency);
        exch.process_recv_order(t_cancel_exch, None).unwrap();

        let t_cancel_resp = local.earliest_recv_order_timestamp();
        assert_eq!(t_cancel_resp, t_cancel_exch + resp_latency);
        local.process_recv_order(t_cancel_resp, None).unwrap();

        assert_eq!(
            local.order_latency(),
            Some((t_cancel, t_cancel_exch, t_cancel_resp))
        );
    }

    #[test]
    fn cancel_ack_preserves_request_local_timestamp_for_latency_l3_nopartialfill_exchange() {
        let entry_latency = 3;
        let resp_latency = 4;
        let (order_e2l, order_l2e) = order_bus(ConstantLatency::new(entry_latency, resp_latency));

        let mut local = L3Local::new(make_depth(), make_state(), 0, order_l2e);
        let mut exch = L3NoPartialFillExchange::new(
            make_depth(),
            make_state(),
            L3FIFOQueueModel::new(),
            order_e2l,
        );

        let order_id = 1;
        let t_new = 10;
        let t_cancel = 40;

        local
            .submit_order(
                order_id,
                Side::Buy,
                100.0,
                1.0,
                OrdType::Limit,
                TimeInForce::GTC,
                t_new,
            )
            .unwrap();

        let t_new_exch = exch.earliest_recv_order_timestamp();
        assert_eq!(t_new_exch, t_new + entry_latency);
        exch.process_recv_order(t_new_exch, None).unwrap();

        let t_new_resp = local.earliest_recv_order_timestamp();
        assert_eq!(t_new_resp, t_new_exch + resp_latency);
        local.process_recv_order(t_new_resp, None).unwrap();

        local.cancel(order_id, t_cancel).unwrap();

        let t_cancel_exch = exch.earliest_recv_order_timestamp();
        assert_eq!(t_cancel_exch, t_cancel + entry_latency);
        exch.process_recv_order(t_cancel_exch, None).unwrap();

        let t_cancel_resp = local.earliest_recv_order_timestamp();
        assert_eq!(t_cancel_resp, t_cancel_exch + resp_latency);
        local.process_recv_order(t_cancel_resp, None).unwrap();

        assert_eq!(
            local.order_latency(),
            Some((t_cancel, t_cancel_exch, t_cancel_resp))
        );
    }
}
