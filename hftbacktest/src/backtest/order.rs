use std::{cell::UnsafeCell, collections::VecDeque, rc::Rc};

use crate::{backtest::models::LatencyModel, types::Order};

const MAX_TIMESTAMP: i64 = i64::MAX - 1;

#[inline]
fn checked_add_timestamp(base: i64, offset: i64) -> i64 {
    match base.checked_add(offset) {
        Some(ts) if ts < i64::MAX => ts,
        _ if offset >= 0 => MAX_TIMESTAMP,
        _ => i64::MIN,
    }
}

/// Provides a bus for transporting backtesting orders between the exchange and the local model
/// based on the given timestamp.
#[derive(Clone, Debug, Default)]
pub struct OrderBus {
    order_list: Rc<UnsafeCell<VecDeque<(Order, i64)>>>,
    max_timestamp_reordering: i64,
}

impl OrderBus {
    /// Constructs an instance of ``OrderBus``.
    pub fn new() -> Self {
        Default::default()
    }

    /// Constructs an `OrderBus` that allows timestamp reordering up to `max_timestamp_reordering`.
    ///
    /// The reordering window is expressed in the same time unit as the timestamps used by the
    /// backtest (e.g., nanoseconds). A value of `0` matches the strict FIFO/clamp behavior.
    pub fn with_max_timestamp_reordering(max_timestamp_reordering: i64) -> Self {
        Self {
            max_timestamp_reordering: max_timestamp_reordering.max(0),
            ..Default::default()
        }
    }

    /// Returns the timestamp of the earliest order in the bus.
    pub fn earliest_timestamp(&self) -> Option<i64> {
        unsafe { &*self.order_list.get() }
            .front()
            .map(|(_order, ts)| *ts)
    }

    /// Appends the order to the bus with the timestamp.
    ///
    /// By default, timestamps are clamped to be non-decreasing (strict FIFO) to avoid out-of-order
    /// delivery. If `max_timestamp_reordering` is set to a positive value, timestamps may be
    /// reordered within that window while preserving the bus invariant (earliest timestamp is at
    /// the front).
    ///
    /// In crypto exchanges that use REST APIs, it may be still possible for order requests sent
    /// later to reach the matching engine before order requests sent earlier. However, for the
    /// purpose of simplifying the backtesting process, all requests and responses are assumed to be
    /// in order.
    pub fn append(&mut self, order: Order, timestamp: i64) {
        let order_list = unsafe { &mut *self.order_list.get() };
        let latest_timestamp = order_list.back().map(|(_order, ts)| *ts).unwrap_or(0);

        let timestamp = timestamp.max(0).min(MAX_TIMESTAMP);
        if timestamp >= latest_timestamp || self.max_timestamp_reordering <= 0 {
            let timestamp = timestamp.max(latest_timestamp);
            order_list.push_back((order, timestamp));
            return;
        }

        let min_timestamp = latest_timestamp.saturating_sub(self.max_timestamp_reordering);
        let timestamp = timestamp.max(min_timestamp);

        // Keep the queue ordered by timestamp so that `earliest_timestamp()` remains correct.
        let insert_at = order_list
            .iter()
            .position(|(_order, ts)| *ts > timestamp)
            .unwrap_or(order_list.len());
        order_list.insert(insert_at, (order, timestamp));
    }

    /// Resets this to clear it.
    pub fn reset(&mut self) {
        unsafe { &mut *self.order_list.get() }.clear();
    }

    /// Returns the number of orders in the bus.
    pub fn len(&self) -> usize {
        unsafe { &*self.order_list.get() }.len()
    }

    /// Returns ``true`` if the ``OrderBus`` is empty.
    pub fn is_empty(&self) -> bool {
        unsafe { &*self.order_list.get() }.is_empty()
    }

    /// Removes the first order and its timestamp and returns it, or ``None`` if the bus is empty.
    pub fn pop_front(&mut self) -> Option<(Order, i64)> {
        unsafe { &mut *self.order_list.get() }.pop_front()
    }
}

/// Provides a bidirectional order bus connecting the exchange to the local.
pub struct ExchToLocal<LM> {
    to_exch: OrderBus,
    to_local: OrderBus,
    order_latency: LM,
}

impl<LM> ExchToLocal<LM>
where
    LM: LatencyModel,
{
    /// Returns the timestamp of the earliest order to be received by the exchange from the local.
    pub fn earliest_recv_order_timestamp(&self) -> Option<i64> {
        self.to_exch.earliest_timestamp()
    }

    /// Returns the timestamp of the earliest order sent from the exchange to the local.
    pub fn earliest_send_order_timestamp(&self) -> Option<i64> {
        self.to_local.earliest_timestamp()
    }

    /// Responds to the local with the order processed by the exchange.
    pub fn respond(&mut self, order: Order) {
        let local_recv_timestamp = checked_add_timestamp(
            order.exch_timestamp,
            self.order_latency.response(order.exch_timestamp, &order),
        );
        self.to_local.append(order, local_recv_timestamp);
    }

    /// Receives the order request from the local, which is expected to be received at
    /// `receipt_timestamp`.
    pub fn receive(&mut self, receipt_timestamp: i64) -> Option<Order> {
        if let Some(timestamp) = self.to_exch.earliest_timestamp() {
            if timestamp == receipt_timestamp {
                self.to_exch.pop_front().map(|(order, _)| order)
            } else {
                assert!(timestamp > receipt_timestamp);
                None
            }
        } else {
            None
        }
    }
}

/// Provides a bidirectional order bus connecting the local to the exchange.
pub struct LocalToExch<LM> {
    to_exch: OrderBus,
    to_local: OrderBus,
    order_latency: LM,
}

impl<LM> LocalToExch<LM>
where
    LM: LatencyModel,
{
    /// Returns the timestamp of the earliest order to be received by the local from the exchange.
    pub fn earliest_recv_order_timestamp(&self) -> Option<i64> {
        self.to_local.earliest_timestamp()
    }

    /// Returns the timestamp of the earliest order sent from the local to the exchange.
    pub fn earliest_send_order_timestamp(&self) -> Option<i64> {
        self.to_exch.earliest_timestamp()
    }

    /// Sends the order request to the exchange.
    /// If it is rejected before reaching the matching engine (as reflected in the order latency
    /// information), `reject` is invoked and the rejection response is appended to the local order
    /// bus.
    pub fn request<F>(&mut self, mut order: Order, mut reject: F)
    where
        F: FnMut(&mut Order),
    {
        let order_entry_latency = self.order_latency.entry(order.local_timestamp, &order);
        // Negative latency indicates that the order is rejected for technical reasons, and its
        // value represents the latency that the local experiences when receiving the rejection
        // notification.
        if order_entry_latency < 0 {
            // Rejects the order.
            reject(&mut order);
            let rej_recv_timestamp = checked_add_timestamp(
                order.local_timestamp,
                order_entry_latency.checked_neg().unwrap_or(i64::MAX),
            );
            self.to_local.append(order, rej_recv_timestamp);
        } else {
            let exch_recv_timestamp = checked_add_timestamp(order.local_timestamp, order_entry_latency);
            self.to_exch.append(order, exch_recv_timestamp);
        }
    }

    /// Receives the order response from the exchange, which is expected to be received at
    /// `receipt_timestamp`.
    pub fn receive(&mut self, receipt_timestamp: i64) -> Option<Order> {
        if let Some(timestamp) = self.to_local.earliest_timestamp() {
            if timestamp == receipt_timestamp {
                self.to_local.pop_front().map(|(order, _)| order)
            } else {
                assert!(timestamp > receipt_timestamp);
                None
            }
        } else {
            None
        }
    }
}

/// Creates bidirectional order buses with the order latency model.
pub fn order_bus<LM>(order_latency: LM) -> (ExchToLocal<LM>, LocalToExch<LM>)
where
    LM: LatencyModel + Clone,
{
    let to_exch = OrderBus::new();
    let to_local = OrderBus::new();
    (
        ExchToLocal {
            to_exch: to_exch.clone(),
            to_local: to_local.clone(),
            order_latency: order_latency.clone(),
        },
        LocalToExch {
            to_exch,
            to_local,
            order_latency,
        },
    )
}

/// Creates bidirectional order buses with the order latency model and a timestamp reordering
/// window.
///
/// `max_timestamp_reordering` is expressed in the same time unit as the timestamps used by the
/// backtest (e.g., nanoseconds). A value of `0` matches `order_bus()` (strict FIFO/clamp).
pub fn order_bus_with_max_timestamp_reordering<LM>(
    order_latency: LM,
    max_timestamp_reordering: i64,
) -> (ExchToLocal<LM>, LocalToExch<LM>)
where
    LM: LatencyModel + Clone,
{
    let to_exch = OrderBus::with_max_timestamp_reordering(max_timestamp_reordering);
    let to_local = OrderBus::with_max_timestamp_reordering(max_timestamp_reordering);
    (
        ExchToLocal {
            to_exch: to_exch.clone(),
            to_local: to_local.clone(),
            order_latency: order_latency.clone(),
        },
        LocalToExch {
            to_exch,
            to_local,
            order_latency,
        },
    )
}

#[cfg(test)]
mod tests {
    use super::{OrderBus, order_bus};
    use crate::types::{OrdType, Order, Side, Status, TimeInForce};

    fn make_order(order_id: u64) -> Order {
        Order::new(
            order_id,
            /* price_tick */ 0,
            /* tick_size */ 1.0,
            /* qty */ 1.0,
            Side::Buy,
            OrdType::Limit,
            TimeInForce::GTC,
        )
    }

    #[test]
    fn order_bus_clamps_timestamps_by_default() {
        let mut bus = OrderBus::new();
        bus.append(make_order(1), 150);
        bus.append(make_order(2), 120);

        assert_eq!(bus.earliest_timestamp(), Some(150));

        let (first, ts1) = bus.pop_front().unwrap();
        assert_eq!(first.order_id, 1);
        assert_eq!(ts1, 150);

        let (second, ts2) = bus.pop_front().unwrap();
        assert_eq!(second.order_id, 2);
        assert_eq!(ts2, 150);
    }

    #[test]
    fn order_bus_allows_timestamp_reordering_with_window() {
        let mut bus = OrderBus::with_max_timestamp_reordering(40);
        bus.append(make_order(1), 150);
        bus.append(make_order(2), 120);

        assert_eq!(bus.earliest_timestamp(), Some(120));

        let (first, ts1) = bus.pop_front().unwrap();
        assert_eq!(first.order_id, 2);
        assert_eq!(ts1, 120);

        let (second, ts2) = bus.pop_front().unwrap();
        assert_eq!(second.order_id, 1);
        assert_eq!(ts2, 150);
    }

    #[test]
    fn order_bus_saturates_reordering_to_window() {
        let mut bus = OrderBus::with_max_timestamp_reordering(20);
        bus.append(make_order(1), 150);
        bus.append(make_order(2), 100);

        assert_eq!(bus.earliest_timestamp(), Some(130));

        let (first, ts1) = bus.pop_front().unwrap();
        assert_eq!(first.order_id, 2);
        assert_eq!(ts1, 130);
    }

    #[derive(Clone)]
    struct UnitLatency;

    impl crate::backtest::models::LatencyModel for UnitLatency {
        fn entry(&mut self, _timestamp: i64, _order: &Order) -> i64 {
            1
        }

        fn response(&mut self, _timestamp: i64, _order: &Order) -> i64 {
            1
        }
    }

    #[derive(Clone)]
    struct RejectLatency;

    impl crate::backtest::models::LatencyModel for RejectLatency {
        fn entry(&mut self, _timestamp: i64, _order: &Order) -> i64 {
            -1
        }

        fn response(&mut self, _timestamp: i64, _order: &Order) -> i64 {
            0
        }
    }

    #[test]
    fn respond_never_schedules_at_reserved_i64_max() {
        let (mut e2l, _l2e) = order_bus(UnitLatency);
        let mut order = Order::new(1, 0, 1.0, 1.0, Side::Buy, OrdType::Limit, TimeInForce::GTC);
        order.exch_timestamp = i64::MAX - 1;
        order.status = Status::New;

        e2l.respond(order);

        assert_eq!(e2l.earliest_send_order_timestamp(), Some(i64::MAX - 1));
    }

    #[test]
    fn request_never_schedules_at_reserved_i64_max() {
        let (_e2l, mut l2e) = order_bus(UnitLatency);
        let mut order = Order::new(1, 0, 1.0, 1.0, Side::Buy, OrdType::Limit, TimeInForce::GTC);
        order.local_timestamp = i64::MAX - 1;
        order.status = Status::New;

        l2e.request(order, |_| {});

        assert_eq!(l2e.earliest_send_order_timestamp(), Some(i64::MAX - 1));
    }

    #[test]
    fn rejected_request_never_schedules_at_reserved_i64_max() {
        let (_e2l, mut l2e) = order_bus(RejectLatency);
        let mut order = Order::new(1, 0, 1.0, 1.0, Side::Buy, OrdType::Limit, TimeInForce::GTC);
        order.local_timestamp = i64::MAX - 1;
        order.status = Status::New;

        l2e.request(order, |_| {});

        assert_eq!(l2e.earliest_recv_order_timestamp(), Some(i64::MAX - 1));
    }
}
