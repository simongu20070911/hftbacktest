use std::collections::{HashMap, hash_map::Entry};

use super::{ApplySnapshot, INVALID_MAX, INVALID_MIN, L3MarketDepth, L3Order, MarketDepth};
use crate::{
    backtest::{BacktestError, data::Data},
    prelude::{L2MarketDepth, OrderId, Side},
    types::{
        BUY_EVENT,
        DEPTH_SNAPSHOT_EVENT,
        EXCH_EVENT,
        Event,
        LOCAL_EVENT,
        SELL_EVENT,
    },
};

/// L2/L3 market depth implementation based on a vector within the range of interest.
///
/// This is a variant of the HashMap-based market depth implementation, which only handles the
/// specific range of interest. By doing so, it improves performance, especially when the strategy
/// requires computing values based on the order book around the mid-price.
pub struct ROIVectorMarketDepth {
    pub tick_size: f64,
    pub lot_size: f64,
    pub timestamp: i64,
    pub ask_depth: Vec<f64>,
    pub bid_depth: Vec<f64>,
    pub best_bid_tick: i64,
    pub best_ask_tick: i64,
    pub low_bid_tick: i64,
    pub high_ask_tick: i64,
    pub roi_ub: i64,
    pub roi_lb: i64,
    pub orders: HashMap<OrderId, L3Order>,
}

#[inline(always)]
fn depth_below(depth: &[f64], start: i64, end: i64, roi_lb: i64, roi_ub: i64) -> i64 {
    if roi_lb > roi_ub {
        return INVALID_MIN;
    }
    if depth.is_empty() {
        return INVALID_MIN;
    }

    let start_tick = start.clamp(roi_lb, roi_ub);
    let end_tick = end.clamp(roi_lb, roi_ub);
    if end_tick >= start_tick {
        return INVALID_MIN;
    }

    let start_idx = (start_tick - roi_lb) as usize;
    let end_idx = (end_tick - roi_lb) as usize;
    for t in (end_idx..start_idx).rev() {
        if unsafe { *depth.get_unchecked(t) } > 0f64 {
            return t as i64 + roi_lb;
        }
    }
    INVALID_MIN
}

#[inline(always)]
fn depth_above(depth: &[f64], start: i64, end: i64, roi_lb: i64, roi_ub: i64) -> i64 {
    if roi_lb > roi_ub {
        return INVALID_MAX;
    }
    if depth.is_empty() {
        return INVALID_MAX;
    }

    let start_tick = start.clamp(roi_lb, roi_ub);
    let end_tick = end.clamp(roi_lb, roi_ub);
    if start_tick >= end_tick {
        return INVALID_MAX;
    }

    let start_idx = (start_tick - roi_lb) as usize;
    let end_idx = (end_tick - roi_lb) as usize;
    for t in (start_idx + 1)..=end_idx {
        if unsafe { *depth.get_unchecked(t) } > 0f64 {
            return t as i64 + roi_lb;
        }
    }
    INVALID_MAX
}

impl ROIVectorMarketDepth {
    /// Constructs an instance of `ROIVectorMarketDepth`.
    pub fn new(tick_size: f64, lot_size: f64, roi_lb: f64, roi_ub: f64) -> Self {
        let roi_lb = (roi_lb / tick_size).round() as i64;
        let roi_ub = (roi_ub / tick_size).round() as i64;
        let roi_range = (roi_ub + 1 - roi_lb) as usize;
        Self {
            tick_size,
            lot_size,
            timestamp: 0,
            ask_depth: {
                let mut v = (0..roi_range).map(|_| 0.0).collect::<Vec<_>>();
                v.shrink_to_fit();
                v
            },
            bid_depth: {
                let mut v = (0..roi_range).map(|_| 0.0).collect::<Vec<_>>();
                v.shrink_to_fit();
                v
            },
            best_bid_tick: INVALID_MIN,
            best_ask_tick: INVALID_MAX,
            low_bid_tick: INVALID_MAX,
            high_ask_tick: INVALID_MIN,
            roi_lb,
            roi_ub,
            orders: HashMap::new(),
        }
    }

    fn add(&mut self, order: L3Order) -> Result<(), BacktestError> {
        let order = match self.orders.entry(order.order_id) {
            Entry::Occupied(_) => return Err(BacktestError::OrderIdExist),
            Entry::Vacant(entry) => entry.insert(order),
        };
        if order.price_tick < self.roi_lb || order.price_tick > self.roi_ub {
            // This is outside the range of interest.
            return Ok(());
        }
        let t = (order.price_tick - self.roi_lb) as usize;
        if order.side == Side::Buy {
            unsafe {
                *self.bid_depth.get_unchecked_mut(t) += order.qty;
            }
        } else {
            unsafe {
                *self.ask_depth.get_unchecked_mut(t) += order.qty;
            }
        }
        Ok(())
    }

    /// Returns the bid market depth array, which contains the quantity at each price. Its length is
    /// `ROI upper bound in ticks + 1 - ROI lower bound in ticks`, the array contains the quantities
    /// at prices from the ROI lower bound to the ROI upper bound.
    /// The index is calculated as `price in ticks - ROI lower bound in ticks`.
    /// Respectively, the price is `(index + ROI lower bound in ticks) * tick_size`.
    pub fn bid_depth(&self) -> &[f64] {
        self.bid_depth.as_slice()
    }

    /// Returns the ask market depth array, which contains the quantity at each price. Its length is
    /// `ROI upper bound in ticks + 1 - ROI lower bound in ticks`, the array contains the quantities
    /// at prices from the ROI lower bound to the ROI upper bound.
    /// The index is calculated as `price in ticks - ROI lower bound in ticks`.
    /// Respectively, the price is `(index + ROI lower bound in ticks) * tick_size`.
    pub fn ask_depth(&self) -> &[f64] {
        self.ask_depth.as_slice()
    }

    /// Returns the lower and the upper bound of the range of interest, in price.
    pub fn roi(&self) -> (f64, f64) {
        (
            self.roi_lb as f64 * self.tick_size,
            self.roi_ub as f64 * self.tick_size,
        )
    }

    /// Returns the lower and the upper bound of the range of interest, in ticks.
    pub fn roi_tick(&self) -> (i64, i64) {
        (self.roi_lb, self.roi_ub)
    }
}

impl L2MarketDepth for ROIVectorMarketDepth {
    fn update_bid_depth(
        &mut self,
        price: f64,
        qty: f64,
        timestamp: i64,
    ) -> (i64, i64, i64, f64, f64, i64) {
        let price_tick = (price / self.tick_size).round() as i64;
        let qty_lot = (qty / self.lot_size).round() as i64;
        let prev_best_bid_tick = self.best_bid_tick;
        let prev_qty;

        if price_tick < self.roi_lb || price_tick > self.roi_ub {
            // This is outside the range of interest.
            return (
                price_tick,
                prev_best_bid_tick,
                self.best_bid_tick,
                0.0,
                qty,
                timestamp,
            );
        }
        let t = (price_tick - self.roi_lb) as usize;
        unsafe {
            let v = self.bid_depth.get_unchecked_mut(t);
            prev_qty = *v;
            *v = qty;
        }

        if qty_lot == 0 {
            if price_tick == self.best_bid_tick {
                self.best_bid_tick = depth_below(
                    &self.bid_depth,
                    self.best_bid_tick,
                    self.low_bid_tick,
                    self.roi_lb,
                    self.roi_ub,
                );
                if self.best_bid_tick == INVALID_MIN {
                    self.low_bid_tick = INVALID_MAX
                }
            }
        } else {
            if price_tick > self.best_bid_tick {
                self.best_bid_tick = price_tick;
                if self.best_bid_tick >= self.best_ask_tick {
                    self.best_ask_tick = depth_above(
                        &self.ask_depth,
                        self.best_bid_tick,
                        self.high_ask_tick,
                        self.roi_lb,
                        self.roi_ub,
                    );
                }
            }
            self.low_bid_tick = self.low_bid_tick.min(price_tick);
        }
        (
            price_tick,
            prev_best_bid_tick,
            self.best_bid_tick,
            prev_qty,
            qty,
            timestamp,
        )
    }

    fn update_ask_depth(
        &mut self,
        price: f64,
        qty: f64,
        timestamp: i64,
    ) -> (i64, i64, i64, f64, f64, i64) {
        let price_tick = (price / self.tick_size).round() as i64;
        let qty_lot = (qty / self.lot_size).round() as i64;
        let prev_best_ask_tick = self.best_ask_tick;
        let prev_qty;

        if price_tick < self.roi_lb || price_tick > self.roi_ub {
            // This is outside the range of interest.
            return (
                price_tick,
                prev_best_ask_tick,
                self.best_ask_tick,
                0.0,
                qty,
                timestamp,
            );
        }
        let t = (price_tick - self.roi_lb) as usize;
        unsafe {
            let v = self.ask_depth.get_unchecked_mut(t);
            prev_qty = *v;
            *v = qty;
        }

        if qty_lot == 0 {
            if price_tick == self.best_ask_tick {
                self.best_ask_tick = depth_above(
                    &self.ask_depth,
                    self.best_ask_tick,
                    self.high_ask_tick,
                    self.roi_lb,
                    self.roi_ub,
                );
                if self.best_ask_tick == INVALID_MAX {
                    self.high_ask_tick = INVALID_MIN
                }
            }
        } else {
            if price_tick < self.best_ask_tick {
                self.best_ask_tick = price_tick;
                if self.best_bid_tick >= self.best_ask_tick {
                    self.best_bid_tick = depth_below(
                        &self.bid_depth,
                        self.best_ask_tick,
                        self.low_bid_tick,
                        self.roi_lb,
                        self.roi_ub,
                    );
                }
            }
            self.high_ask_tick = self.high_ask_tick.max(price_tick);
        }
        (
            price_tick,
            prev_best_ask_tick,
            self.best_ask_tick,
            prev_qty,
            qty,
            timestamp,
        )
    }

    fn clear_depth(&mut self, side: Side, clear_upto_price: f64) {
        match side {
            Side::Buy => {
                if clear_upto_price.is_finite() {
                    let clear_upto = (clear_upto_price / self.tick_size).round() as i64;
                    if self.best_bid_tick != INVALID_MIN {
                        let len = self.bid_depth.len() as i64;
                        let from = clear_upto
                            .saturating_sub(self.roi_lb)
                            .clamp(0, len);
                        let to = self
                            .best_bid_tick
                            .saturating_add(1)
                            .saturating_sub(self.roi_lb)
                            .clamp(0, len);
                        for t in from..to {
                            unsafe {
                                *self.bid_depth.get_unchecked_mut(t as usize) = 0.0;
                            }
                        }
                    }
                    if self.best_bid_tick != INVALID_MIN && clear_upto <= self.best_bid_tick {
                        let low_bid_tick = if self.low_bid_tick == INVALID_MAX {
                            self.roi_lb
                        } else {
                            self.low_bid_tick
                        };
                        let clear_upto = if clear_upto < self.roi_lb {
                            self.roi_lb
                        } else if clear_upto > self.roi_ub {
                            self.roi_ub
                        } else {
                            clear_upto
                        };
                        self.best_bid_tick = depth_below(
                            &self.bid_depth,
                            clear_upto,
                            low_bid_tick,
                            self.roi_lb,
                            self.roi_ub,
                        );
                    }
                } else {
                    self.bid_depth.iter_mut().for_each(|q| *q = 0.0);
                    self.best_bid_tick = INVALID_MIN;
                }
                if self.best_bid_tick == INVALID_MIN {
                    self.low_bid_tick = INVALID_MAX;
                }
            }
            Side::Sell => {
                if clear_upto_price.is_finite() {
                    let clear_upto = (clear_upto_price / self.tick_size).round() as i64;
                    if self.best_ask_tick != INVALID_MAX {
                        let len = self.ask_depth.len() as i64;
                        let from = self.best_ask_tick
                            .saturating_sub(self.roi_lb)
                            .clamp(0, len);
                        let to = clear_upto
                            .saturating_add(1)
                            .saturating_sub(self.roi_lb)
                            .clamp(0, len);
                        for t in from..to {
                            unsafe {
                                *self.ask_depth.get_unchecked_mut(t as usize) = 0.0;
                            }
                        }
                    }
                    if self.best_ask_tick != INVALID_MAX && clear_upto >= self.best_ask_tick {
                        let high_ask_tick = if self.high_ask_tick == INVALID_MIN {
                            self.roi_ub
                        } else {
                            self.high_ask_tick
                        };
                        let clear_upto = if clear_upto < self.roi_lb {
                            self.roi_lb
                        } else if clear_upto > self.roi_ub {
                            self.roi_ub
                        } else {
                            clear_upto
                        };
                        self.best_ask_tick = depth_above(
                            &self.ask_depth,
                            clear_upto,
                            high_ask_tick,
                            self.roi_lb,
                            self.roi_ub,
                        );
                    }
                } else {
                    self.ask_depth.iter_mut().for_each(|q| *q = 0.0);
                    self.best_ask_tick = INVALID_MAX;
                }
                if self.best_ask_tick == INVALID_MAX {
                    self.high_ask_tick = INVALID_MIN;
                }
            }
            Side::None => {
                self.bid_depth.iter_mut().for_each(|q| *q = 0.0);
                self.ask_depth.iter_mut().for_each(|q| *q = 0.0);
                self.best_bid_tick = INVALID_MIN;
                self.best_ask_tick = INVALID_MAX;
                self.low_bid_tick = INVALID_MAX;
                self.high_ask_tick = INVALID_MIN;
            }
            Side::Unsupported => {
                unreachable!();
            }
        }
    }
}

impl MarketDepth for ROIVectorMarketDepth {
    #[inline(always)]
    fn best_bid(&self) -> f64 {
        if self.best_bid_tick == INVALID_MIN {
            f64::NAN
        } else {
            self.best_bid_tick as f64 * self.tick_size
        }
    }

    #[inline(always)]
    fn best_ask(&self) -> f64 {
        if self.best_ask_tick == INVALID_MAX {
            f64::NAN
        } else {
            self.best_ask_tick as f64 * self.tick_size
        }
    }

    #[inline(always)]
    fn best_bid_tick(&self) -> i64 {
        self.best_bid_tick
    }

    #[inline(always)]
    fn best_ask_tick(&self) -> i64 {
        self.best_ask_tick
    }

    #[inline(always)]
    fn best_bid_qty(&self) -> f64 {
        if self.best_bid_tick < self.roi_lb || self.best_bid_tick > self.roi_ub {
            // This is outside the range of interest.
            0.0
        } else {
            unsafe {
                *self
                    .bid_depth
                    .get_unchecked((self.best_bid_tick - self.roi_lb) as usize)
            }
        }
    }

    #[inline(always)]
    fn best_ask_qty(&self) -> f64 {
        if self.best_ask_tick < self.roi_lb || self.best_ask_tick > self.roi_ub {
            // This is outside the range of interest.
            0.0
        } else {
            unsafe {
                *self
                    .ask_depth
                    .get_unchecked((self.best_ask_tick - self.roi_lb) as usize)
            }
        }
    }

    #[inline(always)]
    fn tick_size(&self) -> f64 {
        self.tick_size
    }

    #[inline(always)]
    fn lot_size(&self) -> f64 {
        self.lot_size
    }

    #[inline(always)]
    fn bid_qty_at_tick(&self, price_tick: i64) -> f64 {
        if price_tick < self.roi_lb || price_tick > self.roi_ub {
            // This is outside the range of interest.
            0.0
        } else {
            unsafe {
                *self
                    .bid_depth
                    .get_unchecked((price_tick - self.roi_lb) as usize)
            }
        }
    }

    #[inline(always)]
    fn ask_qty_at_tick(&self, price_tick: i64) -> f64 {
        if price_tick < self.roi_lb || price_tick > self.roi_ub {
            // This is outside the range of interest.
            0.0
        } else {
            unsafe {
                *self
                    .ask_depth
                    .get_unchecked((price_tick - self.roi_lb) as usize)
            }
        }
    }
}

impl ApplySnapshot for ROIVectorMarketDepth {
    fn apply_snapshot(&mut self, data: &Data<Event>) {
        self.best_bid_tick = INVALID_MIN;
        self.best_ask_tick = INVALID_MAX;
        self.low_bid_tick = INVALID_MAX;
        self.high_ask_tick = INVALID_MIN;
        for qty in &mut self.bid_depth {
            *qty = 0.0;
        }
        for qty in &mut self.ask_depth {
            *qty = 0.0;
        }
        for row_num in 0..data.len() {
            let price = data[row_num].px;
            let qty = data[row_num].qty;

            let price_tick = (price / self.tick_size).round() as i64;
            if price_tick < self.roi_lb || price_tick > self.roi_ub {
                continue;
            }
            if data[row_num].ev & BUY_EVENT == BUY_EVENT {
                self.best_bid_tick = self.best_bid_tick.max(price_tick);
                self.low_bid_tick = self.low_bid_tick.min(price_tick);
                let t = (price_tick - self.roi_lb) as usize;
                unsafe {
                    *self.bid_depth.get_unchecked_mut(t) = qty;
                }
            } else if data[row_num].ev & SELL_EVENT == SELL_EVENT {
                self.best_ask_tick = self.best_ask_tick.min(price_tick);
                self.high_ask_tick = self.high_ask_tick.max(price_tick);
                let t = (price_tick - self.roi_lb) as usize;
                unsafe {
                    *self.ask_depth.get_unchecked_mut(t) = qty;
                }
            }
        }
    }

    fn snapshot(&self) -> Vec<Event> {
        let mut events = Vec::new();

        if self.best_bid_tick != INVALID_MIN && self.low_bid_tick != INVALID_MAX {
            let start_tick = self.best_bid_tick.min(self.roi_ub);
            let end_tick = self.low_bid_tick.max(self.roi_lb);
            if end_tick <= start_tick {
                for px_tick in (end_tick..=start_tick).rev() {
                    let t = (px_tick - self.roi_lb) as usize;
                    let qty = unsafe { *self.bid_depth.get_unchecked(t) };
                    if (qty / self.lot_size).round() as i64 == 0 {
                        continue;
                    }
                    events.push(Event {
                        ev: EXCH_EVENT | LOCAL_EVENT | BUY_EVENT | DEPTH_SNAPSHOT_EVENT,
                        exch_ts: 0,
                        local_ts: 0,
                        px: px_tick as f64 * self.tick_size,
                        qty,
                        order_id: 0,
                        ival: 0,
                        fval: 0.0,
                    });
                }
            }
        }

        if self.best_ask_tick != INVALID_MAX && self.high_ask_tick != INVALID_MIN {
            let start_tick = self.best_ask_tick.max(self.roi_lb);
            let end_tick = self.high_ask_tick.min(self.roi_ub);
            if start_tick <= end_tick {
                for px_tick in start_tick..=end_tick {
                    let t = (px_tick - self.roi_lb) as usize;
                    let qty = unsafe { *self.ask_depth.get_unchecked(t) };
                    if (qty / self.lot_size).round() as i64 == 0 {
                        continue;
                    }
                    events.push(Event {
                        ev: EXCH_EVENT | LOCAL_EVENT | SELL_EVENT | DEPTH_SNAPSHOT_EVENT,
                        exch_ts: 0,
                        local_ts: 0,
                        px: px_tick as f64 * self.tick_size,
                        qty,
                        order_id: 0,
                        ival: 0,
                        fval: 0.0,
                    });
                }
            }
        }

        events
    }
}

impl L3MarketDepth for ROIVectorMarketDepth {
    type Error = BacktestError;

    fn add_buy_order(
        &mut self,
        order_id: OrderId,
        px: f64,
        qty: f64,
        timestamp: i64,
    ) -> Result<(i64, i64), Self::Error> {
        let price_tick = (px / self.tick_size).round() as i64;
        self.add(L3Order {
            order_id,
            side: Side::Buy,
            price_tick,
            qty,
            timestamp,
        })?;
        let prev_best_tick = self.best_bid_tick;
        if price_tick > self.best_bid_tick {
            self.best_bid_tick = price_tick;
            if self.best_bid_tick >= self.best_ask_tick {
                self.best_ask_tick = depth_above(
                    &self.ask_depth,
                    self.best_bid_tick,
                    self.high_ask_tick,
                    self.roi_lb,
                    self.roi_ub,
                );
            }
        }
        self.low_bid_tick = self.low_bid_tick.min(price_tick);
        Ok((prev_best_tick, self.best_bid_tick))
    }

    fn add_sell_order(
        &mut self,
        order_id: OrderId,
        px: f64,
        qty: f64,
        timestamp: i64,
    ) -> Result<(i64, i64), Self::Error> {
        let price_tick = (px / self.tick_size).round() as i64;
        self.add(L3Order {
            order_id,
            side: Side::Sell,
            price_tick,
            qty,
            timestamp,
        })?;
        let prev_best_tick = self.best_ask_tick;
        if price_tick < self.best_ask_tick {
            self.best_ask_tick = price_tick;
            if self.best_bid_tick >= self.best_ask_tick {
                self.best_bid_tick = depth_below(
                    &self.bid_depth,
                    self.best_ask_tick,
                    self.low_bid_tick,
                    self.roi_lb,
                    self.roi_ub,
                );
            }
        }
        self.high_ask_tick = self.high_ask_tick.max(price_tick);
        Ok((prev_best_tick, self.best_ask_tick))
    }

    fn delete_order(
        &mut self,
        order_id: OrderId,
        _timestamp: i64,
    ) -> Result<(Side, i64, i64), Self::Error> {
        let order = self
            .orders
            .remove(&order_id)
            .ok_or(BacktestError::OrderNotFound)?;
        if order.side == Side::Buy {
            let prev_best_tick = self.best_bid_tick;

            if !(order.price_tick < self.roi_lb || order.price_tick > self.roi_ub) {
                let t = (order.price_tick - self.roi_lb) as usize;
                let depth_qty = unsafe { self.bid_depth.get_unchecked_mut(t) };
                *depth_qty -= order.qty;
                if (*depth_qty / self.lot_size).round() as i64 == 0 {
                    *depth_qty = 0.0;
                    if order.price_tick == self.best_bid_tick {
                        self.best_bid_tick = depth_below(
                            &self.bid_depth,
                            self.best_bid_tick,
                            self.low_bid_tick,
                            self.roi_lb,
                            self.roi_ub,
                        );
                        if self.best_bid_tick == INVALID_MIN {
                            self.low_bid_tick = INVALID_MAX
                        }
                    }
                }
            }
            Ok((Side::Buy, prev_best_tick, self.best_bid_tick))
        } else {
            let prev_best_tick = self.best_ask_tick;

            if !(order.price_tick < self.roi_lb || order.price_tick > self.roi_ub) {
                let t = (order.price_tick - self.roi_lb) as usize;
                let depth_qty = unsafe { self.ask_depth.get_unchecked_mut(t) };
                *depth_qty -= order.qty;
                if (*depth_qty / self.lot_size).round() as i64 == 0 {
                    *depth_qty = 0.0;
                    if order.price_tick == self.best_ask_tick {
                        self.best_ask_tick = depth_above(
                            &self.ask_depth,
                            self.best_ask_tick,
                            self.high_ask_tick,
                            self.roi_lb,
                            self.roi_ub,
                        );
                        if self.best_ask_tick == INVALID_MAX {
                            self.high_ask_tick = INVALID_MIN
                        }
                    }
                }
            }
            Ok((Side::Sell, prev_best_tick, self.best_ask_tick))
        }
    }

    fn modify_order(
        &mut self,
        order_id: OrderId,
        px: f64,
        qty: f64,
        timestamp: i64,
    ) -> Result<(Side, i64, i64), Self::Error> {
        let order = self
            .orders
            .get_mut(&order_id)
            .ok_or(BacktestError::OrderNotFound)?;
        if order.side == Side::Buy {
            let prev_best_tick = self.best_bid_tick;
            let price_tick = (px / self.tick_size).round() as i64;
            if price_tick != order.price_tick {
                if !(order.price_tick < self.roi_lb || order.price_tick > self.roi_ub) {
                    let t = (order.price_tick - self.roi_lb) as usize;
                    let depth_qty = unsafe { self.bid_depth.get_unchecked_mut(t) };
                    *depth_qty -= order.qty;
                    if (*depth_qty / self.lot_size).round() as i64 == 0 {
                        *depth_qty = 0.0;
                        if order.price_tick == self.best_bid_tick {
                            self.best_bid_tick = depth_below(
                                &self.bid_depth,
                                self.best_bid_tick,
                                self.low_bid_tick,
                                self.roi_lb,
                                self.roi_ub,
                            );
                            if self.best_bid_tick == INVALID_MIN {
                                self.low_bid_tick = INVALID_MAX
                            }
                        }
                    }
                }

                order.price_tick = price_tick;
                order.qty = qty;
                order.timestamp = timestamp;

                if !(price_tick < self.roi_lb || price_tick > self.roi_ub) {
                    let t = (price_tick - self.roi_lb) as usize;
                    let depth_qty = unsafe { self.bid_depth.get_unchecked_mut(t) };
                    *depth_qty += order.qty;

                    if price_tick > self.best_bid_tick {
                        self.best_bid_tick = price_tick;
                        if self.best_bid_tick >= self.best_ask_tick {
                            self.best_ask_tick = depth_above(
                                &self.ask_depth,
                                self.best_bid_tick,
                                self.high_ask_tick,
                                self.roi_lb,
                                self.roi_ub,
                            );
                        }
                    }
                    self.low_bid_tick = self.low_bid_tick.min(price_tick);
                }
                Ok((Side::Buy, prev_best_tick, self.best_bid_tick))
            } else {
                if !(order.price_tick < self.roi_lb || order.price_tick > self.roi_ub) {
                    let t = (order.price_tick - self.roi_lb) as usize;
                    let depth_qty = unsafe { self.bid_depth.get_unchecked_mut(t) };
                    *depth_qty += qty - order.qty;
                }
                order.qty = qty;
                Ok((Side::Buy, self.best_bid_tick, self.best_bid_tick))
            }
        } else {
            let prev_best_tick = self.best_ask_tick;
            let price_tick = (px / self.tick_size).round() as i64;
            if price_tick != order.price_tick {
                if !(order.price_tick < self.roi_lb || order.price_tick > self.roi_ub) {
                    let t = (order.price_tick - self.roi_lb) as usize;
                    let depth_qty = unsafe { self.ask_depth.get_unchecked_mut(t) };
                    *depth_qty -= order.qty;
                    if (*depth_qty / self.lot_size).round() as i64 == 0 {
                        *depth_qty = 0.0;
                        if order.price_tick == self.best_ask_tick {
                            self.best_ask_tick = depth_above(
                                &self.ask_depth,
                                self.best_ask_tick,
                                self.high_ask_tick,
                                self.roi_lb,
                                self.roi_ub,
                            );
                            if self.best_ask_tick == INVALID_MAX {
                                self.high_ask_tick = INVALID_MIN
                            }
                        }
                    }
                }

                order.price_tick = price_tick;
                order.qty = qty;
                order.timestamp = timestamp;

                if !(price_tick < self.roi_lb || price_tick > self.roi_ub) {
                    let t = (price_tick - self.roi_lb) as usize;
                    let depth_qty = unsafe { self.ask_depth.get_unchecked_mut(t) };
                    *depth_qty += order.qty;

                    if price_tick < self.best_ask_tick {
                        self.best_ask_tick = price_tick;
                        if self.best_bid_tick >= self.best_ask_tick {
                            self.best_bid_tick = depth_below(
                                &self.bid_depth,
                                self.best_ask_tick,
                                self.low_bid_tick,
                                self.roi_lb,
                                self.roi_ub,
                            );
                        }
                    }
                    self.high_ask_tick = self.high_ask_tick.max(price_tick);
                }
                Ok((Side::Sell, prev_best_tick, self.best_ask_tick))
            } else {
                if !(order.price_tick < self.roi_lb || order.price_tick > self.roi_ub) {
                    let t = (order.price_tick - self.roi_lb) as usize;
                    let depth_qty = unsafe { self.ask_depth.get_unchecked_mut(t) };
                    *depth_qty += qty - order.qty;
                }
                order.qty = qty;
                Ok((Side::Sell, self.best_ask_tick, self.best_ask_tick))
            }
        }
    }

    fn clear_orders(&mut self, side: Side) {
        match side {
            Side::Buy => {
                L2MarketDepth::clear_depth(self, side, f64::NEG_INFINITY);
                let order_ids: Vec<_> = self
                    .orders
                    .iter()
                    .filter(|(_, order)| order.side == Side::Buy)
                    .map(|(order_id, _)| *order_id)
                    .collect();
                order_ids
                    .iter()
                    .for_each(|order_id| _ = self.orders.remove(order_id).unwrap());
            }
            Side::Sell => {
                L2MarketDepth::clear_depth(self, side, f64::INFINITY);
                let order_ids: Vec<_> = self
                    .orders
                    .iter()
                    .filter(|(_, order)| order.side == Side::Sell)
                    .map(|(order_id, _)| *order_id)
                    .collect();
                order_ids
                    .iter()
                    .for_each(|order_id| _ = self.orders.remove(order_id).unwrap());
            }
            Side::None => {
                L2MarketDepth::clear_depth(self, side, f64::NAN);
                self.orders.clear();
            }
            Side::Unsupported => {
                unreachable!();
            }
        }
    }

    fn orders(&self) -> &HashMap<OrderId, L3Order> {
        &self.orders
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        backtest::data::Data,
        backtest::models::{QueueModel, RiskAdverseQueueModel},
        depth::{
            ApplySnapshot,
            INVALID_MAX,
            INVALID_MIN,
            L2MarketDepth,
            L3MarketDepth,
            MarketDepth,
            ROIVectorMarketDepth,
        },
        types::{BUY_EVENT, DEPTH_SNAPSHOT_EVENT, EXCH_EVENT, Event, LOCAL_EVENT, OrdType, Order, SELL_EVENT, Side, TimeInForce},
    };

    macro_rules! assert_eq_qty {
        ( $a:expr, $b:expr, $lot_size:ident ) => {{
            assert_eq!(
                ($a / $lot_size).round() as i64,
                ($b / $lot_size).round() as i64
            );
        }};
    }

    #[test]
    fn test_out_of_roi_qty_queries_return_zero_instead_of_nan() {
        let depth = ROIVectorMarketDepth::new(1.0, 1.0, 0.0, 10.0);

        let bid_qty = depth.bid_qty_at_tick(-1);
        assert_eq!(bid_qty, 0.0);
        assert!(bid_qty.is_finite());

        let ask_qty = depth.ask_qty_at_tick(11);
        assert_eq!(ask_qty, 0.0);
        assert!(ask_qty.is_finite());
    }

    #[test]
    fn test_best_ask_qty_is_zero_when_no_best_ask() {
        let depth = ROIVectorMarketDepth::new(1.0, 1.0, 0.0, 10.0);

        let best_ask_qty = depth.best_ask_qty();
        assert_eq!(best_ask_qty, 0.0);
        assert!(best_ask_qty.is_finite());
    }

    #[test]
    fn test_queue_model_new_order_out_of_roi_does_not_nan_poison() {
        let depth = ROIVectorMarketDepth::new(1.0, 1.0, 0.0, 10.0);
        let queue_model = RiskAdverseQueueModel::<ROIVectorMarketDepth>::new();

        let mut order = Order::new(
            1,
            50,
            1.0,
            1.0,
            Side::Buy,
            OrdType::Limit,
            TimeInForce::GTC,
        );
        queue_model.new_order(&mut order, &depth);

        let front_q_qty = order.q.as_any().downcast_ref::<f64>().unwrap();
        assert_eq!(*front_q_qty, 0.0);
        assert!(front_q_qty.is_finite());
    }

    #[test]
    fn test_l2_clear_depth_sell_clears_and_recomputes_best_ask() {
        let mut depth = ROIVectorMarketDepth::new(1.0, 1.0, 0.0, 10.0);

        depth.update_ask_depth(5.0, 1.0, 0);
        depth.update_ask_depth(6.0, 1.0, 0);
        assert_eq!(depth.best_ask_tick(), 5);

        depth.clear_depth(Side::Sell, 5.0);
        assert_eq!(depth.best_ask_tick(), 6);
        assert_eq!(depth.ask_qty_at_tick(5), 0.0);
    }

    #[test]
    fn test_l2_clear_depth_buy_does_not_move_best_when_clear_above_roi_ub() {
        let mut depth = ROIVectorMarketDepth::new(1.0, 1.0, 0.0, 10.0);

        depth.update_bid_depth(10.0, 1.0, 0);
        depth.update_bid_depth(9.0, 1.0, 0);
        assert_eq!(depth.best_bid_tick(), 10);

        depth.clear_depth(Side::Buy, 11.0);
        assert_eq!(depth.best_bid_tick(), 10);
    }

    #[test]
    fn test_recompute_depth_above_clamps_into_roi_and_returns_invalid_max() {
        let mut depth = ROIVectorMarketDepth::new(1.0, 1.0, -1.0, 1.0);
        depth.update_ask_depth(0.0, 1.0, 0);

        let _ = depth.add_buy_order(1, f64::MAX, 1.0, 0).unwrap();
        assert_eq!(depth.best_ask_tick(), INVALID_MAX);
    }

    #[test]
    fn test_recompute_depth_below_clamps_into_roi_and_returns_invalid_min() {
        let mut depth = ROIVectorMarketDepth::new(1.0, 1.0, 1.0, 2.0);

        let _ = depth.add_sell_order(1, -f64::MAX, 1.0, 0).unwrap();
        assert_eq!(depth.best_bid_tick(), INVALID_MIN);
    }

    #[test]
    fn test_l3_add_delete_buy_order() {
        let lot_size = 0.001;
        let mut depth = ROIVectorMarketDepth::new(0.1, lot_size, 0.0, 2000.0);

        let (prev_best, best) = depth.add_buy_order(1, 500.1, 0.001, 0).unwrap();
        assert_eq!(prev_best, INVALID_MIN);
        assert_eq!(best, 5001);
        assert_eq!(depth.best_bid_tick(), 5001);
        assert_eq_qty!(depth.bid_qty_at_tick(5001), 0.001, lot_size);

        assert!(depth.add_buy_order(1, 500.2, 0.001, 0).is_err());

        let (prev_best, best) = depth.add_buy_order(2, 500.3, 0.005, 0).unwrap();
        assert_eq!(prev_best, 5001);
        assert_eq!(best, 5003);
        assert_eq!(depth.best_bid_tick(), 5003);
        assert_eq_qty!(depth.bid_qty_at_tick(5003), 0.005, lot_size);

        let (prev_best, best) = depth.add_buy_order(3, 500.1, 0.005, 0).unwrap();
        assert_eq!(prev_best, 5003);
        assert_eq!(best, 5003);
        assert_eq!(depth.best_bid_tick(), 5003);
        assert_eq_qty!(depth.bid_qty_at_tick(5001), 0.006, lot_size);

        let (prev_best, best) = depth.add_buy_order(4, 500.5, 0.005, 0).unwrap();
        assert_eq!(prev_best, 5003);
        assert_eq!(best, 5005);
        assert_eq!(depth.best_bid_tick(), 5005);
        assert_eq_qty!(depth.bid_qty_at_tick(5005), 0.005, lot_size);

        assert!(depth.delete_order(10, 0).is_err());

        let (side, prev_best, best) = depth.delete_order(2, 0).unwrap();
        assert_eq!(side, Side::Buy);
        assert_eq!(prev_best, 5005);
        assert_eq!(best, 5005);
        assert_eq!(depth.best_bid_tick(), 5005);
        assert_eq_qty!(depth.bid_qty_at_tick(5003), 0.0, lot_size);

        let (side, prev_best, best) = depth.delete_order(4, 0).unwrap();
        assert_eq!(side, Side::Buy);
        assert_eq!(prev_best, 5005);
        assert_eq!(best, 5001);
        assert_eq!(depth.best_bid_tick(), 5001);
        assert_eq_qty!(depth.bid_qty_at_tick(5005), 0.0, lot_size);

        let (side, prev_best, best) = depth.delete_order(3, 0).unwrap();
        assert_eq!(side, Side::Buy);
        assert_eq!(prev_best, 5001);
        assert_eq!(best, 5001);
        assert_eq!(depth.best_bid_tick(), 5001);
        assert_eq_qty!(depth.bid_qty_at_tick(5001), 0.001, lot_size);

        let (side, prev_best, best) = depth.delete_order(1, 0).unwrap();
        assert_eq!(side, Side::Buy);
        assert_eq!(prev_best, 5001);
        assert_eq!(best, INVALID_MIN);
        assert_eq!(depth.best_bid_tick(), INVALID_MIN);
        assert_eq_qty!(depth.bid_qty_at_tick(5001), 0.0, lot_size);
    }

    #[test]
    fn test_l3_add_delete_sell_order() {
        let lot_size = 0.001;
        let mut depth = ROIVectorMarketDepth::new(0.1, lot_size, 0.0, 2000.0);

        let (prev_best, best) = depth.add_sell_order(1, 500.1, 0.001, 0).unwrap();
        assert_eq!(prev_best, INVALID_MAX);
        assert_eq!(best, 5001);
        assert_eq!(depth.best_ask_tick(), 5001);
        assert_eq_qty!(depth.ask_qty_at_tick(5001), 0.001, lot_size);

        assert!(depth.add_sell_order(1, 500.2, 0.001, 0).is_err());

        let (prev_best, best) = depth.add_sell_order(2, 499.3, 0.005, 0).unwrap();
        assert_eq!(prev_best, 5001);
        assert_eq!(best, 4993);
        assert_eq!(depth.best_ask_tick(), 4993);
        assert_eq_qty!(depth.ask_qty_at_tick(4993), 0.005, lot_size);

        let (prev_best, best) = depth.add_sell_order(3, 500.1, 0.005, 0).unwrap();
        assert_eq!(prev_best, 4993);
        assert_eq!(best, 4993);
        assert_eq!(depth.best_ask_tick(), 4993);
        assert_eq_qty!(depth.ask_qty_at_tick(5001), 0.006, lot_size);

        let (prev_best, best) = depth.add_sell_order(4, 498.5, 0.005, 0).unwrap();
        assert_eq!(prev_best, 4993);
        assert_eq!(best, 4985);
        assert_eq!(depth.best_ask_tick(), 4985);
        assert_eq_qty!(depth.ask_qty_at_tick(4985), 0.005, lot_size);

        assert!(depth.delete_order(10, 0).is_err());

        let (side, prev_best, best) = depth.delete_order(2, 0).unwrap();
        assert_eq!(side, Side::Sell);
        assert_eq!(prev_best, 4985);
        assert_eq!(best, 4985);
        assert_eq!(depth.best_ask_tick(), 4985);
        assert_eq_qty!(depth.ask_qty_at_tick(4993), 0.0, lot_size);

        let (side, prev_best, best) = depth.delete_order(4, 0).unwrap();
        assert_eq!(side, Side::Sell);
        assert_eq!(prev_best, 4985);
        assert_eq!(best, 5001);
        assert_eq!(depth.best_ask_tick(), 5001);
        assert_eq_qty!(depth.ask_qty_at_tick(4985), 0.0, lot_size);

        let (side, prev_best, best) = depth.delete_order(3, 0).unwrap();
        assert_eq!(side, Side::Sell);
        assert_eq!(prev_best, 5001);
        assert_eq!(best, 5001);
        assert_eq!(depth.best_ask_tick(), 5001);
        assert_eq_qty!(depth.ask_qty_at_tick(5001), 0.001, lot_size);

        let (side, prev_best, best) = depth.delete_order(1, 0).unwrap();
        assert_eq!(side, Side::Sell);
        assert_eq!(prev_best, 5001);
        assert_eq!(best, INVALID_MAX);
        assert_eq!(depth.best_ask_tick(), INVALID_MAX);
        assert_eq_qty!(depth.ask_qty_at_tick(5001), 0.0, lot_size);
    }

    #[test]
    fn test_l3_modify_buy_order() {
        let lot_size = 0.001;
        let mut depth = ROIVectorMarketDepth::new(0.1, lot_size, 0.0, 2000.0);

        depth.add_buy_order(1, 500.1, 0.001, 0).unwrap();
        depth.add_buy_order(2, 500.3, 0.005, 0).unwrap();
        depth.add_buy_order(3, 500.1, 0.005, 0).unwrap();
        depth.add_buy_order(4, 500.5, 0.005, 0).unwrap();

        assert!(depth.modify_order(10, 500.5, 0.001, 0).is_err());

        let (side, prev_best, best) = depth.modify_order(2, 500.5, 0.001, 0).unwrap();
        assert_eq!(side, Side::Buy);
        assert_eq!(prev_best, 5005);
        assert_eq!(best, 5005);
        assert_eq!(depth.best_bid_tick(), 5005);
        assert_eq_qty!(depth.bid_qty_at_tick(5005), 0.006, lot_size);

        let (side, prev_best, best) = depth.modify_order(2, 500.7, 0.002, 0).unwrap();
        assert_eq!(side, Side::Buy);
        assert_eq!(prev_best, 5005);
        assert_eq!(best, 5007);
        assert_eq!(depth.best_bid_tick(), 5007);
        assert_eq_qty!(depth.bid_qty_at_tick(5005), 0.005, lot_size);
        assert_eq_qty!(depth.bid_qty_at_tick(5007), 0.002, lot_size);

        let (side, prev_best, best) = depth.modify_order(2, 500.6, 0.002, 0).unwrap();
        assert_eq!(side, Side::Buy);
        assert_eq!(prev_best, 5007);
        assert_eq!(best, 5006);
        assert_eq!(depth.best_bid_tick(), 5006);
        assert_eq_qty!(depth.bid_qty_at_tick(5007), 0.0, lot_size);

        let _ = depth.delete_order(4, 0).unwrap();
        let (side, prev_best, best) = depth.modify_order(2, 500.0, 0.002, 0).unwrap();
        assert_eq!(side, Side::Buy);
        assert_eq!(prev_best, 5006);
        assert_eq!(best, 5001);
        assert_eq!(depth.best_bid_tick(), 5001);
        assert_eq_qty!(depth.bid_qty_at_tick(5006), 0.0, lot_size);
        assert_eq_qty!(depth.bid_qty_at_tick(5000), 0.002, lot_size);
    }

    #[test]
    fn test_l3_modify_sell_order() {
        let lot_size = 0.001;
        let mut depth = ROIVectorMarketDepth::new(0.1, lot_size, 0.0, 2000.0);

        depth.add_sell_order(1, 500.1, 0.001, 0).unwrap();
        depth.add_sell_order(2, 499.3, 0.005, 0).unwrap();
        depth.add_sell_order(3, 500.1, 0.005, 0).unwrap();
        depth.add_sell_order(4, 498.5, 0.005, 0).unwrap();

        assert!(depth.modify_order(10, 500.5, 0.001, 0).is_err());

        let (side, prev_best, best) = depth.modify_order(2, 498.5, 0.001, 0).unwrap();
        assert_eq!(side, Side::Sell);
        assert_eq!(prev_best, 4985);
        assert_eq!(best, 4985);
        assert_eq!(depth.best_ask_tick(), 4985);
        assert_eq_qty!(depth.ask_qty_at_tick(4985), 0.006, lot_size);

        let (side, prev_best, best) = depth.modify_order(2, 497.7, 0.002, 0).unwrap();
        assert_eq!(side, Side::Sell);
        assert_eq!(prev_best, 4985);
        assert_eq!(best, 4977);
        assert_eq!(depth.best_ask_tick(), 4977);
        assert_eq_qty!(depth.ask_qty_at_tick(4985), 0.005, lot_size);
        assert_eq_qty!(depth.ask_qty_at_tick(4977), 0.002, lot_size);

        let (side, prev_best, best) = depth.modify_order(2, 498.1, 0.002, 0).unwrap();
        assert_eq!(side, Side::Sell);
        assert_eq!(prev_best, 4977);
        assert_eq!(best, 4981);
        assert_eq!(depth.best_ask_tick(), 4981);
        assert_eq_qty!(depth.ask_qty_at_tick(4977), 0.0, lot_size);

        let _ = depth.delete_order(4, 0).unwrap();
        let (side, prev_best, best) = depth.modify_order(2, 500.2, 0.002, 0).unwrap();
        assert_eq!(side, Side::Sell);
        assert_eq!(prev_best, 4981);
        assert_eq!(best, 5001);
        assert_eq!(depth.best_ask_tick(), 5001);
        assert_eq_qty!(depth.ask_qty_at_tick(4981), 0.0, lot_size);
        assert_eq_qty!(depth.ask_qty_at_tick(5002), 0.002, lot_size);
    }

    #[test]
    fn snapshot_round_trip_preserves_bbo_and_qty() {
        let tick_size = 1.0;
        let lot_size = 0.01;
        let roi_lb = 90.0;
        let roi_ub = 110.0;
        let snapshot = vec![
            Event {
                ev: EXCH_EVENT | LOCAL_EVENT | BUY_EVENT | DEPTH_SNAPSHOT_EVENT,
                exch_ts: 0,
                local_ts: 0,
                px: 99.0,
                qty: 1.0,
                order_id: 0,
                ival: 0,
                fval: 0.0,
            },
            Event {
                ev: EXCH_EVENT | LOCAL_EVENT | BUY_EVENT | DEPTH_SNAPSHOT_EVENT,
                exch_ts: 0,
                local_ts: 0,
                px: 98.0,
                qty: 2.0,
                order_id: 0,
                ival: 0,
                fval: 0.0,
            },
            Event {
                ev: EXCH_EVENT | LOCAL_EVENT | SELL_EVENT | DEPTH_SNAPSHOT_EVENT,
                exch_ts: 0,
                local_ts: 0,
                px: 101.0,
                qty: 1.5,
                order_id: 0,
                ival: 0,
                fval: 0.0,
            },
            Event {
                ev: EXCH_EVENT | LOCAL_EVENT | SELL_EVENT | DEPTH_SNAPSHOT_EVENT,
                exch_ts: 0,
                local_ts: 0,
                px: 102.0,
                qty: 3.0,
                order_id: 0,
                ival: 0,
                fval: 0.0,
            },
        ];

        let data = Data::from_data(&snapshot);
        let mut depth = ROIVectorMarketDepth::new(tick_size, lot_size, roi_lb, roi_ub);
        depth.apply_snapshot(&data);

        let snapshot = depth.snapshot();
        let data = Data::from_data(&snapshot);
        let mut round_tripped = ROIVectorMarketDepth::new(tick_size, lot_size, roi_lb, roi_ub);
        round_tripped.apply_snapshot(&data);

        assert_eq!(round_tripped.best_bid_tick(), depth.best_bid_tick());
        assert_eq!(round_tripped.best_ask_tick(), depth.best_ask_tick());
        assert_eq_qty!(round_tripped.bid_qty_at_tick(99), depth.bid_qty_at_tick(99), lot_size);
        assert_eq_qty!(round_tripped.bid_qty_at_tick(98), depth.bid_qty_at_tick(98), lot_size);
        assert_eq_qty!(round_tripped.ask_qty_at_tick(101), depth.ask_qty_at_tick(101), lot_size);
        assert_eq_qty!(round_tripped.ask_qty_at_tick(102), depth.ask_qty_at_tick(102), lot_size);
    }
}
