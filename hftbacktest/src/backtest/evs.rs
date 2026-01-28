use std::mem;

use crate::utils::{AlignedArray, CACHE_LINE_SIZE};

#[derive(Clone, Copy)]
#[repr(C, align(32))]
pub struct EventIntent {
    pub timestamp: i64,
    pub asset_no: usize,
    pub kind: EventIntentKind,
}

/// This is constructed by using transmute in `EventSet::next`.
#[allow(dead_code)]
#[derive(Eq, PartialEq, Clone, Copy)]
#[repr(usize)]
pub enum EventIntentKind {
    LocalData = 0,
    LocalOrder = 1,
    /// Exchange order receipt/response events (from the order bus). Their ordering vs
    /// [`EventIntentKind::ExchData`] for equal timestamps is controlled by the caller via the
    /// `(timestamp, seq)` keys stored in [`EventSet`].
    ExchOrder = 2,
    ExchData = 3,
}

/// Manages the event timestamps to determine the next event to be processed.
pub struct EventSet {
    timestamp: AlignedArray<i64, CACHE_LINE_SIZE>,
    seq: AlignedArray<i64, CACHE_LINE_SIZE>,
    use_seq_tie_break: bool,
}

impl EventSet {
    /// Constructs an instance of `EventSet`.
    pub fn new(num_assets: usize, use_seq_tie_break: bool) -> Self {
        if num_assets == 0 {
            panic!();
        }
        let mut timestamp = AlignedArray::<i64, CACHE_LINE_SIZE>::new(num_assets * 4);
        let mut seq = AlignedArray::<i64, CACHE_LINE_SIZE>::new(num_assets * 4);
        for i in 0..(num_assets * 4) {
            timestamp[i] = i64::MAX;
            seq[i] = i64::MAX;
        }
        Self {
            timestamp,
            seq,
            use_seq_tie_break,
        }
    }

    /// Returns the next event to be processed, which has the earliest timestamp.
    pub fn next(&self) -> Option<EventIntent> {
        let mut evst_no = 0;
        let mut timestamp = unsafe { *self.timestamp.get_unchecked(0) };
        let mut seq = unsafe { *self.seq.get_unchecked(0) };
        for (i, &ev_timestamp) in self.timestamp[1..].iter().enumerate() {
            let j = i + 1;
            if ev_timestamp < timestamp {
                timestamp = ev_timestamp;
                seq = unsafe { *self.seq.get_unchecked(j) };
                evst_no = j;
                continue;
            }

            if ev_timestamp == timestamp && self.use_seq_tie_break {
                let ev_seq = unsafe { *self.seq.get_unchecked(j) };
                if ev_seq < seq {
                    seq = ev_seq;
                    evst_no = j;
                }
            }
        }
        // Returns None if no valid events are found.
        if timestamp == i64::MAX {
            return None;
        }
        let asset_no = evst_no >> 2;
        let kind = unsafe { mem::transmute::<usize, EventIntentKind>(evst_no & 3) };
        Some(EventIntent {
            timestamp,
            asset_no,
            kind,
        })
    }

    #[inline]
    fn update(&mut self, evst_no: usize, timestamp: i64, seq: i64) {
        let ts_item = unsafe { self.timestamp.get_unchecked_mut(evst_no) };
        let seq_item = unsafe { self.seq.get_unchecked_mut(evst_no) };
        *ts_item = timestamp;
        *seq_item = seq;
    }

    #[inline]
    pub fn update_local_data(&mut self, asset_no: usize, timestamp: i64, seq: i64) {
        self.update(4 * asset_no, timestamp, seq);
    }

    #[inline]
    pub fn update_local_order(&mut self, asset_no: usize, timestamp: i64, seq: i64) {
        self.update(4 * asset_no + 1, timestamp, seq);
    }

    #[inline]
    pub fn update_exch_data(&mut self, asset_no: usize, timestamp: i64, seq: i64) {
        self.update(4 * asset_no + 3, timestamp, seq);
    }

    #[inline]
    pub fn update_exch_order(&mut self, asset_no: usize, timestamp: i64, seq: i64) {
        self.update(4 * asset_no + 2, timestamp, seq);
    }

    #[inline]
    fn invalidate(&mut self, evst_no: usize) {
        let ts_item = unsafe { self.timestamp.get_unchecked_mut(evst_no) };
        let seq_item = unsafe { self.seq.get_unchecked_mut(evst_no) };
        *ts_item = i64::MAX;
        *seq_item = i64::MAX;
    }

    #[inline]
    pub fn invalidate_local_data(&mut self, asset_no: usize) {
        self.invalidate(4 * asset_no);
    }

    #[inline]
    pub fn invalidate_exch_data(&mut self, asset_no: usize) {
        self.invalidate(4 * asset_no + 3);
    }

    #[inline]
    pub fn peek_exch_data_timestamp(&self, asset_no: usize) -> i64 {
        self.timestamp[4 * asset_no + 3]
    }

    #[inline]
    pub fn peek_exch_order_timestamp(&self, asset_no: usize) -> i64 {
        self.timestamp[4 * asset_no + 2]
    }

    #[cfg(test)]
    pub fn debug_slot(&self, asset_no: usize, kind: EventIntentKind) -> (i64, i64) {
        let idx = 4 * asset_no + kind as usize;
        (self.timestamp[idx], self.seq[idx])
    }
}
