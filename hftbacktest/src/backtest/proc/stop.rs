use crate::types::AnyClone;
use std::any::Any;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TriggerOrderKind {
    StopMarket,
    StopLimit,
    Mit,
}

#[derive(Clone, Debug)]
pub struct TriggerOrderParams {
    pub kind: TriggerOrderKind,
    /// Trigger price in ticks.
    pub trigger_tick: i64,
}

impl AnyClone for TriggerOrderParams {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

