import unittest

import numpy as np
from numba import njit

import hftbacktest as hbt
from hftbacktest.types import (
    ADD_ORDER_EVENT,
    BUY_EVENT,
    DEPTH_BBO_EVENT,
    EXCH_EVENT,
    event_dtype,
)


@njit
def run_until_end(bt):
    while bt.elapse(10_000_000_000) == 0:
        _ = bt.current_timestamp
        _ = bt.depth(0).best_bid
        _ = bt.depth(0).best_ask
    return bt.current_timestamp


class TestPyHftBacktest(unittest.TestCase):
    def test_build_and_elapse_hashmap_l2(self):
        data = np.zeros(2, dtype=event_dtype)
        # Seed a best-bid update so depth getters are exercised under numba.
        data[0]["ev"] = EXCH_EVENT | DEPTH_BBO_EVENT | BUY_EVENT
        data[0]["exch_ts"] = 1
        data[0]["local_ts"] = 1
        data[0]["px"] = 100.0
        data[0]["qty"] = 1.0
        data[0]["ival"] = 1

        # A second event to ensure the reader advances.
        data[1]["ev"] = EXCH_EVENT | ADD_ORDER_EVENT | BUY_EVENT
        data[1]["exch_ts"] = 2
        data[1]["local_ts"] = 2
        data[1]["px"] = 100.0
        data[1]["qty"] = 1.0
        data[1]["order_id"] = 1
        data[1]["ival"] = 2

        asset = (
            hbt.L2Asset()
              .data(data)
              .tick_size(1.0)
              .lot_size(1.0)
              .linear_asset(1.0)
              .no_partial_fill_exchange()
              .constant_latency(0, 0)
        )
        bt = hbt.BacktestBuilder.hashmap().add_asset(asset).build()
        ts = run_until_end(bt)
        self.assertGreaterEqual(int(ts), 0)


if __name__ == "__main__":
    unittest.main()

