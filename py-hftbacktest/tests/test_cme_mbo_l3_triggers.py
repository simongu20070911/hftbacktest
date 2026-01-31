import unittest

import numpy as np
from numba import njit

import hftbacktest as hbt
from hftbacktest.types import (
    ADD_ORDER_EVENT,
    BUY_EVENT,
    EXCH_EVENT,
    FILL_EVENT,
    SELL_EVENT,
    TRADE_EVENT,
    event_dtype,
)


@njit
def submit_stop_once(bt):
    bt.submit_stop_market(0, 1, hbt.BUY, 104.0, 1.0, hbt.IOC, False)
    return bt.uses_seq_tie_break


class TestCmeMboL3Triggers(unittest.TestCase):
    def test_stop_market_defers_activation_until_exch_ts_advance(self):
        data = np.zeros(4, dtype=event_dtype)

        # Best ask = 101, qty = 2 (visible top-of-book liquidity).
        mkt_ask_order_id = 10
        data[0]["ev"] = EXCH_EVENT | ADD_ORDER_EVENT | SELL_EVENT
        data[0]["exch_ts"] = 1
        data[0]["local_ts"] = 1
        data[0]["px"] = 101.0
        data[0]["qty"] = 2.0
        data[0]["order_id"] = mkt_ask_order_id
        data[0]["ival"] = 1_000_001

        # Trade triggers the stop, but activation must be deferred until leaving the bucket.
        data[1]["ev"] = EXCH_EVENT | TRADE_EVENT | BUY_EVENT
        data[1]["exch_ts"] = 10
        data[1]["local_ts"] = 10
        data[1]["px"] = 105.0
        data[1]["qty"] = 1.0
        data[1]["order_id"] = mkt_ask_order_id
        data[1]["ival"] = 1_000_002

        # Same exch_ts bucket (Databento packet/sequence).
        data[2]["ev"] = EXCH_EVENT | FILL_EVENT | SELL_EVENT
        data[2]["exch_ts"] = 10
        data[2]["local_ts"] = 10
        data[2]["px"] = 101.0
        data[2]["qty"] = 1.0
        data[2]["order_id"] = mkt_ask_order_id
        data[2]["ival"] = 1_000_002

        # Next bucket: stop should activate here and fill at 101.
        data[3]["ev"] = EXCH_EVENT | FILL_EVENT | SELL_EVENT
        data[3]["exch_ts"] = 11
        data[3]["local_ts"] = 11
        data[3]["px"] = 101.0
        data[3]["qty"] = 1.0
        data[3]["order_id"] = mkt_ask_order_id
        data[3]["ival"] = 1_000_002

        asset = (
            hbt.L3Asset()
              .data(data)
              .tick_size(1.0)
              .lot_size(1.0)
              .linear_asset(1.0)
              .cme_databento_mbo(True)
              .constant_latency(0, 0)
              .trading_value_fee_model(0.0, 0.0)
        )

        bt = (
            hbt.BacktestBuilder.hashmap()
              .add_asset(asset)
              .exch_order_equal_ts_policy(hbt.EXCH_EQUAL_TS_AFTER_DATA)
              .build()
        )

        # CME Databento MBO mode requires (timestamp, seq) ordering.
        self.assertTrue(bt.uses_seq_tie_break)

        # Also ensure the trigger submit API is Numba-callable (use a separate instance so we
        # don't mutate the state used for the behavior assertions below).
        bt_nb = (
            hbt.BacktestBuilder.hashmap()
              .add_asset(asset)
              .exch_order_equal_ts_policy(hbt.EXCH_EQUAL_TS_AFTER_DATA)
              .build()
        )
        bt_nb.elapse(0)
        self.assertTrue(submit_stop_once(bt_nb))
        bt_nb.close()

        # Initialize backtest time (and build the book at exch_ts=1) so submits don't occur at
        # `i64::MAX` before the first feed event.
        bt.elapse(0)
        self.assertEqual(int(bt.current_timestamp), 1)

        # Submit the stop with wait=True so it is accepted/acked before we replay market data.
        rc = bt.submit_stop_market(0, 2, hbt.BUY, 104.0, 1.0, hbt.IOC, True)
        self.assertEqual(rc, 0)
        ord0 = bt.orders(0).get(2)
        self.assertIsNotNone(ord0)
        self.assertTrue(ord0.is_trigger)
        self.assertEqual(int(ord0.trigger_kind), hbt.STOP_MARKET)
        self.assertAlmostEqual(float(ord0.trigger_price), 104.0)
        self.assertAlmostEqual(float(ord0.tick_size), 1.0)

        while bt.elapse(10_000_000_000) == 0:
            pass

        ord_final = bt.orders(0).get(2)
        self.assertEqual(int(ord_final.status), hbt.FILLED)
        self.assertEqual(float(ord_final.exec_price), 101.0)
        self.assertEqual(float(ord_final.exec_qty), 1.0)
        self.assertEqual(int(ord_final.exch_timestamp), 11)


if __name__ == "__main__":
    unittest.main()
