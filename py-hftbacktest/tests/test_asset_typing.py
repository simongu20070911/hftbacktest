import unittest

import numpy as np

import hftbacktest as hbt
from hftbacktest.types import event_dtype, EXCH_EVENT, ADD_ORDER_EVENT, BUY_EVENT


class TestAssetTypingGuardrails(unittest.TestCase):
    def test_l3_partial_fill_exchange_is_guarded(self):
        asset = hbt.L3Asset()
        with self.assertRaises(ValueError) as cm:
            asset.partial_fill_exchange()
        self.assertIn("requires cme_databento_mbo(True)", str(cm.exception))

    def test_l2_cme_mbo_is_guarded(self):
        asset = hbt.L2Asset()
        with self.assertRaises(ValueError) as cm:
            asset.cme_databento_mbo(True)
        self.assertIn("only valid for L3Asset", str(cm.exception))

    def test_l2_l3_queue_model_is_guarded(self):
        asset = hbt.L2Asset()
        with self.assertRaises(ValueError) as cm:
            asset.l3_fifo_queue_model()
        self.assertIn("use L3Asset()", str(cm.exception))

    def test_l3_l2_queue_model_is_guarded(self):
        asset = hbt.L3Asset()
        with self.assertRaises(ValueError) as cm:
            asset.power_prob_queue_model3(3.0)
        self.assertIn("not valid for L3Asset", str(cm.exception))

    def test_l3_requires_seq_tie_break_tracks_cme_switch(self):
        data = np.zeros(1, dtype=event_dtype)
        data[0]["ev"] = EXCH_EVENT | ADD_ORDER_EVENT | BUY_EVENT
        data[0]["exch_ts"] = 1
        data[0]["local_ts"] = 1
        data[0]["px"] = 100.0
        data[0]["qty"] = 1.0
        data[0]["order_id"] = 1
        data[0]["ival"] = 1

        asset = (
            hbt.L3Asset()
            .data(data)
            .tick_size(1.0)
            .lot_size(1.0)
            .linear_asset(1.0)
            .constant_latency(0, 0)
            .trading_value_fee_model(0.0, 0.0)
        )
        self.assertFalse(asset.requires_seq_tie_break)
        asset.cme_databento_mbo(True)
        self.assertTrue(asset.requires_seq_tie_break)


if __name__ == "__main__":
    unittest.main()

