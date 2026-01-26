import os
import sys
import types
import unittest


class TestDatabentoTimestampPrecision(unittest.TestCase):
    def setUp(self) -> None:
        repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
        # Avoid importing `hftbacktest/__init__.py` (requires compiled extension). Instead, stub
        # the package path so we can import pure-Python modules under `hftbacktest.data.*`.
        pkg_path = os.path.join(repo_root, "hftbacktest")
        pkg = types.ModuleType("hftbacktest")
        pkg.__path__ = [pkg_path]
        sys.modules["hftbacktest"] = pkg

        data_pkg_path = os.path.join(pkg_path, "data")
        data_pkg = types.ModuleType("hftbacktest.data")
        data_pkg.__path__ = [data_pkg_path]
        sys.modules["hftbacktest.data"] = data_pkg

        utils_pkg_path = os.path.join(data_pkg_path, "utils")
        utils_pkg = types.ModuleType("hftbacktest.data.utils")
        utils_pkg.__path__ = [utils_pkg_path]
        sys.modules["hftbacktest.data.utils"] = utils_pkg

    def test_epoch_ns_preserves_nanoseconds(self) -> None:
        import pandas as pd
        import polars as pl

        import importlib

        databento_utils = importlib.import_module("hftbacktest.data.utils.databento")

        ns = 1768401000000015607  # includes sub-microsecond remainder (..5607ns)
        ts = pd.Timestamp(ns, unit="ns", tz="UTC")

        pdf = pd.DataFrame(
            {
                "ts_event": [ts],
                "action": ["A"],
                "side": ["B"],
                "price": [1.0],
                "size": [1],
                "order_id": [1],
                "flags": [0],
            }
        )
        pdf.index = pd.DatetimeIndex([ts])
        df = pl.DataFrame(pdf).with_columns(pl.Series("ts_recv", pdf.index))
        df = df.select(
            ["ts_event", "action", "side", "price", "size", "order_id", "flags", "ts_recv"]
        )

        # Without epoch conversion, iter_rows yields Python datetimes and loses the nanoseconds.
        (ts_event_py, ts_recv_py) = next(df.select(["ts_event", "ts_recv"]).iter_rows())
        self.assertIsInstance(ts_event_py, object)
        self.assertNotEqual(int(ts_event_py.timestamp() * 1_000_000_000), ns)

        df = df.with_columns(
            databento_utils._epoch_ns_col("ts_event"),
            databento_utils._epoch_ns_col("ts_recv"),
        )
        (ts_event_ns, *_rest, ts_recv_ns) = next(df.iter_rows())
        self.assertEqual(int(ts_event_ns), ns)
        self.assertEqual(int(ts_recv_ns), ns)

    def test_converter_does_not_use_datetime_timestamp(self) -> None:
        repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
        path = os.path.join(repo_root, "hftbacktest", "data", "utils", "databento.py")
        with open(path, "r", encoding="utf-8") as f:
            src = f.read()
        self.assertNotIn(".timestamp() * 1_000_000_000", src)
        self.assertNotIn(".timestamp()*1_000_000_000", src)


if __name__ == "__main__":
    unittest.main()
