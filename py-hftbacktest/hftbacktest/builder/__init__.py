from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List

import numpy as np
from numpy.typing import NDArray

from .._hftbacktest import BacktestAsset as BacktestAsset_, build_hashmap_backtest, build_roivec_backtest
from ..binding import (
    HashMapMarketDepthBacktest as HashMapMarketDepthBacktest_TypeHint,
    HashMapMarketDepthBacktest_,
    ROIVectorMarketDepthBacktest as ROIVectorMarketDepthBacktest_TypeHint,
    ROIVectorMarketDepthBacktest_,
    event_dtype,
)
from ..types import EVENT_ARRAY


EXCH_EQUAL_TS_BEFORE_DATA = 0
EXCH_EQUAL_TS_AFTER_DATA = 1


def EXCH_EQUAL_TS_RANDOM_SEEDED(seed: int) -> tuple[int, int]:
    return 2, int(seed)


@dataclass(frozen=True)
class _BacktestBuildMode:
    kind: str  # "hashmap" | "roivec"
    roi_lb: float | None = None
    roi_ub: float | None = None


class _AssetBase(BacktestAsset_):
    _symbol: str | None
    _roi_lb_set: bool
    _roi_ub_set: bool

    def __init__(self):
        super().__init__()
        self._symbol = None
        self._roi_lb_set = False
        self._roi_ub_set = False

    def symbol(self, symbol: str) -> "_AssetBase":
        self._symbol = str(symbol)
        return self

    @property
    def requires_seq_tie_break(self) -> bool:
        return False

    def add_data(self, data: EVENT_ARRAY):
        self._add_data_ndarray(data.ctypes.data, len(data))
        return self

    def data(self, data: str | List[str] | EVENT_ARRAY | List[EVENT_ARRAY]):
        if isinstance(data, str):
            self.add_file(data)
        elif isinstance(data, np.ndarray):
            self.add_data(data)
        elif isinstance(data, list):
            for item in data:
                if isinstance(item, str):
                    self.add_file(item)
                elif isinstance(item, np.ndarray):
                    self.add_data(item)
                else:
                    raise ValueError("data list must contain str paths or numpy arrays")
        else:
            raise ValueError("data must be a str path, numpy array, or a list of those")
        return self

    def intp_order_latency(self, data: str | NDArray | List[str], latency_offset: int = 0):
        if isinstance(data, str):
            super().intp_order_latency([data], latency_offset)
        elif isinstance(data, np.ndarray):
            self._intp_order_latency_ndarray(data.ctypes.data, len(data), latency_offset)
        elif isinstance(data, list):
            super().intp_order_latency(data, latency_offset)
        else:
            raise ValueError("data must be a str path, numpy array, or a list[str]")
        return self

    def roi_lb(self, roi_lb: float):
        self._roi_lb_set = True
        return super().roi_lb(float(roi_lb))

    def roi_ub(self, roi_ub: float):
        self._roi_ub_set = True
        return super().roi_ub(float(roi_ub))

    def initial_snapshot(self, data: str | np.ndarray[Any, event_dtype]):
        if isinstance(data, str):
            super().initial_snapshot(data)
        elif isinstance(data, np.ndarray):
            self._initial_snapshot_ndarray(data.ctypes.data, len(data))
        else:
            raise ValueError("initial_snapshot must be a str path or numpy array")
        return self


class BacktestAsset(_AssetBase):
    """
    Legacy asset builder (kept for backwards compatibility).
    Prefer `L2Asset` / `L3Asset`.
    """


class L2Asset(_AssetBase):
    def l3_fifo_queue_model(self):
        raise ValueError("L3 queue model is not valid for L2Asset; use L3Asset()")

    def cme_databento_mbo(self, enabled: bool):
        raise ValueError("cme_databento_mbo is only valid for L3Asset")


class L3Asset(_AssetBase):
    _cme_databento_mbo_enabled: bool

    def __init__(self):
        super().__init__()
        self._cme_databento_mbo_enabled = False
        super().l3_fifo_queue_model()

    @property
    def requires_seq_tie_break(self) -> bool:
        return self._cme_databento_mbo_enabled

    def cme_databento_mbo(self, enabled: bool):
        self._cme_databento_mbo_enabled = bool(enabled)
        return super().cme_databento_mbo(bool(enabled))

    def partial_fill_exchange(self):
        raise ValueError("L3 PartialFillExchange requires cme_databento_mbo(True)")

    def risk_adverse_queue_model(self):
        raise ValueError("L2 queue model is not valid for L3Asset")

    def log_prob_queue_model(self):
        raise ValueError("L2 queue model is not valid for L3Asset")

    def log_prob_queue_model2(self):
        raise ValueError("L2 queue model is not valid for L3Asset")

    def power_prob_queue_model(self, n: float):
        raise ValueError("L2 queue model is not valid for L3Asset")

    def power_prob_queue_model2(self, n: float):
        raise ValueError("L2 queue model is not valid for L3Asset")

    def power_prob_queue_model3(self, n: float):
        raise ValueError("L2 queue model is not valid for L3Asset")


class BacktestBuilder:
    def __init__(self, mode: _BacktestBuildMode):
        self._mode = mode
        self._assets: List[BacktestAsset_] = []
        self._policy_kind: int = EXCH_EQUAL_TS_BEFORE_DATA
        self._policy_seed: int = 0

    @classmethod
    def hashmap(cls) -> "BacktestBuilder":
        return cls(_BacktestBuildMode(kind="hashmap"))

    @classmethod
    def roivec(cls, roi_lb: float | None = None, roi_ub: float | None = None) -> "BacktestBuilder":
        return cls(_BacktestBuildMode(kind="roivec", roi_lb=roi_lb, roi_ub=roi_ub))

    def add_asset(self, asset: BacktestAsset_):
        self._assets.append(asset)
        return self

    def exch_order_equal_ts_policy(self, policy: int | tuple[int, int]):
        if isinstance(policy, tuple):
            kind, seed = policy
            self._policy_kind = int(kind)
            self._policy_seed = int(seed)
        else:
            self._policy_kind = int(policy)
            self._policy_seed = 0
        return self

    def build(self) -> HashMapMarketDepthBacktest_TypeHint | ROIVectorMarketDepthBacktest_TypeHint:
        if self._mode.kind == "hashmap":
            ptr = build_hashmap_backtest(
                self._assets,
                self._policy_kind,
                self._policy_seed,
            )
            return HashMapMarketDepthBacktest_(ptr)

        if self._mode.kind == "roivec":
            if self._mode.roi_lb is not None and self._mode.roi_ub is not None:
                for asset in self._assets:
                    if hasattr(asset, "_roi_lb_set") and hasattr(asset, "_roi_ub_set"):
                        if not getattr(asset, "_roi_lb_set") and not getattr(asset, "_roi_ub_set"):
                            asset.roi_lb(float(self._mode.roi_lb))
                            asset.roi_ub(float(self._mode.roi_ub))
            ptr = build_roivec_backtest(
                self._assets,
                self._policy_kind,
                self._policy_seed,
            )
            return ROIVectorMarketDepthBacktest_(ptr)

        raise RuntimeError("unknown build mode")

