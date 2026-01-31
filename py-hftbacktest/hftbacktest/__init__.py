from typing import List

from ._hftbacktest import LiveInstrument, build_hashmap_backtest, build_roivec_backtest
from .binding import (
    HashMapMarketDepthBacktest_,
    HashMapMarketDepthBacktest as HashMapMarketDepthBacktest_TypeHint,
    ROIVectorMarketDepthBacktest_,
    ROIVectorMarketDepthBacktest as ROIVectorMarketDepthBacktest_TypeHint,
)
from .builder import (
    BacktestAsset,
    BacktestBuilder,
    EXCH_EQUAL_TS_AFTER_DATA,
    EXCH_EQUAL_TS_BEFORE_DATA,
    EXCH_EQUAL_TS_RANDOM_SEEDED,
    L2Asset,
    L3Asset,
)
from .order import (
    BUY,
    SELL,
    NONE,
    NEW,
    EXPIRED,
    FILLED,
    CANCELED,
    GTC,
    GTX,
    FOK,
    IOC,
    LIMIT,
    MARKET,
    STOP_MARKET,
    STOP_LIMIT,
    MIT,
)
from .recorder import Recorder
from .types import (
    ALL_ASSETS,
    EVENT_ARRAY,
    DEPTH_EVENT,
    TRADE_EVENT,
    DEPTH_CLEAR_EVENT,
    DEPTH_SNAPSHOT_EVENT,
    DEPTH_BBO_EVENT,
    ADD_ORDER_EVENT,
    CANCEL_ORDER_EVENT,
    MODIFY_ORDER_EVENT,
    FILL_EVENT,
    EXCH_EVENT,
    LOCAL_EVENT,
    BUY_EVENT,
    SELL_EVENT
)
try:
    from ._hftbacktest import (
        build_hashmap_livebot,
        build_roivec_livebot
    )
    from .binding import (
        HashMapMarketDepthLiveBot_,
        HashMapMarketDepthLiveBot as HashMapMarketDepthLiveBot_TypeHint,
        ROIVectorMarketDepthLiveBot_,
        ROIVectorMarketDepthLiveBot as ROIVectorMarketDepthLiveBot_TypeHint,
    )
    LIVE_FEATURE = True
except:
    LIVE_FEATURE = False

__all__ = (
    'BacktestAsset',
    'L2Asset',
    'L3Asset',
    'BacktestBuilder',
    'EXCH_EQUAL_TS_BEFORE_DATA',
    'EXCH_EQUAL_TS_AFTER_DATA',
    'EXCH_EQUAL_TS_RANDOM_SEEDED',
    'HashMapMarketDepthBacktest',
    'ROIVectorMarketDepthBacktest',

    'LiveInstrument',
    'HashMapMarketDepthLiveBot',
    'ROIVectorMarketDepthLiveBot',

    'ALL_ASSETS',

    # Event flags
    'DEPTH_EVENT',
    'TRADE_EVENT',
    'DEPTH_CLEAR_EVENT',
    'DEPTH_SNAPSHOT_EVENT',
    'DEPTH_BBO_EVENT',
    'ADD_ORDER_EVENT',
    'CANCEL_ORDER_EVENT',
    'MODIFY_ORDER_EVENT',
    'FILL_EVENT',
    'EXCH_EVENT',
    'LOCAL_EVENT',
    'EXCH_EVENT',
    'LOCAL_EVENT',
    'BUY_EVENT',
    'SELL_EVENT',

    # Side
    'BUY',
    'SELL',

    # Order status
    'NONE',
    'NEW',
    'EXPIRED',
    'FILLED',
    'CANCELED',

    # Time-In-Force
    'GTC',
    'GTX',
    'FOK',
    'IOC',

    'LIMIT',
    'MARKET',

    'STOP_MARKET',
    'STOP_LIMIT',
    'MIT',
    
    'Recorder'
)

__version__ = '2.4.4'


def HashMapMarketDepthBacktest(
        assets: List[BacktestAsset]
) -> HashMapMarketDepthBacktest_TypeHint:
    """
    Constructs an instance of `HashMapMarketDepthBacktest`.

    Args:
        assets: A list of backtesting assets constructed using :class:`BacktestAsset`.

    Returns:
        A jit`ed `HashMapMarketDepthBacktest` that can be used in an ``njit`` function.
    """
    ptr = build_hashmap_backtest(assets)
    return HashMapMarketDepthBacktest_(ptr)


def ROIVectorMarketDepthBacktest(
        assets: List[BacktestAsset]
) -> ROIVectorMarketDepthBacktest_TypeHint:
    """
    Constructs an instance of `ROIVectorMarketBacktest`.

    Args:
        assets: A list of backtesting assets constructed using :class:`BacktestAsset`.

    Returns:
        A jit`ed `ROIVectorMarketBacktest` that can be used in an ``njit`` function.
    """
    ptr = build_roivec_backtest(assets)
    return ROIVectorMarketDepthBacktest_(ptr)


if LIVE_FEATURE:
    def ROIVectorMarketDepthLiveBot(
            assets: List[LiveInstrument]
    ) -> ROIVectorMarketDepthLiveBot_TypeHint:
        """
        Constructs an instance of `ROIVectorMarketDepthLiveBot`.

        Args:
            assets: A list of live instruments constructed using :class:`LiveInstrument`.

        Returns:
            A jit`ed `ROIVectorMarketDepthLiveBot` that can be used in an ``njit`` function.
        """
        ptr = build_roivec_livebot(assets)
        return ROIVectorMarketDepthLiveBot_(ptr)
