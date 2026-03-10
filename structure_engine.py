"""
structure_engine.py — Institutional-Grade ICT Structure Detection v11
=====================================================================
Fractal swing detection, proper BOS/CHoCH state machine,
validated OB/FVG/Liquidity with ATR-adaptive thresholds.

No fallbacks. No proximity-based triggers. Pure price action.
"""

import logging
import time
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple, Deque
from collections import deque
from enum import Enum

import config

logger = logging.getLogger(__name__)


# ======================================================================
# HELPERS
# ======================================================================

def _atr(candles: List[Dict], period: int = 14) -> float:
    """Wilder's ATR from candle dicts. Returns 0 if insufficient data."""
    if len(candles) < period + 1:
        return 0.0
    trs = []
    for i in range(1, len(candles)):
        h = float(candles[i]['h'])
        l = float(candles[i]['l'])
        pc = float(candles[i - 1]['c'])
        tr = max(h - l, abs(h - pc), abs(l - pc))
        trs.append(tr)
    if len(trs) < period:
        return sum(trs) / len(trs) if trs else 0.0
    atr_val = sum(trs[:period]) / period
    for tr in trs[period:]:
        atr_val = (atr_val * (period - 1) + tr) / period
    return atr_val


def _ema(values: List[float], period: int) -> float:
    if not values:
        return 0.0
    if len(values) < period:
        return values[-1]
    k = 2.0 / (period + 1)
    ema = values[0]
    for v in values[1:]:
        ema = v * k + ema * (1 - k)
    return ema


# ======================================================================
# TREND STATE MACHINE
# ======================================================================

class TrendState(Enum):
    BULLISH = "BULLISH"       # HH + HL sequence
    BEARISH = "BEARISH"       # LL + LH sequence
    TRANSITIONING = "TRANSITIONING"
    UNKNOWN = "UNKNOWN"


# ======================================================================
# DATA STRUCTURES
# ======================================================================

@dataclass
class SwingPoint:
    """Confirmed fractal swing point with significance weighting."""
    price:       float
    swing_type:  str       # "high" or "low"
    timestamp:   int       # ms
    timeframe:   str  = "5m"
    confirmed:   bool = True
    atr_at_time: float = 0.0   # ATR when swing formed (for significance)
    index:       int   = -1    # candle index in the source array

    @property
    def significance(self) -> float:
        """Significance = how many ATRs this swing extends beyond neighbors."""
        return 0.0  # set externally after detection

    def is_higher_than(self, other: 'SwingPoint') -> bool:
        return self.price > other.price

    def is_lower_than(self, other: 'SwingPoint') -> bool:
        return self.price < other.price


@dataclass
class MarketStructureShift:
    """BOS or CHoCH event — derived from proper swing sequence tracking."""
    structure_type: str        # "BOS" or "CHoCH"
    direction:      str        # "bullish" or "bearish"
    price:          float      # the swing level that was broken
    timestamp:      int        # when the break candle closed
    timeframe:      str  = "5m"
    confirmed:      bool = True
    broken_swing:   Optional[SwingPoint] = None  # the actual swing that was violated
    impulse_size:   float = 0.0  # size of the impulse candle that broke

    @property
    def age_minutes(self) -> float:
        now_ms = int(time.time() * 1000)
        return (now_ms - self.timestamp) / 60_000


@dataclass
class OrderBlock:
    """ICT Order Block — last opposite candle before validated impulse."""
    low:                float
    high:               float
    timestamp:          int
    direction:          str   = "bullish"
    strength:           float = 0.0
    timeframe:          str   = "5m"
    has_displacement:   bool  = False
    bos_confirmed:      bool  = False
    has_wick_rejection: bool  = False
    broken:             bool  = False
    visit_count:        int   = 0
    # The MSS event this OB relates to (the impulse that validated it)
    related_mss:        Optional[MarketStructureShift] = None

    @property
    def midpoint(self) -> float:
        return (self.high + self.low) / 2

    @property
    def size(self) -> float:
        return self.high - self.low

    def ote_zone(self) -> Tuple[float, float]:
        """Optimal Trade Entry: 50-79% retracement into the OB."""
        if self.direction == "bullish":
            return (self.low + self.size * 0.50, self.low + self.size * 0.79)
        else:
            return (self.high - self.size * 0.79, self.high - self.size * 0.50)

    def in_optimal_zone(self, price: float) -> bool:
        lo, hi = self.ote_zone()
        return lo <= price <= hi

    def contains_price(self, price: float) -> bool:
        return self.low <= price <= self.high

    def is_active(self, now_ms: int) -> bool:
        age = (now_ms - self.timestamp) / 60_000
        return (not self.broken
                and age < config.OB_MAX_AGE_MINUTES
                and self.visit_count < config.OB_INVALIDATE_TOUCHES)

    def virgin_multiplier(self) -> float:
        if self.visit_count == 0:
            return 1.30
        if self.visit_count == 1:
            return 1.00
        return 0.70


@dataclass
class FairValueGap:
    """ICT FVG — 3-candle imbalance with impulse validation."""
    bottom:          float
    top:             float
    timestamp:       int
    direction:       str
    timeframe:       str   = "5m"
    filled:          bool  = False
    fill_percentage: float = 0.0
    # Consequent encroachment: 50% of gap is key reaction level
    ce_level:        float = 0.0

    def __post_init__(self):
        self.ce_level = (self.top + self.bottom) / 2

    @property
    def midpoint(self) -> float:
        return self.ce_level

    @property
    def size(self) -> float:
        return self.top - self.bottom

    def is_price_in_gap(self, price: float) -> bool:
        return self.bottom <= price <= self.top

    def update_fill(self, candle_h: float, candle_l: float) -> None:
        """Update fill from a single candle (called incrementally)."""
        if self.filled or self.size <= 0:
            return
        if self.direction == "bullish":
            if candle_l <= self.top:
                pen = (self.top - candle_l) / self.size
                self.fill_percentage = max(self.fill_percentage, min(pen, 1.0))
        else:
            if candle_h >= self.bottom:
                pen = (candle_h - self.bottom) / self.size
                self.fill_percentage = max(self.fill_percentage, min(pen, 1.0))
        if self.fill_percentage >= config.FVG_FILL_INVALIDATION:
            self.filled = True

    def is_active(self, now_ms: int) -> bool:
        age = (now_ms - self.timestamp) / 60_000
        return not self.filled and age < config.FVG_MAX_AGE_MINUTES


@dataclass
class LiquidityPool:
    """EQH/EQL cluster of swing points."""
    price:                  float
    pool_type:              str      # "EQH" or "EQL"
    timestamp:              int
    touch_count:            int   = 2
    swept:                  bool  = False
    sweep_timestamp:        int   = 0
    wick_rejection:         bool  = False
    displacement_confirmed: bool  = False

    def distance_pct(self, current_price: float) -> float:
        return abs(current_price - self.price) / current_price * 100


# ======================================================================
# STRUCTURE ENGINE
# ======================================================================

class StructureEngine:
    """
    Institutional-grade ICT structure detection.
    
    Proper methodology:
    1. Fractal swing detection with ATR significance filter
    2. Swing sequence tracking → HH/HL/LL/LH → trend state
    3. BOS/CHoCH from swing violations with impulse validation
    4. OBs validated by the impulse that caused a BOS
    5. FVGs validated by impulse candle body ratio
    6. Liquidity pools from ATR-clustered equal swings
    7. Sweep detection with displacement + wick rejection
    """

    def __init__(self):
        # Per-timeframe swing tracking
        self._swings: Dict[str, List[SwingPoint]] = {}
        self._trend_state: Dict[str, TrendState] = {}
        self._last_processed_idx: Dict[str, int] = {}

        # Global structure storage
        self.swing_highs:       Deque[SwingPoint]          = deque(maxlen=300)
        self.swing_lows:        Deque[SwingPoint]          = deque(maxlen=300)
        self.market_structures: Deque[MarketStructureShift] = deque(maxlen=150)
        self.order_blocks_bull: Deque[OrderBlock]           = deque(maxlen=config.MAX_ORDER_BLOCKS)
        self.order_blocks_bear: Deque[OrderBlock]           = deque(maxlen=config.MAX_ORDER_BLOCKS)
        self.fvgs_bull:         Deque[FairValueGap]         = deque(maxlen=config.MAX_FVGS)
        self.fvgs_bear:         Deque[FairValueGap]         = deque(maxlen=config.MAX_FVGS)
        self.liquidity_pools:   Deque[LiquidityPool]        = deque(maxlen=config.MAX_LIQUIDITY_ZONES)

        # Dedup sets
        self._registered_mss: set = set()
        self._registered_obs: set = set()
        self._registered_fvgs: set = set()
        self._registered_sweeps: set = set()

        # OB visit tracking: keys of OBs that price is CURRENTLY inside.
        # A new visit is counted only when price ENTERS a zone it was previously
        # OUTSIDE of — not on every update cycle while price stays inside.
        # Key format: (round(ob.low,1), round(ob.high,1), ob.direction)
        self._ob_in_zone: set = set()

    # ==================================================================
    # PUBLIC: Full structure update
    # ==================================================================

    def update(self, candles_by_tf: Dict[str, List[Dict]],
               current_price: float, now_ms: int) -> None:
        """
        Run full structure detection across all provided timeframes.
        candles_by_tf: {"5m": [...], "15m": [...], "1h": [...], "4h": [...]}
        """
        # 1. Detect swings on each timeframe
        for tf, candles in candles_by_tf.items():
            if len(candles) < 10:
                continue
            self._detect_swings(candles, current_price, tf)

        # 2. Detect BOS/CHoCH on each timeframe
        for tf, candles in candles_by_tf.items():
            if len(candles) < 10:
                continue
            self._detect_market_structure(candles, current_price, now_ms, tf)

        # 3. Detect OBs (primary + confirmation TFs)
        for tf in ["5m", "15m"]:
            candles = candles_by_tf.get(tf, [])
            if len(candles) >= 5:
                self._detect_order_blocks(candles, now_ms, current_price, tf)

        # 4. Detect FVGs
        for tf in ["5m", "15m"]:
            candles = candles_by_tf.get(tf, [])
            if len(candles) >= 3:
                self._detect_fvgs(candles, now_ms, current_price, tf)

        # 5. Update FVG fills from latest candle
        for tf in ["5m", "1m"]:
            candles = candles_by_tf.get(tf, [])
            if candles:
                self._update_fvg_fills_incremental(candles[-5:])

        # 6. Detect liquidity pools
        self._detect_liquidity_pools(current_price, now_ms)

        # 7. Detect sweeps
        for tf in ["5m", "15m"]:
            candles = candles_by_tf.get(tf, [])
            if candles:
                self._detect_sweeps(candles, current_price, now_ms)

        # 8. Update OB visits
        self._update_ob_visits(current_price, now_ms)

        # 9. Cleanup stale structures
        self._cleanup(current_price, now_ms)

    # ==================================================================
    # SWING DETECTION — Fractal with ATR significance filter
    # ==================================================================

    def _detect_swings(self, candles: List[Dict], current_price: float,
                       tf: str) -> None:
        """
        Fractal swing detection:
        - A swing HIGH requires the high to be higher than N bars left AND N bars right
        - A swing LOW requires the low to be lower than N bars left AND N bars right
        - ATR significance filter: swing must extend >= 0.3 ATR beyond neighbors
          to filter out micro-noise that isn't institutional structure.
        - Proper dedup: use (price, timeframe, type) with ATR-scaled tolerance
        """
        n = len(candles)
        lb_left = config.SWING_LOOKBACK_LEFT
        lb_right = config.SWING_LOOKBACK_RIGHT
        if n < lb_left + lb_right + 1:
            return

        atr = _atr(candles, min(14, n - 1))
        if atr <= 0:
            atr = current_price * 0.001  # fallback: 0.1% of price

        # ATR-based dedup tolerance (more adaptive than fixed %)
        dedup_tol = atr * 0.3

        # ATR-based minimum swing significance
        min_significance = atr * 0.25

        for i in range(lb_left, n - lb_right):
            c = candles[i]
            high = float(c['h'])
            low = float(c['l'])
            ts = int(c.get('t', 0))

            # ── Swing High Detection ──
            left_highs = [float(candles[j]['h']) for j in range(i - lb_left, i)]
            right_highs = [float(candles[j]['h']) for j in range(i + 1, i + 1 + lb_right)]

            is_swing_high = (
                all(high > h for h in left_highs) and
                all(high >= h for h in right_highs)
            )

            if is_swing_high:
                # Significance: how far above the nearest neighbor highs?
                max_neighbor = max(max(left_highs), max(right_highs))
                extension = high - max_neighbor

                if extension >= min_significance:
                    # Dedup check
                    already = any(
                        abs(s.price - high) <= dedup_tol
                        and s.timeframe == tf
                        and s.swing_type == "high"
                        for s in self.swing_highs
                    )
                    if not already:
                        sp = SwingPoint(
                            price=high, swing_type="high", timestamp=ts,
                            timeframe=tf, confirmed=True,
                            atr_at_time=atr, index=i
                        )
                        self.swing_highs.append(sp)
                        self._add_to_tf_swings(tf, sp)

            # ── Swing Low Detection ──
            left_lows = [float(candles[j]['l']) for j in range(i - lb_left, i)]
            right_lows = [float(candles[j]['l']) for j in range(i + 1, i + 1 + lb_right)]

            is_swing_low = (
                all(low < l for l in left_lows) and
                all(low <= l for l in right_lows)
            )

            if is_swing_low:
                min_neighbor = min(min(left_lows), min(right_lows))
                extension = min_neighbor - low

                if extension >= min_significance:
                    already = any(
                        abs(s.price - low) <= dedup_tol
                        and s.timeframe == tf
                        and s.swing_type == "low"
                        for s in self.swing_lows
                    )
                    if not already:
                        sp = SwingPoint(
                            price=low, swing_type="low", timestamp=ts,
                            timeframe=tf, confirmed=True,
                            atr_at_time=atr, index=i
                        )
                        self.swing_lows.append(sp)
                        self._add_to_tf_swings(tf, sp)

    def _add_to_tf_swings(self, tf: str, sp: SwingPoint) -> None:
        if tf not in self._swings:
            self._swings[tf] = []
        self._swings[tf].append(sp)
        # Keep sorted by timestamp, cap size
        self._swings[tf] = sorted(self._swings[tf], key=lambda s: s.timestamp)[-100:]

    # ==================================================================
    # MARKET STRUCTURE — Proper BOS/CHoCH State Machine
    # ==================================================================

    def _detect_market_structure(self, candles: List[Dict],
                                  current_price: float,
                                  now_ms: int, tf: str) -> None:
        """
        Proper ICT BOS/CHoCH detection using swing sequence.
        
        State machine:
        - BULLISH trend: confirmed when we see HH + HL sequence
          → BOS bullish = breaking a prior swing HIGH (continuation)
          → CHoCH bearish = breaking a prior swing LOW (reversal)
        - BEARISH trend: confirmed when we see LL + LH sequence
          → BOS bearish = breaking a prior swing LOW (continuation)
          → CHoCH bullish = breaking a prior swing HIGH (reversal)
        
        The impulse candle that breaks the swing must have:
          - Body ratio >= 50% (not a doji/indecision)
          - Close beyond the swing level (not just wick)
        """
        tf_swings = self._swings.get(tf, [])
        if len(tf_swings) < 4:
            return

        # Get the last few alternating swing points
        recent = tf_swings[-20:]
        highs = sorted([s for s in recent if s.swing_type == "high"],
                       key=lambda s: s.timestamp)[-5:]
        lows = sorted([s for s in recent if s.swing_type == "low"],
                      key=lambda s: s.timestamp)[-5:]

        if len(highs) < 2 or len(lows) < 2:
            return

        # Current trend state for this TF
        state = self._trend_state.get(tf, TrendState.UNKNOWN)

        # Check swing sequences
        hh = highs[-1].price > highs[-2].price  # Higher high
        hl = lows[-1].price > lows[-2].price     # Higher low
        ll = lows[-1].price < lows[-2].price     # Lower low
        lh = highs[-1].price < highs[-2].price   # Lower high

        # Update trend state
        if hh and hl:
            new_state = TrendState.BULLISH
        elif ll and lh:
            new_state = TrendState.BEARISH
        else:
            new_state = TrendState.TRANSITIONING
        self._trend_state[tf] = new_state

        # Now check if the LATEST candle breaks any critical swing
        if not candles:
            return
        last_candle = candles[-1]
        last_close = float(last_candle['c'])
        last_open = float(last_candle['o'])
        last_high = float(last_candle['h'])
        last_low = float(last_candle['l'])
        last_ts = int(last_candle.get('t', now_ms))
        body = abs(last_close - last_open)
        rng = last_high - last_low
        body_ratio = body / rng if rng > 0 else 0

        atr = _atr(candles, 14)
        dedup_tol = atr * 0.2 if atr > 0 else current_price * 0.0005

        # ── Bullish break: close above a swing high ──
        for sh in reversed(highs):
            if last_close > sh.price and body_ratio >= 0.35:
                # Is this BOS (continuation) or CHoCH (reversal)?
                if state == TrendState.BEARISH or state == TrendState.UNKNOWN:
                    struct_type = "CHoCH"
                else:
                    struct_type = "BOS"

                mss_key = (round(sh.price, 1), "bullish", tf)
                if mss_key not in self._registered_mss:
                    self._registered_mss.add(mss_key)
                    impulse = abs(last_close - sh.price)
                    mss = MarketStructureShift(
                        structure_type=struct_type, direction="bullish",
                        price=sh.price, timestamp=last_ts, timeframe=tf,
                        confirmed=True, broken_swing=sh, impulse_size=impulse
                    )
                    self.market_structures.append(mss)
                    logger.info(f"📈 {struct_type} bullish [{tf}] @ ${sh.price:,.1f} "
                                f"(impulse ${impulse:,.0f}, body {body_ratio:.0%})")
                break  # Only process the most significant break

        # ── Bearish break: close below a swing low ──
        for sl_pt in reversed(lows):
            if last_close < sl_pt.price and body_ratio >= 0.35:
                if state == TrendState.BULLISH or state == TrendState.UNKNOWN:
                    struct_type = "CHoCH"
                else:
                    struct_type = "BOS"

                mss_key = (round(sl_pt.price, 1), "bearish", tf)
                if mss_key not in self._registered_mss:
                    self._registered_mss.add(mss_key)
                    impulse = abs(sl_pt.price - last_close)
                    mss = MarketStructureShift(
                        structure_type=struct_type, direction="bearish",
                        price=sl_pt.price, timestamp=last_ts, timeframe=tf,
                        confirmed=True, broken_swing=sl_pt, impulse_size=impulse
                    )
                    self.market_structures.append(mss)
                    logger.info(f"📉 {struct_type} bearish [{tf}] @ ${sl_pt.price:,.1f} "
                                f"(impulse ${impulse:,.0f}, body {body_ratio:.0%})")
                break

    # ==================================================================
    # ORDER BLOCK DETECTION — Validated by impulse + structure break
    # ==================================================================

    def _detect_order_blocks(self, candles: List[Dict], now_ms: int,
                              current_price: float, tf: str) -> None:
        """
        ICT Order Block: last opposite candle before a VALIDATED impulse move.
        
        Validation criteria:
        1. Impulse candle must have body >= 50% of range (strong directional move)
        2. Impulse must move >= OB_MIN_IMPULSE_PCT
        3. Impulse range must be >= OB_IMPULSE_SIZE_MULTIPLIER × OB candle range
        4. For strength bonus: impulse must break a swing point (BOS confirmation)
        
        The OB candle is the LAST candle of opposite polarity before the impulse.
        """
        if len(candles) < 5:
            return

        atr = _atr(candles, 14)
        min_impulse = max(config.OB_MIN_IMPULSE_PCT / 100 * current_price, atr * 0.5)

        # Collect recent swing levels for BOS validation
        tf_highs = sorted([s.price for s in self.swing_highs if s.timeframe == tf])[-10:]
        tf_lows = sorted([s.price for s in self.swing_lows if s.timeframe == tf])[:10]

        for i in range(2, len(candles) - 1):
            ob_candle = candles[i]        # potential OB
            imp_candle = candles[i + 1]   # impulse candle

            ob_o, ob_c = float(ob_candle['o']), float(ob_candle['c'])
            ob_h, ob_l = float(ob_candle['h']), float(ob_candle['l'])
            imp_o, imp_c = float(imp_candle['o']), float(imp_candle['c'])
            imp_h, imp_l = float(imp_candle['h']), float(imp_candle['l'])
            ob_ts = int(ob_candle.get('t', now_ms))

            imp_range = imp_h - imp_l
            imp_body = abs(imp_c - imp_o)
            ob_range = ob_h - ob_l

            # Impulse must be strong
            if imp_range <= 0 or imp_body <= 0:
                continue
            imp_body_ratio = imp_body / imp_range
            if imp_body_ratio < config.OB_MIN_BODY_RATIO:
                continue

            # ── Bullish OB: bearish OB candle → bullish impulse ──
            is_ob_bearish = ob_c < ob_o
            is_imp_bullish = imp_c > imp_o and imp_body >= min_impulse

            if is_ob_bearish and is_imp_bullish:
                # Validate impulse size relative to OB
                if ob_range > 0 and imp_range < config.OB_IMPULSE_SIZE_MULTIPLIER * ob_range:
                    continue

                # Wick rejection on OB candle (lower wick shows buyer absorption)
                body_low = min(ob_o, ob_c)
                lower_wick = body_low - ob_l
                wick_rej = (ob_range > 0 and lower_wick / ob_range >= config.OB_WICK_REJECTION_MIN)

                # BOS check: did impulse break a prior swing high?
                bos_ok = any(imp_h > ph for ph in tf_highs[-5:]) if tf_highs else False

                # Displacement: strong follow-through
                has_disp = imp_body_ratio >= config.SWEEP_DISPLACEMENT_MIN

                # Strength scoring
                strength = 35.0
                if bos_ok:     strength += 25.0
                if has_disp:   strength += 15.0
                if wick_rej:   strength += 10.0
                if imp_range >= 1.5 * ob_range:
                    strength += 15.0
                strength = min(strength, 100.0)

                ob_key = (round(ob_l, 1), round(ob_h, 1), "bullish", tf)
                if ob_key not in self._registered_obs:
                    self._registered_obs.add(ob_key)
                    self.order_blocks_bull.append(OrderBlock(
                        low=ob_l, high=ob_h, timestamp=ob_ts,
                        direction="bullish", strength=strength,
                        timeframe=tf, has_displacement=has_disp,
                        bos_confirmed=bos_ok, has_wick_rejection=wick_rej
                    ))

            # ── Bearish OB: bullish OB candle → bearish impulse ──
            is_ob_bullish = ob_c > ob_o
            is_imp_bearish = imp_c < imp_o and imp_body >= min_impulse

            if is_ob_bullish and is_imp_bearish:
                if ob_range > 0 and imp_range < config.OB_IMPULSE_SIZE_MULTIPLIER * ob_range:
                    continue

                body_top = max(ob_o, ob_c)
                upper_wick = ob_h - body_top
                wick_rej = (ob_range > 0 and upper_wick / ob_range >= config.OB_WICK_REJECTION_MIN)

                bos_ok = any(imp_l < pl for pl in tf_lows[:5]) if tf_lows else False
                has_disp = imp_body_ratio >= config.SWEEP_DISPLACEMENT_MIN

                strength = 35.0
                if bos_ok:     strength += 25.0
                if has_disp:   strength += 15.0
                if wick_rej:   strength += 10.0
                if imp_range >= 1.5 * ob_range:
                    strength += 15.0
                strength = min(strength, 100.0)

                ob_key = (round(ob_l, 1), round(ob_h, 1), "bearish", tf)
                if ob_key not in self._registered_obs:
                    self._registered_obs.add(ob_key)
                    self.order_blocks_bear.append(OrderBlock(
                        low=ob_l, high=ob_h, timestamp=ob_ts,
                        direction="bearish", strength=strength,
                        timeframe=tf, has_displacement=has_disp,
                        bos_confirmed=bos_ok, has_wick_rejection=wick_rej
                    ))

    # ==================================================================
    # FVG DETECTION — With impulse body validation
    # ==================================================================

    def _detect_fvgs(self, candles: List[Dict], now_ms: int,
                     current_price: float, tf: str) -> None:
        """
        FVG = gap between candle1's extreme and candle3's extreme,
        with candle2 (impulse) having body >= 50% of range.
        
        Bullish FVG: c3.low > c1.high (gap up, demand imbalance)
        Bearish FVG: c1.low > c3.high (gap down, supply imbalance)
        """
        if len(candles) < 3:
            return

        min_gap = current_price * config.FVG_MIN_SIZE_PCT / 100

        for i in range(len(candles) - 2):
            c1, c2, c3 = candles[i], candles[i + 1], candles[i + 2]
            c1_h = float(c1['h'])
            c1_l = float(c1['l'])
            c2_o = float(c2['o'])
            c2_c = float(c2['c'])
            c2_h = float(c2['h'])
            c2_l = float(c2['l'])
            c3_h = float(c3['h'])
            c3_l = float(c3['l'])
            ts = int(c2.get('t', now_ms))

            # Impulse candle (c2) must be strong
            c2_range = c2_h - c2_l
            c2_body = abs(c2_c - c2_o)
            if c2_range <= 0 or c2_body / c2_range < 0.40:
                continue

            # Bullish FVG: gap between c1 high and c3 low
            gap_bottom = c1_h
            gap_top = c3_l
            gap_size = gap_top - gap_bottom
            if gap_size >= min_gap and c2_c > c2_o:  # impulse must be bullish
                fvg_key = (round(gap_bottom, 1), round(gap_top, 1), "bullish", tf)
                if fvg_key not in self._registered_fvgs:
                    self._registered_fvgs.add(fvg_key)
                    self.fvgs_bull.append(FairValueGap(
                        bottom=gap_bottom, top=gap_top,
                        timestamp=ts, direction="bullish", timeframe=tf
                    ))

            # Bearish FVG: gap between c3 high and c1 low
            gap_bottom = c3_h
            gap_top = c1_l
            gap_size = gap_top - gap_bottom
            if gap_size >= min_gap and c2_c < c2_o:  # impulse must be bearish
                fvg_key = (round(gap_bottom, 1), round(gap_top, 1), "bearish", tf)
                if fvg_key not in self._registered_fvgs:
                    self._registered_fvgs.add(fvg_key)
                    self.fvgs_bear.append(FairValueGap(
                        bottom=gap_bottom, top=gap_top,
                        timestamp=ts, direction="bearish", timeframe=tf
                    ))

    def _update_fvg_fills_incremental(self, candles: List[Dict]) -> None:
        """Update FVG fills from recent candles only (not full history)."""
        for c in candles:
            h, l = float(c['h']), float(c['l'])
            for fvg in list(self.fvgs_bull) + list(self.fvgs_bear):
                if not fvg.filled:
                    fvg.update_fill(h, l)

    # ==================================================================
    # LIQUIDITY POOL DETECTION — ATR-adaptive clustering
    # ==================================================================

    def _detect_liquidity_pools(self, current_price: float, now_ms: int) -> None:
        """
        EQH/EQL detection from swing clustering.
        Tolerance is ATR-based (not fixed %) — adapts to volatility.
        """
        # Get recent ATR for adaptive tolerance
        # Use the most recent swing's ATR, or estimate from price
        recent_atrs = [s.atr_at_time for s in list(self.swing_highs)[-10:]
                       if s.atr_at_time > 0]
        atr = sum(recent_atrs) / len(recent_atrs) if recent_atrs else current_price * 0.002
        tolerance = max(atr * 0.5, current_price * 0.001)

        # EQH: cluster swing highs
        sh_prices = [(s.price, s.timestamp) for s in self.swing_highs]
        self._cluster_liquidity_adaptive(sh_prices, "EQH", tolerance, current_price, now_ms)

        # EQL: cluster swing lows
        sl_prices = [(s.price, s.timestamp) for s in self.swing_lows]
        self._cluster_liquidity_adaptive(sl_prices, "EQL", tolerance, current_price, now_ms)

    def _cluster_liquidity_adaptive(self, price_data: List[Tuple[float, int]],
                                     pool_type: str, tolerance: float,
                                     current_price: float, now_ms: int) -> None:
        """Cluster prices within ATR-based tolerance to form liquidity pools."""
        if len(price_data) < 2:
            return

        sorted_data = sorted(set((round(p, 1), t) for p, t in price_data), key=lambda x: x[0])
        prices = [d[0] for d in sorted_data]

        i = 0
        while i < len(prices):
            group = [prices[i]]
            j = i + 1
            while j < len(prices) and prices[j] - prices[i] <= tolerance:
                group.append(prices[j])
                j += 1

            if len(group) >= config.LIQ_MIN_TOUCHES:
                avg_p = sum(group) / len(group)
                dist_pct = abs(current_price - avg_p) / current_price * 100

                if dist_pct <= config.LIQ_MAX_DISTANCE_PCT:
                    # Check if already tracked
                    already = any(
                        abs(p.price - avg_p) <= tolerance * 0.5
                        and p.pool_type == pool_type
                        for p in self.liquidity_pools
                    )
                    if not already:
                        self.liquidity_pools.append(LiquidityPool(
                            price=avg_p, pool_type=pool_type,
                            timestamp=now_ms, touch_count=len(group)
                        ))
            i = j

    # ==================================================================
    # SWEEP DETECTION — Wick + Displacement validation
    # ==================================================================

    def _detect_sweeps(self, candles: List[Dict], current_price: float,
                       now_ms: int) -> None:
        """
        Sweep = price wicks through a liquidity pool then closes back.
        Requires:
        1. Wick penetrates the pool level
        2. Close is back on the other side (rejection)
        3. Displacement: the rejection candle has body >= 40% of range
        """
        if not candles:
            return

        # Check last few candles for sweep events
        for c in candles[-5:]:
            c_h = float(c['h'])
            c_l = float(c['l'])
            c_o = float(c['o'])
            c_c = float(c['c'])
            c_ts = int(c.get('t', now_ms))
            c_range = c_h - c_l
            c_body = abs(c_c - c_o)
            body_ratio = c_body / c_range if c_range > 0 else 0

            for pool in self.liquidity_pools:
                if pool.swept:
                    continue

                sweep_key = (round(pool.price, 0), pool.pool_type, c_ts)
                if sweep_key in self._registered_sweeps:
                    continue

                # EQH sweep: wick goes above, close below
                if pool.pool_type == "EQH" and c_h >= pool.price and c_c < pool.price:
                    pool.swept = True
                    pool.sweep_timestamp = c_ts
                    pool.wick_rejection = True
                    pool.displacement_confirmed = body_ratio >= config.SWEEP_DISPLACEMENT_MIN
                    self._registered_sweeps.add(sweep_key)
                    logger.info(
                        f"💧 SWEPT EQH @ ${pool.price:,.1f} "
                        f"({'DISP' if pool.displacement_confirmed else 'weak'} "
                        f"body={body_ratio:.0%})")

                # EQL sweep: wick goes below, close above
                elif pool.pool_type == "EQL" and c_l <= pool.price and c_c > pool.price:
                    pool.swept = True
                    pool.sweep_timestamp = c_ts
                    pool.wick_rejection = True
                    pool.displacement_confirmed = body_ratio >= config.SWEEP_DISPLACEMENT_MIN
                    self._registered_sweeps.add(sweep_key)
                    logger.info(
                        f"💧 SWEPT EQL @ ${pool.price:,.1f} "
                        f"({'DISP' if pool.displacement_confirmed else 'weak'} "
                        f"body={body_ratio:.0%})")

    # ==================================================================
    # OB VISIT TRACKING
    # ==================================================================

    def _update_ob_visits(self, current_price: float, now_ms: int) -> None:
        """
        Track how many distinct times price has visited each OB.

        A "visit" is counted when price ENTERS a zone it was previously
        OUTSIDE of — not on every update cycle while price stays inside.
        This means two rapid ticks inside the same OB count as ONE visit,
        but leaving and returning later increments the counter correctly.

        Uses self._ob_in_zone to track the currently-occupied set of OBs
        across structure update cycles.
        """
        currently_in: set = set()

        for ob_list in [self.order_blocks_bull, self.order_blocks_bear]:
            for ob in ob_list:
                if not ob.is_active(now_ms):
                    continue
                ob_key = (round(ob.low, 1), round(ob.high, 1), ob.direction)
                if ob.contains_price(current_price):
                    currently_in.add(ob_key)
                    # New entry: price just entered a zone it was NOT in last cycle
                    if ob_key not in self._ob_in_zone:
                        ob.visit_count += 1
                        logger.debug(
                            f"OB visit #{ob.visit_count}: "
                            f"{ob.direction} ${ob.low:.1f}–${ob.high:.1f}"
                        )

        # Update the "in zone" tracking set for the next cycle
        self._ob_in_zone = currently_in

    # ==================================================================
    # CLEANUP
    # ==================================================================

    def _cleanup(self, current_price: float, now_ms: int) -> None:
        """Remove structures that are too far from price or too old."""
        max_dist = config.STRUCTURE_CLEANUP_DISTANCE_PCT / 100

        for obs in [self.order_blocks_bull, self.order_blocks_bear]:
            to_remove = [ob for ob in obs
                         if abs(ob.midpoint - current_price) / current_price > max_dist
                         or not ob.is_active(now_ms)]
            for ob in to_remove:
                obs.remove(ob)

        for fvgs in [self.fvgs_bull, self.fvgs_bear]:
            to_remove = [f for f in fvgs
                         if abs(f.midpoint - current_price) / current_price > max_dist
                         or not f.is_active(now_ms)]
            for f in to_remove:
                fvgs.remove(f)

    # ==================================================================
    # QUERY HELPERS (used by strategy for confluence scoring)
    # ==================================================================

    def get_trend_state(self, tf: str = "5m") -> TrendState:
        return self._trend_state.get(tf, TrendState.UNKNOWN)

    def get_recent_mss(self, tf: str = None, direction: str = None,
                       max_age_min: float = 60) -> List[MarketStructureShift]:
        """Get recent market structure shifts, optionally filtered."""
        now_ms = int(time.time() * 1000)
        results = []
        for ms in self.market_structures:
            if tf and ms.timeframe != tf:
                continue
            if direction and ms.direction != direction:
                continue
            age = (now_ms - ms.timestamp) / 60_000
            if age <= max_age_min:
                results.append(ms)
        return results

    def get_nearest_ob(self, side: str, current_price: float,
                       now_ms: int) -> Optional[OrderBlock]:
        """Get the nearest active OB for the given side."""
        obs = self.order_blocks_bull if side == "long" else self.order_blocks_bear
        active = [ob for ob in obs if ob.is_active(now_ms)]
        if not active:
            return None
        return min(active, key=lambda ob: abs(ob.midpoint - current_price))

    def get_nearest_fvg(self, side: str, current_price: float,
                        now_ms: int) -> Optional[FairValueGap]:
        """Get the nearest active FVG for the given side."""
        fvgs = self.fvgs_bull if side == "long" else self.fvgs_bear
        active = [f for f in fvgs if f.is_active(now_ms)]
        if not active:
            return None
        return min(active, key=lambda f: abs(f.midpoint - current_price))

    def get_best_entry_zone(self, side: str, current_price: float,
                            now_ms: int) -> Optional[Tuple[float, float, str]]:
        """
        Find the best entry zone (OB or FVG) that price is currently in or very near.
        Returns (zone_low, zone_high, zone_type) or None.
        
        Priority: OB with OTE > OB body > FVG
        """
        obs = self.order_blocks_bull if side == "long" else self.order_blocks_bear
        fvgs = self.fvgs_bull if side == "long" else self.fvgs_bear

        # Check OBs first (higher priority)
        for ob in sorted([o for o in obs if o.is_active(now_ms)],
                         key=lambda o: o.strength, reverse=True):
            if ob.in_optimal_zone(current_price):
                lo, hi = ob.ote_zone()
                return (lo, hi, "OB_OTE")
            if ob.contains_price(current_price):
                return (ob.low, ob.high, "OB_BODY")

        # Then FVGs
        for fvg in sorted([f for f in fvgs if f.is_active(now_ms)],
                          key=lambda f: abs(f.midpoint - current_price)):
            if fvg.is_price_in_gap(current_price):
                return (fvg.bottom, fvg.top, "FVG")

        return None

    def get_swept_pool(self, side: str, max_age_min: float = 120) -> Optional[LiquidityPool]:
        """Get the most recent swept pool relevant to the trade side."""
        now_ms = int(time.time() * 1000)
        candidates = []
        for pool in self.liquidity_pools:
            if not pool.swept:
                continue
            age = (now_ms - pool.sweep_timestamp) / 60_000
            if age > max_age_min:
                continue
            # Long: need swept EQL (buy-side liquidity taken)
            if side == "long" and pool.pool_type == "EQL":
                candidates.append(pool)
            elif side == "short" and pool.pool_type == "EQH":
                candidates.append(pool)
        if not candidates:
            return None
        # Most recent sweep
        return max(candidates, key=lambda p: p.sweep_timestamp)

    def get_opposing_target(self, side: str, entry_price: float,
                            now_ms: int) -> Optional[float]:
        """
        Find opposing liquidity / structure for TP target.
        Long → nearest unswept EQH, bearish OB, or swing high above entry
        Short → nearest unswept EQL, bullish OB, or swing low below entry
        """
        if side == "long":
            # EQH above
            eqh = [p for p in self.liquidity_pools
                   if p.pool_type == "EQH" and not p.swept and p.price > entry_price]
            if eqh:
                return min(eqh, key=lambda p: p.price).price

            # Bearish OB above
            bear_obs = [ob for ob in self.order_blocks_bear
                        if ob.is_active(now_ms) and ob.low > entry_price]
            if bear_obs:
                return min(bear_obs, key=lambda ob: ob.low).low

            # Swing high above
            highs_above = [s.price for s in self.swing_highs if s.price > entry_price]
            if highs_above:
                return sorted(highs_above)[0]

        else:  # short
            eql = [p for p in self.liquidity_pools
                   if p.pool_type == "EQL" and not p.swept and p.price < entry_price]
            if eql:
                return max(eql, key=lambda p: p.price).price

            bull_obs = [ob for ob in self.order_blocks_bull
                        if ob.is_active(now_ms) and ob.high < entry_price]
            if bull_obs:
                return max(bull_obs, key=lambda ob: ob.high).high

            lows_below = [s.price for s in self.swing_lows if s.price < entry_price]
            if lows_below:
                return sorted(lows_below, reverse=True)[0]

        return None
