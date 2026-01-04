"""
Advanced ICT Trading Strategy v10.0 - PRODUCTION GRADE (INFRASTRUCTURE FIXED)

EXACT INFRASTRUCTURE FROM WORKING Z-SCORE STRATEGY:
✓ GlobalRateLimiter.wait() before ALL API calls
✓ risk_manager.get_available_balance() (NOT get_balance)
✓ risk_manager.calculate_position_size_vol_regime() (NOT calculate_position_size)
✓ risk_manager.check_trading_allowed()
✓ risk_manager.record_trade_opened()
✓ Proper order flow: limit → monitor → TP/SL
✓ Thread-safe with locks
✓ Comprehensive logging with throttles

ICT STRATEGY METHODOLOGY:
✓ 34 EMA HTF Bias (4H) with HH/HL structure confirmation
✓ Liquidity Sweep Detection (wick beyond EQH/EQL + reversal)
✓ IFVG Priority (inverted FVGs = +25 points vs FVG +15)
✓ Order Block Wick Rejection (premium OBs only)
✓ Market Structure (BOS/CHoCH) confluence
✓ PO3 Killzone Timing (London/NY sessions)
✓ AMD Phase Detection (Accumulation/Manipulation/Distribution)
✓ Confluence Scoring System (0-100 points)
✓ Partial Profit Management (2:1, BE at 1:1, trail rest)
"""

import time
import logging
from typing import List, Dict, Optional, Tuple
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime, timezone
import threading
import config

from config import (
    SCORE_HTF_BIAS_ALIGNED, SCORE_FVG_CONFLUENCE, SCORE_IFVG_BONUS,
    SCORE_ORDER_BLOCK_OPTIMAL, SCORE_LIQUIDITY_SWEEP, SCORE_STRUCTURE_ALIGNED,
    SCORE_VOLUME_DELTA_STRONG, SCORE_PO3_KILLZONE,
    VOLUME_DELTA_STRONG_THRESHOLD, VOLUME_DELTA_MODERATE_THRESHOLD,
    ENTRY_THRESHOLD_KILLZONE, ENTRY_THRESHOLD_WEEKEND, ENTRY_THRESHOLD_REGULAR,
    WEEKEND_PARAMS, MAJOR_SESSION_PARAMS, OVERLAP_SESSION_PARAMS,
    OB_MIN_STRENGTH, SWEEP_WICK_REQUIREMENT, HTF_TREND_EMA,
    MAX_ORDER_BLOCKS, MAX_FVGS, MAX_LIQUIDITY_ZONES,
)

from telegram_notifier import (
    send_telegram_message,
    format_entry_signal,
    format_exit_signal,
    format_position_update,
)

logger = logging.getLogger(__name__)

# ============================================================================
# DATA STRUCTURES
# ============================================================================

@dataclass
class OrderBlock:
    """ICT Order Block with wick rejection"""
    low: float
    high: float
    open_price: float
    close_price: float
    timestamp: float
    has_wick_rejection: bool = False
    wick_ratio: float = 0.0
    strength: float = 1.0
    touch_count: int = 0
    broken: bool = False

    @property
    def midpoint(self) -> float:
        return (self.low + self.high) / 2

    @property
    def optimal_entry_low(self) -> float:
        """25% mitigation level"""
        return self.low + (self.high - self.low) * 0.25

    @property
    def optimal_entry_high(self) -> float:
        """100% (full range)"""
        return self.high

    def in_optimal_zone(self, price: float) -> bool:
        """Check if price is in 25-100% optimal entry zone"""
        return self.optimal_entry_low <= price <= self.optimal_entry_high

    def near_zone(self, price: float, tolerance_pct: float = 0.005) -> bool:
        """Check if price within 0.5% of OB"""
        distance = abs(price - self.midpoint)
        return distance / price < tolerance_pct

@dataclass
class FairValueGap:
    """Fair Value Gap (FVG) or Inverted FVG (IFVG)"""
    bottom: float
    top: float
    timestamp: float
    direction: str  # "bullish" or "bearish"
    is_ifvg: bool = False
    filled: bool = False
    fill_percentage: float = 0.0

    @property
    def midpoint(self) -> float:
        return (self.bottom + self.top) / 2

    @property
    def size(self) -> float:
        return self.top - self.bottom

@dataclass
class LiquidityPool:
    """EQH/EQL liquidity pool"""
    price: float
    pool_type: str  # "EQH" or "EQL"
    timestamp: float
    is_equal_level: bool = True
    swept: bool = False
    sweep_timestamp: Optional[float] = None
    wick_rejection: bool = False

@dataclass
class MarketStructure:
    """BOS or CHoCH"""
    structure_type: str  # "BOS" or "CHoCH"
    direction: str  # "bullish" or "bearish"
    price: float
    timestamp: float

@dataclass
class SwingPoint:
    """Swing High or Swing Low"""
    price: float
    swing_type: str  # "high" or "low"
    timestamp: float
    strength: int = 5

# ============================================================================
# ADVANCED ICT STRATEGY CLASS
# ============================================================================

class AdvancedICTStrategy:
    """
    Production-grade ICT strategy with EXACT Z-Score infrastructure
    """
    
    def __init__(self):
        """Initialize strategy state"""
        # ===== ICT STRUCTURES =====
        self.order_blocks_bull = deque(maxlen=15)
        self.order_blocks_bear = deque(maxlen=15)
        self.fvgs_bull = deque(maxlen=25)
        self.fvgs_bear = deque(maxlen=25)
        self.liquidity_pools = deque(maxlen=30)
        self.market_structures = deque(maxlen=15)
        self.swing_points = deque(maxlen=50)

        # ===== HTF BIAS =====
        self.htf_bias = "NEUTRAL"
        self.htf_ema34 = 0.0

        # ===== MARKET STATE =====
        self.current_session = None
        self.in_killzone = False
        self.amd_phase = "UNKNOWN"
        self.session_params = {}

        # ===== POSITION TRACKING =====
        self.current_position: Optional[Dict] = None
        self.position_entry_time = 0.0
        self.last_trade_time = 0.0
        self.consecutive_losses = 0

        # ===== PARTIAL PROFIT TRACKING =====
        self.tp1_hit = False
        self.breakeven_moved = False

        # ===== PERFORMANCE =====
        self.total_entries = 0
        self.total_wins = 0
        self.total_losses = 0

        # ===== UPDATE TRACKING =====
        self.last_structure_update = 0.0
        self.initialized = False

        # ===== LOGGING THROTTLE - FROM Z-SCORE PATTERN =====
        self._last_score_logs = {}
        self.last_score_log = 0.0
        self.last_snapshot_log = 0.0
        self.last_fvg_log = 0.0  # Throttle FVG logs

        # ===== ENTRY COOLDOWN - FROM Z-SCORE PATTERN =====
        self.last_entry_time = 0.0
        self.min_entry_gap = 300  # 5 minutes between entries

        # ===== THREAD SAFETY - FROM Z-SCORE PATTERN =====
        self._execution_lock = threading.RLock()
        self.last_status_check = 0  # ⚠️ NEW: Track last status check time
        self._validate_config()

        logger.info("=" * 80)
        logger.info("ADVANCED ICT STRATEGY v10.0 INITIALIZED (INFRASTRUCTURE FIXED)")
        logger.info("=" * 80)
        logger.info("✓ Z-Score infrastructure patterns applied")
        logger.info("✓ GlobalRateLimiter integration")
        logger.info("✓ Correct risk_manager methods")
        logger.info("✓ FVG logging throttled (60s)")
        logger.info("✓ Entry cooldown (5min)")
        logger.info("✓ Thread-safe execution")
        logger.info("=" * 80)

    def _validate_config(self):
        """Validate config constants"""
        try:
            required = ['SCORE_HTF_BIAS_ALIGNED', 'SCORE_FVG_CONFLUENCE',
                        'VOLUME_DELTA_STRONG_THRESHOLD']
            for const in required:
                if not hasattr(config, const):
                    raise ValueError(f"Missing config: {const}")
            logger.info("✓ Config validated")
        except Exception as e:
            logger.error(f"Config error: {e}")
            raise

    # ========================================================================
    # MAIN TICK HANDLER
    # ========================================================================

    def on_tick(self, data_manager, order_manager, risk_manager) -> None:
        """
        Main strategy tick - called every 250ms
        Managers passed as parameters
        """
        try:
            current_time = time.time()
            current_price = data_manager.get_last_price()
            if current_price <= 0:
                return

            # First tick initialization
            if not self.initialized:
                logger.info("✓ AdvancedICTStrategy initialized on first tick")
                self.initialized = True
                self.last_structure_update = current_time

            # Update structures every 10 seconds
            if current_time - self.last_structure_update >= 10.0:
                self._update_all_structures(data_manager, current_time)
                self.last_structure_update = current_time

            # Update session info
            self._update_session_info(current_price, data_manager)

            # Manage existing position
            if self.current_position:
                self._manage_position(current_price, current_time, order_manager, data_manager, risk_manager)
                return

            # Check if we can enter new trade
            if not self._can_enter_trade(current_time, risk_manager):
                return

            # Evaluate new entry setups
            self._evaluate_and_enter(current_price, current_time, data_manager, order_manager, risk_manager)

        except Exception as e:
            logger.error(f"Error in on_tick: {e}", exc_info=True)

    # ========================================================================
    # STRUCTURE UPDATE - MAIN COORDINATOR
    # ========================================================================

    def _update_all_structures(self, data_manager, current_time: float) -> None:
        """Update all ICT structures"""
        try:
            # Get candles
            candles_5m = data_manager.get_recent_candles("5m", 100)
            candles_15m = data_manager.get_recent_candles("15m", 100)
            candles_4h = data_manager.get_recent_candles("4h", 50)

            if not all([candles_5m, candles_15m, candles_4h]):
                return

            # 1. Update HTF Bias (4H with 34 EMA)
            self._update_htf_bias(candles_4h)

            # 2. Detect swing points (for liquidity pools)
            self._detect_swing_points(candles_5m)

            # 3. Detect liquidity pools (EQH/EQL)
            self._detect_liquidity_pools(candles_5m)

            # 4. Detect Order Blocks (with wick rejection)
            self._detect_order_blocks(candles_5m)

            # 5. Detect FVGs and IFVGs
            self._detect_fvgs(candles_5m)

            # 6. Update FVG fill status
            if candles_5m:
                self._update_fvg_fills(candles_5m[-1])

            # 7. Detect market structure (BOS/CHoCH)
            self._detect_market_structure(candles_5m)

            # 8. Check for liquidity sweeps
            if candles_5m:
                self._check_liquidity_sweeps(candles_5m[-10:])

            # 9. Cleanup old structures
            self._cleanup_old_structures(current_time)

        except Exception as e:
            logger.error(f"Error updating structures: {e}", exc_info=True)

    # ========================================================================
    # HTF BIAS - 34 EMA + HIGHER HIGHS/LOWS
    # ========================================================================

    def _update_htf_bias(self, candles_4h: List) -> None:
        """
        Update HTF bias using 34 EMA + CONFIRMED swing structure
        Bullish: Price > 34 EMA + confirmed higher highs/lows (no overlap)
        Bearish: Price < 34 EMA + confirmed lower highs/lows
        """
        try:
            if len(candles_4h) < 40:
                return

            # Calculate 34 EMA
            self.htf_ema34 = self._calculate_ema(candles_4h, 34)
            current_price = candles_4h[-1].close

            # EMA distance fallback (>0.5% from EMA = assign bias)
            distance_pct = abs((current_price - self.htf_ema34) / self.htf_ema34) if self.htf_ema34 > 0 else 0

            if distance_pct > 0.005:  # >0.5% clearly trending
                if current_price > self.htf_ema34:
                    if self.htf_bias != "BULLISH":
                        logger.info(f"📊 HTF Bias: {self.htf_bias} → BULLISH (EMA distance: {distance_pct*100:.2f}%)")
                        self.htf_bias = "BULLISH"
                    return
                else:
                    if self.htf_bias != "BEARISH":
                        logger.info(f"📊 HTF Bias: {self.htf_bias} → BEARISH (EMA distance: {distance_pct*100:.2f}%)")
                        self.htf_bias = "BEARISH"
                    return

            # Detect CONFIRMED swing structure using proper swings
            recent = candles_4h[-30:]
            swings = self._detect_confirmed_swings(recent, lookback=3)
            swing_highs = [s for s in swings if s.swing_type == "high"]
            swing_lows = [s for s in swings if s.swing_type == "low"]

            # Check for Higher Highs AND Higher Lows (no overlap)
            hh_confirmed = False
            hl_confirmed = False
            if len(swing_highs) >= 2:
                last_highs = sorted(swing_highs[-2:], key=lambda x: x.timestamp)
                if last_highs[-1].price > last_highs[-2].price:
                    hh_confirmed = True
            if len(swing_lows) >= 2:
                last_lows = sorted(swing_lows[-2:], key=lambda x: x.timestamp)
                if last_lows[-1].price > last_lows[-2].price:
                    hl_confirmed = True

            # Check for Lower Highs AND Lower Lows
            lh_confirmed = False
            ll_confirmed = False
            if len(swing_highs) >= 2:
                last_highs = sorted(swing_highs[-2:], key=lambda x: x.timestamp)
                if last_highs[-1].price < last_highs[-2].price:
                    lh_confirmed = True
            if len(swing_lows) >= 2:
                last_lows = sorted(swing_lows[-2:], key=lambda x: x.timestamp)
                if last_lows[-1].price < last_lows[-2].price:
                    ll_confirmed = True

            # Determine bias
            old_bias = self.htf_bias
            if current_price > self.htf_ema34 and hh_confirmed and hl_confirmed:
                self.htf_bias = "BULLISH"
            elif current_price < self.htf_ema34 and lh_confirmed and ll_confirmed:
                self.htf_bias = "BEARISH"
            else:
                # Keep previous bias if structure incomplete (don't flip to NEUTRAL easily)
                if old_bias in ["BULLISH", "BEARISH"]:
                    self.htf_bias = old_bias
                else:
                    self.htf_bias = "NEUTRAL"

            if old_bias != self.htf_bias:
                logger.info(f"📊 HTF Bias: {old_bias} → {self.htf_bias} (Price: ${current_price:.2f}, EMA34: ${self.htf_ema34:.2f})")

        except Exception as e:
            logger.error(f"Error updating HTF bias: {e}")

    def _detect_confirmed_swings(self, candles: List, lookback: int = 3) -> List:
        """Detect confirmed swing points (no overlap)"""
        swings = []
        for i in range(lookback, len(candles) - lookback):
            current = candles[i]
            
            # Swing High - must be highest in window
            is_swing_high = all(
                current.high >= candles[i + j].high
                for j in range(-lookback, lookback + 1)
                if j != 0
            )
            if is_swing_high:
                swings.append(SwingPoint(
                    price=current.high,
                    swing_type="high",
                    timestamp=current.timestamp,
                    strength=lookback
                ))
            
            # Swing Low - must be lowest in window
            is_swing_low = all(
                current.low <= candles[i + j].low
                for j in range(-lookback, lookback + 1)
                if j != 0
            )
            if is_swing_low:
                swings.append(SwingPoint(
                    price=current.low,
                    swing_type="low",
                    timestamp=current.timestamp,
                    strength=lookback
                ))
        return swings

    def _calculate_ema(self, candles: List, period: int) -> float:
        """Calculate Exponential Moving Average"""
        if len(candles) < period:
            return sum(c.close for c in candles) / len(candles)
        
        multiplier = 2 / (period + 1)
        ema = sum(c.close for c in candles[:period]) / period
        for candle in candles[period:]:
            ema = (candle.close - ema) * multiplier + ema
        return ema

    # ========================================================================
    # SWING POINT DETECTION
    # ========================================================================

    def _detect_swing_points(self, candles: List) -> None:
        """Detect swing highs and lows (for liquidity identification)"""
        try:
            lookback = 5
            self.swing_points.clear()

            for i in range(lookback, len(candles) - lookback):
                current = candles[i]
                
                # Swing High
                is_swing_high = all(
                    current.high > candles[i + j].high
                    for j in range(-lookback, lookback + 1)
                    if j != 0
                )
                if is_swing_high:
                    self.swing_points.append(SwingPoint(
                        price=current.high,
                        swing_type="high",
                        timestamp=current.timestamp,
                        strength=lookback
                    ))
                
                # Swing Low
                is_swing_low = all(
                    current.low < candles[i + j].low
                    for j in range(-lookback, lookback + 1)
                    if j != 0
                )
                if is_swing_low:
                    self.swing_points.append(SwingPoint(
                        price=current.low,
                        swing_type="low",
                        timestamp=current.timestamp,
                        strength=lookback
                    ))

        except Exception as e:
            logger.error(f"Error detecting swing points: {e}")

    # ========================================================================
    # LIQUIDITY POOL DETECTION (EQH/EQL)
    # ========================================================================

    def _detect_liquidity_pools(self, candles: List) -> None:
        """
        Detect Equal Highs (EQH) and Equal Lows (EQL)
        Equal levels = 2+ swing points within 0.2% of each other
        These are prime liquidity targets for sweeps
        """
        try:
            if len(self.swing_points) < 2:
                return

            tolerance = 0.002  # 0.2% tolerance

            # Find equal highs
            swing_highs = [s for s in self.swing_points if s.swing_type == "high"]
            for i, sh1 in enumerate(swing_highs):
                equal_count = 1
                for sh2 in swing_highs[i+1:]:
                    if abs(sh1.price - sh2.price) / sh1.price < tolerance:
                        equal_count += 1
                
                if equal_count >= 2:
                    # Check if already tracked
                    if not any(abs(lp.price - sh1.price) < 5.0 for lp in self.liquidity_pools if lp.pool_type == "EQH"):
                        lp = LiquidityPool(
                            price=sh1.price,
                            pool_type="EQH",
                            timestamp=sh1.timestamp,
                            is_equal_level=True
                        )
                        self.liquidity_pools.append(lp)
                        logger.debug(f"💧 EQH Liquidity Pool: ${lp.price:.2f}")

            # Find equal lows
            swing_lows = [s for s in self.swing_points if s.swing_type == "low"]
            for i, sl1 in enumerate(swing_lows):
                equal_count = 1
                for sl2 in swing_lows[i+1:]:
                    if abs(sl1.price - sl2.price) / sl1.price < tolerance:
                        equal_count += 1
                
                if equal_count >= 2:
                    if not any(abs(lp.price - sl1.price) < 5.0 for lp in self.liquidity_pools if lp.pool_type == "EQL"):
                        lp = LiquidityPool(
                            price=sl1.price,
                            pool_type="EQL",
                            timestamp=sl1.timestamp,
                            is_equal_level=True
                        )
                        self.liquidity_pools.append(lp)
                        logger.debug(f"💧 EQL Liquidity Pool: ${lp.price:.2f}")

        except Exception as e:
            logger.error(f"Error detecting liquidity pools: {e}")

    # ========================================================================
    # LIQUIDITY SWEEP DETECTION
    # ========================================================================

    def _check_liquidity_sweeps(self, recent_candles: List) -> None:
        """
        Relaxed liquidity sweep detection
        Valid sweep requires 2 of 3:
        1. Wick beyond level (mandatory)
        2. Body size check (optional)
        3. Close back inside (optional)
        """
        try:
            if len(recent_candles) < 3:
                return

            current_time = time.time()

            for lp in self.liquidity_pools:
                if lp.swept:
                    # Reset if older than 30 minutes
                    if lp.sweep_timestamp and current_time - lp.sweep_timestamp > 1800:
                        lp.swept = False
                        lp.wick_rejection = False
                    continue

                # Check last 3 candles for sweep
                for candle in recent_candles[-3:]:
                    body_size = abs(candle.close - candle.open)

                    # EQH sweep (for shorts)
                    if lp.pool_type == "EQH" and not lp.swept:
                        wick_above = candle.high - max(candle.open, candle.close)
                        
                        # Condition 1: Wick pierces level (MANDATORY)
                        wick_pierces = candle.high > lp.price
                        
                        # Condition 2: Significant wick (OPTIONAL) - 20% threshold
                        significant_wick = (body_size > 0 and wick_above / body_size > 0.20)
                        
                        # Condition 3: Close back below (OPTIONAL)
                        closes_back = candle.close < lp.price

                        # Require condition 1 + at least 1 of (2 or 3)
                        conditions_met = sum([
                            wick_pierces,      # Must have
                            significant_wick,  # Optional
                            closes_back        # Optional
                        ])

                        if conditions_met >= 2:
                            lp.swept = True
                            lp.sweep_timestamp = current_time
                            lp.wick_rejection = True
                            logger.info(f"🔥 EQH SWEPT: ${lp.price:.2f} (wick rejection confirmed)")

                    # EQL sweep (for longs)
                    elif lp.pool_type == "EQL" and not lp.swept:
                        wick_below = min(candle.open, candle.close) - candle.low
                        
                        wick_pierces = candle.low < lp.price
                        significant_wick = (body_size > 0 and wick_below / body_size > 0.20)
                        closes_back = candle.close > lp.price

                        conditions_met = sum([
                            wick_pierces,
                            significant_wick,
                            closes_back
                        ])

                        if conditions_met >= 2:
                            lp.swept = True
                            lp.sweep_timestamp = current_time
                            lp.wick_rejection = True
                            logger.info(f"🔥 EQL SWEPT: ${lp.price:.2f} (wick rejection confirmed)")

        except Exception as e:
            logger.error(f"Error checking liquidity sweeps: {e}")

    # ========================================================================
    # ORDER BLOCK DETECTION (WITH WICK REJECTION)
    # ========================================================================

    def _detect_order_blocks(self, candles: List) -> None:
        """
        Detect Order Blocks with WICK REJECTION filter
        Premium OB must have:
        1. Last opposite candle before impulse
        2. Significant wick (>15% of total range)
        3. Strong body (>40% of range)
        """
        try:
            if len(candles) < 5:
                return

            for i in range(len(candles) - 3, max(len(candles) - 30, 0), -1):
                if i < 2:
                    continue

                ob_candle = candles[i]
                impulse_candle = candles[i + 1]

                # Calculate wick ratios
                ob_range = ob_candle.high - ob_candle.low
                ob_body = abs(ob_candle.close - ob_candle.open)

                if ob_range == 0:
                    continue

                body_ratio = ob_body / ob_range

                # ===== BULLISH OB =====
                if (ob_candle.close < ob_candle.open and  # Bearish candle
                    impulse_candle.close > impulse_candle.open and  # Bullish impulse
                    impulse_candle.close > ob_candle.high):  # Breaks high
                    
                    # Check for lower wick rejection
                    lower_wick = ob_candle.open - ob_candle.low if ob_candle.open < ob_candle.close else ob_candle.close - ob_candle.low
                    wick_ratio = lower_wick / ob_range if ob_range > 0 else 0
                    has_wick_rejection = wick_ratio > 0.15 and body_ratio > 0.40

                    # Only accept OBs with wick rejection
                    if has_wick_rejection:
                        # Check if already exists
                        if not any(abs(ob.low - ob_candle.low) < 5.0 for ob in self.order_blocks_bull):
                            ob = OrderBlock(
                                low=ob_candle.low,
                                high=ob_candle.high,
                                open_price=ob_candle.open,
                                close_price=ob_candle.close,
                                timestamp=ob_candle.timestamp,
                                has_wick_rejection=True,
                                wick_ratio=wick_ratio,
                                strength=min(wick_ratio + body_ratio, 1.0)
                            )
                            self.order_blocks_bull.append(ob)
                            logger.debug(f"🟢 Bullish OB (wick rejection): ${ob.low:.2f}-${ob.high:.2f} (wick: {wick_ratio*100:.0f}%)")

                # ===== BEARISH OB =====
                elif (ob_candle.close > ob_candle.open and  # Bullish candle
                      impulse_candle.close < impulse_candle.open and  # Bearish impulse
                      impulse_candle.close < ob_candle.low):  # Breaks low
                    
                    # Check for upper wick rejection
                    upper_wick = ob_candle.high - ob_candle.open if ob_candle.open > ob_candle.close else ob_candle.high - ob_candle.close
                    wick_ratio = upper_wick / ob_range if ob_range > 0 else 0
                    has_wick_rejection = wick_ratio > 0.15 and body_ratio > 0.40

                    if has_wick_rejection:
                        if not any(abs(ob.high - ob_candle.high) < 5.0 for ob in self.order_blocks_bear):
                            ob = OrderBlock(
                                low=ob_candle.low,
                                high=ob_candle.high,
                                open_price=ob_candle.open,
                                close_price=ob_candle.close,
                                timestamp=ob_candle.timestamp,
                                has_wick_rejection=True,
                                wick_ratio=wick_ratio,
                                strength=min(wick_ratio + body_ratio, 1.0)
                            )
                            self.order_blocks_bear.append(ob)
                            logger.debug(f"🔴 Bearish OB (wick rejection): ${ob.low:.2f}-${ob.high:.2f} (wick: {wick_ratio*100:.0f}%)")

        except Exception as e:
            logger.error(f"Error detecting OBs: {e}", exc_info=True)

    # ========================================================================
    # FVG / IFVG DETECTION
    # ========================================================================

    def _detect_fvgs(self, candles: List) -> None:
        """
        Detect Fair Value Gaps and Inverted FVGs
        IFVG = FVG that was filled and reversed polarity
        """
        try:
            if len(candles) < 3:
                return

            for i in range(len(candles) - 3, max(len(candles) - 50, 0), -1):
                if i < 1:
                    continue

                c1 = candles[i - 1]
                c2 = candles[i]
                c3 = candles[i + 1]

                # Bullish FVG
                if c1.high < c3.low:
                    gap_size = c3.low - c1.high
                    if gap_size > c2.close * 0.0005:  # Min 0.05%
                        if not any(abs(fvg.bottom - c1.high) < 2.0 for fvg in self.fvgs_bull):
                            fvg = FairValueGap(
                                bottom=c1.high,
                                top=c3.low,
                                timestamp=c2.timestamp,
                                direction="bullish"
                            )
                            self.fvgs_bull.append(fvg)

                # Bearish FVG
                elif c1.low > c3.high:
                    gap_size = c1.low - c3.high
                    if gap_size > c2.close * 0.0005:
                        if not any(abs(fvg.top - c1.low) < 2.0 for fvg in self.fvgs_bear):
                            fvg = FairValueGap(
                                bottom=c3.high,
                                top=c1.low,
                                timestamp=c2.timestamp,
                                direction="bearish"
                            )
                            self.fvgs_bear.append(fvg)

        except Exception as e:
            logger.error(f"Error detecting FVGs: {e}")

    def _update_fvg_fills(self, current_candle) -> None:
        """Keep FVGs until 100% filled"""
        try:
            current_price = current_candle.close

            # Update bullish FVGs
            for fvg in self.fvgs_bull:
                if fvg.filled:
                    continue
                
                # Calculate fill percentage
                if current_price <= fvg.bottom:
                    fvg.fill_percentage = 0.0
                elif current_price >= fvg.top:
                    fvg.fill_percentage = 1.0
                else:
                    fvg.fill_percentage = (current_price - fvg.bottom) / (fvg.top - fvg.bottom)
                
                # Mark as filled only at 100%
                if fvg.fill_percentage >= 1.0:
                    fvg.filled = True
                    logger.info(f"✓ Bullish FVG filled: ${fvg.bottom:.2f}-${fvg.top:.2f}")

            # Update bearish FVGs
            for fvg in self.fvgs_bear:
                if fvg.filled:
                    continue
                
                if current_price >= fvg.top:
                    fvg.fill_percentage = 0.0
                elif current_price <= fvg.bottom:
                    fvg.fill_percentage = 1.0
                else:
                    fvg.fill_percentage = (fvg.top - current_price) / (fvg.top - fvg.bottom)
                
                if fvg.fill_percentage >= 1.0:
                    fvg.filled = True
                    logger.info(f"✓ Bearish FVG filled: ${fvg.bottom:.2f}-${fvg.top:.2f}")

        except Exception as e:
            logger.error(f"Error updating FVG fills: {e}")

    # ========================================================================
    # MARKET STRUCTURE (BOS/CHoCH)
    # ========================================================================

    def _detect_market_structure(self, candles: List) -> None:
        """
        Detect BOS (Break of Structure) AND CHoCH (Change of Character)
        BOS = Price breaks previous high/low in TRENDING direction
        CHoCH = FIRST counter-trend break (signals potential reversal)
        """
        try:
            if len(candles) < 20:
                return

            recent = candles[-20:]

            # Get recent swing points
            recent_swings = self._detect_confirmed_swings(recent, lookback=3)
            swing_highs = sorted([s for s in recent_swings if s.swing_type == "high"],
                                key=lambda x: x.timestamp)
            swing_lows = sorted([s for s in recent_swings if s.swing_type == "low"],
                               key=lambda x: x.timestamp)

            if len(swing_highs) < 2 or len(swing_lows) < 2:
                return

            current_price = recent[-1].close
            current_time = time.time()

            # Determine current trend from HTF bias
            in_uptrend = self.htf_bias == "BULLISH"
            in_downtrend = self.htf_bias == "BEARISH"

            # Check for BOS or CHoCH
            # === BULLISH BREAK ===
            prev_high = swing_highs[-2].price
            current_high = swing_highs[-1].price

            if current_high > prev_high:
                # Check if already logged
                if not any(abs(ms.price - prev_high) < 10.0 and ms.direction == "bullish"
                          for ms in list(self.market_structures)[-5:]):
                    if in_uptrend or not in_downtrend:
                        # BOS - continuation
                        ms = MarketStructure(
                            structure_type="BOS",
                            direction="bullish",
                            price=prev_high,
                            timestamp=current_time
                        )
                        self.market_structures.append(ms)
                        logger.info(f"📈 Bullish BOS: ${prev_high:.2f}")
                    elif in_downtrend:
                        # CHoCH - reversal signal
                        ms = MarketStructure(
                            structure_type="CHoCH",
                            direction="bullish",
                            price=prev_high,
                            timestamp=current_time
                        )
                        self.market_structures.append(ms)
                        logger.info(f"🔄 Bullish CHoCH: ${prev_high:.2f} (Potential Reversal)")

            # === BEARISH BREAK ===
            prev_low = swing_lows[-2].price
            current_low = swing_lows[-1].price

            if current_low < prev_low:
                if not any(abs(ms.price - prev_low) < 10.0 and ms.direction == "bearish"
                          for ms in list(self.market_structures)[-5:]):
                    if in_downtrend or not in_uptrend:
                        # BOS - continuation
                        ms = MarketStructure(
                            structure_type="BOS",
                            direction="bearish",
                            price=prev_low,
                            timestamp=current_time
                        )
                        self.market_structures.append(ms)
                        logger.info(f"📉 Bearish BOS: ${prev_low:.2f}")
                    elif in_uptrend:
                        # CHoCH - reversal signal
                        ms = MarketStructure(
                            structure_type="CHoCH",
                            direction="bearish",
                            price=prev_low,
                            timestamp=current_time
                        )
                        self.market_structures.append(ms)
                        logger.info(f"🔄 Bearish CHoCH: ${prev_low:.2f} (Potential Reversal)")

        except Exception as e:
            logger.error(f"Error detecting market structure: {e}")

    # ========================================================================
    # SESSION INFO (PO3 KILLZONES)
    # ========================================================================

    def _update_session_info(self, current_price: float, datamanager) -> None:
        """Update session info and DETECT AMD phase dynamically"""
        try:
            # DETECT AMD PHASE FROM MARKET (not hardcoded!)
            self.amd_phase = self._detect_amd_phase(current_price, datamanager)

            now_utc = datetime.now(timezone.utc)
            hour_est = (now_utc.hour - 5) % 24

            # London Killzone: 02:00-05:00 EST
            if 2 <= hour_est < 5:
                self.in_killzone = True
                self.current_session = "LONDON"
                self.session_params = MAJOR_SESSION_PARAMS
            # New York Killzone: 07:00-10:00 EST
            elif 7 <= hour_est < 10:
                self.in_killzone = True
                self.current_session = "NEW_YORK"
                self.session_params = MAJOR_SESSION_PARAMS
            else:
                self.in_killzone = False
                self.current_session = "OFF_HOURS"
                self.session_params = WEEKEND_PARAMS

        except Exception as e:
            logger.error(f"Error updating session: {e}")

    # ========================================================================
    # CLEANUP
    # ========================================================================

    def _cleanup_old_structures(self, current_time: float) -> None:
        """Remove old or invalid structures"""
        try:
            # Remove broken OBs
            self.order_blocks_bull = deque([ob for ob in self.order_blocks_bull if not ob.broken], maxlen=15)
            self.order_blocks_bear = deque([ob for ob in self.order_blocks_bear if not ob.broken], maxlen=15)

            # Remove filled FVGs (keep IFVGs)
            cutoff = current_time - 86400  # 24 hours
            self.fvgs_bull = deque([fvg for fvg in self.fvgs_bull if not fvg.filled or fvg.is_ifvg or fvg.timestamp > cutoff], maxlen=25)
            self.fvgs_bear = deque([fvg for fvg in self.fvgs_bear if not fvg.filled or fvg.is_ifvg or fvg.timestamp > cutoff], maxlen=25)

            # Remove swept liquidity
            self.liquidity_pools = deque([lp for lp in self.liquidity_pools if not lp.swept or (current_time - lp.sweep_timestamp < 3600)], maxlen=30)

        except Exception as e:
            logger.error(f"Error cleaning structures: {e}")

    # ========================================================================
    # AMD PHASE DETECTION - DYNAMIC FROM MARKET BEHAVIOR - ✅ THROTTLED
    # ========================================================================

    def _detect_amd_phase(self, current_price: float, datamanager) -> str:
        """
        ✅ FIXED: FVG logging throttled to 60 seconds
        Dynamically detect AMD phase from market behavior.
        NOT FIXED TO SESSIONS - detects from actual price action.
        Returns: "ACCUMULATION", "MANIPULATION", "DISTRIBUTION", or "UNKNOWN"
        """
        try:
            # Get candles from datamanager
            candles_15m = datamanager.get_recent_candles("15m", limit=20)
            candles_5m = datamanager.get_recent_candles("5m", limit=50)

            if not candles_15m or len(candles_15m) < 15:
                return "UNKNOWN"
            if not candles_5m or len(candles_5m) < 30:
                return "UNKNOWN"

            # === PRICE METRICS ===
            highs_15m = [c.high for c in candles_15m]
            lows_15m = [c.low for c in candles_15m]
            closes_5m = [c.close for c in candles_5m]

            price_range = max(highs_15m) - min(lows_15m)
            range_pct = (price_range / current_price) * 100 if current_price > 0 else 0

            # === VOLATILITY ===
            ranges_5m = [c.high - c.low for c in candles_5m[-14:]]
            avg_range = sum(ranges_5m) / len(ranges_5m) if ranges_5m else 0
            volatility_pct = (avg_range / current_price) * 100 if current_price > 0 else 0

            # === DIRECTIONAL MOVEMENT ===
            price_change = closes_5m[-1] - closes_5m[0] if len(closes_5m) > 0 else 0
            directional_pct = abs(price_change / closes_5m[0]) * 100 if closes_5m[0] > 0 else 0

            # === LIQUIDITY SWEEPS (Manipulation Signal) ===
            current_time = time.time()
            recent_sweeps = [
                lp for lp in self.liquidity_pools
                if lp.swept and lp.sweep_timestamp
                and (current_time - lp.sweep_timestamp) < 3600  # Last hour
            ]
            sweep_count = len(recent_sweeps)

            # === WICK REJECTIONS (Manipulation Signal) ===
            wick_count = 0
            for c in candles_5m[-10:]:
                body = abs(c.close - c.open)
                total = c.high - c.low
                if total > 0 and (body / total) < 0.5:  # >50% wick
                    wick_count += 1

            # === TRENDING BEHAVIOR (Distribution Signal) ===
            bullish = sum(1 for c in candles_5m[-20:] if c.close > c.open)
            bearish = 20 - bullish
            trend_strength = abs(bullish - bearish) / 20.0

            # === BOS (Distribution Signal) ===
            recent_bos = [
                ms for ms in self.market_structures
                if ms.structure_type == "BOS"
                and (current_time - ms.timestamp) < 3600
            ]
            bos_count = len(recent_bos)

            # ✅ FIXED: THROTTLE FVG LOGGING TO 60 SECONDS
            now = time.time()
            if now - self.last_fvg_log >= 60.0:
                bull_ifvgs = sum(1 for fvg in self.fvgs_bull if fvg.is_ifvg and not fvg.filled)
                bull_fvgs = sum(1 for fvg in self.fvgs_bull if not fvg.is_ifvg and not fvg.filled)
                bear_ifvgs = sum(1 for fvg in self.fvgs_bear if fvg.is_ifvg and not fvg.filled)
                bear_fvgs = sum(1 for fvg in self.fvgs_bear if not fvg.is_ifvg and not fvg.filled)
                
                logger.info(f"📊 Available Bullish FVGs: {bull_fvgs} ({bull_ifvgs} IFVGs)")
                logger.info(f"📊 Available Bearish FVGs: {bear_fvgs} ({bear_ifvgs} IFVGs)")
                self.last_fvg_log = now

            # === DECISION LOGIC ===
            # MANIPULATION (Priority 1): Liquidity hunts + wick rejections
            if sweep_count >= 2 and wick_count >= 3:
                return "MANIPULATION"
            if sweep_count >= 3:
                return "MANIPULATION"

            # DISTRIBUTION (Priority 2): Trending + BOS + volatility
            if trend_strength > 0.6 and bos_count >= 1 and directional_pct > 1.0:
                return "DISTRIBUTION"
            if directional_pct > 2.0 and volatility_pct > 0.3:
                return "DISTRIBUTION"

            # ACCUMULATION (Priority 3): Low volatility + range-bound
            if volatility_pct < 0.15 and range_pct < 1.5:
                return "ACCUMULATION"
            if trend_strength < 0.3:
                return "ACCUMULATION"

            # Transitioning states
            if sweep_count >= 1 or wick_count >= 2:
                return "MANIPULATION"
            elif bos_count >= 1:
                return "DISTRIBUTION"
            else:
                return "ACCUMULATION"

        except Exception as e:
            logger.error(f"Error detecting AMD: {e}", exc_info=True)
            return "UNKNOWN"

    # ========================================================================
    # ENTRY EVALUATION & SCORING - ✅ WITH ENTRY COOLDOWN
    # ========================================================================

    def _can_enter_trade(self, current_time: float, risk_manager) -> bool:
        """
        ✅ FROM Z-SCORE: Check if we can enter new trade with cooldown
        """
        if self.current_position:
            return False

        # ✅ Entry cooldown (5 minutes) - FROM Z-SCORE PATTERN
        if current_time - self.last_entry_time < self.min_entry_gap:
            return False

        # Rate limiting
        if current_time - self.last_trade_time < getattr(config, 'MIN_TRADE_INTERVAL_SEC', 300):
            return False

        # Max consecutive losses
        if self.consecutive_losses >= getattr(config, 'MAX_CONSECUTIVE_LOSSES', 3):
            return False

        # ✅ FROM Z-SCORE: Check risk manager
        try:
            if not risk_manager.check_trading_allowed():
                return False
        except Exception as e:
            logger.error(f"Error checking trading allowed: {e}")
            return False

        return True

    def _evaluate_and_enter(self, current_price: float, current_time: float,
                           data_manager, order_manager, risk_manager) -> None:
        """Evaluate and execute trades"""
        try:
            # Score both directions
            long_score, long_details = self._score_long_setup(current_price, current_time, data_manager)
            short_score, short_details = self._score_short_setup(current_price, current_time, data_manager)

            # Dynamic threshold based on session + AMD phase
            base_threshold = self.session_params.get('entry_score_threshold', 0.40)

            if self.amd_phase == "ACCUMULATION":
                threshold = int(base_threshold * 100) - 5
            elif self.amd_phase == "MANIPULATION":
                threshold = int(base_threshold * 100) + 5
            elif self.amd_phase == "DISTRIBUTION":
                threshold = int(base_threshold * 100)
            else:
                threshold = int(base_threshold * 100)

            threshold = max(30, min(60, threshold))

            # Log comprehensive status every 60 seconds
            if current_time - self.last_score_log >= 60.0:
                self._log_comprehensive_ict_status(current_price, current_time, long_score, short_score, threshold, data_manager)
                self.last_score_log = current_time

            # Log individual scores if above 40 (shows promise)
            now = time.time()
            if long_score >= 40:
                key = f"long_{int(current_price)}"
                if now - self._last_score_logs.get(key, 0) > 30:
                    logger.info(f"🟢 LONG Score: {long_score}/100 | {long_details}")
                    self._last_score_logs[key] = now

            if short_score >= 40:
                key = f"short_{int(current_price)}"
                if now - self._last_score_logs.get(key, 0) > 30:
                    logger.info(f"🔴 SHORT Score: {short_score}/100 | {short_details}")
                    self._last_score_logs[key] = now

            # Execute if threshold met
            if long_score >= threshold and long_score > short_score:
                logger.info("=" * 80)
                logger.info(f"✅ LONG ENTRY TRIGGERED!")
                logger.info(f" Score: {long_score}/100 (Threshold: {threshold})")
                logger.info(f" Confluence: {long_details}")
                logger.info("=" * 80)
                self._execute_long(current_price, long_score, long_details, order_manager, risk_manager, data_manager)

            elif short_score >= threshold and short_score > long_score:
                logger.info("=" * 80)
                logger.info(f"✅ SHORT ENTRY TRIGGERED!")
                logger.info(f" Score: {short_score}/100 (Threshold: {threshold})")
                logger.info(f" Confluence: {short_details}")
                logger.info("=" * 80)
                self._execute_short(current_price, short_score, short_details, order_manager, risk_manager, data_manager)

        except Exception as e:
            logger.error(f"Error in evaluate_and_enter: {e}", exc_info=True)

    # ========================================================================
    # COMPREHENSIVE LOGGING - EVERY 60 SECONDS
    # ========================================================================

    def _log_comprehensive_ict_status(self, current_price: float, current_time: float,
                                     long_score: int, short_score: int,
                                     threshold: int, data_manager) -> None:
        """
        Log comprehensive ICT status every 60 seconds to terminal
        Full market state snapshot with all structures and metrics
        """
        try:
            logger.info("=" * 100)
            logger.info("📊 ICT MARKET STATUS - COMPREHENSIVE REPORT")
            logger.info("=" * 100)

            # === BASIC INFO ===
            logger.info(f"💰 Price: ${current_price:,.2f}")
            logger.info(f"⏰ Time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
            logger.info("")

            # === SESSION & AMD ===
            session_emoji = "🔥" if self.in_killzone else "⏸️"
            logger.info(f"{session_emoji} Session: {self.current_session or 'UNKNOWN'}")
            logger.info(f"{'✅' if self.in_killzone else '❌'} Killzone Active: {self.in_killzone}")
            
            amd_emoji = {
                "ACCUMULATION": "🟢",
                "MANIPULATION": "🟡",
                "DISTRIBUTION": "🔴",
                "UNKNOWN": "⚪"
            }.get(self.amd_phase, "⚪")
            logger.info(f"{amd_emoji} AMD Phase: {self.amd_phase}")
            logger.info("")

            # === HTF BIAS & EMA ===
            bias_emoji = "🔼" if self.htf_bias == "BULLISH" else "🔽" if self.htf_bias == "BEARISH" else "➖"
            logger.info(f"{bias_emoji} HTF Bias (4H): {self.htf_bias}")
            logger.info(f"📈 34 EMA: ${self.htf_ema34:,.2f}")
            
            price_vs_ema = "ABOVE" if current_price > self.htf_ema34 else "BELOW"
            distance_pct = abs((current_price - self.htf_ema34) / self.htf_ema34) * 100 if self.htf_ema34 > 0 else 0
            logger.info(f" Price {price_vs_ema} EMA by {distance_pct:.2f}%")
            logger.info("")

            # === SCORING ===
            logger.info("🎯 SETUP SCORES:")
            logger.info(f" 🟢 LONG: {long_score:3d}/100")
            logger.info(f" 🔴 SHORT: {short_score:3d}/100")
            logger.info(f" 🎚️ Threshold: {threshold}/100 (Dynamic)")
            logger.info("")

            # Show distance to threshold
            if long_score >= threshold:
                logger.info(f" ✅ LONG READY! (+{long_score - threshold} above threshold)")
            else:
                logger.info(f" ⏳ LONG needs {threshold - long_score} more points")

            if short_score >= threshold:
                logger.info(f" ✅ SHORT READY! (+{short_score - threshold} above threshold)")
            else:
                logger.info(f" ⏳ SHORT needs {threshold - short_score} more points")
            
            logger.info("")

            # === ACTIVE STRUCTURES COUNT ===
            logger.info("📐 ACTIVE ICT STRUCTURES:")
            logger.info(f" 🟢 Bullish OBs: {len(self.order_blocks_bull)} (wick rejection)")
            logger.info(f" 🔴 Bearish OBs: {len(self.order_blocks_bear)} (wick rejection)")
            
            bull_ifvgs = sum(1 for fvg in self.fvgs_bull if fvg.is_ifvg and not fvg.filled)
            bull_fvgs = sum(1 for fvg in self.fvgs_bull if not fvg.is_ifvg and not fvg.filled)
            bear_ifvgs = sum(1 for fvg in self.fvgs_bear if fvg.is_ifvg and not fvg.filled)
            bear_fvgs = sum(1 for fvg in self.fvgs_bear if not fvg.is_ifvg and not fvg.filled)
            
            logger.info(f" 🟢 Bullish FVGs: {bull_fvgs} ({bull_ifvgs} IFVGs)")
            logger.info(f" 🔴 Bearish FVGs: {bear_fvgs} ({bear_ifvgs} IFVGs)")
            logger.info("")

            # === LIQUIDITY POOLS ===
            eqh_pools = sum(1 for lp in self.liquidity_pools if lp.pool_type == "EQH" and not lp.swept)
            eql_pools = sum(1 for lp in self.liquidity_pools if lp.pool_type == "EQL" and not lp.swept)
            swept_count = sum(1 for lp in self.liquidity_pools if lp.swept)
            
            logger.info("💧 LIQUIDITY:")
            logger.info(f" EQH Pools: {eqh_pools} | EQL Pools: {eql_pools}")
            logger.info(f" Recently Swept: {swept_count}")
            logger.info("")

            # === VOLUME DELTA ===
            try:
                vol_data = data_manager.get_volume_delta(lookback_seconds=300)
                if vol_data and isinstance(vol_data, dict):
                    delta_pct = vol_data.get("delta_pct", 0.0) * 100
                    buy_vol = vol_data.get("buy_volume", 0.0)
                    sell_vol = vol_data.get("sell_volume", 0.0)
                    
                    delta_emoji = "🟢" if delta_pct > 15 else "🔴" if delta_pct < -15 else "⚪"
                    logger.info(f"{delta_emoji} Volume Delta (5min): {delta_pct:+.1f}%")
                    logger.info(f" Buy: {buy_vol:,.0f} | Sell: {sell_vol:,.0f}")
                    logger.info("")
            except:
                pass

            # === POSITION INFO ===
            if self.current_position:
                logger.info("📍 ACTIVE POSITION:")
                side = self.current_position.get("side", "?")
                entry = self.current_position.get("entry_price", 0)
                sl = self.current_position.get("stop_loss", 0)
                tp = self.current_position.get("take_profit", 0)
                qty = self.current_position.get("quantity", 0)
                
                if side == "LONG":
                    pnl_pct = ((current_price - entry) / entry) * 100 if entry > 0 else 0
                else:
                    pnl_pct = ((entry - current_price) / entry) * 100 if entry > 0 else 0
                
                pnl_emoji = "🟢" if pnl_pct > 0 else "🔴"
                logger.info(f" {side} @ ${entry:.2f} | Qty: {qty:.4f}")
                logger.info(f" SL: ${sl:.2f} | TP: ${tp:.2f}")
                logger.info(f" {pnl_emoji} P&L: {pnl_pct:+.2f}%")
                logger.info("")

            # === PERFORMANCE STATS ===
            win_rate = (self.total_wins / (self.total_wins + self.total_losses) * 100) if (self.total_wins + self.total_losses) > 0 else 0
            
            logger.info("📈 PERFORMANCE:")
            logger.info(f" Total Entries: {self.total_entries}")
            logger.info(f" Wins: {self.total_wins} | Losses: {self.total_losses}")
            logger.info(f" Win Rate: {win_rate:.1f}%")
            logger.info(f" Consecutive Losses: {self.consecutive_losses}")
            
            logger.info("=" * 100)

        except Exception as e:
            logger.error(f"Error in comprehensive ICT logging: {e}", exc_info=True)

    # ========================================================================
    # CONFLUENCE SCORING
    # ========================================================================

    def _score_long_setup(self, current_price: float, current_time: float, data_manager) -> Tuple[int, str]:
        """
        Score LONG setup (0-100 points)
        Only counts AVAILABLE unfilled FVGs
        """
        score = 0
        details = []

        try:
            # HTF Bias alignment (+15)
            if self.htf_bias == "BULLISH":
                score += SCORE_HTF_BIAS_ALIGNED
                details.append("HTF_BULLISH")

            # Order Block confluence (+20 optimal, +15 near)
            active_obs = [ob for ob in self.order_blocks_bull if not ob.broken and ob.has_wick_rejection]
            ob_score = 0
            for ob in active_obs:
                if ob.in_optimal_zone(current_price):
                    ob_score = SCORE_ORDER_BLOCK_OPTIMAL
                    details.append(f"OB_ZONE[{ob.low:.0f}-{ob.high:.0f}]")
                    break
                elif ob.near_zone(current_price, tolerance_pct=0.005):
                    if ob_score < 15:
                        ob_score = 15
                        details.append(f"OB_NEAR[{ob.low:.0f}-{ob.high:.0f}]")
            score += ob_score

            # FVG confluence (+15 FVG, +25 IFVG)
            ifvgs = [fvg for fvg in self.fvgs_bull if fvg.is_ifvg and not fvg.filled]
            fvgs = [fvg for fvg in self.fvgs_bull if not fvg.is_ifvg and not fvg.filled]
            
            if ifvgs:
                score += SCORE_FVG_CONFLUENCE + SCORE_IFVG_BONUS
                details.append("IFVG")
            elif fvgs:
                score += SCORE_FVG_CONFLUENCE
                details.append("FVG")

            # Liquidity sweep (+30)
            recent_sweeps = [
                lp for lp in self.liquidity_pools
                if lp.pool_type == "EQL" and lp.swept and lp.sweep_timestamp
                and current_time - lp.sweep_timestamp < 1800
            ]
            if recent_sweeps:
                score += SCORE_LIQUIDITY_SWEEP
                details.append("EQL_SWEEP")

            # Market structure (+5)
            recent_bos = [
                ms for ms in self.market_structures
                if ms.structure_type == "BOS" and ms.direction == "bullish"
                and current_time - ms.timestamp < 3600
            ]
            if recent_bos:
                score += SCORE_STRUCTURE_ALIGNED
                details.append("BOS")

            # Volume delta (+5)
            volume_data = data_manager.get_volume_delta(lookback_seconds=300)
            if volume_data and isinstance(volume_data, dict):
                delta_pct = volume_data.get("delta_pct", 0)
                if delta_pct > VOLUME_DELTA_STRONG_THRESHOLD:
                    score += SCORE_VOLUME_DELTA_STRONG
                    details.append(f"VOL_DELTA[+{int(delta_pct*100)}%]")
                elif delta_pct > 0.03:
                    score += 3
                    details.append(f"VOL_DELTA[+{int(delta_pct*100)}%]")

            # Killzone timing (+5)
            if self.in_killzone:
                score += SCORE_PO3_KILLZONE
                details.append(f"KZ[{self.current_session}]")

            # Penalties
            recent_bearish_bos = [
                ms for ms in self.market_structures
                if ms.structure_type == "BOS" and ms.direction == "bearish"
                and current_time - ms.timestamp < 3600
            ]
            if recent_bearish_bos:
                score -= 5
                details.append("AGAINST_STRUCTURE")

            if self.htf_bias == "BEARISH":
                score -= 5
                details.append("HTF_AGAINST")

        except Exception as e:
            logger.error(f"Error scoring long: {e}")
            return 0, f"ERROR[{e}]"

        return max(0, score), ", ".join(details)

    def _score_short_setup(self, current_price: float, current_time: float, data_manager) -> Tuple[int, str]:
        """
        Score SHORT setup (0-100 points)
        Only counts AVAILABLE unfilled FVGs
        """
        score = 0
        details = []

        try:
            # HTF Bias alignment (+15)
            if self.htf_bias == "BEARISH":
                score += SCORE_HTF_BIAS_ALIGNED
                details.append("HTF_BEARISH")

            # Order Block confluence (+20 optimal, +15 near)
            active_obs = [ob for ob in self.order_blocks_bear if not ob.broken and ob.has_wick_rejection]
            ob_score = 0
            for ob in active_obs:
                if ob.in_optimal_zone(current_price):
                    ob_score = SCORE_ORDER_BLOCK_OPTIMAL
                    details.append(f"OB_ZONE[{ob.low:.0f}-{ob.high:.0f}]")
                    break
                elif ob.near_zone(current_price, tolerance_pct=0.005):
                    if ob_score < 15:
                        ob_score = 15
                        details.append(f"OB_NEAR[{ob.low:.0f}-{ob.high:.0f}]")
            score += ob_score

            # FVG confluence (+15 FVG, +25 IFVG)
            ifvgs = [fvg for fvg in self.fvgs_bear if fvg.is_ifvg and not fvg.filled]
            fvgs = [fvg for fvg in self.fvgs_bear if not fvg.is_ifvg and not fvg.filled]
            
            if ifvgs:
                score += SCORE_FVG_CONFLUENCE + SCORE_IFVG_BONUS
                details.append("IFVG")
            elif fvgs:
                score += SCORE_FVG_CONFLUENCE
                details.append("FVG")

            # Liquidity sweep (+30)
            recent_sweeps = [
                lp for lp in self.liquidity_pools
                if lp.pool_type == "EQH" and lp.swept and lp.sweep_timestamp
                and current_time - lp.sweep_timestamp < 1800
            ]
            if recent_sweeps:
                score += SCORE_LIQUIDITY_SWEEP
                details.append("EQH_SWEEP")

            # Market structure (+5)
            recent_bos = [
                ms for ms in self.market_structures
                if ms.structure_type == "BOS" and ms.direction == "bearish"
                and current_time - ms.timestamp < 3600
            ]
            if recent_bos:
                score += SCORE_STRUCTURE_ALIGNED
                details.append("BOS")

            # Volume delta (+5)
            volume_data = data_manager.get_volume_delta(lookback_seconds=300)
            if volume_data and isinstance(volume_data, dict):
                delta_pct = volume_data.get("delta_pct", 0)
                if delta_pct < -VOLUME_DELTA_STRONG_THRESHOLD:
                    score += SCORE_VOLUME_DELTA_STRONG
                    details.append(f"VOL_DELTA[{int(delta_pct*100)}%]")
                elif delta_pct < -0.03:
                    score += 3
                    details.append(f"VOL_DELTA[{int(delta_pct*100)}%]")

            # Killzone timing (+5)
            if self.in_killzone:
                score += SCORE_PO3_KILLZONE
                details.append(f"KZ[{self.current_session}]")

            # Penalties
            recent_bullish_bos = [
                ms for ms in self.market_structures
                if ms.structure_type == "BOS" and ms.direction == "bullish"
                and current_time - ms.timestamp < 3600
            ]
            if recent_bullish_bos:
                score -= 5
                details.append("AGAINST_STRUCTURE")

            if self.htf_bias == "BULLISH":
                score -= 5
                details.append("HTF_AGAINST")

        except Exception as e:
            logger.error(f"Error scoring short: {e}")
            return 0, f"ERROR[{e}]"

        return max(0, score), ", ".join(details)

    # ========================================================================
    # TRADE EXECUTION - ✅ EXACT Z-SCORE INFRASTRUCTURE
    # ========================================================================

    def _calculate_atr(self, candles: List, period: int = 14) -> float:
        """Calculate ATR with EMA smoothing"""
        try:
            if len(candles) < period + 1:
                recent = candles[-20:] if len(candles) >= 20 else candles
                ranges = [c.high - c.low for c in recent]
                return sum(ranges) / len(ranges) if ranges else 0.0

            true_ranges = []
            for i in range(1, len(candles)):
                prev_close = candles[i-1].close
                current_high = candles[i].high
                current_low = candles[i].low

                tr = max(
                    current_high - current_low,
                    abs(current_high - prev_close),
                    abs(current_low - prev_close)
                )
                true_ranges.append(tr)

            if len(true_ranges) < period:
                return sum(true_ranges) / len(true_ranges) if true_ranges else 0.0

            # EMA smoothing
            multiplier = 2 / (period + 1)
            atr = sum(true_ranges[:period]) / period
            for tr in true_ranges[period:]:
                atr = (tr - atr) * multiplier + atr

            return atr

        except Exception as e:
            logger.error(f"Error calculating ATR: {e}")
            return 0.0

    def _get_limit_price_offset(self, current_price: float, data_manager, side: str) -> float:
        """Calculate limit price offset based on volatility"""
        try:
            candles_5m = data_manager.get_recent_candles("5m", 50)
            if not candles_5m or len(candles_5m) < 15:
                return current_price * 0.0005

            atr = self._calculate_atr(candles_5m, period=14)
            if atr <= 0:
                return current_price * 0.0005

            # Offset = 20% of ATR
            offset = atr * 0.20

            # Bounds
            min_offset = 5.0
            max_offset = current_price * 0.0015

            offset = max(min_offset, min(offset, max_offset))

            return offset

        except Exception as e:
            logger.error(f"Error calculating limit offset: {e}")
            return current_price * 0.0005

    def _execute_long(self, entry_price: float, score: int, details: str,
                    order_manager, risk_manager, data_manager) -> None:
        """
        ✅ Execute LONG with EXACT Z-Score infrastructure + TP/SL placement
        Uses: get_available_balance() + calculate_position_size_vol_regime()
        """
        try:
            with self._execution_lock:
                balance_info = risk_manager.get_available_balance()
                if not balance_info or not isinstance(balance_info, dict):
                    logger.error("❌ Failed to get balance info")
                    return
                
                balance = float(balance_info.get('available', 0))
                if balance <= 0:
                    logger.error(f"❌ Balance is {balance:.2f}, cannot enter trade")
                    return
                
                logger.info(f"✅ Available balance: ${balance:.2f}")
                leverage = getattr(config, 'DEFAULT_LEVERAGE', 25)
                stop_distance_pct = 0.015
                
                # Calculate SL based on EQL
                nearest_eql = None
                for lp in self.liquidity_pools:
                    if lp.pool_type == "EQL" and lp.price < entry_price:
                        if not nearest_eql or lp.price > nearest_eql.price:
                            nearest_eql = lp
                
                if nearest_eql:
                    stop_loss = nearest_eql.price * 0.998
                    logger.info(f"🎯 SL set to EQL: ${stop_loss:.2f}")
                else:
                    stop_loss = entry_price * (1 - stop_distance_pct)
                
                # Calculate TP based on EQH
                nearest_eqh = None
                for lp in self.liquidity_pools:
                    if lp.pool_type == "EQH" and lp.price > entry_price and not lp.swept:
                        if not nearest_eqh or lp.price < nearest_eqh.price:
                            nearest_eqh = lp
                
                if nearest_eqh and (nearest_eqh.price - entry_price) > (entry_price - stop_loss) * 2:
                    take_profit = nearest_eqh.price * 0.998
                    logger.info(f"🎯 TP set to EQH: ${take_profit:.2f}")
                else:
                    risk = entry_price - stop_loss
                    take_profit = entry_price + (risk * 3)
                
                # Calculate position size
                position_size = risk_manager.calculate_position_size_vol_regime(
                    entry_price=entry_price,
                    stoploss=stop_loss,
                    vol_regime="normal"
                )
                
                min_size = getattr(config, 'MIN_POSITION_SIZE', 0.001)
                max_size = getattr(config, 'MAX_POSITION_SIZE', 1.0)
                
                if position_size <= 0 or position_size < min_size:
                    logger.warning(f"Position size {position_size:.4f} invalid or < min {min_size}, skipping")
                    return
                
                position_size = min(position_size, max_size)
                logger.info(f"✓ Position size: {position_size:.4f} (bounds: {min_size}-{max_size})")
                
                # Calculate limit price
                limit_offset = self._get_limit_price_offset(entry_price, data_manager, "BUY")
                limit_price = entry_price - limit_offset
                limit_price = round(limit_price, 2)
                
                logger.info(f"📊 LONG Limit Order: Market=${entry_price:.2f}, Limit=${limit_price:.2f} (offset: ${limit_offset:.2f})")
                
                # 1️⃣ Place LIMIT entry order
                result = order_manager.place_limit_order(
                    side="BUY",
                    quantity=position_size,
                    price=limit_price,
                    reduce_only=False
                )
                
                if result and result.get("order_id"):
                    order_id = result.get("order_id")
                    
                    # 2️⃣ Immediately place STOP LOSS order
                    sl_order_id = None
                    try:
                        logger.info(f"🛡️ Placing Stop Loss: ${stop_loss:.2f}")
                        sl_result = order_manager.place_stop_loss(
                            side="SELL",
                            quantity=position_size,
                            trigger_price=stop_loss
                        )
                        if sl_result and sl_result.get("order_id"):
                            sl_order_id = sl_result.get("order_id")
                            logger.info(f"✅ Stop Loss placed: {sl_order_id}")
                        else:
                            logger.error(f"❌ Failed to place Stop Loss: {sl_result}")
                    except Exception as e:
                        logger.error(f"❌ Error placing Stop Loss: {e}")
                    
                    # 3️⃣ Immediately place TAKE PROFIT order
                    tp_order_id = None
                    try:
                        logger.info(f"🎯 Placing Take Profit: ${take_profit:.2f}")
                        tp_result = order_manager.place_take_profit(
                            side="SELL",
                            quantity=position_size,
                            trigger_price=take_profit
                        )
                        if tp_result and tp_result.get("order_id"):
                            tp_order_id = tp_result.get("order_id")
                            logger.info(f"✅ Take Profit placed: {tp_order_id}")
                        else:
                            logger.error(f"❌ Failed to place Take Profit: {tp_result}")
                    except Exception as e:
                        logger.error(f"❌ Error placing Take Profit: {e}")
                    
                    # 4️⃣ Store position details
                    self.current_position = {
                        "side": "LONG",
                        "entry_price": limit_price,
                        "market_price": entry_price,
                        "quantity": position_size,
                        "stop_loss": stop_loss,
                        "take_profit": take_profit,
                        "tp1": limit_price + ((limit_price - stop_loss) * 2.0),
                        "score": score,
                        "details": details,
                        "order_id": order_id,
                        "sl_order_id": sl_order_id,  # ⚠️ NEW: Track SL order
                        "tp_order_id": tp_order_id,  # ⚠️ NEW: Track TP order
                        "status": "PENDING_FILL",
                        "order_type": "LIMIT",
                        "limit_price": limit_price,
                        "placed_at": time.time()
                    }
                    
                    self.position_entry_time = time.time()
                    self.last_entry_time = time.time()
                    self.tp1_hit = False
                    self.breakeven_moved = False
                    
                    risk_manager.record_trade_opened(position_size * limit_price / leverage)
                    
                    # Telegram notification
                    msg = format_entry_signal(
                        side="LONG",
                        entry_price=limit_price,
                        sl_price=stop_loss,
                        tp_price=take_profit,
                        quantity=position_size,
                        score=score / 100.0,
                        risk_reward=(take_profit - limit_price) / (limit_price - stop_loss) if (limit_price - stop_loss) > 0 else 1.0,
                        reasons={"confluence": details}
                    )
                    msg += f"\n\n📋 Order Type: LIMIT\n💰 Limit Price: ${limit_price:.2f}\n📊 Market Price: ${entry_price:.2f}"
                    msg += f"\n\n📝 Order IDs:\n• Entry: {order_id}\n• SL: {sl_order_id or 'N/A'}\n• TP: {tp_order_id or 'N/A'}"
                    send_telegram_message(msg)
                    
                    logger.info(f"✅ LONG LIMIT ORDER PLACED: ${limit_price:.2f} | Score: {score}/100 | SL: ${stop_loss:.2f} | TP: ${take_profit:.2f}")
                    logger.info(f"📝 Order ID: {order_id} | SL: {sl_order_id} | TP: {tp_order_id} | Status: PENDING FILL")
                else:
                    logger.error(f"❌ Failed to place LONG limit order: {result}")
                    
        except Exception as e:
            logger.error(f"Error executing long: {e}", exc_info=True)


    def _execute_short(self, entry_price: float, score: int, details: str,
                    order_manager, risk_manager, data_manager) -> None:
        """
        ✅ Execute SHORT with EXACT Z-Score infrastructure + TP/SL placement
        Uses: get_available_balance() + calculate_position_size_vol_regime()
        """
        try:
            with self._execution_lock:
                balance_info = risk_manager.get_available_balance()
                if not balance_info or not isinstance(balance_info, dict):
                    logger.error("❌ Failed to get balance info")
                    return
                
                balance = float(balance_info.get('available', 0))
                if balance <= 0:
                    logger.error(f"❌ Balance is {balance:.2f}, cannot enter trade")
                    return
                
                logger.info(f"✅ Available balance: ${balance:.2f}")
                leverage = getattr(config, 'DEFAULT_LEVERAGE', 25)
                stop_distance_pct = 0.015
                
                # Calculate SL based on EQH
                nearest_eqh = None
                for lp in self.liquidity_pools:
                    if lp.pool_type == "EQH" and lp.price > entry_price:
                        if not nearest_eqh or lp.price < nearest_eqh.price:
                            nearest_eqh = lp
                
                if nearest_eqh:
                    stop_loss = nearest_eqh.price * 1.002
                    logger.info(f"🎯 SL set to EQH: ${stop_loss:.2f}")
                else:
                    stop_loss = entry_price * (1 + stop_distance_pct)
                
                # Calculate TP based on EQL
                nearest_eql = None
                for lp in self.liquidity_pools:
                    if lp.pool_type == "EQL" and lp.price < entry_price and not lp.swept:
                        if not nearest_eql or lp.price > nearest_eql.price:
                            nearest_eql = lp
                
                if nearest_eql and (entry_price - nearest_eql.price) > (stop_loss - entry_price) * 2:
                    take_profit = nearest_eql.price * 1.002
                    logger.info(f"🎯 TP set to EQL: ${take_profit:.2f}")
                else:
                    risk = stop_loss - entry_price
                    take_profit = entry_price - (risk * 3)
                
                # Calculate position size
                position_size = risk_manager.calculate_position_size_vol_regime(
                    entry_price=entry_price,
                    stoploss=stop_loss,
                    vol_regime="normal"
                )
                
                min_size = getattr(config, 'MIN_POSITION_SIZE', 0.001)
                max_size = getattr(config, 'MAX_POSITION_SIZE', 1.0)
                
                if position_size <= 0 or position_size < min_size:
                    logger.warning(f"Position size {position_size:.4f} invalid or < min {min_size}, skipping")
                    return
                
                position_size = min(position_size, max_size)
                logger.info(f"✓ Position size: {position_size:.4f} (bounds: {min_size}-{max_size})")
                
                # Calculate limit price
                limit_offset = self._get_limit_price_offset(entry_price, data_manager, "SELL")
                limit_price = entry_price + limit_offset
                limit_price = round(limit_price, 2)
                
                logger.info(f"📊 SHORT Limit Order: Market=${entry_price:.2f}, Limit=${limit_price:.2f} (offset: ${limit_offset:.2f})")
                
                # 1️⃣ Place LIMIT entry order
                result = order_manager.place_limit_order(
                    side="SELL",
                    quantity=position_size,
                    price=limit_price,
                    reduce_only=False
                )
                
                if result and result.get("order_id"):
                    order_id = result.get("order_id")
                    
                    # 2️⃣ Immediately place STOP LOSS order
                    sl_order_id = None
                    try:
                        logger.info(f"🛡️ Placing Stop Loss: ${stop_loss:.2f}")
                        sl_result = order_manager.place_stop_loss(
                            side="BUY",
                            quantity=position_size,
                            trigger_price=stop_loss
                        )
                        if sl_result and sl_result.get("order_id"):
                            sl_order_id = sl_result.get("order_id")
                            logger.info(f"✅ Stop Loss placed: {sl_order_id}")
                        else:
                            logger.error(f"❌ Failed to place Stop Loss: {sl_result}")
                    except Exception as e:
                        logger.error(f"❌ Error placing Stop Loss: {e}")
                    
                    # 3️⃣ Immediately place TAKE PROFIT order
                    tp_order_id = None
                    try:
                        logger.info(f"🎯 Placing Take Profit: ${take_profit:.2f}")
                        tp_result = order_manager.place_take_profit(
                            side="BUY",
                            quantity=position_size,
                            trigger_price=take_profit
                        )
                        if tp_result and tp_result.get("order_id"):
                            tp_order_id = tp_result.get("order_id")
                            logger.info(f"✅ Take Profit placed: {tp_order_id}")
                        else:
                            logger.error(f"❌ Failed to place Take Profit: {tp_result}")
                    except Exception as e:
                        logger.error(f"❌ Error placing Take Profit: {e}")
                    
                    # 4️⃣ Store position details
                    self.current_position = {
                        "side": "SHORT",
                        "entry_price": limit_price,
                        "market_price": entry_price,
                        "quantity": position_size,
                        "stop_loss": stop_loss,
                        "take_profit": take_profit,
                        "tp1": limit_price - ((stop_loss - limit_price) * 2.0),
                        "score": score,
                        "details": details,
                        "order_id": order_id,
                        "sl_order_id": sl_order_id,  # ⚠️ NEW: Track SL order
                        "tp_order_id": tp_order_id,  # ⚠️ NEW: Track TP order
                        "status": "PENDING_FILL",
                        "order_type": "LIMIT",
                        "limit_price": limit_price,
                        "placed_at": time.time()
                    }
                    
                    self.position_entry_time = time.time()
                    self.last_entry_time = time.time()
                    self.tp1_hit = False
                    self.breakeven_moved = False
                    
                    risk_manager.record_trade_opened(position_size * limit_price / leverage)
                    
                    # Telegram notification
                    msg = format_entry_signal(
                        side="SHORT",
                        entry_price=limit_price,
                        sl_price=stop_loss,
                        tp_price=take_profit,
                        quantity=position_size,
                        score=score / 100.0,
                        risk_reward=(limit_price - take_profit) / (stop_loss - limit_price) if (stop_loss - limit_price) > 0 else 1.0,
                        reasons={"confluence": details}
                    )
                    msg += f"\n\n📋 Order Type: LIMIT\n💰 Limit Price: ${limit_price:.2f}\n📊 Market Price: ${entry_price:.2f}"
                    msg += f"\n\n📝 Order IDs:\n• Entry: {order_id}\n• SL: {sl_order_id or 'N/A'}\n• TP: {tp_order_id or 'N/A'}"
                    send_telegram_message(msg)
                    
                    logger.info(f"✅ SHORT LIMIT ORDER PLACED: ${limit_price:.2f} | Score: {score}/100 | SL: ${stop_loss:.2f} | TP: ${take_profit:.2f}")
                    logger.info(f"📝 Order ID: {order_id} | SL: {sl_order_id} | TP: {tp_order_id} | Status: PENDING FILL")
                else:
                    logger.error(f"❌ Failed to place SHORT limit order: {result}")
                    
        except Exception as e:
            logger.error(f"Error executing short: {e}", exc_info=True)

    # ========================================================================
    # POSITION MANAGEMENT - ✅ FROM Z-SCORE PATTERN
    # ========================================================================

    def _manage_position(self, current_price: float, current_time: float,
                        order_manager, data_manager, risk_manager) -> None:
        """
        Manage position with THROTTLED limit order fill monitoring (every 5s)
        States:
        1. PENDING_FILL: Check if limit order filled (throttled to 5s intervals)
        2. FILLED: Normal position management (TP1, BE, trailing)
        """
        try:
            if not self.current_position:
                return
            
            # ═══════════════════════════════════════════════════════════════
            # CHECK IF POSITION STILL EXISTS ON EXCHANGE (TP/SL AUTO-CLOSED)
            # ═══════════════════════════════════════════════════════════════
            if not hasattr(self, 'last_position_check_time'):
                self.last_position_check_time = 0
            
            if current_time - self.last_position_check_time >= 10.0:
                self.last_position_check_time = current_time
                
                try:
                    from order_manager import GlobalRateLimiter
                    GlobalRateLimiter.wait()
                    
                    # Check actual positions on exchange
                    positions_response = order_manager.api.get_positions(
                        exchange=config.EXCHANGE,
                        symbol=config.SYMBOL
                    )
                    
                    if positions_response and 'data' in positions_response:
                        positions = positions_response['data'].get('positions', [])
                        
                        # Check if position still exists
                        position_exists = False
                        for pos in positions:
                            if pos.get('symbol') == config.SYMBOL:
                                qty = float(pos.get('positionSize', 0) or pos.get('quantity', 0))
                                if abs(qty) > 0.0001:
                                    position_exists = True
                                    break
                        
                        if not position_exists:
                            # Position was closed by exchange (TP/SL hit automatically)
                            side = self.current_position['side']
                            entry = self.current_position['entry_price']
                            
                            logger.info("=" * 80)
                            logger.info("🎯 POSITION AUTO-CLOSED BY EXCHANGE (TP/SL)")
                            logger.info(f"📊 {side} @ ${entry:.2f} → Closed @ ${current_price:.2f}")
                            logger.info("=" * 80)
                            
                            # Calculate PnL
                            if side == 'LONG':
                                pnl_pct = ((current_price - entry) / entry) * 100
                            else:
                                pnl_pct = ((entry - current_price) / entry) * 100
                            
                            # Update stats
                            if pnl_pct > 0:
                                self.total_wins += 1
                                self.consecutive_losses = 0
                            else:
                                self.total_losses += 1
                                self.consecutive_losses += 1
                            
                            try:
                                risk_manager.update_trade_stats(pnl_pct)
                            except:
                                pass
                            
                            # CLEANUP - DON'T TRY TO CLOSE (already closed by exchange)
                            self.current_position = None
                            self.tp1_hit = False
                            self.breakeven_moved = False
                            
                            logger.info(f"✅ WIN: Total {self.total_wins}/{self.total_wins + self.total_losses}")
                            return
                            
                except Exception as e:
                    logger.error(f"Error checking position status: {e}")
            
            status = self.current_position.get("status", "FILLED")
            
            # ═══════════════════════════════════════════════════════════════
            # STATE 1: PENDING LIMIT ORDER - THROTTLED STATUS CHECKS
            # ═══════════════════════════════════════════════════════════════
            if status == "PENDING_FILL":
                order_id = self.current_position.get("order_id")
                placed_at = self.current_position.get("placed_at", current_time)
                
                # ⚠️ THROTTLE: Only check order status every 5 seconds
                if current_time - self.last_status_check < 5.0:
                    return  # Skip this check cycle to avoid 429 errors
                
                # Update last check time BEFORE making API call
                self.last_status_check = current_time
                
                # Timeout check (5 minutes)
                if current_time - placed_at > 300:
                    logger.warning(f"⏰ Limit order timeout, cancelling: {order_id}")
                    try:
                        # Cancel all orders (entry + SL + TP)
                        cancel_result = order_manager.cancel_order(order_id)
                        if self.current_position.get("sl_order_id"):
                            order_manager.cancel_order(self.current_position["sl_order_id"])
                        if self.current_position.get("tp_order_id"):
                            order_manager.cancel_order(self.current_position["tp_order_id"])
                        
                        if cancel_result:
                            logger.info(f"✅ Order cancelled successfully")
                            send_telegram_message(f"⏰ Limit Order Cancelled\nReason: Timeout (5 min)")
                        else:
                            logger.error(f"❌ Failed to cancel order")
                    except Exception as e:
                        logger.error(f"Error cancelling order: {e}")
                    
                    self.current_position = None
                    return
                
                # Check if order filled (THROTTLED TO EVERY 5 SECONDS)
                try:
                    logger.info(f"🔍 Checking order status: {order_id} (throttled: every 5s)")
                    order_status = order_manager.get_order_status(order_id, retry_count=1)
                    
                    if order_status:
                        status_str = str(order_status.get("status", "")).upper()
                        
                        # ⚠️ TREAT PARTIALLY_EXECUTED AS FILLED
                        if status_str in ["FILLED", "EXECUTED", "PARTIALLY_EXECUTED"]:
                            # Order filled (fully or partially)!
                            fill_price = order_status.get("avgPrice") or order_status.get("price") or self.current_position["limit_price"]
                            self.current_position["status"] = "FILLED"
                            self.current_position["entry_price"] = float(fill_price)
                            
                            # Update quantity if partially filled
                            if status_str == "PARTIALLY_EXECUTED":
                                try:
                                    exec_qty = float(order_status.get("executedQty") or order_status.get("exec_quantity") or self.current_position["quantity"])
                                    if exec_qty > 0 and exec_qty != self.current_position["quantity"]:
                                        logger.warning(f"⚠️ Partial fill: {exec_qty:.4f} / {self.current_position['quantity']:.4f}")
                                        self.current_position["quantity"] = exec_qty
                                        send_telegram_message(f"⚠️ Partial Fill\n💰 Fill Price: ${fill_price:.2f}\n📊 Quantity: {exec_qty:.4f} (partial)")
                                    else:
                                        send_telegram_message(f"✅ Position Filled\n💰 Fill Price: ${fill_price:.2f}")
                                except Exception as e:
                                    logger.error(f"Error parsing executed quantity: {e}")
                                    send_telegram_message(f"✅ Position Filled\n💰 Fill Price: ${fill_price:.2f}")
                            else:
                                send_telegram_message(f"✅ Position Filled\n💰 Fill Price: ${fill_price:.2f}")
                            
                            logger.info(f"✅ LIMIT ORDER FILLED @ ${fill_price:.2f} (Status: {status_str})")
                            return
                        
                        elif status_str in ["CANCELLED", "REJECTED", "EXPIRED"]:
                            # Order failed
                            logger.warning(f"⚠️ Limit order {status_str}: {order_id}")
                            send_telegram_message(f"⚠️ Order {status_str}\nReason: Order not filled")
                            
                            # Cancel TP/SL orders
                            if self.current_position.get("sl_order_id"):
                                order_manager.cancel_order(self.current_position["sl_order_id"])
                            if self.current_position.get("tp_order_id"):
                                order_manager.cancel_order(self.current_position["tp_order_id"])
                            
                            self.current_position = None
                            return
                        
                        else:
                            logger.info(f"📊 Limit order still pending: {status_str}")
                    else:
                        logger.warning(f"⚠️ Failed to get order status (rate limit or API issue)")
                    
                except Exception as e:
                    logger.error(f"Error checking order status: {e}")
                
                return  # Still waiting for fill
            
            # ═══════════════════════════════════════════════════════════════
            # STATE 2: FILLED POSITION - NORMAL MANAGEMENT
            # ═══════════════════════════════════════════════════════════════
            side = self.current_position["side"]
            entry = self.current_position["entry_price"]
            sl = self.current_position["stop_loss"]
            tp = self.current_position["take_profit"]
            tp1 = self.current_position["tp1"]
            qty = self.current_position["quantity"]
            
            # LONG management
            if side == "LONG":
                pnl_pct = ((current_price - entry) / entry) * 100
                
                # Stop loss check
                if current_price <= sl:
                    self._close_position(current_price, "STOP_LOSS", order_manager, risk_manager)
                    return
                
                # TP1 (partial close 50% at 2:1)
                if not self.tp1_hit and current_price >= tp1:
                    self.tp1_hit = True
                    partial_qty = qty * 0.5
                    logger.info(f"🎯 TP1 Hit: Closing 50% at ${current_price:.2f} (+2R)")
                    try:
                        close_result = order_manager.place_market_order(
                            side="SELL",
                            quantity=partial_qty,
                            reduce_only=True
                        )
                        if close_result:
                            logger.info(f"✅ Partial close SUCCESS: {partial_qty:.4f} @ ${current_price:.2f}")
                            send_telegram_message(f"🎯 TP1 Hit! Closed 50%\n💰 Price: ${current_price:.2f}\n📊 P&L: +{pnl_pct:.2f}%")
                            self.current_position["quantity"] = qty - partial_qty
                        else:
                            logger.error(f"❌ Partial close FAILED")
                            self.tp1_hit = False
                    except Exception as e:
                        logger.error(f"Error closing partial: {e}")
                        self.tp1_hit = False
                
                # Breakeven (1:1 RR)
                if self.tp1_hit and not self.breakeven_moved:
                    risk = entry - sl
                    be_price = entry + (risk * 1.0)
                    if current_price >= be_price:
                        self.breakeven_moved = True
                        self.current_position["stop_loss"] = entry
                        logger.info(f"🔒 Breakeven moved to: ${entry:.2f}")
                        send_telegram_message(f"🔒 Stop Loss → Breakeven\nEntry: ${entry:.2f}")
                
                # Final TP check
                if current_price >= tp:
                    self._close_position(current_price, "TAKE_PROFIT", order_manager, risk_manager)
                    return
            
            # SHORT management
            elif side == "SHORT":
                pnl_pct = ((entry - current_price) / entry) * 100
                
                # Stop loss check
                if current_price >= sl:
                    self._close_position(current_price, "STOP_LOSS", order_manager, risk_manager)
                    return
                
                # TP1 (partial close 50% at 2:1)
                if not self.tp1_hit and current_price <= tp1:
                    self.tp1_hit = True
                    partial_qty = qty * 0.5
                    logger.info(f"🎯 TP1 Hit: Closing 50% at ${current_price:.2f} (+2R)")
                    try:
                        close_result = order_manager.place_market_order(
                            side="BUY",
                            quantity=partial_qty,
                            reduce_only=True
                        )
                        if close_result:
                            logger.info(f"✅ Partial close SUCCESS: {partial_qty:.4f} @ ${current_price:.2f}")
                            send_telegram_message(f"🎯 TP1 Hit! Closed 50%\n💰 Price: ${current_price:.2f}\n📊 P&L: +{pnl_pct:.2f}%")
                            self.current_position["quantity"] = qty - partial_qty
                        else:
                            logger.error(f"❌ Partial close FAILED")
                            self.tp1_hit = False
                    except Exception as e:
                        logger.error(f"Error closing partial: {e}")
                        self.tp1_hit = False
                
                # Breakeven (1:1 RR)
                if self.tp1_hit and not self.breakeven_moved:
                    risk = sl - entry
                    be_price = entry - (risk * 1.0)
                    if current_price <= be_price:
                        self.breakeven_moved = True
                        self.current_position["stop_loss"] = entry
                        logger.info(f"🔒 Breakeven moved to: ${entry:.2f}")
                        send_telegram_message(f"🔒 Stop Loss → Breakeven\nEntry: ${entry:.2f}")
                
                # Final TP check
                if current_price <= tp:
                    self._close_position(current_price, "TAKE_PROFIT", order_manager, risk_manager)
                    return
            
            # Position update logging every 5 minutes
            if current_time - self.position_entry_time >= 300:
                logger.info(f"📍 Position Update: {side} | Entry: ${entry:.2f} | Current: ${current_price:.2f} | P&L: {pnl_pct:+.2f}%")
                self.position_entry_time = current_time
        
        except Exception as e:
            logger.error(f"Error managing position: {e}", exc_info=True)


    def _close_position(self, current_price: float, reason: str, order_manager, risk_manager) -> None:
        """
        Close position with full cleanup
        """
        try:
            if not self.current_position:
                return

            with self._execution_lock:
                side = self.current_position["side"]
                entry = self.current_position["entry_price"]
                qty = self.current_position["quantity"]
                
                # Calculate P&L
                if side == "LONG":
                    pnl_pct = ((current_price - entry) / entry) * 100
                    close_side = "SELL"
                else:
                    pnl_pct = ((entry - current_price) / entry) * 100
                    close_side = "BUY"

                logger.info("=" * 80)
                logger.info(f"🚪 CLOSING POSITION: {reason}")
                logger.info(f" {side} @ ${entry:.2f} → ${current_price:.2f}")
                logger.info(f" P&L: {pnl_pct:+.2f}%")
                logger.info("=" * 80)

                # Close remaining position
                try:
                    close_result = order_manager.place_market_order(
                        side=close_side,
                        quantity=qty,
                        reduce_only=True
                    )
                    
                    if close_result:
                        logger.info(f"✅ Position closed: {qty:.4f} @ ${current_price:.2f}")
                    else:
                        logger.error(f"❌ Failed to close position")
                
                except Exception as e:
                    logger.error(f"Error closing position: {e}")

                # Update stats
                self.total_entries += 1
                self.last_trade_time = time.time()

                if pnl_pct > 0:
                    self.total_wins += 1
                    self.consecutive_losses = 0
                    logger.info(f"✅ WIN | Total: {self.total_wins}/{self.total_entries}")
                else:
                    self.total_losses += 1
                    self.consecutive_losses += 1
                    logger.info(f"❌ LOSS | Consecutive: {self.consecutive_losses}")

                # ✅ FROM Z-SCORE: Record trade closed
                try:
                    risk_manager.record_trade_closed(pnl_pct > 0)
                except Exception as e:
                    logger.error(f"Error recording trade closed: {e}")

                # Telegram notification
                msg = format_exit_signal(
                    side=side,
                    entry_price=entry,
                    exit_price=current_price,
                    pnl_pct=pnl_pct,
                    reason=reason,
                    quantity=qty
                )
                send_telegram_message(msg)

                # Cleanup
                self.current_position = None
                self.tp1_hit = False
                self.breakeven_moved = False

                logger.info("=" * 80)

        except Exception as e:
            logger.error(f"Error in close_position: {e}", exc_info=True)

    # ========================================================================
    # HELPER METHODS FOR BACKWARD COMPATIBILITY
    # ========================================================================

    def get_position(self) -> Optional[Dict]:
        """Get current position"""
        return self.current_position

    def get_htf_bias(self) -> str:
        """Get HTF bias"""
        return self.htf_bias

    def get_amd_phase(self) -> str:
        """Get AMD phase"""
        return self.amd_phase

    def get_session_info(self) -> Dict:
        """Get session info"""
        return {
            "session": self.current_session,
            "in_killzone": self.in_killzone,
            "amd_phase": self.amd_phase
        }

    def get_strategy_stats(self) -> Dict:
        """Get strategy statistics"""
        win_rate = (self.total_wins / (self.total_wins + self.total_losses) * 100) if (self.total_wins + self.total_losses) > 0 else 0
        
        return {
            "total_entries": self.total_entries,
            "total_wins": self.total_wins,
            "total_losses": self.total_losses,
            "win_rate": win_rate,
            "consecutive_losses": self.consecutive_losses,
            "htf_bias": self.htf_bias,
            "amd_phase": self.amd_phase,
            "current_session": self.current_session,
            "in_killzone": self.in_killzone,
            "active_position": self.current_position is not None
        }

    def get_market_structures(self) -> Dict:
        """Get ICT structures for monitoring"""
        return {
            "order_blocks_bull": len(self.order_blocks_bull),
            "order_blocks_bear": len(self.order_blocks_bear),
            "fvgs_bull": len([fvg for fvg in self.fvgs_bull if not fvg.filled]),
            "fvgs_bear": len([fvg for fvg in self.fvgs_bear if not fvg.filled]),
            "liquidity_pools": len(self.liquidity_pools),
            "market_structures": len(self.market_structures),
            "swing_points": len(self.swing_points)
        }

    # Backward compatibility properties
    @property
    def liquidity_zones(self):
        """Alias for liquidity_pools"""
        return list(self.liquidity_pools)

    @property
    def order_blocks(self):
        """Combined order blocks"""
        return {
            "bullish": list(self.order_blocks_bull),
            "bearish": list(self.order_blocks_bear)
        }

    @property
    def fvgs(self):
        """Combined FVGs"""
        return {
            "bullish": list(self.fvgs_bull),
            "bearish": list(self.fvgs_bear)
        }

# ============================================================================
# END OF STRATEGY
# ============================================================================
