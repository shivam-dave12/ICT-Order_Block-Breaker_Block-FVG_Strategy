"""
Advanced ICT + SMC + FVG + OB + Liquidity Strategy — v10 Institutional Engine
===============================================================================

Core ICT/SMC Flow (MANDATORY for every trade):
  1. HTF Bias         → 4H/1D market structure determines directional bias
  2. Dealing Range     → IPDA premium/discount zone alignment
  3. Liquidity Sweep   → EQH/EQL swept with displacement candle
  4. Market Structure   → BOS/CHoCH confirms direction after sweep
  5. Entry Zone        → Price retraces into OB or FVG (OTE 50-79%)
  6. Confluence Score  → Gated cascade L1→L2→L3 must all pass
  7. Entry             → Limit order at OB/FVG optimal zone
  8. SL               → Beyond the triggering structure (single, structure-based)
  9. TP               → Opposing liquidity pool / OB / swing (single, structure-based)
  10. Trailing SL      → Ratchets to new swing points as they form

NO FALLBACKS. NO SYNTHETIC DATA. EVERYTHING FROM LIVE MARKET STRUCTURE.
"""

import time
import logging
import threading
from typing import List, Dict, Optional, Tuple
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone

import config
from telegram_notifier import (
    send_telegram_message, format_market_outlook,
    format_entry_alert, format_trail_update,
    format_position_close, format_rejection_log,
)
from order_manager import GlobalRateLimiter, CancelResult
from regime_engine import (
    RegimeEngine, RegimeSnapshot, NestedDealingRanges,
    REGIME_TRENDING_BULL, REGIME_TRENDING_BEAR,
    REGIME_RANGING, REGIME_VOLATILE_EXPANSION,
    REGIME_DISTRIBUTION, REGIME_ACCUMULATION,
)

logger = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────
PLACEMENT_LOCK_SECONDS    = 60
CASCADE_L2_MIN_TRIGGERS   = 2
CASCADE_L3_MIN_CONFIRMS   = 1

# Virgin OB multipliers
OB_VIRGIN_MULT            = 1.30
OB_ONE_TOUCH_MULT         = 1.00
OB_TWO_TOUCH_MULT         = 0.70
OB_INVALIDATE_TOUCHES     = 3

# ======================================================================
# DATA STRUCTURES
# ======================================================================

@dataclass
class OrderBlock:
    """ICT Order Block — last opposite candle before strong impulse move."""
    low:                float
    high:               float
    timestamp:          int
    direction:          str   = "bullish"     # bullish / bearish
    strength:           float = 0.0           # 0-100 quality score
    has_wick_rejection: bool  = False
    has_displacement:   bool  = False
    bos_confirmed:      bool  = False
    inducement_near:    bool  = False
    broken:             bool  = False
    visit_count:        int   = 0

    @property
    def midpoint(self) -> float:
        return (self.high + self.low) / 2

    @property
    def size(self) -> float:
        return self.high - self.low

    def in_optimal_zone(self, price: float) -> bool:
        """OTE zone: 50-79% Fibonacci retracement of OB range."""
        if self.direction == "bullish":
            zone_low  = self.low + self.size * config.OB_OPTIMAL_ENTRY_MIN
            zone_high = self.low + self.size * config.OB_OPTIMAL_ENTRY_MAX
        else:
            zone_low  = self.high - self.size * config.OB_OPTIMAL_ENTRY_MAX
            zone_high = self.high - self.size * config.OB_OPTIMAL_ENTRY_MIN
        return zone_low <= price <= zone_high

    def contains_price(self, price: float) -> bool:
        return self.low <= price <= self.high

    def is_active(self, current_time: int) -> bool:
        age_min = (current_time - self.timestamp) / 60_000
        return (not self.broken
                and age_min < config.OB_MAX_AGE_MINUTES
                and self.visit_count < OB_INVALIDATE_TOUCHES)

    def virgin_multiplier(self) -> float:
        if self.visit_count == 0:
            return OB_VIRGIN_MULT
        if self.visit_count == 1:
            return OB_ONE_TOUCH_MULT
        return OB_TWO_TOUCH_MULT


@dataclass
class FairValueGap:
    """ICT Fair Value Gap — 3-candle inefficiency zone."""
    bottom:          float
    top:             float
    timestamp:       int
    direction:       str           # bullish / bearish
    filled:          bool = False
    fill_percentage: float = 0.0

    @property
    def midpoint(self) -> float:
        return (self.top + self.bottom) / 2

    @property
    def size(self) -> float:
        return self.top - self.bottom

    def is_price_in_gap(self, price: float) -> bool:
        return self.bottom <= price <= self.top

    def update_fill(self, candles: List[Dict]) -> None:
        """
        Track how much of the FVG has been filled by price returning into it.
        Uses ALL candles provided (not just recent), looking for touches after creation.
        Filled = price traded through the entire gap (100% fill = invalidated).
        """
        if not candles or self.size <= 0:
            return

        # Only check candles AFTER the FVG was formed (no historical pre-fill)
        post_fvg = [c for c in candles if int(c.get('t', 0)) > self.timestamp]
        if not post_fvg:
            return

        max_fill = self.fill_percentage  # Don't regress fill percentage

        for c in post_fvg:
            if self.direction == "bullish":
                # Bullish FVG: bottom = c1_h, top = c3_l
                # Fill = price returns DOWN into the gap (candle low drops into gap)
                if float(c['l']) <= self.top:
                    penetration = (self.top - float(c['l'])) / self.size
                    max_fill = max(max_fill, min(penetration, 1.0))
            else:
                # Bearish FVG: bottom = c3_h, top = c1_l
                # Fill = price returns UP into the gap (candle high enters gap)
                if float(c['h']) >= self.bottom:
                    penetration = (float(c['h']) - self.bottom) / self.size
                    max_fill = max(max_fill, min(penetration, 1.0))

        self.fill_percentage = max_fill
        if self.fill_percentage >= config.FVG_FILL_INVALIDATION:
            self.filled = True

    def is_active(self, current_time: int) -> bool:
        age_min = (current_time - self.timestamp) / 60_000
        return not self.filled and age_min < config.FVG_MAX_AGE_MINUTES


@dataclass
class LiquidityPool:
    """SMC Liquidity Pool — EQH / EQL cluster."""
    price:                  float
    pool_type:              str      # "EQH" or "EQL"
    timestamp:              int
    touch_count:            int
    swept:                  bool  = False
    sweep_timestamp:        int   = 0
    wick_rejection:         bool  = False
    displacement_confirmed: bool  = False

    def distance_pct(self, current_price: float) -> float:
        return abs(current_price - self.price) / current_price * 100


@dataclass
class SwingPoint:
    price:      float
    swing_type: str       # "high" or "low"
    timestamp:  int
    confirmed:  bool = False
    timeframe:  str  = "5m"


@dataclass
class MarketStructure:
    """BOS (Break of Structure) or CHoCH (Change of Character)."""
    structure_type:     str       # "BOS" or "CHoCH"
    price:              float
    timestamp:          int
    direction:          str       # "bullish" or "bearish"
    timeframe:          str  = "5m"
    confirmed:          bool = False


@dataclass
class TriggerContext:
    """Bundles the exact trigger structure for coherent SL/TP derivation."""
    trigger_ob:           Optional[OrderBlock]    = None
    trigger_fvg:          Optional[FairValueGap]  = None
    nearest_swing_low:    Optional[float]         = None
    nearest_swing_high:   Optional[float]         = None
    sweep_pool:           Optional[LiquidityPool] = None
    mss_event:            Optional[MarketStructure] = None


# ======================================================================
# VOLUME PROFILE
# ======================================================================

class VolumeProfileAnalyzer:
    """CVD and basic volume profile from candle data."""

    def __init__(self, history_size: int = 1000):
        self.cvd_history: deque = deque(maxlen=history_size)
        self._lock = threading.Lock()

    def on_candle(self, candle: Dict) -> None:
        with self._lock:
            try:
                o = float(candle['o'])
                c = float(candle['c'])
                v = float(candle.get('v', 0))
                ts = candle.get('t', int(time.time() * 1000))
                if v <= 0:
                    return
                body_ratio = (c - o) / max(abs(c - o) + 1e-9, 1e-9)
                buy_vol  = v * (0.5 + 0.5 * body_ratio)
                sell_vol = v - buy_vol
                self.cvd_history.append({
                    'delta': buy_vol - sell_vol,
                    'is_buy': c >= o,
                    'ts': ts,
                })
            except Exception:
                pass

    def get_cvd_signal(self, lookback: int = 100) -> Dict:
        with self._lock:
            if len(self.cvd_history) < 10:
                return {"signal": "NEUTRAL", "cvd_total": 0.0, "cvd_slope": 0.0}
            recent = list(self.cvd_history)[-min(lookback, len(self.cvd_history)):]
            deltas = [t['delta'] for t in recent]
            total  = sum(deltas)
            mid    = len(deltas) // 2
            slope  = sum(deltas[mid:]) - sum(deltas[:mid])
            vol_sum = sum(abs(d) for d in deltas) or 1.0
            pct = total / vol_sum

            if   pct >  0.30 and slope > 0: signal = "STRONG_BULL"
            elif pct >  0.10:                signal = "BULL"
            elif pct < -0.30 and slope < 0: signal = "STRONG_BEAR"
            elif pct < -0.10:                signal = "BEAR"
            else:                            signal = "NEUTRAL"

            return {"signal": signal, "cvd_total": total, "cvd_slope": slope}


# ======================================================================
# ABSORPTION MODEL
# ======================================================================

class AbsorptionModel:
    """Detects institutional absorption (high volume, tiny body) at key levels."""

    def __init__(self):
        self._events: deque = deque(maxlen=200)

    def on_candle(self, candle: Dict) -> None:
        try:
            h, l = float(candle['h']), float(candle['l'])
            o, c = float(candle['o']), float(candle['c'])
            v    = float(candle.get('v', 0))
            ts   = candle.get('t', int(time.time() * 1000))
            rng  = h - l
            if v <= 0 or rng <= 0:
                return
            body_ratio = abs(c - o) / rng
            if body_ratio < 0.25:
                self._events.append({
                    'price': (h + l) / 2,
                    'volume': v,
                    'direction': "bull" if c >= o else "bear",
                    'ts': ts,
                })
        except Exception:
            pass

    def absorption_near_price(self, price: float, side: str,
                              lookback_ms: int = 3_600_000) -> float:
        now = int(time.time() * 1000)
        tol = price * 0.003
        relevant = [ev for ev in self._events
                    if (now - ev['ts']) <= lookback_ms
                    and abs(ev['price'] - price) <= tol]
        if not relevant:
            return 0.0
        aligned = sum(1 for ev in relevant
                      if (side == "long" and ev['direction'] == "bull")
                      or (side == "short" and ev['direction'] == "bear"))
        return 12.0 if aligned >= 2 else (6.0 if aligned == 1 else 0.0)


# ======================================================================
# ADVANCED ICT STRATEGY
# ======================================================================

class AdvancedICTStrategy:
    """
    v10 Institutional Engine — Pure ICT/SMC.
    Single TP, single SL, structure-based trailing.
    """

    _STRUCTURE_UPDATE_MS = getattr(config, "STRUCTURE_UPDATE_INTERVAL_SECONDS", 30) * 1000
    _ENTRY_EVAL_MS       = getattr(config, "ENTRY_EVALUATION_INTERVAL_SECONDS", 5) * 1000

    def __init__(self, order_manager):
        self._order_manager = order_manager
        self._risk_manager  = None

        # Regime + IPDA
        self.regime_engine = RegimeEngine()
        self._ndr          = NestedDealingRanges()

        # Volume / absorption
        self.volume_analyzer = VolumeProfileAnalyzer()
        self.absorption_model = AbsorptionModel()

        # HTF Bias
        self.htf_bias             = "NEUTRAL"
        self.htf_bias_strength    = 0.0
        self.htf_bias_components: Dict = {}
        self.daily_bias           = "NEUTRAL"

        # Market structures (deques)
        self.order_blocks_bull: deque = deque(maxlen=config.MAX_ORDER_BLOCKS)
        self.order_blocks_bear: deque = deque(maxlen=config.MAX_ORDER_BLOCKS)
        self.fvgs_bull:         deque = deque(maxlen=config.MAX_FVGS)
        self.fvgs_bear:         deque = deque(maxlen=config.MAX_FVGS)
        self.liquidity_pools:   deque = deque(maxlen=config.MAX_LIQUIDITY_ZONES)
        self.swing_highs:       deque = deque(maxlen=300)
        self.swing_lows:        deque = deque(maxlen=300)
        self.market_structures: deque = deque(maxlen=150)

        # Dedup
        self._registered_sweeps: set = set()

        # Session / AMD
        self.current_session = "REGULAR"
        self.in_killzone     = False
        self.amd_phase       = "UNKNOWN"

        # Position state (single position, single TP/SL)
        self.state                  = "READY"
        self.active_position:  Optional[Dict]            = None
        self.entry_order_id:   Optional[str]             = None
        self.sl_order_id:      Optional[str]             = None
        self.tp_order_id:      Optional[str]             = None
        self._pending_ctx:     Optional[TriggerContext]   = None

        self.initial_entry_price:   Optional[float] = None
        self.initial_sl_price:      Optional[float] = None
        self.initial_tp_price:      Optional[float] = None
        self.current_sl_price:      Optional[float] = None
        self.current_tp_price:      Optional[float] = None
        self.highest_price_reached: Optional[float] = None
        self.lowest_price_reached:  Optional[float] = None
        self.breakeven_moved        = False
        self._trail_activated      : bool          = False
        self.profit_locked_pct      = 0.0
        self.entry_pending_start:   Optional[int]   = None

        # Stats
        self.consecutive_losses     = 0
        self.total_entries          = 0
        self.total_exits            = 0
        self.winning_trades         = 0
        self.total_pnl              = 0.0
        self.daily_pnl              = 0.0

        # Trade tracking (for detailed close reports)
        self.entry_score            = 0.0
        self.entry_reasons: List[str] = []
        self.entry_quantity         = 0.0
        self.max_favorable_excursion = 0.0
        self.max_adverse_excursion   = 0.0

        # Timing
        self._last_structure_update_ms = 0
        self._last_entry_eval_ms       = 0
        self._last_outlook_ms          = 0
        self._placement_locked_until   = 0
        self._last_sl_update_time      = 0
        self._last_sl_health_check     = 0
        self._last_pos_check_time      = 0      # throttle _check_position_closed
        _POS_CHECK_INTERVAL_SEC        = 5.0    # check every 5s, not every 250ms tick

        # Outlook interval (5 minutes)
        self._OUTLOOK_INTERVAL_MS = getattr(config, "OUTLOOK_INTERVAL_SECONDS", 300) * 1000

        # ── Spam suppression for repeated identical rejections ────────────
        self._last_rejection_key: Dict[str, str] = {}   # side → "reason hash"
        self._last_rejection_time: Dict[str, float] = {}  # side → timestamp
        self._REJECTION_LOG_COOLDOWN_MS = 60_000  # only re-log same rejection once per minute

        # ── Displacement / retracement tracking ───────────────────────────
        self._displacement_detected     = False
        self._displacement_direction    = ""     # "bearish" or "bullish"
        self._displacement_low: Optional[float]  = None
        self._displacement_high: Optional[float] = None
        self._displacement_bos_price: Optional[float] = None
        self._displacement_timestamp    = 0
        self._DISPLACEMENT_MIN_PCT      = 2.0    # min 2% move to count as displacement
        self._DISPLACEMENT_MAX_AGE_MS   = 4 * 3_600_000  # 4 hours relevance window
        self._RETRACEMENT_FVG_TP        = True   # use opposing FVGs as retracement TP

        self._initialized = False
        self._trade_side: Optional[str] = None

        # ── TP Guardian ─────────────────────────────────────────────────────
        # Background thread that keeps retrying TP placement until it succeeds
        # or the position closes.  Ensures we NEVER stay in a position without
        # a TP order on the exchange, even after repeated API failures.
        self._tp_guardian_active: bool = False
        self._tp_guardian_stop:   threading.Event = threading.Event()
        self._tp_guardian_thread: Optional[threading.Thread] = None

        logger.info("✅ AdvancedICTStrategy v10 created (single TP/SL, structure trailing)")

    # ==================================================================
    # PUBLIC ACCESSORS
    # ==================================================================

    def get_position(self) -> Optional[Dict]:
        return self.active_position if self.state == "POSITION_ACTIVE" else None

    # ==================================================================
    # ON TICK — main entry point (called every 250ms)
    # ==================================================================

    def on_tick(self, data_manager, order_manager, risk_manager,
                current_time: int) -> None:
        try:
            if self._risk_manager is None:
                self._risk_manager = risk_manager

            if not data_manager.is_ready:
                return

            current_price = data_manager.get_last_price()
            if not current_price or current_price <= 0:
                return

            if not self._initialized:
                self._run_initialization(data_manager, current_time)
                return

            if self.state == "READY" and current_time < self._placement_locked_until:
                return

            self._update_session_and_killzone(current_time)

            # Structure rebuild on interval
            if (current_time - self._last_structure_update_ms) >= self._STRUCTURE_UPDATE_MS:
                self._update_all_structures(data_manager, current_price, current_time)
                self._last_structure_update_ms = current_time
                # Log what the bot sees and plans
                self._log_market_outlook(data_manager, current_price, current_time)

            # State machine
            if self.state == "ENTRY_PENDING":
                self._handle_entry_pending(order_manager, risk_manager, current_time)

            elif self.state == "POSITION_ACTIVE":
                self._manage_active_position(data_manager, order_manager, current_price, current_time)

            elif self.state == "READY":
                if (current_time - self._last_entry_eval_ms) >= self._ENTRY_EVAL_MS:
                    self._evaluate_entry(data_manager, order_manager, risk_manager, current_time)
                    self._last_entry_eval_ms = current_time

        except Exception as e:
            logger.error(f"❌ on_tick error: {e}", exc_info=True)

    # ==================================================================
    # INITIALIZATION
    # ==================================================================

    def _run_initialization(self, data_manager, current_time: int) -> None:
        try:
            logger.info("🔧 Strategy v10 initialization starting...")
            current_price = data_manager.get_last_price()
            if not current_price:
                return

            c5m  = data_manager.get_candles("5m")  or []
            c15m = data_manager.get_candles("15m") or []
            c1h  = data_manager.get_candles("1h")  or []
            c4h  = data_manager.get_candles("4h")  or []
            c1d  = data_manager.get_candles("1d")  or []

            if len(c5m) < 30:
                logger.warning("⏳ Insufficient 5m candles — waiting...")
                return

            # Detect structures on all timeframes
            self._detect_swing_points(c5m, current_price, "5m")
            if c15m:
                self._detect_swing_points(c15m, current_price, "15m")
            if c1h:
                self._detect_swing_points(c1h, current_price, "1h")
            if c4h:
                self._detect_swing_points(c4h, current_price, "4h")

            self._detect_market_structure(c5m, current_price, current_time, "5m")
            if len(c15m) >= 10:
                self._detect_market_structure(c15m, current_price, current_time, "15m")
            if len(c1h) >= 10:
                self._detect_market_structure(c1h, current_price, current_time, "1h")
            if len(c4h) >= 10:
                self._detect_market_structure(c4h, current_price, current_time, "4h")

            self._detect_order_blocks(c5m, current_time, current_price, "5m")
            if len(c15m) >= 5:
                self._detect_order_blocks(c15m, current_time, current_price, "15m")

            self._detect_fvgs(c5m, current_time, current_price, "5m")
            if len(c15m) >= 5:
                self._detect_fvgs(c15m, current_time, current_price, "15m")

            self._detect_liquidity_pools(current_price, current_time)

            # HTF bias
            self._update_htf_bias(c4h, c1d, current_price)
            self._update_daily_bias(c5m, current_price)

            # Dealing ranges
            self._update_dealing_ranges(c5m, c1h, c4h, c1d, current_price, current_time)

            # Regime
            if len(c4h) >= 30:
                self.regime_engine.update(c4h)
            elif len(c1h) >= 30:
                self.regime_engine.update(c1h)

            # Feed volume analyzer
            for c in c5m[-100:]:
                self.volume_analyzer.on_candle(c)
                self.absorption_model.on_candle(c)

            self._initialized = True
            logger.info(
                f"✅ Strategy v10 initialized: "
                f"HTF={self.htf_bias} | Regime={self.regime_engine.state.regime} | "
                f"OBs={len(self.order_blocks_bull)}B/{len(self.order_blocks_bear)}S | "
                f"FVGs={len(self.fvgs_bull)}B/{len(self.fvgs_bear)}S | "
                f"Liq={len(self.liquidity_pools)} | "
                f"Swings={len(self.swing_highs)}H/{len(self.swing_lows)}L | "
                f"MSS={len(self.market_structures)}")

            # Build init message with actual price levels
            init_lines = [
                f"✅ <b>Strategy v10 Initialized</b>",
                f"HTF: <b>{self.htf_bias}</b> ({self.htf_bias_strength:.0%})",
                f"Regime: {self.regime_engine.state.regime} (ADX {self.regime_engine.state.adx:.1f})",
                f"",
                f"📦 Structures:",
                f"  OBs: {len(self.order_blocks_bull)}B / {len(self.order_blocks_bear)}S",
                f"  FVGs: {len(self.fvgs_bull)}B / {len(self.fvgs_bear)}S",
                f"  Liquidity: {len(self.liquidity_pools)} pools",
                f"  Swings: {len(self.swing_highs)}H / {len(self.swing_lows)}L",
                f"  MSS: {len(self.market_structures)}",
            ]
            # Include top OBs
            if self.order_blocks_bull:
                nearest_bull = sorted(self.order_blocks_bull,
                                       key=lambda o: abs(current_price - o.midpoint))[:2]
                for ob in nearest_bull:
                    init_lines.append(f"  🟢 OB ${ob.low:,.1f}–${ob.high:,.1f} str={ob.strength:.0f}")
            if self.order_blocks_bear:
                nearest_bear = sorted(self.order_blocks_bear,
                                       key=lambda o: abs(current_price - o.midpoint))[:2]
                for ob in nearest_bear:
                    init_lines.append(f"  🔴 OB ${ob.low:,.1f}–${ob.high:,.1f} str={ob.strength:.0f}")
            # DR levels
            if self._ndr.weekly:
                w = self._ndr.weekly
                init_lines.append(f"\n📏 DR Weekly: ${w.low:,.1f}–${w.high:,.1f}")
            if self._ndr.daily:
                d = self._ndr.daily
                init_lines.append(f"📏 DR Daily: ${d.low:,.1f}–${d.high:,.1f}")

            send_telegram_message("\n".join(init_lines))

            # ── STARTUP RECONCILIATION ────────────────────────────────
            # Check if there's an existing position on the exchange
            # (e.g. from a partial fill that survived a restart,
            #  or from a previous session that wasn't cleanly closed).
            try:
                pos = self._order_manager.get_open_position()
                if pos and pos.get("side") and pos.get("size", 0) > 0:
                    logger.warning(
                        f"⚠️ STARTUP: Existing {pos['side']} position detected "
                        f"on exchange: size={pos['size']} "
                        f"entry=${pos.get('entry_price', 0):,.2f}")
                    send_telegram_message(
                        f"⚠️ <b>STARTUP RECONCILIATION</b>\n"
                        f"Existing {pos['side']} position found:\n"
                        f"  Size: {pos['size']} BTC\n"
                        f"  Entry: ${pos.get('entry_price', 0):,.2f}\n"
                        f"  uPnL: ${pos.get('unrealized_pnl', 0):+.2f}\n\n"
                        f"⚠️ Bot will monitor but NOT manage this position.\n"
                        f"Verify SL/TP orders are in place on exchange."
                    )
            except Exception as e:
                logger.warning(f"Startup reconciliation check failed: {e}")

        except Exception as e:
            logger.error(f"❌ Initialization error: {e}", exc_info=True)

    # ==================================================================
    # STRUCTURE UPDATES
    # ==================================================================

    def _update_all_structures(self, data_manager, current_price: float,
                                current_time: int) -> None:
        try:
            c5m  = data_manager.get_candles("5m")  or []
            c15m = data_manager.get_candles("15m") or []
            c1h  = data_manager.get_candles("1h")  or []
            c4h  = data_manager.get_candles("4h")  or []
            c1d  = data_manager.get_candles("1d")  or []

            if not c5m:
                return

            # Feed analytics
            if c5m:
                self.volume_analyzer.on_candle(c5m[-1])
                self.absorption_model.on_candle(c5m[-1])

            # Swing points
            self._detect_swing_points(c5m, current_price, "5m")
            if len(c15m) >= 8:
                self._detect_swing_points(c15m, current_price, "15m")
            if len(c1h) >= 8:
                self._detect_swing_points(c1h, current_price, "1h")

            # Market structure
            self._detect_market_structure(c5m, current_price, current_time, "5m")
            if len(c15m) >= 10:
                self._detect_market_structure(c15m, current_price, current_time, "15m")

            # Order blocks
            self._detect_order_blocks(c5m, current_time, current_price, "5m")
            if len(c15m) >= 5:
                self._detect_order_blocks(c15m, current_time, current_price, "15m")

            # FVGs
            self._detect_fvgs(c5m, current_time, current_price, "5m")
            self._update_fvg_fills(c5m)

            # Liquidity
            self._detect_liquidity_pools(current_price, current_time)
            self._detect_liquidity_sweeps(c5m, c15m, current_price, current_time)

            # Update OB visit counts
            self._update_ob_visits(current_price, current_time)

            # HTF bias (less frequent)
            self._update_htf_bias(c4h, c1d, current_price)
            self._update_daily_bias(c5m, current_price)

            # Dealing ranges
            self._update_dealing_ranges(c5m, c1h, c4h, c1d, current_price, current_time)

            # Regime
            if len(c4h) >= 30:
                self.regime_engine.update(c4h)

            # Displacement / retracement detection (ICT post-BOS logic)
            self._detect_displacement(current_price, current_time)

            # Cleanup stale structures
            self._cleanup_structures(current_price, current_time)

        except Exception as e:
            logger.error(f"❌ Structure update error: {e}", exc_info=True)

    # ==================================================================
    # MARKET OUTLOOK — "Thinking" Log + Telegram
    # ==================================================================

    def _log_market_outlook(self, data_manager, current_price: float,
                             current_time: int) -> None:
        """
        Log the bot's complete market read and trade plan.
        Called on interval — logs to console always, Telegram on outlook interval.
        """
        try:
            now_ms = current_time

            # ── Console log: concise structure summary ──────────
            active_bull_obs = [o for o in self.order_blocks_bull if o.is_active(now_ms)]
            active_bear_obs = [o for o in self.order_blocks_bear if o.is_active(now_ms)]
            active_bull_fvgs = [f for f in self.fvgs_bull if f.is_active(now_ms)]
            active_bear_fvgs = [f for f in self.fvgs_bear if f.is_active(now_ms)]
            active_pools = [p for p in self.liquidity_pools if not p.swept]
            swept_pools = [p for p in self.liquidity_pools if p.swept]

            logger.info("=" * 80)
            logger.info(f"🧠 MARKET OUTLOOK @ ${current_price:,.1f}")
            logger.info(f"   HTF={self.htf_bias} ({self.htf_bias_strength:.0%}) | "
                        f"Daily={self.daily_bias} | "
                        f"Regime={self.regime_engine.state.regime} (ADX {self.regime_engine.state.adx:.1f})")
            logger.info(f"   Session={self.current_session} | "
                        f"Killzone={'YES' if self.in_killzone else 'no'} | "
                        f"AMD={self.amd_phase}")

            # DR levels
            if self._ndr.weekly:
                w = self._ndr.weekly
                logger.info(f"   DR Weekly:  ${w.low:,.1f} — EQ ${(w.low+w.high)/2:,.1f} — ${w.high:,.1f}")
            if self._ndr.daily:
                d = self._ndr.daily
                logger.info(f"   DR Daily:   ${d.low:,.1f} — EQ ${(d.low+d.high)/2:,.1f} — ${d.high:,.1f}")
            if self._ndr.intraday:
                i = self._ndr.intraday
                logger.info(f"   DR Intra:   ${i.low:,.1f} — EQ ${(i.low+i.high)/2:,.1f} — ${i.high:,.1f}")

            # DR zone classification
            dr_zone_tag = self._get_dr_zone_tag(current_price)
            logger.info(f"   Price zone: {dr_zone_tag}")

            # Key structure levels
            logger.info(f"   OBs: {len(active_bull_obs)}B / {len(active_bear_obs)}S active")
            for ob in sorted(active_bull_obs, key=lambda o: abs(current_price - o.midpoint))[:3]:
                dist = abs(current_price - ob.midpoint) / current_price * 100
                in_tag = "** IN OB **" if ob.contains_price(current_price) else ""
                ote_tag = "OTE✓" if ob.in_optimal_zone(current_price) else ""
                logger.info(f"     🟢 OB ${ob.low:,.1f}–${ob.high:,.1f} str={ob.strength:.0f} "
                            f"v={ob.visit_count} dist={dist:.2f}% {in_tag} {ote_tag}")
            for ob in sorted(active_bear_obs, key=lambda o: abs(current_price - o.midpoint))[:3]:
                dist = abs(current_price - ob.midpoint) / current_price * 100
                in_tag = "** IN OB **" if ob.contains_price(current_price) else ""
                ote_tag = "OTE✓" if ob.in_optimal_zone(current_price) else ""
                logger.info(f"     🔴 OB ${ob.low:,.1f}–${ob.high:,.1f} str={ob.strength:.0f} "
                            f"v={ob.visit_count} dist={dist:.2f}% {in_tag} {ote_tag}")

            # FVGs
            logger.info(f"   FVGs: {len(active_bull_fvgs)}B / {len(active_bear_fvgs)}S active")
            for fvg in sorted(active_bull_fvgs, key=lambda f: abs(current_price - f.midpoint))[:2]:
                in_tag = "** IN FVG **" if fvg.is_price_in_gap(current_price) else ""
                logger.info(f"     🟢 FVG ${fvg.bottom:,.1f}–${fvg.top:,.1f} fill={fvg.fill_percentage*100:.0f}% {in_tag}")
            for fvg in sorted(active_bear_fvgs, key=lambda f: abs(current_price - f.midpoint))[:2]:
                in_tag = "** IN FVG **" if fvg.is_price_in_gap(current_price) else ""
                logger.info(f"     🔴 FVG ${fvg.bottom:,.1f}–${fvg.top:,.1f} fill={fvg.fill_percentage*100:.0f}% {in_tag}")

            # Liquidity pools
            eqh = [p for p in active_pools if p.pool_type == "EQH"]
            eql = [p for p in active_pools if p.pool_type == "EQL"]
            logger.info(f"   Liquidity: {len(eqh)} EQH / {len(eql)} EQL active, {len(swept_pools)} swept")
            for p in sorted(eqh, key=lambda x: x.price)[:2]:
                dist = abs(current_price - p.price) / current_price * 100
                logger.info(f"     🔺 EQH @ ${p.price:,.1f} x{p.touch_count} ({dist:.2f}% away)")
            for p in sorted(eql, key=lambda x: x.price, reverse=True)[:2]:
                dist = abs(current_price - p.price) / current_price * 100
                logger.info(f"     🔻 EQL @ ${p.price:,.1f} x{p.touch_count} ({dist:.2f}% away)")
            for p in swept_pools[-2:]:
                disp = "+DISP" if p.displacement_confirmed else ""
                logger.info(f"     ✅ SWEPT {p.pool_type} @ ${p.price:,.1f} {disp}")

            # Nearest swings
            highs_above = sorted([s for s in self.swing_highs if s.price > current_price],
                                 key=lambda s: s.price)[:3]
            lows_below = sorted([s for s in self.swing_lows if s.price < current_price],
                                key=lambda s: s.price, reverse=True)[:3]
            if highs_above:
                h_str = " | ".join(f"${s.price:,.1f}[{s.timeframe}]" for s in highs_above)
                logger.info(f"   Highs above: {h_str}")
            if lows_below:
                l_str = " | ".join(f"${s.price:,.1f}[{s.timeframe}]" for s in lows_below)
                logger.info(f"   Lows below:  {l_str}")

            # Recent MSS
            recent_ms = list(self.market_structures)[-3:]
            for ms in reversed(recent_ms):
                icon = "📈" if ms.direction == "bullish" else "📉"
                elapsed = (now_ms - ms.timestamp) / 60_000
                logger.info(f"   {icon} {ms.structure_type} {ms.direction} [{ms.timeframe}] @ ${ms.price:,.1f} ({elapsed:.0f}m ago)")

            # Displacement / retracement status
            if self._displacement_detected:
                retrace_dir = "LONG" if self._displacement_direction == "bearish" else "SHORT"
                logger.info(f"   📐 DISPLACEMENT: {self._displacement_direction.upper()} "
                            f"(BOS @ ${self._displacement_bos_price:,.0f}) → "
                            f"expecting retracement {retrace_dir} into FVGs")

            # ── Build trade plans for both sides ────────────────
            long_plan = self._build_trade_plan("long", current_price, data_manager, now_ms)
            short_plan = self._build_trade_plan("short", current_price, data_manager, now_ms)

            # Log trade plans to console
            logger.info("─" * 40)
            self._log_trade_plan("LONG", long_plan, current_price)
            self._log_trade_plan("SHORT", short_plan, current_price)
            logger.info("=" * 80)

            # ── Send to Telegram on outlook interval ────────────
            if (now_ms - self._last_outlook_ms) >= self._OUTLOOK_INTERVAL_MS:
                self._last_outlook_ms = now_ms

                msg = format_market_outlook(
                    current_price=current_price,
                    htf_bias=self.htf_bias,
                    htf_bias_strength=self.htf_bias_strength,
                    htf_components=self.htf_bias_components,
                    daily_bias=self.daily_bias,
                    regime=self.regime_engine.state.regime,
                    regime_adx=self.regime_engine.state.adx,
                    session=self.current_session,
                    in_killzone=self.in_killzone,
                    amd_phase=self.amd_phase,
                    dr_weekly=self._ndr.weekly,
                    dr_daily=self._ndr.daily,
                    dr_intraday=self._ndr.intraday,
                    dr_zone_tag=dr_zone_tag,
                    bullish_obs=list(self.order_blocks_bull),
                    bearish_obs=list(self.order_blocks_bear),
                    bullish_fvgs=list(self.fvgs_bull),
                    bearish_fvgs=list(self.fvgs_bear),
                    liquidity_pools=list(self.liquidity_pools),
                    market_structures=list(self.market_structures),
                    swing_highs=list(self.swing_highs),
                    swing_lows=list(self.swing_lows),
                    long_plan=long_plan,
                    short_plan=short_plan,
                )
                send_telegram_message(msg)

        except Exception as e:
            logger.error(f"❌ Market outlook error: {e}", exc_info=True)

    def _get_dr_zone_tag(self, price: float) -> str:
        """Classify current price within dealing ranges."""
        tags = []
        dr = self._ndr.best_dr()
        if dr is not None:
            zone = dr.zone_pct(price)
            if dr.is_premium(price):
                tags.append(f"PREMIUM ({zone*100:.0f}%)")
            elif dr.is_discount(price):
                tags.append(f"DISCOUNT ({zone*100:.0f}%)")
            else:
                tags.append(f"EQUILIBRIUM ({zone*100:.0f}%)")
        else:
            tags.append("No DR")
        return " / ".join(tags)

    def _build_trade_plan(self, side: str, current_price: float,
                          data_manager, now_ms: int) -> Dict:
        """
        Dry-run the full entry evaluation for one side.
        Returns a dict describing the trade plan or why it's blocked.
        Does NOT place any orders.
        """
        plan: Dict = {"side": side, "status": "EVALUATING"}

        try:
            # L1 Gate
            l1_pass, l1_reason = self._cascade_l1(side, current_price, now_ms)
            if not l1_pass:
                plan["status"] = "BLOCKED_L1"
                plan["gate_failed"] = f"L1: {l1_reason}"
                return plan

            # Score confluence
            score, reasons, ctx = self._score_confluence(side, current_price, data_manager, now_ms)
            plan["score"] = score
            plan["reasons"] = reasons

            # L2 Gate — dynamic threshold for high-conviction
            l2_count, l2_labels = self._cascade_l2(side, current_price, ctx, now_ms)
            l2_needed = CASCADE_L2_MIN_TRIGGERS  # default 2
            high_conviction = (
                (self.in_killzone and score >= 70) or
                (self.htf_bias_strength >= 0.80 and any("MSS" in l for l in l2_labels))
            )
            if high_conviction:
                l2_needed = 1
            if l2_count < l2_needed:
                plan["status"] = "BLOCKED_L2"
                missing_l2 = []
                if not any("SWEEP" in l for l in l2_labels):
                    missing_l2.append("sweep+displacement")
                if not any("OB" in l or "FVG" in l for l in l2_labels):
                    missing_l2.append("OB/FVG touch")
                if not any("MSS" in l for l in l2_labels):
                    missing_l2.append("MSS confirmation")
                plan["gate_failed"] = f"L2: {l2_count}/{l2_needed} (have: {', '.join(l2_labels) if l2_labels else 'none'})"
                plan["missing"] = ", ".join(missing_l2)
                return plan

            # L3 Gate
            l3_count, l3_labels = self._cascade_l3(side, current_price, ctx, score, now_ms)
            if l3_count < CASCADE_L3_MIN_CONFIRMS:
                plan["status"] = "BLOCKED_L3"
                plan["gate_failed"] = f"L3: {l3_count}/{CASCADE_L3_MIN_CONFIRMS} (need CVD/absorption/killzone/AMD/daily_bias)"
                plan["missing"] = "CVD alignment, killzone, absorption, or daily bias"
                return plan

            # Threshold check
            threshold = self._get_entry_threshold()
            plan["threshold"] = threshold
            if score < threshold:
                plan["status"] = f"BELOW_THRESHOLD"
                plan["gate_failed"] = f"Score {score:.0f} / need {threshold:.0f}"
                plan["missing"] = f"need +{threshold - score:.0f} more confluence"
                return plan

            # Calculate levels (dry run)
            entry_price, sl_price, tp_price = self._calculate_levels(
                side, current_price, ctx, current_time=now_ms, quiet=True)
            if entry_price is None:
                plan["status"] = "NO_STRUCTURE"
                plan["gate_failed"] = "Cannot calculate valid SL/TP from structure"
                return plan

            risk = abs(entry_price - sl_price)
            reward = abs(tp_price - entry_price)
            rr = reward / risk if risk > 0 else 0

            plan["status"] = "READY" if rr >= config.MIN_RISK_REWARD_RATIO - 1e-9 else "LOW_RR"
            plan["entry"] = entry_price
            plan["sl"] = sl_price
            plan["tp"] = tp_price
            plan["rr"] = rr

            # SL reasoning
            if ctx.trigger_ob:
                plan["sl_reason"] = f"below OB {ctx.trigger_ob.low:.0f}"
            elif ctx.nearest_swing_low and side == "long":
                plan["sl_reason"] = f"below swing {ctx.nearest_swing_low:.0f}"
            elif ctx.nearest_swing_high and side == "short":
                plan["sl_reason"] = f"above swing {ctx.nearest_swing_high:.0f}"
            else:
                plan["sl_reason"] = "structure-based"

            # TP reasoning
            for pool in self.liquidity_pools:
                if not pool.swept:
                    if side == "long" and pool.pool_type == "EQH" and pool.price > entry_price:
                        plan["tp_reason"] = f"EQH pool @ {pool.price:.0f}"
                        break
                    elif side == "short" and pool.pool_type == "EQL" and pool.price < entry_price:
                        plan["tp_reason"] = f"EQL pool @ {pool.price:.0f}"
                        break
            if "tp_reason" not in plan:
                plan["tp_reason"] = "opposing structure"

            if rr < config.MIN_RISK_REWARD_RATIO - 1e-9:
                plan["gate_failed"] = f"RR {rr:.1f}x (min {config.MIN_RISK_REWARD_RATIO}x)"

            return plan

        except Exception as e:
            plan["status"] = "ERROR"
            plan["gate_failed"] = str(e)
            return plan

    def _log_trade_plan(self, label: str, plan: Dict, price: float) -> None:
        """Log a trade plan to console."""
        status = plan.get("status", "?")
        if status == "READY":
            logger.info(f"   🎯 {label}: READY @ ${plan.get('entry', 0):,.1f} "
                        f"SL=${plan.get('sl', 0):,.1f} TP=${plan.get('tp', 0):,.1f} "
                        f"RR={plan.get('rr', 0):.1f} Score={plan.get('score', 0):.0f}")
        elif "BLOCKED" in status or "BELOW" in status:
            logger.info(f"   ⛔ {label}: {status} — {plan.get('gate_failed', '?')}")
            if plan.get("missing"):
                logger.info(f"     ⏳ Need: {plan['missing']}")
            if plan.get("score"):
                logger.info(f"     Score so far: {plan['score']:.0f} | Reasons: {', '.join(plan.get('reasons', [])[:4])}")
        elif status == "NO_STRUCTURE":
            logger.info(f"   ⛔ {label}: {plan.get('gate_failed', 'no valid structure')}")
        else:
            logger.info(f"   ⛔ {label}: {status} — {plan.get('gate_failed', '?')}")

    # ==================================================================
    # SWING POINT DETECTION
    # ==================================================================

    def _detect_swing_points(self, candles: List[Dict], current_price: float,
                             tf: str = "5m") -> None:
        n = len(candles)
        lb_left  = config.SWING_LOOKBACK_LEFT
        lb_right = config.SWING_LOOKBACK_RIGHT
        if n < lb_left + lb_right + 1:
            return
        dedup_tol = current_price * config.STRUCTURE_MIN_SWING_SIZE_PCT / 100

        for i in range(lb_left, n - lb_right):
            c = candles[i]
            high, low = float(c['h']), float(c['l'])
            ts = int(c.get('t', 0))

            left_highs  = [float(candles[j]['h']) for j in range(i - lb_left, i)]
            right_highs = [float(candles[j]['h']) for j in range(i + 1, i + 1 + lb_right)]
            if all(high > h for h in left_highs) and all(high >= h for h in right_highs):
                if not any(abs(s.price - high) <= dedup_tol and s.swing_type == "high"
                           for s in self.swing_highs):
                    self.swing_highs.append(SwingPoint(
                        price=high, swing_type="high", timestamp=ts,
                        confirmed=True, timeframe=tf))

            left_lows  = [float(candles[j]['l']) for j in range(i - lb_left, i)]
            right_lows = [float(candles[j]['l']) for j in range(i + 1, i + 1 + lb_right)]
            if all(low < l for l in left_lows) and all(low <= l for l in right_lows):
                if not any(abs(s.price - low) <= dedup_tol and s.swing_type == "low"
                           for s in self.swing_lows):
                    self.swing_lows.append(SwingPoint(
                        price=low, swing_type="low", timestamp=ts,
                        confirmed=True, timeframe=tf))

    # ==================================================================
    # MARKET STRUCTURE DETECTION (BOS / CHoCH)
    # ==================================================================

    def _detect_market_structure(self, candles: List[Dict], current_price: float,
                                  current_time: int, tf: str = "5m") -> None:
        """
        BOS:  Price breaks a swing in the SAME direction as the prior swing break.
        CHoCH: Price breaks a swing in the OPPOSITE direction (trend reversal).
        """
        if len(candles) < 10:
            return

        recent_highs = sorted([s for s in self.swing_highs if s.timeframe == tf],
                              key=lambda x: x.timestamp)[-10:]
        recent_lows  = sorted([s for s in self.swing_lows if s.timeframe == tf],
                              key=lambda x: x.timestamp)[-10:]

        dedup_tol = current_price * 0.001
        last_close = float(candles[-1]['c'])
        last_ts    = int(candles[-1].get('t', current_time))

        # Bullish break: close above a swing high
        for sh in reversed(recent_highs):
            if last_close > sh.price:
                # Determine if BOS or CHoCH
                prev_bearish = any(ms.direction == "bearish" and ms.timestamp < sh.timestamp
                                   for ms in list(self.market_structures)[-10:])
                struct_type = "CHoCH" if prev_bearish else "BOS"

                if not any(abs(ms.price - sh.price) <= dedup_tol
                           and ms.direction == "bullish"
                           and ms.timeframe == tf
                           for ms in self.market_structures):
                    self.market_structures.append(MarketStructure(
                        structure_type=struct_type, price=sh.price,
                        timestamp=last_ts, direction="bullish",
                        timeframe=tf, confirmed=True))
                    logger.info(f"📈 {struct_type} bullish [{tf}] @ {sh.price:.2f}")
                break

        # Bearish break: close below a swing low
        for sl in reversed(recent_lows):
            if last_close < sl.price:
                prev_bullish = any(ms.direction == "bullish" and ms.timestamp < sl.timestamp
                                   for ms in list(self.market_structures)[-10:])
                struct_type = "CHoCH" if prev_bullish else "BOS"

                if not any(abs(ms.price - sl.price) <= dedup_tol
                           and ms.direction == "bearish"
                           and ms.timeframe == tf
                           for ms in self.market_structures):
                    self.market_structures.append(MarketStructure(
                        structure_type=struct_type, price=sl.price,
                        timestamp=last_ts, direction="bearish",
                        timeframe=tf, confirmed=True))
                    logger.info(f"📉 {struct_type} bearish [{tf}] @ {sl.price:.2f}")
                break

    # ==================================================================
    # ORDER BLOCK DETECTION
    # ==================================================================

    def _detect_order_blocks(self, candles: List[Dict], current_time: int,
                              current_price: float, tf: str = "5m") -> None:
        """
        OB = Last opposite candle before a strong impulse move.
        Bullish OB: last bearish candle before bullish impulse that broke a swing high.
        Bearish OB: last bullish candle before bearish impulse that broke a swing low.
        """
        if len(candles) < 5:
            return

        min_impulse_pct = config.OB_MIN_IMPULSE_PCT
        tol = current_price * 0.001

        prior_highs = sorted([s.price for s in self.swing_highs if s.timeframe == tf],
                             reverse=True)[:5]
        prior_lows  = sorted([s.price for s in self.swing_lows if s.timeframe == tf])[:5]

        for i in range(2, len(candles) - 1):
            cur = candles[i]
            nxt = candles[i + 1]
            prev = candles[i - 1]

            cur_o, cur_c = float(cur['o']), float(cur['c'])
            cur_h, cur_l = float(cur['h']), float(cur['l'])
            nxt_o, nxt_c = float(nxt['o']), float(nxt['c'])
            nxt_h, nxt_l = float(nxt['h']), float(nxt['l'])
            prev_h, prev_l = float(prev['h']), float(prev['l'])
            cur_ts = int(cur.get('t', current_time))

            nxt_range = nxt_h - nxt_l
            nxt_body  = abs(nxt_c - nxt_o)

            impulse_up   = (nxt_c > nxt_o
                            and (nxt_c - nxt_o) / max(nxt_o, 1) * 100 >= min_impulse_pct
                            and nxt_body / max(nxt_range, 1e-9) >= config.OB_MIN_BODY_RATIO)
            impulse_down = (nxt_c < nxt_o
                            and (nxt_o - nxt_c) / max(nxt_o, 1) * 100 >= min_impulse_pct
                            and nxt_body / max(nxt_range, 1e-9) >= config.OB_MIN_BODY_RATIO)

            # Bullish OB: bearish candle before bullish impulse
            if impulse_up and cur_c < cur_o:
                bos_ok = any(nxt_h > ph for ph in prior_highs[:3]) if prior_highs else False

                # Check displacement: impulse broke prior swing
                has_disp = nxt_range > 0 and nxt_body / nxt_range >= config.SWEEP_DISPLACEMENT_MIN

                # Wick rejection on OB candle (lower wick of bearish candle)
                body_low = min(cur_o, cur_c)
                wick = body_low - cur_l if body_low > cur_l else 0.0
                wick_rej = (cur_h - cur_l) > 0 and wick / (cur_h - cur_l) >= config.OB_WICK_REJECTION_MIN

                # Strength scoring
                strength = 40.0
                if bos_ok:    strength += 20.0
                if has_disp:  strength += 15.0
                if wick_rej:  strength += 10.0
                if nxt_range >= config.OB_IMPULSE_SIZE_MULTIPLIER * (cur_h - cur_l):
                    strength += 15.0
                strength = min(strength, 100.0)

                if not any(abs(ob.low - cur_l) <= tol and abs(ob.high - cur_h) <= tol
                           for ob in self.order_blocks_bull):
                    self.order_blocks_bull.append(OrderBlock(
                        low=cur_l, high=cur_h, timestamp=cur_ts,
                        direction="bullish", strength=strength,
                        has_wick_rejection=wick_rej,
                        has_displacement=has_disp,
                        bos_confirmed=bos_ok))

            # Bearish OB: bullish candle before bearish impulse
            if impulse_down and cur_c > cur_o:
                bos_ok = any(nxt_l < pl for pl in prior_lows[:3]) if prior_lows else False
                has_disp = nxt_range > 0 and nxt_body / nxt_range >= config.SWEEP_DISPLACEMENT_MIN

                # Wick rejection on OB candle (upper wick of bullish candle)
                body_top = max(cur_o, cur_c)
                wick = cur_h - body_top if cur_h > body_top else 0.0
                wick_rej = (cur_h - cur_l) > 0 and wick / (cur_h - cur_l) >= config.OB_WICK_REJECTION_MIN

                strength = 40.0
                if bos_ok:    strength += 20.0
                if has_disp:  strength += 15.0
                if wick_rej:  strength += 10.0
                if nxt_range >= config.OB_IMPULSE_SIZE_MULTIPLIER * (cur_h - cur_l):
                    strength += 15.0
                strength = min(strength, 100.0)

                if not any(abs(ob.low - cur_l) <= tol and abs(ob.high - cur_h) <= tol
                           for ob in self.order_blocks_bear):
                    self.order_blocks_bear.append(OrderBlock(
                        low=cur_l, high=cur_h, timestamp=cur_ts,
                        direction="bearish", strength=strength,
                        has_wick_rejection=wick_rej,
                        has_displacement=has_disp,
                        bos_confirmed=bos_ok))

    # ==================================================================
    # FVG DETECTION
    # ==================================================================

    def _detect_fvgs(self, candles: List[Dict], current_time: int,
                     current_price: float, tf: str = "5m") -> None:
        """
        FVG = Gap between candle 1's low/high and candle 3's high/low.
        Bullish FVG: candle3.low > candle1.high (gap up)
        Bearish FVG: candle1.low > candle3.high (gap down)
        """
        if len(candles) < 3:
            return

        min_gap_size = current_price * config.FVG_MIN_SIZE_PCT / 100
        tol = current_price * 0.0005

        for i in range(len(candles) - 2):
            c1, c2, c3 = candles[i], candles[i + 1], candles[i + 2]
            c1_h, c1_l = float(c1['h']), float(c1['l'])
            c3_h, c3_l = float(c3['h']), float(c3['l'])
            ts = int(c2.get('t', current_time))

            # Bullish FVG
            gap_bottom = c1_h
            gap_top    = c3_l
            if gap_top > gap_bottom and (gap_top - gap_bottom) >= min_gap_size:
                if not any(abs(f.bottom - gap_bottom) <= tol and abs(f.top - gap_top) <= tol
                           for f in self.fvgs_bull):
                    self.fvgs_bull.append(FairValueGap(
                        bottom=gap_bottom, top=gap_top,
                        timestamp=ts, direction="bullish"))

            # Bearish FVG
            gap_bottom = c3_h
            gap_top    = c1_l
            if gap_top > gap_bottom and (gap_top - gap_bottom) >= min_gap_size:
                if not any(abs(f.bottom - gap_bottom) <= tol and abs(f.top - gap_top) <= tol
                           for f in self.fvgs_bear):
                    self.fvgs_bear.append(FairValueGap(
                        bottom=gap_bottom, top=gap_top,
                        timestamp=ts, direction="bearish"))

    def _update_fvg_fills(self, candles: List[Dict]) -> None:
        for fvg in list(self.fvgs_bull) + list(self.fvgs_bear):
            if not fvg.filled:
                fvg.update_fill(candles)

    # ==================================================================
    # LIQUIDITY POOL DETECTION
    # ==================================================================

    def _detect_liquidity_pools(self, current_price: float, current_time: int) -> None:
        """
        ICT Liquidity Pool (EQH/EQL) Detection — from raw candle data.

        Method: scan candle highs/lows for EQUAL HIGHS or EQUAL LOWS within tolerance.
        Equal = two or more wicks/bodies at the same price level (liquidity resting above/below).
        This is the canonical ICT definition — NOT derived from swing point clustering.

        Sources:
        - self.swing_highs / self.swing_lows (already-identified structural swings)
        - Priority: structural swing levels only (no synthetic generation)

        Tolerance: 0.20% of price — institutional equal-high definition.
        At $65K this is $130. Tight enough to be meaningful, wide enough to catch real clusters.
        """
        tolerance = current_price * 0.0020   # 0.20% — institutional equal-high/low def

        # ── EQH: Equal Highs — resting sell-side liquidity above ──────────
        sh_prices = [s.price for s in self.swing_highs if s.timeframe in ("5m", "15m", "1h", "4h")]
        self._cluster_liquidity(sh_prices, "EQH", tolerance, current_price, current_time)

        # ── EQL: Equal Lows — resting buy-side liquidity below ────────────
        sl_prices = [s.price for s in self.swing_lows if s.timeframe in ("5m", "15m", "1h", "4h")]
        self._cluster_liquidity(sl_prices, "EQL", tolerance, current_price, current_time)

    def _cluster_liquidity(self, prices: List[float], pool_type: str,
                            tolerance: float, current_price: float,
                            current_time: int) -> None:
        """
        Cluster price levels within tolerance → form liquidity pool.
        Uses greedy grouping: sort prices, then group adjacent within tolerance.
        """
        if len(prices) < 2:
            return

        sorted_prices = sorted(set(round(p, 1) for p in prices))
        seen_clusters: set = set()

        i = 0
        while i < len(sorted_prices):
            group = [sorted_prices[i]]
            j = i + 1
            while j < len(sorted_prices) and sorted_prices[j] - sorted_prices[i] <= tolerance:
                group.append(sorted_prices[j])
                j += 1

            if len(group) >= 2:  # Equal = 2+ touches
                avg_p = sum(group) / len(group)
                cluster_key = round(avg_p, 0)

                if cluster_key in seen_clusters:
                    i = j
                    continue
                seen_clusters.add(cluster_key)

                dist_pct = abs(current_price - avg_p) / current_price * 100
                if dist_pct > config.LIQ_MAX_DISTANCE_PCT:
                    i = j
                    continue

                # Don't re-add if already tracked
                if not any(abs(lp.price - avg_p) <= tolerance and lp.pool_type == pool_type
                           for lp in self.liquidity_pools):
                    self.liquidity_pools.append(LiquidityPool(
                        price=round(avg_p, 1), pool_type=pool_type,
                        timestamp=current_time, touch_count=len(group)))
                    logger.debug(f"💧 {pool_type} identified @ ${avg_p:,.1f} "
                                 f"({len(group)} touches, {dist_pct:.2f}% away)")

            i = j

    # ==================================================================
    # LIQUIDITY SWEEP DETECTION
    # ==================================================================

    def _detect_liquidity_sweeps(self, candles_5m: List[Dict], candles_15m: List[Dict],
                                  current_price: float, current_time: int) -> None:
        sweep_max_age = config.SWEEP_MAX_AGE_MINUTES * 60_000

        recent_5m  = [c for c in candles_5m[-20:] if (current_time - int(c.get('t', 0))) <= sweep_max_age]
        recent_15m = [c for c in candles_15m[-10:] if (current_time - int(c.get('t', 0))) <= sweep_max_age]
        recent_all = recent_5m + recent_15m

        for pool in list(self.liquidity_pools):
            if pool.swept:
                continue
            for c in recent_all:
                h, l = float(c['h']), float(c['l'])
                cl, op = float(c['c']), float(c['o'])
                body = abs(cl - op)
                rng  = h - l
                dedup_k = (round(pool.price, 0), int(c.get('t', 0)))
                if dedup_k in self._registered_sweeps:
                    continue

                if pool.pool_type == "EQH" and h > pool.price:
                    wick_ok = cl < pool.price
                    disp_ok = rng > 0 and (body / rng) >= config.SWEEP_DISPLACEMENT_MIN
                    if wick_ok and (disp_ok or not config.SWEEP_WICK_REQUIREMENT):
                        pool.swept = True
                        pool.sweep_timestamp = current_time
                        pool.wick_rejection = wick_ok
                        pool.displacement_confirmed = disp_ok
                        self._registered_sweeps.add(dedup_k)
                        logger.info(f"💧 EQH swept @ ${pool.price:.0f} wick={wick_ok} disp={disp_ok}")
                        break

                elif pool.pool_type == "EQL" and l < pool.price:
                    wick_ok = cl > pool.price
                    disp_ok = rng > 0 and (body / rng) >= config.SWEEP_DISPLACEMENT_MIN
                    if wick_ok and (disp_ok or not config.SWEEP_WICK_REQUIREMENT):
                        pool.swept = True
                        pool.sweep_timestamp = current_time
                        pool.wick_rejection = wick_ok
                        pool.displacement_confirmed = disp_ok
                        self._registered_sweeps.add(dedup_k)
                        logger.info(f"💧 EQL swept @ ${pool.price:.0f} wick={wick_ok} disp={disp_ok}")
                        break

    # ==================================================================
    # OB VISIT TRACKING
    # ==================================================================

    def _update_ob_visits(self, current_price: float, current_time: int) -> None:
        """Track price visits to OBs. 
        
        FIX: Only increment visit_count once per 5m candle interval (300s).
        Previously incremented every 30s structure update, killing OBs in 90s.
        """
        visit_cooldown_ms = 300_000  # 5 minutes — one candle period
        for ob in list(self.order_blocks_bull) + list(self.order_blocks_bear):
            if ob.is_active(current_time) and ob.contains_price(current_price):
                last_visit = getattr(ob, '_last_visit_time', 0)
                if current_time - last_visit >= visit_cooldown_ms:
                    ob.visit_count += 1
                    ob._last_visit_time = current_time

    # ==================================================================
    # HTF BIAS (4H/1D market structure)
    # ==================================================================

    def _update_htf_bias(self, c4h: List[Dict], c1d: List[Dict],
                          current_price: float) -> None:
        try:
            if len(c4h) < 20:
                return

            closes = [float(c['c']) for c in c4h[-40:]]
            ema_val = self._calculate_ema(closes, config.HTF_TREND_EMA)

            ema_dist = abs(current_price - ema_val) / ema_val * 100 if ema_val > 0 else 0
            min_dist = config.HTF_EMA_MIN_DISTANCE

            # Component weights
            bull, bear = 0.0, 0.0
            comp = {}

            # 1. EMA position (30%)
            if current_price > ema_val:
                bull += 0.30; comp["ema"] = "BULL"
            else:
                bear += 0.30; comp["ema"] = "BEAR"

            # 2. Recent 4H swing structure (30%)
            recent_ms = [ms for ms in self.market_structures
                         if ms.timeframe in ("4h", "1h")][-5:]
            if recent_ms:
                bull_ms = sum(1 for m in recent_ms if m.direction == "bullish")
                bear_ms = sum(1 for m in recent_ms if m.direction == "bearish")
                if bull_ms > bear_ms:
                    bull += 0.30; comp["ms"] = "BULL"
                elif bear_ms > bull_ms:
                    bear += 0.30; comp["ms"] = "BEAR"
                else:
                    bull += 0.15; bear += 0.15; comp["ms"] = "MIXED"

            # 3. Higher highs / lower lows (20%)
            recent_highs = [float(c['h']) for c in c4h[-10:]]
            recent_lows  = [float(c['l']) for c in c4h[-10:]]
            if len(recent_highs) >= 4:
                hh = recent_highs[-1] > recent_highs[-3]
                hl = recent_lows[-1] > recent_lows[-3]
                ll = recent_lows[-1] < recent_lows[-3]
                lh = recent_highs[-1] < recent_highs[-3]
                if hh and hl:
                    bull += 0.20; comp["swing"] = "HH_HL"
                elif ll and lh:
                    bear += 0.20; comp["swing"] = "LL_LH"
                else:
                    bull += 0.10; bear += 0.10; comp["swing"] = "MIXED"

            # 4. Recent BOS (20%)
            recent_bos = [ms for ms in self.market_structures
                          if ms.structure_type == "BOS"
                          and ms.timeframe in ("4h", "1h")][-3:]
            if recent_bos:
                last = recent_bos[-1]
                if last.direction == "bullish":
                    bull += 0.20; comp["bos"] = "BULL"
                else:
                    bear += 0.20; comp["bos"] = "BEAR"

            total = bull + bear or 1.0
            bull_pct, bear_pct = bull / total, bear / total

            THRESH = 0.60
            if bull_pct >= THRESH and ema_dist >= min_dist:
                self.htf_bias          = "BULLISH"
                self.htf_bias_strength = round(bull_pct, 3)
            elif bear_pct >= THRESH and ema_dist >= min_dist:
                self.htf_bias          = "BEARISH"
                self.htf_bias_strength = round(bear_pct, 3)
            else:
                self.htf_bias          = "NEUTRAL"
                self.htf_bias_strength = round(max(bull_pct, bear_pct), 3)

            self.htf_bias_components = comp

        except Exception as e:
            logger.error(f"❌ HTF bias error: {e}", exc_info=True)

    def _update_daily_bias(self, candles_5m: List[Dict], current_price: float) -> None:
        try:
            if len(candles_5m) < 20:
                return
            closes = [float(c['c']) for c in candles_5m[-20:]]
            ema_fast = self._calculate_ema(closes, 8)
            ema_slow = self._calculate_ema(closes, 21)
            if ema_fast > ema_slow:
                self.daily_bias = "BULLISH"
            elif ema_fast < ema_slow:
                self.daily_bias = "BEARISH"
            else:
                self.daily_bias = "NEUTRAL"
        except Exception as e:
            logger.error(f"❌ Daily bias error: {e}", exc_info=True)

    # ==================================================================
    # DEALING RANGES
    # ==================================================================

    def _update_dealing_ranges(self, c5m, c1h, c4h, c1d,
                                current_price, current_time) -> None:
        try:
            if c1d and len(c1d) >= 4:
                self._ndr.update_weekly(c1d, current_time)
            if len(c4h) >= 4:
                self._ndr.update_daily(c4h, current_time)
            if len(c1h) >= 4:
                self._ndr.update_intraday(c1h, current_time)
        except Exception as e:
            logger.error(f"❌ DR update error: {e}", exc_info=True)

    # ==================================================================
    # DISPLACEMENT DETECTION — ICT Retracement Logic
    # ==================================================================

    def _detect_displacement(self, current_price: float, current_time: int) -> None:
        """
        Detect large displacement moves (BOS + strong impulse).

        ICT principle: After displacement, price retraces into the
        FVGs/OBs created by the move BEFORE continuing. Trying to
        trade in the displacement direction from the extreme is wrong;
        you wait for the retracement to complete.

        Sets self._displacement_detected = True when a large recent
        move is detected, allowing retracement trades in the opposite
        direction.
        """
        try:
            # Expire old displacements
            if (self._displacement_detected and
                    (current_time - self._displacement_timestamp) > self._DISPLACEMENT_MAX_AGE_MS):
                self._displacement_detected = False
                self._displacement_direction = ""
                logger.info("📐 Displacement expired — returning to normal mode")

            # Check recent BOS events for displacement
            recent_bos = [ms for ms in self.market_structures
                          if ms.structure_type in ("BOS", "CHoCH")
                          and ms.timeframe in ("5m", "15m", "1h")
                          and (current_time - ms.timestamp) <= self._DISPLACEMENT_MAX_AGE_MS]

            if not recent_bos:
                return

            latest_bos = recent_bos[-1]

            # Check for bearish displacement: price dropped significantly from BOS level
            if latest_bos.direction == "bearish":
                drop_pct = (latest_bos.price - current_price) / latest_bos.price * 100
                if drop_pct >= self._DISPLACEMENT_MIN_PCT:
                    # Verify there are unfilled FVGs above (retracement targets)
                    unfilled_bear_fvgs_above = [
                        f for f in self.fvgs_bear
                        if f.is_active(current_time)
                        and f.bottom > current_price
                        and f.fill_percentage < 0.5
                    ]
                    if unfilled_bear_fvgs_above:
                        if not self._displacement_detected or self._displacement_direction != "bearish":
                            # FIRST detection — set initial state
                            logger.info(
                                f"📐 BEARISH DISPLACEMENT detected: BOS @ ${latest_bos.price:,.0f} "
                                f"→ price @ ${current_price:,.0f} ({drop_pct:.1f}% drop) | "
                                f"{len(unfilled_bear_fvgs_above)} unfilled FVG(s) above for retracement")
                            self._displacement_detected = True
                            self._displacement_direction = "bearish"
                            self._displacement_bos_price = latest_bos.price
                            self._displacement_low = current_price
                            self._displacement_high = latest_bos.price
                            self._displacement_timestamp = latest_bos.timestamp
                        else:
                            # Already tracking — only update extreme (track the actual low)
                            if current_price < self._displacement_low:
                                self._displacement_low = current_price

            # Check for bullish displacement: price rallied significantly from BOS level
            elif latest_bos.direction == "bullish":
                rally_pct = (current_price - latest_bos.price) / latest_bos.price * 100
                if rally_pct >= self._DISPLACEMENT_MIN_PCT:
                    unfilled_bull_fvgs_below = [
                        f for f in self.fvgs_bull
                        if f.is_active(current_time)
                        and f.top < current_price
                        and f.fill_percentage < 0.5
                    ]
                    if unfilled_bull_fvgs_below:
                        if not self._displacement_detected or self._displacement_direction != "bullish":
                            # FIRST detection — set initial state
                            logger.info(
                                f"📐 BULLISH DISPLACEMENT detected: BOS @ ${latest_bos.price:,.0f} "
                                f"→ price @ ${current_price:,.0f} ({rally_pct:.1f}% rally) | "
                                f"{len(unfilled_bull_fvgs_below)} unfilled FVG(s) below for retracement")
                            self._displacement_detected = True
                            self._displacement_direction = "bullish"
                            self._displacement_bos_price = latest_bos.price
                            self._displacement_low = latest_bos.price
                            self._displacement_high = current_price
                            self._displacement_timestamp = latest_bos.timestamp
                        else:
                            # Already tracking — only update extreme (track the actual high)
                            if current_price > self._displacement_high:
                                self._displacement_high = current_price

        except Exception as e:
            logger.error(f"❌ Displacement detection error: {e}", exc_info=True)

    def _is_retracement_trade(self, side: str) -> bool:
        """
        Check if the proposed trade is a retracement play.
        After bearish displacement → allow longs (retracement up into FVGs).
        After bullish displacement → allow shorts (retracement down into FVGs).
        """
        if not self._displacement_detected:
            return False
        if self._displacement_direction == "bearish" and side == "long":
            return True
        if self._displacement_direction == "bullish" and side == "short":
            return True
        return False

    # ==================================================================
    # SESSION / KILLZONE
    # ==================================================================

    def _update_session_and_killzone(self, current_time: int) -> None:
        try:
            utc_hour = datetime.fromtimestamp(current_time / 1000, tz=timezone.utc).hour
            weekday  = datetime.fromtimestamp(current_time / 1000, tz=timezone.utc).weekday()

            is_weekend = weekday >= 5

            london_kz = config.PO3_LONDON_KILLZONE_START <= utc_hour < config.PO3_LONDON_KILLZONE_END
            ny_kz     = config.PO3_NY_KILLZONE_START <= utc_hour < config.PO3_NY_KILLZONE_END
            asia_kz   = config.PO3_ASIA_KILLZONE_START <= utc_hour < config.PO3_ASIA_KILLZONE_END

            self.in_killzone = london_kz or ny_kz or asia_kz

            if london_kz:
                self.current_session = "LONDON"
            elif ny_kz:
                self.current_session = "NEW_YORK"
            elif asia_kz:
                self.current_session = "ASIA"
            elif is_weekend:
                self.current_session = "WEEKEND"
            else:
                self.current_session = "REGULAR"

            # AMD phase based on killzone timing
            if london_kz:
                if utc_hour == config.PO3_LONDON_KILLZONE_START:
                    self.amd_phase = "ACCUMULATION"
                elif utc_hour == config.PO3_LONDON_KILLZONE_START + 1:
                    self.amd_phase = "MANIPULATION"
                else:
                    self.amd_phase = "DISTRIBUTION"
            elif ny_kz:
                if utc_hour == config.PO3_NY_KILLZONE_START:
                    self.amd_phase = "ACCUMULATION"
                elif utc_hour == config.PO3_NY_KILLZONE_START + 1:
                    self.amd_phase = "MANIPULATION"
                else:
                    self.amd_phase = "DISTRIBUTION"
            else:
                self.amd_phase = "REGULAR"

        except Exception as e:
            logger.error(f"Session update error: {e}", exc_info=True)

    # ==================================================================
    # ENTRY EVALUATION — CASCADE GATE SYSTEM
    # ==================================================================

    def _evaluate_entry(self, data_manager, order_manager, risk_manager,
                         current_time: int) -> None:
        try:
            current_price = data_manager.get_last_price()
            if not current_price or current_price <= 0:
                return

            # Risk check
            can_trade, reason = risk_manager.can_trade()
            if not can_trade:
                return

            now_ms = current_time

            # Evaluate both sides
            for side in ["long", "short"]:
                # ── L1 GATE: HTF Bias + DR alignment + no failed setup ────
                l1_pass, l1_reason = self._cascade_l1(side, current_price, now_ms)
                if not l1_pass:
                    logger.debug(f"⛔ {side.upper()} L1 rejected: {l1_reason}")
                    continue

                # ── SCORE: Confluence scoring ──────────────────────────────
                score, reasons, ctx = self._score_confluence(
                    side, current_price, data_manager, now_ms)

                # ── L2 GATE: Need sweep + OB/FVG + MSS ────────────────────
                l2_count, l2_labels = self._cascade_l2(side, current_price, ctx, now_ms)

                # Dynamic L2 threshold: high-conviction setups need fewer triggers
                l2_needed = CASCADE_L2_MIN_TRIGGERS  # default 2
                high_conviction = (
                    (self.in_killzone and score >= 70) or
                    (self.htf_bias_strength >= 0.80 and any("MSS" in l for l in l2_labels))
                )
                if high_conviction:
                    l2_needed = 1  # Strong setup — reduce L2 requirement

                if l2_count < l2_needed:
                    logger.debug(f"⛔ {side.upper()} L2 rejected: {l2_count}/{l2_needed} "
                                 f"(have: {', '.join(l2_labels) if l2_labels else 'none'}) "
                                 f"Score={score:.0f} Reasons=[{', '.join(reasons[:3])}]")
                    continue

                # ── L3 GATE: At least 1 confirmation ──────────────────────
                l3_count, l3_labels = self._cascade_l3(side, current_price, ctx, score, now_ms)
                if l3_count < CASCADE_L3_MIN_CONFIRMS:
                    logger.debug(f"⛔ {side.upper()} L3 rejected: {l3_count}/{CASCADE_L3_MIN_CONFIRMS} "
                                 f"Score={score:.0f}")
                    continue

                # ── THRESHOLD: Score must meet regime-adjusted threshold ───
                threshold = self._get_entry_threshold()
                if score < threshold:
                    # Spam suppression: only log near-threshold once per minute
                    rej_key = f"BELOW_THRESH_{side}_{score:.0f}"
                    if self._should_log_rejection(side, rej_key, now_ms):
                        logger.info(f"📊 {side.upper()} near threshold: {score:.0f}/{threshold:.0f} "
                                    f"L2=[{', '.join(l2_labels)}] L3=[{', '.join(l3_labels)}] "
                                    f"[{', '.join(reasons[:4])}]")
                    continue

                # ── PASSED ALL GATES — attempt entry ──────────────────────
                # Only log "PASSED" once per rejection cycle (not every 5s)
                rej_key = f"PASSED_{side}_{score:.0f}"
                is_retracement = self._is_retracement_trade(side)
                tag = " [RETRACEMENT]" if is_retracement else ""

                if self._should_log_rejection(side, rej_key, now_ms):
                    logger.info(f"🎯 {side.upper()} PASSED all gates!{tag} "
                                f"Score={score:.0f}/{threshold:.0f} "
                                f"L2=[{', '.join(l2_labels)}] L3=[{', '.join(l3_labels)}]")

                self._execute_entry(
                    side, current_price, order_manager, risk_manager,
                    score, reasons, ctx, current_time)
                return  # Only one entry per evaluation

        except Exception as e:
            logger.error(f"❌ Entry evaluation error: {e}", exc_info=True)

    def _should_log_rejection(self, side: str, rejection_key: str, now_ms: int) -> bool:
        """
        Spam suppression: Only log a rejection if it's new or hasn't been
        logged in the last _REJECTION_LOG_COOLDOWN_MS.
        Returns True if the message should be logged.
        """
        prev_key = self._last_rejection_key.get(side)
        prev_time = self._last_rejection_time.get(side, 0)

        if prev_key == rejection_key and (now_ms - prev_time) < self._REJECTION_LOG_COOLDOWN_MS:
            return False  # Same rejection, within cooldown — suppress

        self._last_rejection_key[side] = rejection_key
        self._last_rejection_time[side] = now_ms
        return True

    # ==================================================================
    # CASCADE L1: Hard gates
    # ==================================================================

    def _cascade_l1(self, side: str, price: float, now: int) -> Tuple[bool, str]:
        """
        L1 Gate — HTF bias + IPDA dealing range alignment.

        ICT rules:
        1. HTF must be directional (not NEUTRAL). Trade direction must match HTF.
        2. Weekly DR hard-oppose: only block if trading EXTREME against HTF trend.
           - BEARISH trend → block longs at extreme weekly PREMIUM (>75%)
           - BULLISH trend → block shorts at extreme weekly DISCOUNT (<25%)
        3. Daily DR: price must be in premium for shorts, discount for longs.
           BUT — if HTF strongly confirms, allow entry from equilibrium too.

        RETRACEMENT EXCEPTION (ICT displacement logic):
        After a large displacement (BOS + strong impulse), price retraces into
        FVGs/OBs before continuing. Allow counter-trend retracement trades
        targeting those FVG fills — this IS the institutional model.
        """
        # ── RETRACEMENT OVERRIDE ──────────────────────────────────────────
        # After displacement, allow retracement trades targeting FVG fills
        if self._is_retracement_trade(side):
            # Still block at extreme levels (don't retrace-long at weekly premium)
            if self._ndr.weekly is not None:
                wz = self._ndr.weekly.zone_pct(price)
                if side == "long" and wz > 0.80:
                    return False, f"Retracement long blocked — weekly extreme premium ({wz:.0%})"
                if side == "short" and wz < 0.20:
                    return False, f"Retracement short blocked — weekly extreme discount ({wz:.0%})"
            return True, "L1_OK (retracement after displacement)"

        # ── HTF bias alignment ────────────────────────────────────────────
        if self.htf_bias == "NEUTRAL":
            return False, "HTF NEUTRAL — awaiting directional bias"
        if side == "long"  and self.htf_bias == "BEARISH":
            return False, "HTF BEARISH blocks long"
        if side == "short" and self.htf_bias == "BULLISH":
            return False, "HTF BULLISH blocks short"

        # ── Post-displacement: don't short at the bottom / long at the top ─
        # If displacement happened in this direction, we're already at the extreme
        if self._displacement_detected:
            if self._displacement_direction == "bearish" and side == "short":
                # Price is near displacement low — don't short the bottom
                if self._displacement_low and price < self._displacement_low * 1.005:
                    return False, "Post-displacement: already at bearish extreme — wait for retracement"
            if self._displacement_direction == "bullish" and side == "long":
                if self._displacement_high and price > self._displacement_high * 0.995:
                    return False, "Post-displacement: already at bullish extreme — wait for retracement"

        # ── Weekly DR hard-oppose (HTF-aware) ────────────────────────────
        if self._ndr.hard_opposed(price, side, self.htf_bias):
            wz = self._ndr.weekly.zone_pct(price) if self._ndr.weekly else 0.5
            return False, f"Weekly DR extreme zone ({wz:.0%}) hard-opposes {side}"

        # ── Daily DR zone alignment ───────────────────────────────────────
        if self._ndr.daily is not None:
            dz = self._ndr.daily.zone_pct(price)
            strong_htf = self.htf_bias_strength >= 0.80

            if side == "long":
                if dz > 0.75 and not strong_htf:
                    return False, f"Daily DR extreme premium ({dz:.0%}) blocks long"
            elif side == "short":
                if dz < 0.25 and not strong_htf:
                    return False, f"Daily DR extreme discount ({dz:.0%}) blocks short"

        return True, "L1_OK"

    # ==================================================================
    # CASCADE L2: Need 2 of: Sweep+Disp, OB/FVG touch, MSS
    # ==================================================================

    def _cascade_l2(self, side: str, price: float, ctx: TriggerContext,
                     now: int) -> Tuple[int, List[str]]:
        met = []

        # A. For retracement trades, the displacement itself counts as one L2 trigger
        #    ICT logic: displacement = BOS + strong impulse = directional conviction
        #    Requiring sweep+disp ON TOP of the displacement is double-counting
        if self._is_retracement_trade(side):
            met.append("DISPLACEMENT")

        # B. Swept liquidity with displacement
        if ctx.sweep_pool is not None and ctx.sweep_pool.displacement_confirmed:
            met.append("SWEEP_DISP")

        # C. OB touch — EXPANDED with proximity for when price has moved through
        if ctx.trigger_ob is not None:
            if ctx.trigger_ob.contains_price(price) or ctx.trigger_ob.in_optimal_zone(price):
                met.append("OB_TOUCH")
            else:
                # Price may have already moved through OB — check proximity
                ob_mid = (ctx.trigger_ob.high + ctx.trigger_ob.low) / 2
                dist_pct = abs(price - ob_mid) / price * 100 if price > 0 else 999
                if dist_pct <= 0.5:  # Within 0.5% counts as soft touch
                    met.append("OB_PROXIMITY")

        # D. FVG touch — EXPANDED with proximity
        if ctx.trigger_fvg is not None:
            if ctx.trigger_fvg.is_price_in_gap(price):
                met.append("FVG_TOUCH")
            elif not any(t.startswith("OB_") for t in met):
                # Price may have already moved through FVG — check proximity
                fvg_mid = (ctx.trigger_fvg.top + ctx.trigger_fvg.bottom) / 2
                dist_pct = abs(price - fvg_mid) / price * 100 if price > 0 else 999
                if dist_pct <= 0.5:
                    met.append("FVG_PROXIMITY")

        # E. Recent MSS in direction
        if ctx.mss_event is not None:
            met.append(f"MSS_{ctx.mss_event.structure_type}")

        return len(met), met

    # ==================================================================
    # CASCADE L3: Confirmations (need 1 of N)
    # ==================================================================

    def _cascade_l3(self, side: str, price: float, ctx: TriggerContext,
                     score: float, now: int) -> Tuple[int, List[str]]:
        met = []

        # CVD aligned
        cvd = self.volume_analyzer.get_cvd_signal()
        sig = cvd.get("signal", "NEUTRAL")
        if side == "long" and "BULL" in sig:
            met.append("CVD_BULL")
        elif side == "short" and "BEAR" in sig:
            met.append("CVD_BEAR")

        # Absorption event
        abs_score = self.absorption_model.absorption_near_price(price, side)
        if abs_score > 0:
            met.append("ABSORPTION")

        # Killzone
        if self.in_killzone:
            met.append("KILLZONE")

        # AMD manipulation phase + sweep
        if self.amd_phase == "MANIPULATION" and ctx.sweep_pool is not None:
            met.append("AMD_MANIP")

        # Daily bias alignment
        if (side == "long" and self.daily_bias == "BULLISH") or \
           (side == "short" and self.daily_bias == "BEARISH"):
            met.append("DAILY_BIAS")

        return len(met), met

    # ==================================================================
    # CONFLUENCE SCORING
    # ==================================================================

    def _score_confluence(self, side: str, current_price: float,
                          data_manager, now_ms: int) -> Tuple[float, List[str], TriggerContext]:
        try:
            score   = 0.0
            reasons = []
            ctx     = TriggerContext()

            # ── 1. HTF Bias (0-25) ─────────────────────────────────────
            if (side == "long" and self.htf_bias == "BULLISH") or \
               (side == "short" and self.htf_bias == "BEARISH"):
                bias_score = 15.0 + 10.0 * self.htf_bias_strength
                score += bias_score
                reasons.append(f"HTF {self.htf_bias} str={self.htf_bias_strength:.0%} +{bias_score:.1f}")
            elif self._is_retracement_trade(side):
                # Retracement trade: award moderate score for being in valid
                # ICT retracement setup (displacement → retrace → continue)
                retrace_score = 12.0 + 8.0 * self.htf_bias_strength
                score += retrace_score
                reasons.append(f"RETRACE after {self._displacement_direction.upper()} disp +{retrace_score:.1f}")

            # ── 2. Liquidity Sweep (0-25) ──────────────────────────────
            sweep_age_limit = config.SWEEP_MAX_AGE_MINUTES * 60_000
            for pool in reversed(list(self.liquidity_pools)):
                if not pool.swept:
                    continue
                if (now_ms - pool.sweep_timestamp) > sweep_age_limit:
                    continue
                if (side == "long" and pool.pool_type == "EQL") or \
                   (side == "short" and pool.pool_type == "EQH"):
                    sweep_s = 15.0
                    if pool.displacement_confirmed:
                        sweep_s += 5.0
                    if pool.wick_rejection:
                        sweep_s += 5.0
                    score += sweep_s
                    reasons.append(f"Sweep {pool.pool_type} @ {pool.price:.0f} +{sweep_s:.1f}")
                    ctx.sweep_pool = pool
                    break

            # ── 3. Order Block (0-30) ──────────────────────────────────
            obs = self.order_blocks_bull if side == "long" else self.order_blocks_bear
            for ob in sorted([o for o in obs if o.is_active(now_ms)],
                             key=lambda x: x.strength, reverse=True):
                virgin_m = ob.virgin_multiplier()
                dist_pct = abs(current_price - ob.midpoint) / current_price * 100

                if ob.contains_price(current_price) or ob.in_optimal_zone(current_price):
                    in_ote = ob.in_optimal_zone(current_price)
                    ob_score = ob.strength / 100 * (30.0 if in_ote else 22.0) * virgin_m
                    if ob.bos_confirmed:
                        ob_score += 5.0
                    if ob.has_displacement:
                        ob_score += 3.0
                    score += ob_score
                    tag = "OTE" if in_ote else "BODY"
                    reasons.append(
                        f"OB {tag} {ob.low:.0f}-{ob.high:.0f} str={ob.strength:.0f} "
                        f"v={ob.visit_count} +{ob_score:.1f}")
                    ctx.trigger_ob = ob
                    break
                elif dist_pct <= 0.5:
                    prox = 1.0 - dist_pct / 0.5
                    ob_score = ob.strength / 100 * 15.0 * prox * virgin_m
                    score += ob_score
                    reasons.append(f"Near OB {ob.low:.0f}-{ob.high:.0f} dist={dist_pct:.2f}% +{ob_score:.1f}")
                    ctx.trigger_ob = ob
                    break

            # ── 4. FVG (0-25) ──────────────────────────────────────────
            fvgs = self.fvgs_bull if side == "long" else self.fvgs_bear
            for fvg in [f for f in fvgs if f.is_active(now_ms)]:
                if fvg.is_price_in_gap(current_price):
                    freshness = 1.0 - fvg.fill_percentage
                    fvg_score = 20.0 * (0.5 + 0.5 * freshness)
                    score += fvg_score
                    reasons.append(f"IN FVG {fvg.bottom:.0f}-{fvg.top:.0f} fill={fvg.fill_percentage*100:.0f}% +{fvg_score:.1f}")
                    ctx.trigger_fvg = fvg
                    break
                elif abs(current_price - fvg.midpoint) / current_price * 100 <= 0.5:
                    fvg_score = 10.0
                    score += fvg_score
                    reasons.append(f"Near FVG {fvg.bottom:.0f}-{fvg.top:.0f} +{fvg_score:.1f}")
                    ctx.trigger_fvg = fvg
                    break

            # ── 5. OB + FVG overlap bonus ──────────────────────────────
            if ctx.trigger_ob and ctx.trigger_fvg:
                score += 8.0
                reasons.append("OB+FVG confluence +8")

            # ── 6. Market Structure (0-15) ─────────────────────────────
            target_dir = "bullish" if side == "long" else "bearish"
            mss_window = config.MSS_MAX_AGE_MINUTES * 60_000
            for ms in reversed(list(self.market_structures)):
                if (now_ms - ms.timestamp) > mss_window:
                    break
                if ms.direction == target_dir:
                    ms_score = 15.0 if ms.structure_type == "CHoCH" else 10.0
                    score += ms_score
                    reasons.append(f"{ms.structure_type} {ms.direction} [{ms.timeframe}] +{ms_score}")
                    ctx.mss_event = ms
                    break

            # ── 7. CVD (0-10) ──────────────────────────────────────────
            cvd = self.volume_analyzer.get_cvd_signal()
            sig = cvd.get("signal", "NEUTRAL")
            if side == "long" and "STRONG" in sig and "BULL" in sig:
                score += 10.0; reasons.append("CVD Strong Bull +10")
            elif side == "long" and "BULL" in sig:
                score += 5.0; reasons.append("CVD Bull +5")
            elif side == "short" and "STRONG" in sig and "BEAR" in sig:
                score += 10.0; reasons.append("CVD Strong Bear +10")
            elif side == "short" and "BEAR" in sig:
                score += 5.0; reasons.append("CVD Bear +5")

            # ── 8. Absorption (0-12) ───────────────────────────────────
            abs_bonus = self.absorption_model.absorption_near_price(current_price, side)
            if abs_bonus > 0:
                score += abs_bonus
                reasons.append(f"Absorption +{abs_bonus:.0f}")

            # ── 9. Killzone bonus (0-8) ────────────────────────────────
            if self.in_killzone:
                score += 8.0
                reasons.append(f"Killzone {self.current_session} +8")

            # ── 10. DR Zone (0-12 / -18 penalty) ──────────────────────
            dr = self._ndr.best_dr()
            if dr is not None:
                zone = dr.zone_pct(current_price)
                if side == "long" and dr.is_discount(current_price):
                    dr_bonus = round(12.0 * (config.DR_DISCOUNT_THRESHOLD - zone) / config.DR_DISCOUNT_THRESHOLD, 1)
                    score += dr_bonus
                    reasons.append(f"Discount zone {zone*100:.0f}% +{dr_bonus}")
                elif side == "short" and dr.is_premium(current_price):
                    dr_bonus = round(12.0 * (zone - config.DR_PREMIUM_THRESHOLD) / (1 - config.DR_PREMIUM_THRESHOLD), 1)
                    score += dr_bonus
                    reasons.append(f"Premium zone {zone*100:.0f}% +{dr_bonus}")

            # ── 11. Nearest swings for SL/TP context ───────────────────
            if side == "long":
                lows_below  = [s.price for s in list(self.swing_lows)[-10:] if s.price < current_price]
                highs_above = [s.price for s in list(self.swing_highs)[-10:] if s.price > current_price]
                ctx.nearest_swing_low  = max(lows_below)  if lows_below  else None
                ctx.nearest_swing_high = min(highs_above) if highs_above else None
            else:
                highs_above = [s.price for s in list(self.swing_highs)[-10:] if s.price > current_price]
                lows_below  = [s.price for s in list(self.swing_lows)[-10:] if s.price < current_price]
                ctx.nearest_swing_high = min(highs_above) if highs_above else None
                ctx.nearest_swing_low  = max(lows_below)  if lows_below  else None

            # ── Regime adjustments ─────────────────────────────────────
            rs = self.regime_engine.state
            if ctx.trigger_ob and rs.ob_score_multiplier != 1.0:
                adj = 15.0 * (rs.ob_score_multiplier - 1.0)
                score += adj
                if abs(adj) >= 1.0:
                    reasons.append(f"Regime OB adj {adj:+.1f}")
            if ctx.trigger_fvg and rs.fvg_score_multiplier != 1.0:
                adj = 10.0 * (rs.fvg_score_multiplier - 1.0)
                score += adj
                if abs(adj) >= 1.0:
                    reasons.append(f"Regime FVG adj {adj:+.1f}")

            score = min(max(score, 0.0), 100.0)
            return score, reasons, ctx

        except Exception as e:
            logger.error(f"❌ Confluence scoring error: {e}", exc_info=True)
            return 0.0, [], TriggerContext()

    # ==================================================================
    # ENTRY THRESHOLD
    # ==================================================================

    def _get_entry_threshold(self) -> float:
        if self.current_session == "WEEKEND":
            base = config.ENTRY_THRESHOLD_WEEKEND
        elif self.in_killzone:
            base = config.ENTRY_THRESHOLD_KILLZONE
        else:
            base = config.ENTRY_THRESHOLD_REGULAR

        regime_mod = self.regime_engine.state.entry_threshold_modifier
        return base + regime_mod

    # ==================================================================
    # CALCULATE STRUCTURE-BASED SL/TP
    # ==================================================================

    def _calculate_levels(self, side: str, current_price: float,
                          ctx: TriggerContext,
                          current_time: int = 0,
                          quiet: bool = False) -> Tuple[Optional[float], Optional[float], Optional[float]]:
        """
        Calculate entry, SL, TP from STRUCTURE ONLY. No fallbacks.
        Returns (entry_price, sl_price, tp_price) or (None, None, None).
        Set quiet=True for dry-run calls (e.g. from _build_trade_plan) to suppress warnings.
        """
        try:
            if current_time <= 0:
                current_time = int(time.time() * 1000)
            tick   = config.TICK_SIZE
            buffer = config.SL_BUFFER_TICKS * tick
            entry_offset = config.LIMIT_ORDER_OFFSET_TICKS * tick

            if side == "long":
                entry_price = current_price - entry_offset

                # SL: Below triggering OB low, or below nearest swing low
                sl_price = None
                if ctx.trigger_ob is not None:
                    sl_price = ctx.trigger_ob.low - buffer
                if sl_price is None and ctx.nearest_swing_low is not None:
                    sl_price = ctx.nearest_swing_low - buffer
                if sl_price is None and ctx.trigger_fvg is not None:
                    sl_price = ctx.trigger_fvg.bottom - buffer

                # Validate SL
                if sl_price is None or sl_price >= entry_price:
                    if not quiet: logger.warning("No valid SL structure for LONG — rejecting")
                    return None, None, None

                sl_dist = (entry_price - sl_price) / entry_price
                if sl_dist < config.MIN_SL_DISTANCE_PCT:
                    sl_price = entry_price * (1 - config.MIN_SL_DISTANCE_PCT)
                elif sl_dist > config.MAX_SL_DISTANCE_PCT:
                    if not quiet: logger.warning(f"SL too wide: {sl_dist*100:.2f}% — rejecting")
                    return None, None, None

                risk = entry_price - sl_price

                # TP: Opposing liquidity pool, OB, or swing high
                tp_price = None

                is_retrace = self._is_retracement_trade("long")

                # For retracement longs: FIRST target unfilled FVGs above
                # (bearish FVGs created by the displacement move)
                if is_retrace and self._RETRACEMENT_FVG_TP:
                    retrace_fvgs = sorted(
                        [f for f in self.fvgs_bear
                         if f.is_active(current_time)
                         and f.bottom > entry_price
                         and f.fill_percentage < 0.5],
                        key=lambda f: f.bottom)
                    if retrace_fvgs:
                        target_fvg = retrace_fvgs[0]  # nearest unfilled bearish FVG
                        rr = (target_fvg.midpoint - entry_price) / risk if risk > 0 else 0
                        if rr >= config.MIN_RISK_REWARD_RATIO:
                            tp_price = target_fvg.midpoint - buffer
                            logger.info(f"📐 Retracement TP → bearish FVG ${target_fvg.bottom:,.0f}–${target_fvg.top:,.0f} "
                                        f"(midpoint ${target_fvg.midpoint:,.0f}, RR={rr:.1f})")
                        elif len(retrace_fvgs) > 1:
                            # Try the next FVG if first is too close
                            target_fvg = retrace_fvgs[1]
                            rr = (target_fvg.midpoint - entry_price) / risk if risk > 0 else 0
                            if rr >= config.MIN_RISK_REWARD_RATIO:
                                tp_price = target_fvg.midpoint - buffer

                # First: nearest EQH (opposing liquidity) — only if no retracement TP set
                if tp_price is None:
                    eqh_pools = [lp for lp in self.liquidity_pools
                                 if lp.pool_type == "EQH" and not lp.swept
                                 and lp.price > entry_price]
                    if eqh_pools:
                        nearest = min(eqh_pools, key=lambda p: p.price)
                        rr = (nearest.price - entry_price) / risk if risk > 0 else 0
                        if rr >= config.MIN_RISK_REWARD_RATIO:
                            tp_price = nearest.price - buffer

                # Second: nearest bearish OB (opposing structure)
                if tp_price is None:
                    opposing_obs = [ob for ob in self.order_blocks_bear
                                    if ob.is_active(current_time)
                                    and ob.low > entry_price]
                    if opposing_obs:
                        nearest_ob = min(opposing_obs, key=lambda o: o.low)
                        rr = (nearest_ob.low - entry_price) / risk if risk > 0 else 0
                        if rr >= config.MIN_RISK_REWARD_RATIO:
                            tp_price = nearest_ob.low - buffer

                # Third: nearest swing high
                if tp_price is None and ctx.nearest_swing_high is not None:
                    rr = (ctx.nearest_swing_high - entry_price) / risk if risk > 0 else 0
                    if rr >= config.MIN_RISK_REWARD_RATIO:
                        tp_price = ctx.nearest_swing_high - buffer

                # Fourth: DR high if available
                if tp_price is None:
                    dr = self._ndr.best_dr()
                    if dr is not None and dr.high > entry_price:
                        rr = (dr.high - entry_price) / risk if risk > 0 else 0
                        if rr >= config.MIN_RISK_REWARD_RATIO:
                            tp_price = dr.high - buffer

                if tp_price is None:
                    # FALLBACK: Use fixed risk-multiple when no structural TP meets RR
                    # 2.0x risk is the minimum viable TP — still profitable
                    fallback_tp = entry_price + risk * 2.0
                    tp_price = fallback_tp
                    if not quiet:
                        logger.info(f"📐 LONG TP fallback: 2.0R @ ${tp_price:,.0f} "
                                    f"(no structural TP met {config.MIN_RISK_REWARD_RATIO}R)")

                if tp_price is None:
                    if not quiet: logger.warning("No valid TP structure for LONG — rejecting")
                    return None, None, None

            else:  # short
                entry_price = current_price + entry_offset

                # SL above triggering OB high, or above nearest swing high
                sl_price = None
                if ctx.trigger_ob is not None:
                    sl_price = ctx.trigger_ob.high + buffer
                if sl_price is None and ctx.nearest_swing_high is not None:
                    sl_price = ctx.nearest_swing_high + buffer
                if sl_price is None and ctx.trigger_fvg is not None:
                    sl_price = ctx.trigger_fvg.top + buffer

                if sl_price is None or sl_price <= entry_price:
                    if not quiet: logger.warning("No valid SL structure for SHORT — rejecting")
                    return None, None, None

                sl_dist = (sl_price - entry_price) / entry_price
                if sl_dist < config.MIN_SL_DISTANCE_PCT:
                    sl_price = entry_price * (1 + config.MIN_SL_DISTANCE_PCT)
                elif sl_dist > config.MAX_SL_DISTANCE_PCT:
                    if not quiet: logger.warning(f"SL too wide: {sl_dist*100:.2f}% — rejecting")
                    return None, None, None

                risk = sl_price - entry_price

                # TP: Opposing EQL, bullish OB, swing low, DR low
                tp_price = None

                is_retrace = self._is_retracement_trade("short")

                # For retracement shorts: FIRST target unfilled FVGs below
                # (bullish FVGs created by the displacement move)
                if is_retrace and self._RETRACEMENT_FVG_TP:
                    retrace_fvgs = sorted(
                        [f for f in self.fvgs_bull
                         if f.is_active(current_time)
                         and f.top < entry_price
                         and f.fill_percentage < 0.5],
                        key=lambda f: f.top, reverse=True)
                    if retrace_fvgs:
                        target_fvg = retrace_fvgs[0]
                        rr = (entry_price - target_fvg.midpoint) / risk if risk > 0 else 0
                        if rr >= config.MIN_RISK_REWARD_RATIO:
                            tp_price = target_fvg.midpoint + buffer
                            logger.info(f"📐 Retracement TP → bullish FVG ${target_fvg.bottom:,.0f}–${target_fvg.top:,.0f} "
                                        f"(midpoint ${target_fvg.midpoint:,.0f}, RR={rr:.1f})")

                if tp_price is None:
                    eql_pools = [lp for lp in self.liquidity_pools
                                 if lp.pool_type == "EQL" and not lp.swept
                                 and lp.price < entry_price]
                    if eql_pools:
                        nearest = max(eql_pools, key=lambda p: p.price)
                        rr = (entry_price - nearest.price) / risk if risk > 0 else 0
                        if rr >= config.MIN_RISK_REWARD_RATIO:
                            tp_price = nearest.price + buffer

                if tp_price is None:
                    opposing_obs = [ob for ob in self.order_blocks_bull
                                    if ob.is_active(current_time)
                                    and ob.high < entry_price]
                    if opposing_obs:
                        nearest_ob = max(opposing_obs, key=lambda o: o.high)
                        rr = (entry_price - nearest_ob.high) / risk if risk > 0 else 0
                        if rr >= config.MIN_RISK_REWARD_RATIO:
                            tp_price = nearest_ob.high + buffer

                if tp_price is None and ctx.nearest_swing_low is not None:
                    rr = (entry_price - ctx.nearest_swing_low) / risk if risk > 0 else 0
                    if rr >= config.MIN_RISK_REWARD_RATIO:
                        tp_price = ctx.nearest_swing_low + buffer

                if tp_price is None:
                    dr = self._ndr.best_dr()
                    if dr is not None and dr.low < entry_price:
                        rr = (entry_price - dr.low) / risk if risk > 0 else 0
                        if rr >= config.MIN_RISK_REWARD_RATIO:
                            tp_price = dr.low + buffer

                if tp_price is None:
                    # FALLBACK: Use fixed risk-multiple when no structural TP meets RR
                    fallback_tp = entry_price - risk * 2.0
                    tp_price = fallback_tp
                    if not quiet:
                        logger.info(f"📐 SHORT TP fallback: 2.0R @ ${tp_price:,.0f} "
                                    f"(no structural TP met {config.MIN_RISK_REWARD_RATIO}R)")

                if tp_price is None:
                    if not quiet: logger.warning("No valid TP structure for SHORT — rejecting")
                    return None, None, None

            # Round to tick
            entry_price = round(entry_price / tick) * tick
            sl_price    = round(sl_price / tick) * tick
            tp_price    = round(tp_price / tick) * tick

            return entry_price, sl_price, tp_price

        except Exception as e:
            logger.error(f"❌ Level calculation error: {e}", exc_info=True)
            return None, None, None

    # ==================================================================
    # EXECUTE ENTRY
    # ==================================================================

    def _execute_entry(self, side: str, current_price: float,
                       order_manager, risk_manager, score: float,
                       reasons: List[str], ctx: TriggerContext,
                       current_time: int) -> None:
        try:
            # Spam suppression: only log full HIGH CONFLUENCE block once per cycle
            exec_key = f"EXEC_{side}_{score:.0f}"
            should_log = self._should_log_rejection(side, exec_key, current_time)

            if should_log:
                logger.info("=" * 80)
                logger.info(f"🎯 HIGH CONFLUENCE [{side.upper()}] Score={score:.0f}")
                if self._is_retracement_trade(side):
                    logger.info(f"   📐 RETRACEMENT TRADE after {self._displacement_direction.upper()} displacement")
                for r in reasons:
                    logger.info(f"   {r}")
                logger.info("=" * 80)

            entry_price, sl_price, tp_price = self._calculate_levels(
                side, current_price, ctx, current_time=current_time)

            if entry_price is None:
                # Spam suppression: only log once per minute
                rej_key = f"LEVELS_FAIL_{side}"
                if self._should_log_rejection(side, rej_key, current_time):
                    logger.warning("⚠️ Structure-based levels failed — no entry")
                return

            # Sanity validation
            if side == "long":
                if sl_price >= entry_price or tp_price <= entry_price:
                    logger.error(f"Invalid LONG levels: entry={entry_price} SL={sl_price} TP={tp_price}")
                    return None, None, None
            elif side == "short":
                if sl_price <= entry_price or tp_price >= entry_price:
                    logger.error(f"Invalid SHORT levels: entry={entry_price} SL={sl_price} TP={tp_price}")
                    return None, None, None

            risk   = abs(entry_price - sl_price)
            reward = abs(tp_price - entry_price)
            rr     = reward / risk if risk > 0 else 0

            if rr < config.MIN_RISK_REWARD_RATIO - 1e-9:
                logger.warning(f"⚠️ RR={rr:.4f} < min {config.MIN_RISK_REWARD_RATIO} — skipping")
                return

            # Position size
            _, dr_mult = self._ndr.alignment_score(current_price, side)
            rs = self.regime_engine.state

            position_size = risk_manager.calculate_position_size(
                entry_price, sl_price, side.upper())

            if position_size is None or position_size <= 0:
                logger.warning("⚠️ Position size calculation failed — skipping")
                return

            # Apply regime + DR multipliers
            position_size = round(position_size * rs.size_multiplier * dr_mult, 4)
            position_size = max(config.MIN_POSITION_SIZE,
                               min(position_size, config.MAX_POSITION_SIZE))

            # Place limit entry order
            GlobalRateLimiter.wait()
            entry_side = "BUY" if side == "long" else "SELL"
            result = order_manager.place_limit_order(
                side=entry_side, quantity=position_size, price=entry_price)

            if not result or "error" in result:
                logger.error(f"❌ Entry order failed: {result}")
                return

            order_id = result.get("data", {}).get("order_id") or result.get("order_id")
            if not order_id:
                logger.error("❌ No order_id in response")
                return

            # Update state
            self._trade_side    = side 
            self.entry_order_id   = order_id
            self.state            = "ENTRY_PENDING"
            self.entry_pending_start = current_time
            self._pending_ctx     = ctx

            self.initial_entry_price = entry_price
            self.initial_sl_price    = sl_price
            self.initial_tp_price    = tp_price
            self.current_sl_price    = sl_price
            self.current_tp_price    = tp_price
            self.entry_quantity      = position_size
            
            sl_side = "SELL" if side == "long" else "BUY" 

            # ── Pre-place SL immediately (rate limiter handles timing) ──────
            GlobalRateLimiter.wait()
            sl_result = order_manager.place_stop_loss(
                side=sl_side, quantity=position_size, trigger_price=sl_price)
            if sl_result and "error" not in sl_result:
                self.sl_order_id = (sl_result.get("data", {}).get("order_id")
                                    or sl_result.get("order_id"))
                logger.info(f"✅ SL pre-placed: {self.sl_order_id} @ ${sl_price:,.2f}")
            else:
                logger.error(f"❌ SL pre-placement failed: {sl_result}")
                # Non-fatal — _on_entry_filled() will retry (if not self.sl_order_id guard)

            # ── Pre-place TP immediately (rate limiter handles timing) ──────
            GlobalRateLimiter.wait()
            tp_result = order_manager.place_take_profit(
                side=sl_side, quantity=position_size, trigger_price=tp_price)
            if tp_result and "error" not in tp_result:
                self.tp_order_id = (tp_result.get("data", {}).get("order_id")
                                    or tp_result.get("order_id"))
                logger.info(f"✅ TP pre-placed: {self.tp_order_id} @ ${tp_price:,.2f}")
            else:
                logger.error(f"❌ TP pre-placement failed: {tp_result}")
                # Non-fatal — _on_entry_filled() will retry (if not self.tp_order_id guard)     

            # Notify risk manager
            risk_manager.notify_entry_placed()

            self.total_entries += 1

            # Store entry context for close report
            self.entry_score = score
            self.entry_reasons = list(reasons)
            self.max_favorable_excursion = 0.0
            self.max_adverse_excursion = 0.0

            # DR zone tag for context
            dr_zone_tag = self._get_dr_zone_tag(current_price)

            msg = format_entry_alert(
                side=side,
                score=score,
                threshold=self._get_entry_threshold(),
                entry_price=entry_price,
                sl_price=sl_price,
                tp_price=tp_price,
                position_size=position_size,
                rr=rr,
                reasons=reasons,
                trigger_ob=ctx.trigger_ob,
                trigger_fvg=ctx.trigger_fvg,
                sweep_pool=ctx.sweep_pool,
                mss_event=ctx.mss_event,
                nearest_swing_low=ctx.nearest_swing_low,
                nearest_swing_high=ctx.nearest_swing_high,
                htf_bias=self.htf_bias,
                daily_bias=self.daily_bias,
                regime=rs.regime,
                session=self.current_session,
                in_killzone=self.in_killzone,
                dr_zone=dr_zone_tag,
                regime_size_mult=rs.size_multiplier,
                dr_mult=dr_mult,
                current_price=current_price,
            )
            send_telegram_message(msg)

            logger.info(
                f"✅ Entry placed: {side.upper()} @ {entry_price:.2f} "
                f"SL={sl_price:.2f} TP={tp_price:.2f} "
                f"Qty={position_size} RR={rr:.1f}")

        except Exception as e:
            logger.error(f"❌ Execute entry error: {e}", exc_info=True)


    # ==================================================================
    # ACTIVE POSITION MANAGEMENT — ICT STRUCTURAL TRAILING (NO BREAKEVEN)
    # Trailing logic:
    #   • SL is placed once at entry, beyond FVG/OB low (long) or high (short).
    #   • SL only moves AFTER a confirmed BOS in trade direction forms.
    #   • SL trails to just below the newest higher-low swing OR unmitigated
    #     bullish FVG/OB low (long) / just above lower-high or bearish FVG/OB (short).
    #   • Full position closes IMMEDIATELY on a confirmed CHoCH against the trade.
    #   • SL ratchet is strictly one-directional — never loosens.
    #   • ATR = 0 → skip cycle. No fixed-tick fallbacks ever.
    # ==================================================================
    def _handle_entry_pending(self, order_manager, risk_manager, current_time: int) -> None:
        try:
            if not self.entry_order_id:
                self._reset_position_state()
                return

            # Timeout check
            elapsed = current_time - (self.entry_pending_start or current_time)
            if elapsed > config.ENTRY_PENDING_TIMEOUT_SECONDS * 1000:
                logger.info("⏰ Entry pending timeout — cancelling")
                GlobalRateLimiter.wait()
                cancel_result = order_manager.cancel_order(self.entry_order_id)

                # ── BUG #15 FIX: cancel_result is a CancelResult ENUM, not a dict.
                # Check for partial/full fill using the enum directly.
                from order_manager import CancelResult

                if cancel_result in (CancelResult.PARTIAL_FILL,
                                     CancelResult.ALREADY_FILLED):
                    # Order executed (fully or partially) before cancel arrived.
                    # We MUST adopt this position — never abandon it.
                    logger.warning(
                        f"🔶 Entry {cancel_result.value} on timeout cancel — "
                        f"adopting position and placing SL/TP"
                    )
                    self._on_entry_filled(order_manager, current_time)
                    return

                if cancel_result == CancelResult.FAILED:
                    # Cancel truly failed (API error, not a fill).
                    # Double-check with exchange to determine actual state.
                    logger.warning(
                        f"⚠️ Cancel FAILED on timeout — checking fill status "
                        f"for {self.entry_order_id}"
                    )
                    GlobalRateLimiter.wait()
                    status = order_manager.get_order_status_safe(self.entry_order_id)
                    if status in ("FILLED", "PARTIAL_FILL"):
                        logger.warning(
                            f"🔶 Fill confirmed after failed cancel ({status}) — "
                            f"adopting position"
                        )
                        self._on_entry_filled(order_manager, current_time)
                    else:
                        logger.error(
                            f"❌ Cancel failed and status={status} — resetting state. "
                            f"CHECK EXCHANGE MANUALLY for open position!"
                        )
                        from telegram_notifier import send_telegram_message
                        send_telegram_message(
                            f"🚨 <b>MANUAL CHECK REQUIRED</b>\n"
                            f"Cancel failed for order "
                            f"<code>{self.entry_order_id}</code>\n"
                            f"Status: {status} — possible orphaned position!"
                        )
                        self._reset_position_state()
                        self._placement_locked_until = (
                            current_time + PLACEMENT_LOCK_SECONDS * 1000)
                    return

                # SUCCESS or NOT_FOUND — order cleanly cancelled or gone
                self._cancel_pending_sl_tp(order_manager)   # IDs still valid here
                self._reset_position_state()                # now safe to zero everything
                self._placement_locked_until = current_time + PLACEMENT_LOCK_SECONDS * 1000
                return

            # Check order status
            GlobalRateLimiter.wait()
            status = order_manager.get_order_status_safe(self.entry_order_id)

            if status in ("FILLED", "PARTIAL_FILL"):
                if status == "PARTIAL_FILL":
                    logger.warning(
                        f"🔶 Entry order partially filled — activating with partial position"
                    )
                self._on_entry_filled(order_manager, current_time)

            elif status in ("CANCELLED", "REJECTED", "EXPIRED"):
                logger.info(f"Entry order {status}")
                self._cancel_pending_sl_tp(order_manager)
                self._reset_position_state()

        except Exception as e:
            logger.error(f"❌ Entry pending error: {e}", exc_info=True)

    def _on_entry_filled(self, order_manager, current_time: int) -> None:
        """Place SL and TP after entry is filled (full or partial).

        BUG #18 FIX: Queries exchange for actual filled quantity so SL/TP
        are placed for the real position size, not the originally requested qty.

        PARTIAL FILL FIX: If the entry was pre-placed with the full requested
        qty but only partially filled, the pre-placed SL/TP carry the WRONG
        (too-large) qty.  A reduce_only order whose qty exceeds the actual
        open position will be rejected by the exchange.  We detect this by
        comparing actual_qty vs. the originally stored entry_quantity, then
        cancel and re-place with the correct qty.
        """
        try:
            side = (
                self._trade_side.upper()
                if getattr(self, "_trade_side", None)
                else (
                    "LONG"
                    if (self.initial_entry_price and self.initial_sl_price
                        and self.initial_sl_price < self.initial_entry_price)
                    else "SHORT"
                )
            )

            sl_side = "SELL" if side == "LONG" else "BUY"

            # ── BUG #18 FIX: Get actual filled quantity from exchange ────
            actual_qty = self.entry_quantity  # fallback
            if self.entry_order_id:
                try:
                    GlobalRateLimiter.wait()
                    fill_info = order_manager.get_fill_details(self.entry_order_id)
                    if fill_info and fill_info.get("filled_qty", 0) > 0:
                        actual_qty = fill_info["filled_qty"]
                        if actual_qty != self.entry_quantity:
                            logger.info(
                                f"📐 Partial fill: requested={self.entry_quantity} "
                                f"filled={actual_qty} "
                                f"({fill_info.get('fill_pct', 0):.0f}%)"
                            )
                        # Update fill price if available
                        if fill_info.get("fill_price"):
                            self.initial_entry_price = fill_info["fill_price"]
                            logger.info(f"📐 Fill price updated: ${self.initial_entry_price:,.2f}")
                except Exception as e:
                    logger.warning(f"⚠️ Could not get fill details: {e}")

            # For SL/TP, also verify against exchange position
            qty = self._get_position_quantity(order_manager)
            if qty > 0 and qty != actual_qty:
                logger.info(f"📐 Exchange position qty={qty}, using exchange value")
                actual_qty = qty

            # ── PARTIAL FILL CORRECTION ───────────────────────────────────
            # If a pre-placed SL/TP was created with the originally requested
            # qty but the entry only partially filled, those orders carry the
            # WRONG qty.  Exchange will reject them as reduce_only qty > position.
            # Detect mismatch and replace with actual_qty BEFORE proceeding.
            _QTY_TOLERANCE = 0.0001  # BTC — ignore floating-point noise
            pre_placed_qty = self.entry_quantity   # qty used when pre-placing

            if abs(actual_qty - pre_placed_qty) > _QTY_TOLERANCE:
                logger.warning(
                    f"⚠️ Partial fill detected — pre-placed SL/TP used "
                    f"qty={pre_placed_qty:.4f} BTC but actual fill is "
                    f"{actual_qty:.4f} BTC.  Cancelling and re-placing."
                )

                # ── BUG FIX: update entry_quantity NOW so any early-return path
                # into _on_position_closed uses the correct filled qty, not the
                # stale pre-fill requested qty. ───────────────────────────────
                self.entry_quantity = actual_qty

                # Cancel wrong-qty SL
                if self.sl_order_id:
                    GlobalRateLimiter.wait()
                    cr = order_manager.cancel_order(self.sl_order_id)
                    if cr in (CancelResult.ALREADY_FILLED, CancelResult.PARTIAL_FILL):
                        logger.warning(
                            f"⚠️ SL {self.sl_order_id} fired "
                            f"({cr.value}) during partial-fill correction — "
                            f"cancelling TP orphan then treating position as closed"
                        )
                        # ── BUG FIX: cancel the TP BEFORE calling _on_position_closed.
                        # Without this the pre-placed TP (wrong qty) stays live on the
                        # exchange after _reset_position_state() zeros self.tp_order_id,
                        # creating an orphaned reduce_only order that can misfire. ────
                        if self.tp_order_id:
                            try:
                                GlobalRateLimiter.wait()
                                order_manager.cancel_order(self.tp_order_id)
                                logger.info(f"Orphaned TP {self.tp_order_id} cancelled (SL fired first)")
                            except Exception as _ce:
                                logger.warning(f"Could not cancel orphaned TP: {_ce}")
                            self.tp_order_id = None
                        self._on_position_closed(
                            side.lower(),
                            self.initial_sl_price or self.initial_entry_price,
                            current_time,
                        )
                        return
                    self.sl_order_id = None   # force fresh placement below

                # Cancel wrong-qty TP
                if self.tp_order_id:
                    GlobalRateLimiter.wait()
                    cr = order_manager.cancel_order(self.tp_order_id)
                    if cr in (CancelResult.ALREADY_FILLED, CancelResult.PARTIAL_FILL):
                        logger.warning(
                            f"⚠️ TP {self.tp_order_id} fired "
                            f"({cr.value}) during partial-fill correction — "
                            f"treating position as closed"
                        )
                        # SL was already cancelled successfully above (self.sl_order_id = None)
                        # so no orphan to clean up on that side.
                        self._on_position_closed(
                            side.lower(),
                            self.current_tp_price or self.initial_entry_price,
                            current_time,
                        )
                        return
                    self.tp_order_id = None   # force fresh placement below

            # Update stored quantity to actual fill
            self.entry_quantity = actual_qty

            time.sleep(2.0)
            
            # Place SL — always fresh (pre-placement removed)
            if not self.sl_order_id:
                GlobalRateLimiter.wait()
                sl_result = order_manager.place_stop_loss(
                    side=sl_side, quantity=actual_qty,
                    trigger_price=self.current_sl_price)
                if sl_result and "error" not in sl_result:
                    self.sl_order_id = sl_result.get("data", {}).get("order_id") or sl_result.get("order_id")
                    logger.info(f"✅ SL placed on fill: {self.sl_order_id} @ ${self.current_sl_price:,.2f} qty={actual_qty}")
                else:
                    logger.error(f"❌ SL placement failed on fill: {sl_result}")
                    # Emergency: try with _replace_sl_order which has fallback logic
                    side_str = "long" if side == "LONG" else "short"
                    self._replace_sl_order(order_manager, self.current_sl_price, side_str)

            # Place TP — always fresh
            if not self.tp_order_id:
                GlobalRateLimiter.wait()
                tp_result = order_manager.place_take_profit(
                    side=sl_side, quantity=actual_qty,
                    trigger_price=self.current_tp_price)
                if tp_result and "error" not in tp_result:
                    self.tp_order_id = tp_result.get("data", {}).get("order_id") or tp_result.get("order_id")
                    logger.info(f"✅ TP placed on fill: {self.tp_order_id} @ ${self.current_tp_price:,.2f} qty={actual_qty}")
                else:
                    logger.error(f"❌ TP placement failed on fill: {tp_result} — launching TP Guardian")
                    side_str = "long" if side == "LONG" else "short"
                    # Try _replace_tp_order first (3 immediate attempts); on exhaustion it
                    # will automatically launch the persistent guardian
                    self._replace_tp_order(order_manager, self.current_tp_price, side_str, qty=actual_qty)

            # Transition to active position
            self.state = "POSITION_ACTIVE"
            self.active_position = {
                "side"        : side.lower(),
                "entry_price" : self.initial_entry_price,
                "sl"          : self.current_sl_price,
                "tp"          : self.current_tp_price,
                "quantity"    : actual_qty,
                "entry_time"  : current_time,          # ← ADDED: CHoCH gate uses this
            }
            self.highest_price_reached = self.initial_entry_price
            self.lowest_price_reached  = self.initial_entry_price
            self.breakeven_moved       = False         # kept for Telegram compat, always False
            self._trail_activated      = False         # ← ADDED: True after first BOS post-entry

            logger.info(f"✅ Position active: {side} SL_ID={self.sl_order_id} TP_ID={self.tp_order_id}")

        except Exception as e:
            logger.error(f"❌ Entry fill handler error: {e}", exc_info=True)

    def _manage_active_position(
        self, data_manager, order_manager,
        current_price: float, current_time: int
    ) -> None:
        try:
            if not self.active_position or not self.initial_entry_price:
                return

            side = self.active_position.get("side", "long")

            # ── Track extremes ────────────────────────────────────────────────
            if self.highest_price_reached is None or current_price > self.highest_price_reached:
                self.highest_price_reached = current_price
            if self.lowest_price_reached is None or current_price < self.lowest_price_reached:
                self.lowest_price_reached = current_price

            # ── Track MFE / MAE ───────────────────────────────────────────────
            entry = self.initial_entry_price
            favorable = (current_price - entry) if side == "long" else (entry - current_price)
            adverse   = (entry - current_price) if side == "long" else (current_price - entry)
            if favorable > self.max_favorable_excursion:
                self.max_favorable_excursion = favorable
            if adverse > self.max_adverse_excursion:
                self.max_adverse_excursion = adverse

            # ── Position close check (throttled 5s) ──────────────────────────
            now_sec = current_time / 1000
            if (now_sec - self._last_pos_check_time) >= 5.0:
                self._check_position_closed(order_manager, current_price, current_time)
                self._last_pos_check_time = now_sec

            if self.state != "POSITION_ACTIVE":
                return

            # ── CHoCH — full exit check (every trailing interval) ─────────────
            if now_sec - self._last_sl_update_time >= config.TRAILING_SL_CHECK_INTERVAL:
                self._last_sl_update_time = now_sec          # ← ADD THIS LINE
                choch_detected = self._detect_choch_against_position(side, current_time)
                if choch_detected:
                    logger.warning(f"🔴 CHoCH detected against {side.upper()} — closing full position at market")
                    self._close_position_on_choch(order_manager, current_price, current_time)
                    return

                # ── Structure trailing SL ─────────────────────────────────────
                self._update_trailing_sl(data_manager, order_manager, current_price, current_time)
                self._last_sl_update_time = now_sec

            # ── SL/TP health check (every 60s) ────────────────────────────────
            if (now_sec - self._last_sl_health_check) >= 60:
                self._verify_sl_tp_health(order_manager)
                self._last_sl_health_check = now_sec

        except Exception as e:
            logger.error(f"❌ Position management error: {e}", exc_info=True)


    def _detect_choch_against_position(self, side: str, current_time: int) -> bool:
        """
        Returns True if a confirmed CHoCH has formed AGAINST the open position
        on the trading timeframe (5m) or the next higher timeframe (15m).

        Rules — institutional definition:
          LONG position  → bearish CHoCH = price closes below a prior swing low
                           after a bullish BOS was already in place.
          SHORT position → bullish CHoCH = price closes above a prior swing high
                           after a bearish BOS was already in place.

        Only CHoCH events timestamped AFTER position entry are considered.
        The most recent event wins; age cap = TRAIL_SWING_MAX_AGE_MS.
        """
        if not self.initial_entry_price:
            return False

        entry_time = self.active_position.get("entry_time", 0) if self.active_position else 0
        max_age_ms = getattr(config, 'TRAIL_SWING_MAX_AGE_MS', 4 * 3600 * 1000)
        valid_tfs   = {"5m", "15m"}

        for ms in reversed(list(self.market_structures)):
            if ms.structure_type != "CHoCH":
                continue
            if ms.timestamp <= entry_time:
                continue
            if current_time - ms.timestamp > max_age_ms:
                continue
            if ms.timeframe not in valid_tfs:
                continue
            if side == "long"  and ms.direction == "bearish":
                logger.info(
                    f"🔴 Bearish CHoCH confirmed @ {ms.price:.2f} [{ms.timeframe}] "
                    f"— {(current_time - ms.timestamp) / 60000:.1f}m ago — LONG invalidated"
                )
                return True
            if side == "short" and ms.direction == "bullish":
                logger.info(
                    f"🟢 Bullish CHoCH confirmed @ {ms.price:.2f} [{ms.timeframe}] "
                    f"— {(current_time - ms.timestamp) / 60000:.1f}m ago — SHORT invalidated"
                )
                return True
        return False

    def _close_position_on_choch(
        self, order_manager, current_price: float, current_time: int
    ) -> None:
        """
        Market-close the FULL position immediately on CHoCH confirmation.
        Cancels the live SL and TP orders first to avoid orphan fills,
        then fires a market order for the full quantity.
        Calls _on_position_closed() for PnL accounting and Telegram notification.
        """
        try:
            side = self.active_position.get("side", "long") if self.active_position else "long"
            qty  = self.entry_quantity if self.entry_quantity > 0 else config.MIN_POSITION_SIZE
            close_side = "SELL" if side == "long" else "BUY"

            # ── Cancel SL and TP to prevent orphan orders ─────────────────────
            for order_id, label in [(self.sl_order_id, "SL"), (self.tp_order_id, "TP")]:
                if not order_id:
                    continue
                try:
                    GlobalRateLimiter.wait()
                    cancel_result = order_manager.cancel_order(order_id)
                    if cancel_result in (CancelResult.ALREADY_FILLED, CancelResult.PARTIAL_FILL):
                        logger.warning(
                            f"⚠️ {label} {order_id} already {cancel_result.value} during CHoCH "
                            f"close — position may already be closed"
                        )
                    else:
                        logger.info(f"🗑️ {label} order {order_id} cancelled before CHoCH close")
                except Exception as ce:
                    logger.warning(f"⚠️ Could not cancel {label} {order_id} before CHoCH close: {ce}")

            self.sl_order_id = None
            self.tp_order_id = None

            # ── BUG FIX: Verify position is still open before placing market close.
            # If SL or TP fired during the cancel loop above, the position is already
            # closed. Placing an unrestricted market order at this point would OPEN
            # a new position in the wrong direction (market orders have reduce_only=False
            # by default). ────────────────────────────────────────────────────────────
            GlobalRateLimiter.wait()
            open_pos = order_manager.get_open_position()
            if open_pos is None:
                logger.warning("⚠️ Could not verify position status before CHoCH close — proceeding cautiously")
            elif open_pos.get("size", 0) <= 0:
                logger.info("✅ CHoCH close: position already closed (SL/TP fired during cancel) — skipping market order")
                self._on_position_closed(side, current_price, current_time)
                return
            else:
                # Use exchange-confirmed remaining qty for the close order
                confirmed_qty = open_pos.get("size", qty)
                if abs(confirmed_qty - qty) > 0.0001:
                    logger.info(
                        f"📐 CHoCH close: adjusting qty from {qty:.4f} "
                        f"to exchange-confirmed {confirmed_qty:.4f}"
                    )
                    qty = confirmed_qty

            # ── Market close ──────────────────────────────────────────────────
            GlobalRateLimiter.wait()
            result = order_manager.place_market_order(side=close_side, quantity=qty)

            actual_close_price = current_price
            if result and "error" not in result:
                try:
                    fill = order_manager.get_fill_details(
                        result.get("data", {}).get("order_id") or result.get("order_id", "")
                    )
                    if fill and fill.get("fill_price"):
                        actual_close_price = fill["fill_price"]
                except Exception:
                    pass
                logger.info(
                    f"✅ CHoCH market close executed: {close_side} qty={qty} "
                    f"@ ${actual_close_price:,.2f}"
                )
                send_telegram_message(
                    f"🔴 *CHoCH Exit — {side.upper()}*\n"
                    f"Structure reversed. Full position closed at market.\n"
                    f"Close price: `${actual_close_price:,.2f}`\n"
                    f"Qty: `{qty}`"
                )
            else:
                logger.critical(f"❌ CHoCH market close FAILED: {result}")
                send_telegram_message(
                    f"🚨 *CRITICAL — CHoCH close FAILED* ({side.upper()})\n"
                    f"Manual intervention required!\n`{result}`"
                )

            self._on_position_closed(side, actual_close_price, current_time)

        except Exception as e:
            logger.error(f"❌ CHoCH close error: {e}", exc_info=True)

    def _compute_atr_for_trailing(self, data_manager) -> float:
        """
        Wilder's ATR(SL_ATR_PERIOD) from 5m candles, falls back to 15m.
        Returns 0.0 if insufficient data.
        Caller MUST treat 0.0 as 'cannot trail'. No synthetic fallback, no hardcoded price.
        """
        period: int       = int(getattr(config, 'SL_ATR_PERIOD', 14))
        candles: List[Dict] = (
            data_manager.get_candles("5m") or
            data_manager.get_candles("15m") or
            []
        )
        needed: int = period + 1
        if len(candles) < needed:
            return 0.0

        recent = candles[-needed:]
        true_ranges: List[float] = []
        for i in range(1, len(recent)):
            try:
                h     = float(recent[i].get("h", 0) or 0)
                l     = float(recent[i].get("l", 0) or 0)
                prev_c = float(recent[i - 1].get("c", 0) or 0)
            except (TypeError, ValueError):
                continue
            if h <= 0 or l <= 0 or prev_c <= 0:
                continue
            true_ranges.append(max(h - l, abs(h - prev_c), abs(l - prev_c)))

        if len(true_ranges) < max(period // 2, 5):
            return 0.0

        # Wilder's smoothing: seed on first `period` TRs, then smooth the rest
        atr: float = sum(true_ranges[:period]) / period
        for tr in true_ranges[period:]:
            atr = (atr * (period - 1) + tr) / period
        return atr

    def _find_best_structure_sl(
        self,
        side          : str,
        current_price : float,
        current_time  : int,
        trail_buffer  : float,   # ATR × SL_ATR_BUFFER_MULT — applied beyond structure
        min_clearance : float,   # ATR × SL_MIN_CLEARANCE_ATR_MULT — min dist from price
    ) -> Optional[float]:
        """
        Collects every valid structure anchor (confirmed swings + active OBs + unmitigated FVGs)
        for the given side, applies trail_buffer, filters by clearance and strict improvement,
        then returns the MOST FAVORABLE candidate or None.

        ICT Rules per candidate (LONG):
          • anchor = swing_low.price  OR  ob.low  OR  fvg.bottom
          • anchor > current_sl_price            → strict ratchet improvement
          • current_price - anchor >= min_clearance → enough room; not stopping on noise
          • swing: s.confirmed AND age <= TRAIL_SWING_MAX_AGE_MS
          • OB:    ob.is_active() AND ob.high < current_price  (price cleared above OB)
          • FVG:   fvg.is_active() AND fvg.top < current_price (price cleared above FVG)
          candidate_sl = snap(anchor - trail_buffer)
          candidate_sl must still be > current_sl_price after tick-snap

        SHORT: exact mirror, direction-inverted throughout.
        Returns highest candidate for LONG, lowest for SHORT.
        """
        max_age_ms : int         = getattr(config, 'TRAIL_SWING_MAX_AGE_MS', 4 * 3600 * 1000)
        tick       : float       = config.TICK_SIZE
        candidates : List[Tuple[float, str]] = []

        def _snap(price: float) -> float:
            return round(price / tick) * tick

        if side == "long":

            # ── Confirmed swing lows ──────────────────────────────────────────
            for s in self.swing_lows:
                if not s.confirmed:
                    continue
                if current_time - s.timestamp > max_age_ms:
                    continue
                if s.price <= self.current_sl_price:
                    continue
                if current_price - s.price < min_clearance:
                    continue
                cand = _snap(s.price - trail_buffer)
                if cand > self.current_sl_price:
                    candidates.append((cand, f"swing_low@{s.price:.1f}[{s.timeframe}]"))

            # ── Active bullish OBs (price cleared above ob.high) ─────────────
            for ob in self.order_blocks_bull:
                if not ob.is_active(current_time):
                    continue
                if ob.high >= current_price:
                    continue
                if ob.low <= self.current_sl_price:
                    continue
                if current_price - ob.low < min_clearance:
                    continue
                cand = _snap(ob.low - trail_buffer)
                if cand > self.current_sl_price:
                    candidates.append((cand, f"ob_low@{ob.low:.1f}[str:{ob.strength:.0f}]"))

            # ── Unmitigated bullish FVGs (price cleared above fvg.top) ────────
            for fvg in self.fvgs_bull:
                if not fvg.is_active(current_time):
                    continue
                if fvg.top >= current_price:
                    continue
                if fvg.bottom <= self.current_sl_price:
                    continue
                if current_price - fvg.bottom < min_clearance:
                    continue
                cand = _snap(fvg.bottom - trail_buffer)
                if cand > self.current_sl_price:
                    candidates.append((cand, f"fvg_bottom@{fvg.bottom:.1f}[fill:{fvg.fill_percentage:.0%}]"))

            if not candidates:
                return None
            best_sl, best_desc = max(candidates, key=lambda x: x[0])
            logger.debug(
                f"Trail LONG — {len(candidates)} candidates, "
                f"best: {best_desc} → SL={best_sl:.1f}"
            )
            return best_sl

        else:  # short

            # ── Confirmed swing highs ─────────────────────────────────────────
            for s in self.swing_highs:
                if not s.confirmed:
                    continue
                if current_time - s.timestamp > max_age_ms:
                    continue
                if s.price >= self.current_sl_price:
                    continue
                if s.price - current_price < min_clearance:
                    continue
                cand = _snap(s.price + trail_buffer)
                if cand < self.current_sl_price:
                    candidates.append((cand, f"swing_high@{s.price:.1f}[{s.timeframe}]"))

            # ── Active bearish OBs (price cleared below ob.low) ──────────────
            for ob in self.order_blocks_bear:
                if not ob.is_active(current_time):
                    continue
                if ob.low >= current_price:
                    continue
                if ob.high >= self.current_sl_price:
                    continue
                if ob.high - current_price < min_clearance:
                    continue
                cand = _snap(ob.high + trail_buffer)
                if cand < self.current_sl_price:
                    candidates.append((cand, f"ob_high@{ob.high:.1f}[str:{ob.strength:.0f}]"))

            # ── Unmitigated bearish FVGs (price cleared below fvg.bottom) ─────
            for fvg in self.fvgs_bear:
                if not fvg.is_active(current_time):
                    continue
                if fvg.bottom <= current_price:
                    continue
                if fvg.top >= self.current_sl_price:
                    continue
                if fvg.top - current_price < min_clearance:
                    continue
                cand = _snap(fvg.top + trail_buffer)
                if cand < self.current_sl_price:
                    candidates.append((cand, f"fvg_top@{fvg.top:.1f}[fill:{fvg.fill_percentage:.0%}]"))

            if not candidates:
                return None
            best_sl, best_desc = min(candidates, key=lambda x: x[0])
            logger.debug(
                f"Trail SHORT — {len(candidates)} candidates, "
                f"best: {best_desc} → SL={best_sl:.1f}"
            )
            return best_sl

    def _update_trailing_sl(
        self,
        data_manager,
        order_manager,
        current_price : float,
        current_time  : int,
    ) -> None:
        """
        Pure ICT structural trailing SL — NO breakeven, NO phases, NO RR gates.

        Activation gate:
          Trail starts only after the FIRST confirmed BOS in the trade direction
          has formed after position entry (self._trail_activated flag).
          Until then, the initial SL sits untouched exactly where it was placed —
          below the FVG low (long) or above the FVG high (short).

        Once activated, every cycle:
          1. Compute ATR (Wilder's). ATR=0 → skip, never guess.
          2. Collect all valid structure anchors via _find_best_structure_sl():
             • Most recent higher-low swing point (long) / lower-high (short)
             • Low of newest unmitigated bullish FVG or OB (long)
             • High of newest unmitigated bearish FVG or OB (short)
          3. Apply ATR buffer beyond the anchor.
          4. Strict ratchet: SL can only move in profit direction, never back.
          5. Minimum move gate: ATR × SL_MIN_IMPROVEMENT_ATR_MULT (no micro-updates).
          6. Exchange update via _replace_sl_order() — commit state only on confirmed success.

        Full exit on CHoCH is handled separately in _detect_choch_against_position()
        called from _manage_active_position() before this method runs.
        """
        try:
            if not self.initial_entry_price or not self.initial_sl_price:
                return
            if not self.active_position:
                return

            side  = self.active_position.get("side", "long")
            entry = self.initial_entry_price
            risk  = abs(entry - self.initial_sl_price)
            if risk <= 0:
                return

            entry_time = self.active_position.get("entry_time", 0)

            # ── Activation gate: wait for first BOS in trade direction ────────
            if not self._trail_activated:
                required_direction = "bullish" if side == "long" else "bearish"
                bos_confirmed = any(
                    ms.structure_type == "BOS"
                    and ms.direction   == required_direction
                    and ms.confirmed
                    and ms.timestamp   >  entry_time
                    for ms in self.market_structures
                )
                if not bos_confirmed:
                    logger.debug(
                        f"⏳ Trail inactive — waiting for first {required_direction.upper()} "
                        f"BOS after entry to activate structural trailing"
                    )
                    return
                self._trail_activated = True
                logger.info(
                    f"✅ Trail ACTIVATED — {required_direction.upper()} BOS confirmed "
                    f"after entry. SL will now trail structure."
                )

            # ── ATR — mandatory, no fallback ──────────────────────────────────
            atr = self._compute_atr_for_trailing(data_manager)
            if atr <= 0:
                logger.warning("⚠️ ATR unavailable — trailing SL skipped this cycle")
                return

            # ── Config multipliers ────────────────────────────────────────────
            buf_mult     = getattr(config, 'SL_ATR_BUFFER_MULT',          0.5)
            clear_mult   = getattr(config, 'SL_MIN_CLEARANCE_ATR_MULT',   1.0)
            improve_mult = getattr(config, 'SL_MIN_IMPROVEMENT_ATR_MULT', 0.1)
            tick         = config.TICK_SIZE

            trail_buffer  = atr * buf_mult
            min_clearance = atr * clear_mult
            min_move      = atr * improve_mult

            # ── Find best structural anchor ───────────────────────────────────
            new_sl = self._find_best_structure_sl(
                side, current_price, current_time, trail_buffer, min_clearance
            )
            if new_sl is None:
                logger.debug(
                    f"📍 No structure anchor found for {side.upper()} trail this cycle "
                    f"(price={current_price:.1f} cur_sl={self.current_sl_price:.1f})"
                )
                return

            # ── Ratchet: must strictly improve ───────────────────────────────
            is_improvement = (
                (side == "long"  and new_sl > self.current_sl_price) or
                (side == "short" and new_sl < self.current_sl_price)
            )
            if not is_improvement:
                return

            # ── Minimum meaningful move ───────────────────────────────────────
            if abs(new_sl - self.current_sl_price) < min_move:
                return

            # ── Profit locked % (informational only) ─────────────────────────
            if side == "long"  and new_sl > entry:
                self.profit_locked_pct = (new_sl - entry) / risk
            elif side == "short" and new_sl < entry:
                self.profit_locked_pct = (entry - new_sl) / risk

            # ── Current RR (informational) ────────────────────────────────────
            r_initial  = abs(self.initial_entry_price - self.initial_sl_price) or 1.0
            rr_display = (
                (current_price - self.initial_entry_price) / r_initial
                if side == "long" else
                (self.initial_entry_price - current_price) / r_initial
            )

            old_sl = self.current_sl_price

            # ── Exchange update — commit state ONLY on confirmed success ───────
            if self._replace_sl_order(order_manager, new_sl, side):
                self.current_sl_price = new_sl
                logger.info(
                    f"📈 SL trailed [structure swing/OB/FVG]: "
                    f"${old_sl:,.1f} → ${new_sl:,.1f} | "
                    f"ATR={atr:.1f}  buf={trail_buffer:.1f}  "
                    f"clear={min_clearance:.1f}  RR={rr_display:.2f}R"
                )
                msg = format_trail_update(
                    side=side,
                    old_sl=old_sl,
                    new_sl=new_sl,
                    entry_price=self.initial_entry_price,
                    current_price=current_price,
                    trail_reason="structure trail — swing/OB/FVG",
                    current_rr=rr_display,
                    profit_locked_pct=self.profit_locked_pct,
                    breakeven_moved=False,
                )
                send_telegram_message(msg)
            else:
                logger.warning(
                    f"⚠️ SL trail failed — state unchanged "
                    f"(sl stays at {self.current_sl_price:.1f})"
                )

        except Exception as e:
            logger.error(f"❌ Trailing SL error: {e}", exc_info=True)


    def _replace_sl_order(self, order_manager, new_sl: float, side: str) -> bool:
        """Cancel old SL and place new one. Returns True on success.

        CRITICAL: Never leaves position naked.
        Step 1: Cancel old SL (check for ALREADY_FILLED)
        Step 2: Place new SL using stored entry_quantity (no extra API call)
        Step 3: EMERGENCY — if new SL fails after cancel succeeded:
                a) Try emergency SL at MAX_SL_DISTANCE_PCT
                b) Last resort: market close position
        """
        old_sl_id = self.sl_order_id
        cancel_succeeded = False

        try:
            from order_manager import CancelResult

            # ── Step 1: Cancel old SL ─────────────────────────────────
            if old_sl_id:
                GlobalRateLimiter.wait()
                cancel_result = order_manager.cancel_order(old_sl_id)

                if cancel_result in (CancelResult.ALREADY_FILLED,
                                     CancelResult.PARTIAL_FILL):
                    logger.warning(
                        f"⚠️ SL {old_sl_id} already {cancel_result.value} "
                        f"— position closed by exchange, NOT placing new SL"
                    )
                    return False
                cancel_succeeded = True
                # SUCCESS, NOT_FOUND → old SL is gone, place new one

            # ── Step 2: Place new SL ──────────────────────────────────
            sl_side = "SELL" if side == "long" else "BUY"
            qty = self.entry_quantity if self.entry_quantity > 0 else config.MIN_POSITION_SIZE

            GlobalRateLimiter.wait()
            result = order_manager.place_stop_loss(
                side=sl_side, quantity=qty, trigger_price=new_sl)

            if result and "error" not in result:
                self.sl_order_id = result.get("data", {}).get("order_id") or result.get("order_id")
                logger.info(f"✅ SL updated to {new_sl:.2f} qty={qty}")
                return True

            # ── Step 3: EMERGENCY — SL placement failed ───────────────
            logger.error(f"❌ SL replacement failed: {result}")

            if cancel_succeeded:
                # Position is NAKED — old SL cancelled but new one failed
                logger.critical("🚨 POSITION NAKED — attempting emergency SL")

                # 3a) Try emergency wide SL at MAX_SL_DISTANCE_PCT
                entry = self.initial_entry_price or 0
                if entry > 0:
                    max_dist = entry * config.MAX_SL_DISTANCE_PCT
                    if side == "long":
                        emergency_sl = round((entry - max_dist) / config.TICK_SIZE) * config.TICK_SIZE
                    else:
                        emergency_sl = round((entry + max_dist) / config.TICK_SIZE) * config.TICK_SIZE

                    GlobalRateLimiter.wait()
                    emg_result = order_manager.place_stop_loss(
                        side=sl_side, quantity=qty, trigger_price=emergency_sl)

                    if emg_result and "error" not in emg_result:
                        self.sl_order_id = emg_result.get("data", {}).get("order_id") or emg_result.get("order_id")
                        logger.warning(f"🆘 Emergency SL placed at {emergency_sl:.2f} (wide)")
                        send_telegram_message(
                            f"🚨 <b>EMERGENCY SL</b>\n"
                            f"Normal SL failed — placed wide SL at ${emergency_sl:,.2f}\n"
                            f"Original target was ${new_sl:,.2f}")
                        return False  # SL placed but not at desired price

                # 3b) Last resort: market close
                logger.critical("🚨 EMERGENCY SL ALSO FAILED — market closing position")
                try:
                    close_side = "SELL" if side == "long" else "BUY"
                    GlobalRateLimiter.wait()
                    close_result = order_manager.place_market_order(
                        side=close_side, quantity=qty)
                    if close_result and "error" not in close_result:
                        logger.critical("🆘 Position market-closed as emergency measure")
                        send_telegram_message(
                            "🚨 <b>EMERGENCY MARKET CLOSE</b>\n"
                            "SL placement failed repeatedly — position closed at market")
                    else:
                        logger.critical(f"🚨 MARKET CLOSE ALSO FAILED: {close_result}")
                        send_telegram_message(
                            "🚨🚨 <b>CRITICAL: ALL SL ATTEMPTS FAILED</b>\n"
                            "Position may be unprotected! Manual intervention needed!")
                except Exception as mc_err:
                    logger.critical(f"🚨 Market close exception: {mc_err}")

            return False

        except Exception as e:
            logger.error(f"❌ Replace SL error: {e}", exc_info=True)
            return False

    def _replace_tp_order(self, order_manager, new_tp: float, side: str,
                          qty: float = None) -> bool:
        """Cancel existing TP and place a new one.  Returns True on immediate success.

        Unlike _replace_sl_order, a TP failure is NOT an emergency —
        the SL still protects capital.  On exhausted retries, a TP Guardian
        background thread is launched that keeps trying indefinitely.
        """
        MAX_TP_RETRIES = 3
        tp_side = "SELL" if side == "long" else "BUY"
        use_qty = qty if (qty and qty > 0) else (
            self.entry_quantity if self.entry_quantity > 0 else config.MIN_POSITION_SIZE
        )

        old_tp_id = self.tp_order_id

        try:
            # ── Step 1: Stop any running guardian (TP price may have changed) ──
            self._stop_tp_guardian()

            # ── Step 2: Cancel existing TP if present ─────────────────
            if old_tp_id:
                GlobalRateLimiter.wait()
                cancel_result = order_manager.cancel_order(old_tp_id)

                if cancel_result in (CancelResult.ALREADY_FILLED,
                                     CancelResult.PARTIAL_FILL):
                    logger.info(
                        f"TP {old_tp_id} already {cancel_result.value} — "
                        f"position closed, not placing new TP"
                    )
                    return False   # caller should treat position as closed
                self.tp_order_id = None

            # ── Step 3: Place new TP (immediate attempts) ─────────────
            for attempt in range(MAX_TP_RETRIES):
                GlobalRateLimiter.wait()
                result = order_manager.place_take_profit(
                    side=tp_side, quantity=use_qty, trigger_price=new_tp)

                if result and "error" not in result:
                    self.tp_order_id = (result.get("data", {}).get("order_id")
                                        or result.get("order_id"))
                    logger.info(
                        f"\u2705 TP {'placed' if not old_tp_id else 'replaced'} \u2192 "
                        f"{self.tp_order_id} @ ${new_tp:,.2f} qty={use_qty}"
                    )
                    return True

                logger.warning(
                    f"\u26a0\ufe0f TP placement attempt {attempt + 1}/{MAX_TP_RETRIES} "
                    f"failed: {result}"
                )
                if attempt < MAX_TP_RETRIES - 1:
                    time.sleep(6.0 * (attempt + 1))   # 6s, 12s

            # ── Step 4: Immediate retries exhausted — hand off to guardian ──
            logger.error(
                f"\u274c TP placement failed after {MAX_TP_RETRIES} immediate attempts "
                f"@ ${new_tp:,.2f}. Launching TP Guardian — SL still protects capital."
            )
            send_telegram_message(
                f"\u26a0\ufe0f <b>TP PLACEMENT DELAYED</b>\n"
                f"Tried {MAX_TP_RETRIES}x @ ${new_tp:,.2f}\n"
                f"SL active at ${self.current_sl_price:,.2f}\n"
                f"\U0001f6e1\ufe0f TP Guardian launched — retrying in background..."
            )
            self._start_tp_guardian(order_manager, new_tp, side, use_qty)
            return False

        except Exception as e:
            logger.error(f"\u274c _replace_tp_order error: {e}", exc_info=True)
            try:
                self._start_tp_guardian(order_manager, new_tp, side, use_qty)
            except Exception:
                pass
            return False

    def _verify_sl_tp_health(self, order_manager) -> None:
        """Periodic health check: verify SL/TP orders still exist on exchange.
        Called every 60s from _manage_active_position.

        Handles all status transitions:
          FILLED       → position closed (SL/TP fired) — trigger _on_position_closed
          PARTIAL_FILL → partial TP/SL executed → reconcile qty, re-place with remainder
          CANCELLED    → order disappeared → re-place immediately
          UNKNOWN      → transient API issue → wait 3 consecutive before re-placing
        """
        try:
            if not self.sl_order_id and not self.tp_order_id:
                return

            side    = self.active_position.get("side", "long") if self.active_position else "long"
            sl_side = "SELL" if side == "long" else "BUY"
            qty     = self.entry_quantity if self.entry_quantity > 0 else config.MIN_POSITION_SIZE

            # Track consecutive UNKNOWN counts
            if not hasattr(self, '_sl_unknown_count'):
                self._sl_unknown_count = 0
            if not hasattr(self, '_tp_unknown_count'):
                self._tp_unknown_count = 0

            # ── Check SL ──────────────────────────────────────────────────
            if self.sl_order_id:
                sl_status = order_manager.get_order_status_safe(self.sl_order_id)

                if sl_status == "FILLED":
                    # SL fired — position closed on the losing side
                    logger.warning(f"⚠️ Health check: SL {self.sl_order_id} FILLED — closing position")
                    self._sl_unknown_count = 0
                    current_price = (self.active_position.get("sl") or
                                     self.current_sl_price or 0)
                    self._on_position_closed(side, current_price, int(time.time() * 1000))
                    return  # position is gone; nothing more to check

                elif sl_status == "CANCELLED":
                    logger.warning(f"⚠️ SL {self.sl_order_id} is CANCELLED — re-placing")
                    self._sl_unknown_count = 0
                    GlobalRateLimiter.wait()
                    sl_result = order_manager.place_stop_loss(
                        side=sl_side, quantity=qty,
                        trigger_price=self.current_sl_price)
                    if sl_result and "error" not in sl_result:
                        self.sl_order_id = sl_result.get("data", {}).get("order_id") or sl_result.get("order_id")
                        logger.info(f"✅ SL re-placed: {self.sl_order_id}")
                    else:
                        logger.error(f"❌ SL re-placement failed: {sl_result}")

                elif sl_status == "UNKNOWN":
                    self._sl_unknown_count += 1
                    if self._sl_unknown_count >= 3:
                        logger.warning(f"⚠️ SL {self.sl_order_id} UNKNOWN for {self._sl_unknown_count} checks — re-placing")
                        self._sl_unknown_count = 0
                        GlobalRateLimiter.wait()
                        sl_result = order_manager.place_stop_loss(
                            side=sl_side, quantity=qty,
                            trigger_price=self.current_sl_price)
                        if sl_result and "error" not in sl_result:
                            self.sl_order_id = sl_result.get("data", {}).get("order_id") or sl_result.get("order_id")
                            logger.info(f"✅ SL re-placed after persistent UNKNOWN: {self.sl_order_id}")
                        else:
                            logger.error(f"❌ SL re-placement failed: {sl_result}")
                    else:
                        logger.debug(f"SL status UNKNOWN ({self._sl_unknown_count}/3) — waiting before re-place")
                else:
                    self._sl_unknown_count = 0  # PENDING / active — all good

            elif self.state == "POSITION_ACTIVE":
                # No SL order ID but position is active — critical
                logger.critical("🚨 No SL order ID for active position — placing emergency SL")
                GlobalRateLimiter.wait()
                sl_result = order_manager.place_stop_loss(
                    side=sl_side, quantity=qty,
                    trigger_price=self.current_sl_price)
                if sl_result and "error" not in sl_result:
                    self.sl_order_id = sl_result.get("data", {}).get("order_id") or sl_result.get("order_id")

            # ── Check TP ──────────────────────────────────────────────────
            if self.tp_order_id:
                tp_status = order_manager.get_order_status_safe(self.tp_order_id)

                if tp_status == "FILLED":
                    # TP fired — position closed on the winning side
                    logger.info(f"✅ Health check: TP {self.tp_order_id} FILLED — closing position")
                    self._tp_unknown_count = 0
                    current_price = (self.active_position.get("tp") or
                                     self.current_tp_price or 0)
                    self._on_position_closed(side, current_price, int(time.time() * 1000))
                    return

                elif tp_status == "PARTIAL_FILL":
                    # ── BUG FIX: TP partially executed ────────────────────────
                    # The most dangerous sub-case: part of the position was closed
                    # at TP but the bot still tracks the full qty.  The SL still
                    # carries the ORIGINAL full qty → it will be REJECTED by the
                    # exchange (reduce_only qty > remaining position) if it fires.
                    #
                    # Correct procedure:
                    #   1. Find remaining open position qty from exchange
                    #   2. Cancel old SL (wrong qty) and re-place with remainder
                    #   3. Cancel old TP (partially filled) and re-place with remainder
                    #   4. Update self.entry_quantity to the remainder
                    logger.warning(
                        f"⚠️ TP {self.tp_order_id} PARTIALLY filled — "
                        f"reconciling SL and TP qty"
                    )
                    self._tp_unknown_count = 0

                    # Get remaining position from exchange (source of truth)
                    GlobalRateLimiter.wait()
                    pos = order_manager.get_open_position()
                    remaining_qty = 0.0
                    if pos and pos.get("size", 0) > 0:
                        remaining_qty = pos["size"]

                    if remaining_qty <= 0:
                        # All filled despite PARTIAL_FILL status — treat as closed
                        logger.info("TP partial status but no remaining position — treating as closed")
                        self._on_position_closed(
                            side, self.current_tp_price or 0, int(time.time() * 1000))
                        return

                    logger.info(
                        f"📐 Partial TP: original qty={qty:.4f} remaining={remaining_qty:.4f} BTC"
                    )
                    # Update stored qty
                    self.entry_quantity = remaining_qty

                    # Cancel & re-place SL with correct remaining qty
                    if self.sl_order_id:
                        GlobalRateLimiter.wait()
                        cr = order_manager.cancel_order(self.sl_order_id)
                        if cr in (CancelResult.ALREADY_FILLED, CancelResult.PARTIAL_FILL):
                            logger.warning(f"SL fired during TP partial reconcile — closing position")
                            self._on_position_closed(
                                side, self.current_sl_price or 0, int(time.time() * 1000))
                            return
                        self.sl_order_id = None
                    GlobalRateLimiter.wait()
                    sl_result = order_manager.place_stop_loss(
                        side=sl_side, quantity=remaining_qty,
                        trigger_price=self.current_sl_price)
                    if sl_result and "error" not in sl_result:
                        self.sl_order_id = sl_result.get("data", {}).get("order_id") or sl_result.get("order_id")
                        logger.info(f"✅ SL re-placed with corrected qty={remaining_qty:.4f}: {self.sl_order_id}")
                    else:
                        logger.error(f"❌ SL re-place after partial TP failed: {sl_result}")
                        # Escalate to _replace_sl_order which has emergency fallback
                        self._replace_sl_order(order_manager, self.current_sl_price, side)

                    # Cancel & re-place TP with remaining qty
                    old_tp_id = self.tp_order_id
                    self.tp_order_id = None
                    GlobalRateLimiter.wait()
                    cr = order_manager.cancel_order(old_tp_id)  # might already be gone
                    side_str = "long" if side == "long" else "short"
                    self._replace_tp_order(order_manager, self.current_tp_price,
                                           side_str, qty=remaining_qty)

                    send_telegram_message(
                        f"⚠️ <b>Partial TP Fill Detected</b>\n"
                        f"Filled: {qty - remaining_qty:.4f} BTC @ ${self.current_tp_price:,.2f}\n"
                        f"Remaining: {remaining_qty:.4f} BTC\n"
                        f"SL and TP re-placed with corrected qty."
                    )

                elif tp_status == "CANCELLED":
                    self._tp_unknown_count = 0
                    logger.warning(f"⚠️ TP {self.tp_order_id} is CANCELLED — re-placing")
                    side_str = "long" if side == "long" else "short"
                    self.tp_order_id = None
                    self._replace_tp_order(order_manager, self.current_tp_price,
                                           side_str, qty=qty)

                elif tp_status == "UNKNOWN":
                    self._tp_unknown_count += 1
                    if self._tp_unknown_count >= 3:
                        logger.warning(f"⚠️ TP {self.tp_order_id} UNKNOWN for {self._tp_unknown_count} checks — re-placing")
                        self._tp_unknown_count = 0
                        side_str = "long" if side == "long" else "short"
                        self.tp_order_id = None
                        self._replace_tp_order(order_manager, self.current_tp_price,
                                               side_str, qty=qty)
                    else:
                        logger.debug(f"TP status UNKNOWN ({self._tp_unknown_count}/3) — waiting before re-place")
                else:
                    self._tp_unknown_count = 0  # PENDING / active — all good

            elif self.state == "POSITION_ACTIVE":
                # No TP order ID but position is active — re-place
                if self._tp_guardian_active:
                    logger.debug("No TP order ID but guardian is already running — skipping health re-place")
                else:
                    logger.warning("⚠️ No TP order ID for active position — re-placing")
                    side_str = "long" if side == "long" else "short"
                    self._replace_tp_order(order_manager, self.current_tp_price,
                                           side_str, qty=qty)

        except Exception as e:
            logger.error(f"❌ SL/TP health check error: {e}", exc_info=True)

    # ==================================================================
    # CHECK POSITION CLOSED
    # ==================================================================

    def _check_position_closed(self, order_manager, current_price: float,
                                current_time: int) -> None:
        """Check if SL or TP triggered (position closed by exchange).

        Also detects PARTIAL position close: if exchange position qty is
        significantly less than self.entry_quantity, a partial TP has fired.
        In that case we update entry_quantity and re-correct the SL qty.
        The full reconciliation (re-place SL+TP) is handled by
        _verify_sl_tp_health which runs every 60s.
        """
        try:
            side = self.active_position.get("side", "long")

            GlobalRateLimiter.wait()
            pos = order_manager.get_open_position()

            # get_open_position returns None on API error — don't act on it
            if pos is None:
                return

            exchange_size = pos.get("size", 0) if pos.get("side") is not None else 0
            has_position  = exchange_size > 0

            if not has_position and self.state == "POSITION_ACTIVE":
                # Position fully closed
                self._on_position_closed(side, current_price, current_time)
                return

            # ── Partial close detection ───────────────────────────────────────
            # If the exchange shows significantly less qty than we think we hold,
            # a partial TP (or partial SL) has fired.  We update entry_quantity
            # so SL/TP health check can re-place both orders with the right qty.
            # Threshold: 0.0001 BTC — below this is floating-point noise.
            _QTY_TOL = 0.0001
            if (has_position
                    and self.entry_quantity > 0
                    and (self.entry_quantity - exchange_size) > _QTY_TOL):
                logger.warning(
                    f"⚠️ Partial position close detected: "
                    f"expected {self.entry_quantity:.4f} BTC, "
                    f"exchange shows {exchange_size:.4f} BTC. "
                    f"Updating entry_quantity — health check will correct SL/TP."
                )
                self.entry_quantity = exchange_size
                # Active position dict must also reflect the corrected qty
                if self.active_position:
                    self.active_position["quantity"] = exchange_size

        except Exception as e:
            logger.error(f"Position check error: {e}", exc_info=True)

    def _on_position_closed(self, side: str, close_price: float, current_time: int) -> None:
        try:
            entry = self.initial_entry_price or close_price
            sl    = self.initial_sl_price or 0.0
            tp    = self.initial_tp_price or 0.0
            qty   = self.entry_quantity or (
                self.active_position.get("quantity", 0) if self.active_position else 0)

            # ── Try to get actual fill price from SL/TP orders ──────────
            actual_exit = close_price
            exit_source = "market_price"

            if self._order_manager:
                om = self._order_manager
                for oid, label in [(self.sl_order_id, "SL"),
                                    (self.tp_order_id, "TP")]:
                    if not oid:
                        continue
                    try:
                        fill = om.get_fill_details(oid)
                        if fill and fill.get("fill_price") and \
                           fill.get("status", "").upper() in ("EXECUTED", "FILLED",
                                                               "PARTIALLY_EXECUTED",
                                                               "PARTIALLY_FILLED"):
                            actual_exit = fill["fill_price"]
                            exit_source = f"{label}_fill"
                            if fill.get("filled_qty", 0) > 0:
                                qty = fill["filled_qty"]
                            logger.info(f"📊 Exit price from {label} fill: "
                                        f"${actual_exit:,.2f}")
                            break
                    except Exception:
                        pass

            close_price = actual_exit

            # ── Industry-grade P&L ────────────────────────────────────
            # Gross PnL: direct price delta × qty (qty = actual BTC on exchange)
            # NO multiplication by LEVERAGE — the leverage is already reflected
            # in the position size chosen during entry.
            if side == "long":
                price_delta = close_price - entry
            else:
                price_delta = entry - close_price

            gross_pnl = price_delta * qty

            # Commission (both legs assumed taker)
            fee_rate   = getattr(config, "COMMISSION_RATE", 0.00055)
            commission = (entry + close_price) * qty * fee_rate
            pnl_dollar = gross_pnl - commission

            won = price_delta > 0

            # ── Determine close reason from price proximity ───────────
            # (proximity test, not PnL sign — breakeven SL is still a SL_HIT)
            if sl and tp:
                dist_to_sl = abs(close_price - sl)
                dist_to_tp = abs(close_price - tp)
                reason = "SL_HIT" if dist_to_sl <= dist_to_tp else "TP_HIT"
            elif won:
                reason = "TP_HIT"
            else:
                reason = "SL_HIT"

            # ── Update strategy stats ─────────────────────────────────
            self.total_exits += 1
            self.total_pnl   += pnl_dollar
            self.daily_pnl   += pnl_dollar
            if won:
                self.winning_trades     += 1
                self.consecutive_losses  = 0
            else:
                self.consecutive_losses += 1

            total = self.total_exits
            wr    = self.winning_trades / total * 100 if total > 0 else 0

            # ── Record in risk_manager (canonical stats) ──────────────
            # Pass pnl_override so risk_manager uses the same net P&L value.
            if self._risk_manager:
                self._risk_manager.record_trade(
                    side=side.upper(), entry_price=entry,
                    exit_price=close_price, quantity=qty,
                    reason=reason, pnl_override=pnl_dollar)

            # ── Performance metrics ───────────────────────────────────
            notional_at_entry = entry * qty
            margin_used       = notional_at_entry / config.LEVERAGE if config.LEVERAGE else notional_at_entry
            return_on_margin  = (pnl_dollar / margin_used * 100) if margin_used > 0 else 0.0

            risk     = abs(entry - sl) if sl else 1.0
            pnl_r    = price_delta / risk if risk > 0 else 0.0
            mfe_r    = self.max_favorable_excursion / risk if risk > 0 else 0.0
            mae_r    = self.max_adverse_excursion / risk if risk > 0 else 0.0

            # ── Telegram close notification ───────────────────────────
            msg = format_position_close(
                side=side,
                entry_price=entry,
                close_price=close_price,
                sl_price=sl,
                tp_price=tp,
                pnl=pnl_dollar,
                close_reason=reason,
                entry_score=self.entry_score,
                entry_reasons=self.entry_reasons,
                breakeven_moved=self.breakeven_moved,
                max_favorable=self.max_favorable_excursion,
                max_adverse=self.max_adverse_excursion,
                total_pnl=self.total_pnl,
                win_rate=wr,
                total_trades=total,
                consecutive_losses=self.consecutive_losses,
            )
            send_telegram_message(msg)

            logger.info(
                f"📊 Trade closed: {side.upper()} | "
                f"Gross: ${gross_pnl:+.4f} | Commission: ${commission:.4f} | "
                f"Net: ${pnl_dollar:+.2f} | "
                f"Return on margin: {return_on_margin:+.2f}% | "
                f"R: {pnl_r:+.2f} | MFE: {mfe_r:.2f}R | MAE: {mae_r:.2f}R | "
                f"Reason: {reason} | Exit: {exit_source} | "
                f"WR: {wr:.1f}%"
            )

            self._reset_position_state()

        except Exception as e:
            logger.error(f"❌ Position close handler error: {e}", exc_info=True)

    # ==================================================================
    # TP GUARDIAN — background persistent TP placement
    # ==================================================================

    def _start_tp_guardian(
        self,
        order_manager,
        tp_price: float,
        side: str,
        qty: float,
    ) -> None:
        """
        Launch a daemon thread that keeps retrying TP placement until it either
        succeeds or the position closes.

        Design:
          • If a guardian is already running, stop it and start a fresh one
            (e.g. TP price changed due to trailing).
          • Exponential back-off: 15s → 22s → 33s … capped at 120s.
          • Stops automatically when:
              - TP order is successfully placed (self.tp_order_id is set), OR
              - Position is no longer active (state != "POSITION_ACTIVE"), OR
              - Stop event is signalled externally.
          • Never raises; all exceptions are caught and logged.
        """
        # Stop any existing guardian first
        self._stop_tp_guardian()

        self._tp_guardian_stop.clear()
        self._tp_guardian_active = True

        tp_side = "SELL" if side == "long" else "BUY"

        def _guardian_worker():
            attempt    = 0
            sleep_time = 15.0
            try:
                while not self._tp_guardian_stop.is_set():
                    # Exit conditions: position gone or TP already placed
                    if self.state != "POSITION_ACTIVE":
                        logger.info("TP Guardian: position no longer active — stopping")
                        break
                    if self.tp_order_id:
                        logger.info(f"TP Guardian: TP already placed "
                                    f"({self.tp_order_id}) — stopping")
                        break

                    attempt += 1
                    logger.info(f"🔄 TP Guardian attempt {attempt} — "
                                f"placing TP @ ${tp_price:,.2f} qty={qty}")

                    result = order_manager.place_take_profit(
                        side=tp_side, quantity=qty, trigger_price=tp_price)

                    if result and "error" not in result:
                        order_id = (result.get("data", {}).get("order_id")
                                    or result.get("order_id"))
                        if order_id:
                            self.tp_order_id = order_id
                            logger.info(
                                f"✅ TP Guardian: TP secured "
                                f"{order_id} @ ${tp_price:,.2f} "
                                f"(attempt {attempt})"
                            )
                            from telegram_notifier import send_telegram_message
                            send_telegram_message(
                                f"✅ <b>TP SECURED</b> (Guardian attempt {attempt})\n"
                                f"Order: <code>{order_id}</code>\n"
                                f"Price: ${tp_price:,.2f}\n"
                                f"SL still active at ${self.current_sl_price:,.2f}"
                            )
                            break
                    else:
                        logger.warning(
                            f"⚠️ TP Guardian attempt {attempt} failed: {result} — "
                            f"retrying in {sleep_time:.0f}s"
                        )

                    # Wait with stop-event awareness
                    self._tp_guardian_stop.wait(timeout=sleep_time)
                    sleep_time = min(sleep_time * 1.5, 120.0)   # cap at 2 min

            except Exception as e:
                logger.error(f"❌ TP Guardian crashed: {e}", exc_info=True)
            finally:
                self._tp_guardian_active = False
                logger.info("TP Guardian thread exited")

        self._tp_guardian_thread = threading.Thread(
            target=_guardian_worker,
            name="tp-guardian",
            daemon=True,
        )
        self._tp_guardian_thread.start()
        logger.info(
            f"🛡️ TP Guardian started — will keep retrying TP @ "
            f"${tp_price:,.2f} until placed or position closes"
        )

    def _stop_tp_guardian(self) -> None:
        """Signal and join the TP guardian thread (non-blocking — join with 2s timeout)."""
        if self._tp_guardian_thread and self._tp_guardian_thread.is_alive():
            self._tp_guardian_stop.set()
            self._tp_guardian_thread.join(timeout=2.0)
        self._tp_guardian_active = False
        self._tp_guardian_stop.clear()
        self._tp_guardian_thread = None

    # ==================================================================
    # UTILITIES
    # ==================================================================

    def _reset_position_state(self) -> None:
        self._stop_tp_guardian()                    # ← stop guardian on position close
        self.state                   = "READY"
        self.active_position         = None
        self.entry_order_id          = None
        self.sl_order_id             = None
        self.tp_order_id             = None
        self._pending_ctx            = None
        self.initial_entry_price     = None
        self.initial_sl_price        = None
        self.initial_tp_price        = None
        self.current_sl_price        = None
        self.current_tp_price        = None
        self.highest_price_reached   = None
        self.lowest_price_reached    = None
        self.breakeven_moved         = False
        self._trail_activated        = False           # ← ADDED: reset trail gate
        self.profit_locked_pct       = 0.0
        self.entry_pending_start     = None
        self.entry_score             = 0.0
        self.entry_reasons           = []
        self.entry_quantity          = 0.0
        self.max_favorable_excursion = 0.0
        self.max_adverse_excursion   = 0.0
        self._trade_side             = None

    def _cancel_pending_sl_tp(self, order_manager) -> None:
        """Cancel pre-placed SL and TP orders when entry is cancelled with no fill.

        Called in every path where the entry order itself was cancelled/rejected
        and no position was opened. Prevents orphaned SL/TP orders sitting on the
        exchange with no corresponding position.
        """
        for order_id, label in [(self.sl_order_id, 'SL'), (self.tp_order_id, 'TP')]:
            if not order_id:
                continue
            try:
                GlobalRateLimiter.wait()
                result = order_manager.cancel_order(order_id)
                logger.info(f"🗑️ Cancelled orphaned pre-placed {label} {order_id} (entry not filled): {result}")
            except Exception as e:
                logger.warning(f"Could not cancel orphaned {label} {order_id}: {e}")
        # Clear here so reset_position_state doesn't attempt to use stale IDs
        self.sl_order_id = None
        self.tp_order_id = None

    def _get_position_quantity(self, order_manager) -> float:
        """Get current position quantity from exchange.

        BUG #17 FIX: Uses order_manager.get_open_position() for robust
        multi-field parsing instead of raw API with single field name.
        """
        try:
            pos = order_manager.get_open_position()
            if pos and pos.get("size", 0) > 0:
                return pos["size"]
        except Exception:
            pass
        # Fallback: return stored quantity or minimum
        return self.entry_quantity if self.entry_quantity > 0 else config.MIN_POSITION_SIZE

    def _calculate_ema(self, data: List[float], period: int) -> float:
        if len(data) < period:
            return data[-1] if data else 0.0
        mult = 2.0 / (period + 1)
        ema = data[0]
        for val in data[1:]:
            ema = val * mult + ema * (1 - mult)
        return ema

    def _cleanup_structures(self, current_price: float, current_time: int) -> None:
        """Remove structures too far from current price."""
        max_dist = config.STRUCTURE_CLEANUP_DISTANCE_PCT / 100

        for obs in [self.order_blocks_bull, self.order_blocks_bear]:
            to_remove = [ob for ob in obs
                         if abs(ob.midpoint - current_price) / current_price > max_dist]
            for ob in to_remove:
                obs.remove(ob)

        for fvgs in [self.fvgs_bull, self.fvgs_bear]:
            to_remove = [f for f in fvgs
                         if abs(f.midpoint - current_price) / current_price > max_dist]
            for f in to_remove:
                fvgs.remove(f)

    # ==================================================================
    # STATS
    # ==================================================================

    def get_strategy_stats(self) -> Dict:
        total = self.total_exits
        wr = self.winning_trades / total * 100 if total > 0 else 0.0
        rs = self.regime_engine.state
        ndr = self._ndr

        return {
            "state": self.state,
            "htf_bias": self.htf_bias,
            "htf_bias_strength": round(self.htf_bias_strength, 2),
            "daily_bias": self.daily_bias,
            "amd_phase": self.amd_phase,
            "session": self.current_session,
            "in_killzone": self.in_killzone,
            "bull_obs": len(self.order_blocks_bull),
            "bear_obs": len(self.order_blocks_bear),
            "bull_fvgs": len(self.fvgs_bull),
            "bear_fvgs": len(self.fvgs_bear),
            "liq_pools": len(self.liquidity_pools),
            "swing_highs": len(self.swing_highs),
            "swing_lows": len(self.swing_lows),
            "ms_count": len(self.market_structures),
            "dr_weekly": f"{ndr.weekly.low:.0f}–{ndr.weekly.high:.0f}" if ndr.weekly else "N/A",
            "dr_daily": f"{ndr.daily.low:.0f}–{ndr.daily.high:.0f}" if ndr.daily else "N/A",
            "dr_intraday": f"{ndr.intraday.low:.0f}–{ndr.intraday.high:.0f}" if ndr.intraday else "N/A",
            "regime": rs.regime,
            "adx": round(rs.adx, 1),
            "atr_ratio": round(rs.atr_ratio, 2),
            "size_multiplier": round(rs.size_multiplier, 2),
            "total_entries": self.total_entries,
            "total_exits": self.total_exits,
            "winning_trades": self.winning_trades,
            "consecutive_losses": self.consecutive_losses,
            "win_rate_pct": round(wr, 1),
            "daily_pnl": round(self.daily_pnl, 4),
            "total_pnl": round(self.total_pnl, 4),
            "breakeven_moved": self.breakeven_moved,
            "profit_locked_pct": round(self.profit_locked_pct, 2),
        }
