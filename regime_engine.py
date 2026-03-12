"""
regime_engine.py — Dynamic Market Regime Classifier v10
========================================================
ICT/SMC Institutional Engine

Regimes:
  TRENDING_BULL       — ADX > threshold, price above structure, +DI > -DI
  TRENDING_BEAR       — ADX > threshold, price below structure, -DI > +DI
  RANGING             — ADX below range threshold, tight ATR
  VOLATILE_EXPANSION  — ATR ratio spike above expansion threshold
  ACCUMULATION        — Low ADX + price at discount + volume absorption
  DISTRIBUTION        — Low ADX + price at premium + volume absorption

Each regime modifies:
  - Entry threshold (tighter in expansion, looser in trend)
  - Position size multiplier
  - SL ATR multiplier
  - OB/FVG/Sweep score adjustments
"""

from __future__ import annotations
import logging
from collections import deque
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple

import config

logger = logging.getLogger(__name__)

# ── Regime labels ─────────────────────────────────────────────────────
REGIME_TRENDING_BULL      = "TRENDING_BULL"
REGIME_TRENDING_BEAR      = "TRENDING_BEAR"
REGIME_RANGING            = "RANGING"
REGIME_VOLATILE_EXPANSION = "VOLATILE_EXPANSION"
REGIME_DISTRIBUTION       = "DISTRIBUTION"
REGIME_ACCUMULATION       = "ACCUMULATION"


# ======================================================================
# DEALING RANGE (IPDA)
# ======================================================================

@dataclass
class DealingRange:
    """IPDA Dealing Range — Premium / Discount / Equilibrium zones."""
    high:       float
    low:        float
    formed_ts:  int
    source:     str = "INIT"
    timeframe:  str = "unknown"

    @property
    def size(self) -> float:
        return max(self.high - self.low, 1e-9)

    @property
    def midpoint(self) -> float:
        return (self.high + self.low) / 2.0

    def zone_pct(self, price: float) -> float:
        """0.0 = range low, 1.0 = range high."""
        return max(min((price - self.low) / self.size, 1.0), 0.0)

    def is_premium(self, price: float) -> bool:
        return self.zone_pct(price) > config.DR_PREMIUM_THRESHOLD

    def is_discount(self, price: float) -> bool:
        return self.zone_pct(price) < config.DR_DISCOUNT_THRESHOLD

    def is_equilibrium(self, price: float) -> bool:
        pct = self.zone_pct(price)
        return config.DR_DISCOUNT_THRESHOLD <= pct <= config.DR_PREMIUM_THRESHOLD


# ======================================================================
# NESTED DEALING RANGES — 3-tier IPDA
# ======================================================================

class NestedDealingRanges:
    """
    Three simultaneous IPDA Dealing Ranges — TIME-ANCHORED (not rolling).

    ICT IPDA logic:
      weekly   — HIGH/LOW of the PREVIOUS complete week (Mon 00:00–Sun 23:59 UTC).
                 Locked in at the Monday open of the current week.
                 Gives the range price must trade within for the week.
      daily    — HIGH/LOW of the CURRENT calendar day (midnight UTC to now),
                 built from 4H candles. Expands throughout the day, resets at midnight.
      intraday — HIGH/LOW of the most recent 4-hour session opening range,
                 built from 1H candles. Resets at each session open (London/NY/Asia).

    These ranges are NOT recalculated every tick. They anchor at period boundaries.

    Alignment scoring:
      3/3 aligned → 1.00x   2/3 → 0.75x   1/3 → 0.50x   0/3 → 0.35x
    """

    def __init__(self):
        self.weekly:   Optional[DealingRange] = None
        self.daily:    Optional[DealingRange] = None
        self.intraday: Optional[DealingRange] = None

        # Time-anchor keys — ranges only reform when period changes
        self._weekly_isoweek:     Optional[tuple] = None   # (isoyear, isoweek)
        self._daily_isoday:       Optional[tuple] = None   # (year, month, day) UTC
        self._intraday_session_h: Optional[int]   = None   # UTC hour of last session boundary

    @staticmethod
    def _utc_dt(ts_ms: int):
        from datetime import datetime, timezone
        return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)

    def update_weekly(self, c1d: List[Dict], ts_ms: int,
                      bos_direction: Optional[str] = None) -> None:
        """
        Weekly IPDA range = previous complete week's HIGH and LOW from 1D candles.
        Anchored at Monday open — does NOT change mid-week.
        """
        if len(c1d) < 5:
            return

        dt = self._utc_dt(ts_ms)
        isoweek = (dt.isocalendar()[0], dt.isocalendar()[1])

        # Only reform at the start of a new week (or first run)
        if self._weekly_isoweek == isoweek and self.weekly is not None:
            return

        # Find the 5 daily candles belonging to the PREVIOUS complete Mon-Fri
        # Filter candles whose timestamp falls in Mon-Fri of the prior week
        # Find the prior complete week number (handles ISO week 52 vs 53 correctly)
        from datetime import date as _date
        prior_week_num  = isoweek[1] - 1
        prior_week_year = isoweek[0]
        if prior_week_num == 0:
            prior_week_year -= 1
            # ISO week 53 exists in some years — use date arithmetic to verify
            # The last ISO week of any year is the week containing Dec 28
            dec28 = _date(prior_week_year, 12, 28)
            prior_week_num = dec28.isocalendar()[1]   # correct: 52 or 53

        week_candles = []
        for c in c1d:
            c_dt = self._utc_dt(int(c['t']))
            c_iso = c_dt.isocalendar()
            if c_iso[0] == prior_week_year and c_iso[1] == prior_week_num:
                week_candles.append(c)

        # Fallback: if prior-week candles not available, use last 5 daily candles
        if len(week_candles) < 3:
            week_candles = c1d[-5:]

        if len(week_candles) < 2:
            return

        high = max(float(c['h']) for c in week_candles)
        low  = min(float(c['l']) for c in week_candles)
        if high - low < 1.0:
            return

        src = f"WEEKLY_BOS_{bos_direction.upper()}" if bos_direction else "WEEKLY_PREV_WEEK"
        self.weekly = DealingRange(high=high, low=low, formed_ts=ts_ms,
                                   source=src, timeframe="weekly")
        self._weekly_isoweek = isoweek
        logger.info(f"📐 DR WEEKLY anchored [{src}] ISOweek {prior_week_year}W{prior_week_num}: "
                    f"${low:,.1f}–${high:,.1f} (${high-low:,.0f} range)")

    def update_daily(self, c4h: List[Dict], ts_ms: int,
                     bos_direction: Optional[str] = None) -> None:
        """
        Daily IPDA range = current calendar day's HIGH/LOW from 4H candles.
        Anchored at midnight UTC — resets each day, grows during the day.
        """
        if len(c4h) < 2:
            return

        dt = self._utc_dt(ts_ms)
        today = (dt.year, dt.month, dt.day)

        # Reform daily range at each new day, or first run
        reform = (self._daily_isoday != today) or (self.daily is None)

        # Collect today's 4H candles (ts_ms falls within today UTC)
        today_candles = []
        for c in c4h:
            c_dt = self._utc_dt(int(c['t']))
            if (c_dt.year, c_dt.month, c_dt.day) == today:
                today_candles.append(c)

        # Fallback: use last 6 x 4H candles (= 24 hours) if today's not enough yet
        if len(today_candles) < 2:
            today_candles = c4h[-6:]

        if len(today_candles) < 2:
            return

        high = max(float(c['h']) for c in today_candles)
        low  = min(float(c['l']) for c in today_candles)
        if high - low < 1.0:
            return

        src = f"DAILY_BOS_{bos_direction.upper()}" if bos_direction else "DAILY_ADR"
        # Always update daily (it expands throughout the day)
        self.daily = DealingRange(high=high, low=low, formed_ts=ts_ms,
                                   source=src, timeframe="daily")
        if reform:
            self._daily_isoday = today
            logger.info(f"📐 DR DAILY anchored [{src}] {today}: "
                        f"${low:,.1f}–${high:,.1f} (${high-low:,.0f} range)")

    def update_intraday(self, c1h: List[Dict], ts_ms: int,
                        bos_direction: Optional[str] = None) -> None:
        """
        Intraday IPDA range = current 4-hour session opening range from 1H candles.
        Session boundaries (UTC): Asia=20, London=02, NY=08, NY_PM=14.
        Resets at each session open.
        """
        if len(c1h) < 2:
            return

        dt = self._utc_dt(ts_ms)
        h  = dt.hour

        # Determine which 4H block we are in (UTC)
        # ICT session opens: 00, 04, 08, 12, 16, 20
        session_h = (h // 4) * 4   # 0,4,8,12,16,20

        # Collect 1H candles from the current 4H block
        session_candles = []
        for c in c1h:
            c_dt   = self._utc_dt(int(c['t']))
            c_sess = (c_dt.hour // 4) * 4
            if c_dt.day == dt.day and c_sess == session_h:
                session_candles.append(c)

        # Fallback: last 4 x 1H candles
        if len(session_candles) < 2:
            session_candles = c1h[-4:]

        if len(session_candles) < 2:
            return

        high = max(float(c['h']) for c in session_candles)
        low  = min(float(c['l']) for c in session_candles)
        if high - low < 1.0:
            return

        src = f"INTRADAY_BOS_{bos_direction.upper()}" if bos_direction else f"SESSION_{session_h:02d}H"
        # Always update intraday (it expands during the session)
        self.intraday = DealingRange(high=high, low=low, formed_ts=ts_ms,
                                      source=src, timeframe="intraday")
        if self._intraday_session_h != session_h:
            self._intraday_session_h = session_h
            logger.info(f"📐 DR INTRADAY anchored [{src}]: "
                        f"${low:,.1f}–${high:,.1f} (${high-low:,.0f} range)")

    def best_dr(self) -> Optional[DealingRange]:
        """Return finest-granularity available DR."""
        return self.intraday or self.daily or self.weekly

    def alignment_score(self, price: float, side: str) -> Tuple[int, float]:
        """
        Count how many DRs support the trade direction.
        Long = price in discount zone, Short = price in premium zone.
        Returns (count, size_multiplier).
        """
        count = 0
        for dr in [self.weekly, self.daily, self.intraday]:
            if dr is None:
                continue
            if side == "long" and dr.is_discount(price):
                count += 1
            elif side == "short" and dr.is_premium(price):
                count += 1

        if count >= 3:
            return count, 1.00
        elif count == 2:
            return count, 0.75
        elif count == 1:
            return count, 0.50
        return 0, 0.35

    def hard_opposed(self, price: float, side: str, htf_bias: str = "NEUTRAL") -> bool:
        """
        Hard-oppose: block trades where price is in extreme wrong zone for the HTF trend.

        ICT logic:
          BULLISH trend → only hard-oppose SHORT at weekly DISCOUNT (never sell the bottom)
          BEARISH trend → only hard-oppose LONG at weekly PREMIUM (never buy the top)
          NEUTRAL trend → block both (buying at premium AND selling at discount)

        This ensures the gate NEVER creates a deadlock:
        - In a BEARISH trend, shorts are allowed even at discount (trend continuation)
        - In a BULLISH trend, longs are allowed even at premium (trend continuation)
        """
        if self.weekly is None:
            return False

        wz = self.weekly.zone_pct(price)   # 0.0 = low, 1.0 = high

        if htf_bias == "BULLISH":
            # Only block: selling at deep weekly discount (bottom of bull range)
            if side == "short" and wz < 0.25:
                return True   # Extreme discount — don't short in bull trend

        elif htf_bias == "BEARISH":
            # Only block: buying at deep weekly premium (top of bear range)
            if side == "long" and wz > 0.75:
                return True   # Extreme premium — don't buy in bear trend

        else:  # NEUTRAL — apply both
            if side == "long" and wz > 0.75:
                return True
            if side == "short" and wz < 0.25:
                return True

        return False


# ======================================================================
# REGIME SNAPSHOT
# ======================================================================

@dataclass
class RegimeSnapshot:
    """Immutable snapshot of current regime state."""
    regime:                  str   = REGIME_RANGING
    adx:                     float = 0.0
    plus_di:                 float = 0.0
    minus_di:                float = 0.0
    atr:                     float = 0.0
    atr_ratio:               float = 1.0
    entry_threshold_modifier: float = 0.0
    size_multiplier:         float = 1.0
    sl_atr_multiplier:       float = 1.5
    ob_score_multiplier:     float = 1.0
    fvg_score_multiplier:    float = 1.0
    sweep_score_multiplier:  float = 1.0
    tce_max_age_ms:          int   = 4 * 3600 * 1000


# ======================================================================
# REGIME ENGINE
# ======================================================================

class RegimeEngine:
    """
    Classifies market regime from multi-timeframe candle data.
    Uses Wilder's ADX/DI system + ATR expansion detection.
    """

    def __init__(self):
        self.state = RegimeSnapshot()
        self._adx_period = 14
        self._atr_period = 14
        self._atr_history: deque[float] = deque(maxlen=100)
        self._regime_history: deque[str] = deque(maxlen=20)
        self._hysteresis_counter = 0
        logger.info("✅ RegimeEngine initialized")

    def update(self, candles: List[Dict]) -> RegimeSnapshot:
        """
        Update regime from candle data (typically 4H or 1H).
        Requires at least 2 * adx_period + 1 candles.
        """
        min_bars = 2 * self._adx_period + 1
        if len(candles) < min_bars:
            return self.state

        try:
            adx, plus_di, minus_di = self._compute_adx(candles)
            atr = self._compute_atr(candles)
            atr_ratio = self._compute_atr_ratio(atr)

            regime = self._classify_regime(adx, plus_di, minus_di, atr_ratio)

            # Hysteresis: require N consecutive bars in new regime
            if regime != self.state.regime:
                self._hysteresis_counter += 1
                if self._hysteresis_counter < 2:
                    regime = self.state.regime
                else:
                    self._hysteresis_counter = 0
            else:
                self._hysteresis_counter = 0

            self._regime_history.append(regime)
            params = self._regime_parameters(regime, adx, atr_ratio)

            self.state = RegimeSnapshot(
                regime=regime,
                adx=adx,
                plus_di=plus_di,
                minus_di=minus_di,
                atr=atr,
                atr_ratio=atr_ratio,
                **params
            )

            return self.state

        except Exception as e:
            logger.error(f"RegimeEngine update error: {e}", exc_info=True)
            return self.state

    def _compute_adx(self, candles: List[Dict]) -> Tuple[float, float, float]:
        """Compute Wilder's ADX, +DI, -DI."""
        period = self._adx_period
        highs  = [float(c['h']) for c in candles]
        lows   = [float(c['l']) for c in candles]
        closes = [float(c['c']) for c in candles]

        # True Range, +DM, -DM
        tr_list, plus_dm_list, minus_dm_list = [], [], []
        for i in range(1, len(candles)):
            h, l, pc = highs[i], lows[i], closes[i - 1]
            tr = max(h - l, abs(h - pc), abs(l - pc))
            plus_dm  = max(h - highs[i - 1], 0)
            minus_dm = max(lows[i - 1] - l, 0)
            if plus_dm > minus_dm:
                minus_dm = 0.0
            elif minus_dm > plus_dm:
                plus_dm = 0.0
            else:
                plus_dm = minus_dm = 0.0
            tr_list.append(tr)
            plus_dm_list.append(plus_dm)
            minus_dm_list.append(minus_dm)

        if len(tr_list) < period:
            return 0.0, 0.0, 0.0

        # Wilder smoothing
        atr_s = sum(tr_list[:period])
        pdm_s = sum(plus_dm_list[:period])
        mdm_s = sum(minus_dm_list[:period])

        dx_list = []
        for i in range(period, len(tr_list)):
            atr_s = atr_s - atr_s / period + tr_list[i]
            pdm_s = pdm_s - pdm_s / period + plus_dm_list[i]
            mdm_s = mdm_s - mdm_s / period + minus_dm_list[i]

            plus_di  = (pdm_s / atr_s * 100) if atr_s > 0 else 0
            minus_di = (mdm_s / atr_s * 100) if atr_s > 0 else 0
            di_sum   = plus_di + minus_di
            dx = abs(plus_di - minus_di) / di_sum * 100 if di_sum > 0 else 0
            dx_list.append((dx, plus_di, minus_di))

        if len(dx_list) < period:
            return 0.0, 0.0, 0.0

        # ADX = smoothed DX
        adx = sum(d[0] for d in dx_list[:period]) / period
        for i in range(period, len(dx_list)):
            adx = (adx * (period - 1) + dx_list[i][0]) / period

        last_plus_di  = dx_list[-1][1]
        last_minus_di = dx_list[-1][2]

        return round(adx, 2), round(last_plus_di, 2), round(last_minus_di, 2)

    def _compute_atr(self, candles: List[Dict]) -> float:
        """Compute ATR."""
        period = self._atr_period
        trs = []
        for i in range(1, len(candles)):
            h, l, pc = float(candles[i]['h']), float(candles[i]['l']), float(candles[i-1]['c'])
            tr = max(h - l, abs(h - pc), abs(l - pc))
            trs.append(tr)

        if len(trs) < period:
            return 0.0

        atr = sum(trs[:period]) / period
        for i in range(period, len(trs)):
            atr = (atr * (period - 1) + trs[i]) / period

        self._atr_history.append(atr)
        return atr

    def _compute_atr_ratio(self, current_atr: float) -> float:
        """ATR ratio = current ATR / median of recent ATRs."""
        if len(self._atr_history) < 5 or current_atr <= 0:
            return 1.0
        sorted_atr = sorted(self._atr_history)
        median = sorted_atr[len(sorted_atr) // 2]
        return current_atr / median if median > 0 else 1.0

    def _classify_regime(self, adx: float, plus_di: float,
                         minus_di: float, atr_ratio: float) -> str:
        """Classify market regime."""
        # Volatile expansion supersedes everything
        if atr_ratio >= config.EXPANSION_ATR_RATIO:
            return REGIME_VOLATILE_EXPANSION

        # Strong trend
        if adx >= config.ADX_TREND_THRESHOLD:
            if plus_di > minus_di:
                return REGIME_TRENDING_BULL
            else:
                return REGIME_TRENDING_BEAR

        # Ranging / accumulation / distribution
        if adx < config.ADX_RANGE_THRESHOLD:
            # Use DI spread to distinguish accumulation vs distribution
            di_spread = abs(plus_di - minus_di)
            if di_spread < 5.0:
                # Very tight DI = pure ranging
                return REGIME_RANGING
            elif plus_di > minus_di:
                return REGIME_ACCUMULATION
            else:
                return REGIME_DISTRIBUTION

        # Transitional zone (ADX between range and trend threshold)
        if plus_di > minus_di:
            return REGIME_TRENDING_BULL
        else:
            return REGIME_TRENDING_BEAR

    def _regime_parameters(self, regime: str, adx: float,
                           atr_ratio: float) -> Dict:
        """Get regime-specific trading parameters."""
        params = {
            REGIME_TRENDING_BULL: {
                "entry_threshold_modifier": -5.0,  # Easier entries with trend
                "size_multiplier": 1.10,
                "sl_atr_multiplier": 1.5,
                "ob_score_multiplier": 1.20,
                "fvg_score_multiplier": 1.10,
                "sweep_score_multiplier": 0.80,
                "tce_max_age_ms": 6 * 3600 * 1000,
            },
            REGIME_TRENDING_BEAR: {
                "entry_threshold_modifier": -5.0,
                "size_multiplier": 1.10,
                "sl_atr_multiplier": 1.5,
                "ob_score_multiplier": 1.20,
                "fvg_score_multiplier": 1.10,
                "sweep_score_multiplier": 0.80,
                "tce_max_age_ms": 6 * 3600 * 1000,
            },
            REGIME_RANGING: {
                "entry_threshold_modifier": 5.0,   # Tighter entries
                "size_multiplier": 0.80,
                "sl_atr_multiplier": 1.2,
                "ob_score_multiplier": 1.30,       # OBs are strong in range
                "fvg_score_multiplier": 1.20,
                "sweep_score_multiplier": 1.30,    # Sweeps dominant in range
                "tce_max_age_ms": 3 * 3600 * 1000,
            },
            REGIME_VOLATILE_EXPANSION: {
                "entry_threshold_modifier": 10.0,  # Very tight entries
                "size_multiplier": 0.60,
                "sl_atr_multiplier": 2.0,
                "ob_score_multiplier": 0.70,
                "fvg_score_multiplier": 0.60,
                "sweep_score_multiplier": 1.40,
                "tce_max_age_ms": 2 * 3600 * 1000,
            },
            REGIME_ACCUMULATION: {
                "entry_threshold_modifier": -5.0,  # Structural absorption = directional conviction
                "size_multiplier": 0.90,
                "sl_atr_multiplier": 1.3,
                "ob_score_multiplier": 1.20,
                "fvg_score_multiplier": 1.15,
                "sweep_score_multiplier": 1.20,
                "tce_max_age_ms": 4 * 3600 * 1000,
            },
            REGIME_DISTRIBUTION: {
                "entry_threshold_modifier": -5.0,  # Structural distribution = directional conviction
                "size_multiplier": 0.90,
                "sl_atr_multiplier": 1.3,
                "ob_score_multiplier": 1.20,
                "fvg_score_multiplier": 1.15,
                "sweep_score_multiplier": 1.20,
                "tce_max_age_ms": 4 * 3600 * 1000,
            },
        }

        return params.get(regime, {
            "entry_threshold_modifier": 0.0,
            "size_multiplier": 1.0,
            "sl_atr_multiplier": 1.5,
            "ob_score_multiplier": 1.0,
            "fvg_score_multiplier": 1.0,
            "sweep_score_multiplier": 1.0,
            "tce_max_age_ms": 4 * 3600 * 1000,
        })
