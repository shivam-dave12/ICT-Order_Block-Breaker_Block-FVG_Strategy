"""
config.py — Single source of truth for all bot parameters.
============================================================
ICT + SMC + FVG + OB + Liquidity Institutional Engine v10
"""


import os
from dotenv import load_dotenv


load_dotenv()


# ─────────────────────────────────────────────
# CREDENTIALS
# ─────────────────────────────────────────────
COINSWITCH_API_KEY    = os.getenv("COINSWITCH_API_KEY")
COINSWITCH_SECRET_KEY = os.getenv("COINSWITCH_SECRET_KEY")
TELEGRAM_BOT_TOKEN    = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID      = os.getenv("TELEGRAM_CHAT_ID")


if not COINSWITCH_API_KEY or not COINSWITCH_SECRET_KEY:
    raise ValueError("Missing API credentials in .env")


# ─────────────────────────────────────────────
# EXCHANGE / SYMBOL
# ─────────────────────────────────────────────
SYMBOL   = "BTCUSDT"
EXCHANGE = "EXCHANGE_2"
LEVERAGE = 25


# ─────────────────────────────────────────────
# POSITION SIZING
# ─────────────────────────────────────────────
BALANCE_USAGE_PERCENTAGE = 60       # % of balance for margin
MIN_MARGIN_PER_TRADE     = 4        # USDT minimum margin
MAX_MARGIN_PER_TRADE     = 10_000   # USDT maximum margin
MIN_POSITION_SIZE        = 0.001    # BTC minimum
MAX_POSITION_SIZE        = 1.0      # BTC maximum


# ─────────────────────────────────────────────
# RISK MANAGEMENT
# ─────────────────────────────────────────────
RISK_PER_TRADE          = 0.60      # % of balance risked per trade
MAX_DAILY_LOSS          = 400       # USDT daily loss hard stop
MAX_DAILY_LOSS_PCT      = 5.0       # % of balance alternative
MAX_DRAWDOWN_PCT        = 15.0      # % max drawdown
MAX_CONSECUTIVE_LOSSES  = 3         # halt after N consecutive losses
MAX_DAILY_TRADES        = 8         # max trades per day
ONE_POSITION_AT_A_TIME  = True
MIN_TIME_BETWEEN_TRADES = 10        # minutes
TRADE_COOLDOWN_SECONDS  = 600       # seconds cooldown after loss


# ─────────────────────────────────────────────
# RISK / REWARD
# ─────────────────────────────────────────────
MIN_RISK_REWARD_RATIO    = 2.5
TARGET_RISK_REWARD_RATIO = 4.0
MAX_RR_RATIO             = 12.0


# ─────────────────────────────────────────────
# ENTRY THRESHOLD (confluence score / 100)
# ─────────────────────────────────────────────
ENTRY_THRESHOLD_KILLZONE = 75
ENTRY_THRESHOLD_REGULAR  = 80
ENTRY_THRESHOLD_WEEKEND  = 88


# ─────────────────────────────────────────────
# DATA / READINESS
# ─────────────────────────────────────────────
READY_TIMEOUT_SEC    = 120.0
MIN_CANDLES_1M       = 100
MIN_CANDLES_5M       = 100
MIN_CANDLES_15M      = 100
MIN_CANDLES_1H       = 20
MIN_CANDLES_4H       = 40
MIN_CANDLES_1D       = 7


LOOKBACK_CANDLES_1M  = 100
LOOKBACK_CANDLES_5M  = 100
LOOKBACK_CANDLES_15M = 100
LOOKBACK_CANDLES_4H  = 50


CANDLE_TIMEFRAMES    = ["1m", "5m", "15m", "4h"]
PRIMARY_TIMEFRAME    = "5m"
HTF_TIMEFRAME        = "4h"


# ─────────────────────────────────────────────
# SWING POINTS
# ─────────────────────────────────────────────
SWING_LOOKBACK_LEFT          = 5    # bars left of pivot (3 was too noisy)
SWING_LOOKBACK_RIGHT         = 3    # bars right confirmation
STRUCTURE_LOOKBACK_CANDLES   = 50
STRUCTURE_MIN_SWING_SIZE_PCT = 0.15


# ─────────────────────────────────────────────
# ORDER BLOCKS (ICT)
# ─────────────────────────────────────────────
OB_MIN_IMPULSE_PCT          = 0.50   # impulse candle must move >= 0.5%
OB_MIN_BODY_RATIO           = 0.50   # impulse body >= 50% of range
OB_IMPULSE_SIZE_MULTIPLIER  = 1.30   # impulse range >= 1.30x OB range
OB_MAX_AGE_MINUTES          = 1440   # 24h — OBs remain valid for a full day
OB_WICK_REJECTION_MIN       = 0.20   # wick >= 20% of range
OB_OPTIMAL_ENTRY_MIN        = 0.50   # OTE zone 50-79% retracement
OB_OPTIMAL_ENTRY_MAX        = 0.79
OB_INVALIDATE_TOUCHES       = 3      # invalidate after 3 revisits
MAX_ORDER_BLOCKS             = 20


# ─────────────────────────────────────────────
# FAIR VALUE GAPS (ICT)
# ─────────────────────────────────────────────
FVG_MIN_SIZE_PCT        = 0.020     # gap >= 0.020% of price
FVG_MAX_AGE_MINUTES     = 1440      # 24h — FVGs remain relevant for a full day
FVG_FILL_INVALIDATION   = 1.0       # 100% fill = invalidated (must fully close the gap)
MAX_FVGS                = 30


# ─────────────────────────────────────────────
# LIQUIDITY POOLS (SMC)
# ─────────────────────────────────────────────
LIQ_MIN_TOUCHES          = 2
LIQ_TOUCH_TOLERANCE_PCT  = 0.20    # 0.20% = ~$130 at $65K — institutional equal-high tolerance
LIQ_MAX_DISTANCE_PCT     = 4.0     # expanded to catch further pools
SWEEP_WICK_REQUIREMENT   = True
SWEEP_DISPLACEMENT_MIN   = 0.40     # displacement body ratio minimum
SWEEP_MAX_AGE_MINUTES    = 120
MAX_LIQUIDITY_ZONES      = 30


# ─────────────────────────────────────────────
# MARKET STRUCTURE (BOS / CHoCH)
# ─────────────────────────────────────────────
MSS_LOOKBACK_CANDLES     = 50
MSS_MAX_AGE_MINUTES      = 45       # 45 min — recent MSS window for entry (enforced in L2)


# ─────────────────────────────────────────────
# DEALING RANGE (IPDA)
# ─────────────────────────────────────────────
DR_PREMIUM_THRESHOLD     = 0.618    # above = premium
DR_DISCOUNT_THRESHOLD    = 0.382    # below = discount


# ─────────────────────────────────────────────
# HTF BIAS ENGINE
# ─────────────────────────────────────────────
HTF_TREND_EMA            = 34
HTF_EMA_MIN_DISTANCE     = 0.0      # REMOVED as hard gate — EMA dist is a weight, not a gate
HTF_BIAS_THRESHOLD       = 0.55     # 55% needed for directional bias


# ─────────────────────────────────────────────
# SESSIONS / KILLZONES (UTC)
# ─────────────────────────────────────────────
ENABLE_PO3_FILTER          = True
PO3_LONDON_KILLZONE_START  = 2       # 02:00 UTC
PO3_LONDON_KILLZONE_END    = 5       # 05:00 UTC
PO3_NY_KILLZONE_START      = 8       # 08:00 UTC  (13:00 EST → 13:30 IST)
PO3_NY_KILLZONE_END        = 11      # 11:00 UTC
PO3_ASIA_KILLZONE_START    = 0       # 00:00 UTC
PO3_ASIA_KILLZONE_END      = 2       # 02:00 UTC


# ─────────────────────────────────────────────
# STOP LOSS
# ─────────────────────────────────────────────

# ── Hard limits (used by _replace_sl_order emergency path — do not remove) ──
SL_BUFFER_TICKS         = 5         # kept for emergency SL placement fallback
MIN_SL_DISTANCE_PCT     = 0.004     # 0.4% — minimum SL distance from entry
MAX_SL_DISTANCE_PCT     = 0.03      # 3%  — emergency wide-SL ceiling
SL_MIN_IMPROVEMENT_PCT  = 0.001     # kept for non-trailing SL update guards
SL_RATCHET_ONLY         = True      # SL can only move in favour, never back

# ── Trailing SL — activation & timing (shared) ───────────────────────────────
#TRAILING_SL_ACTIVATION_RR  = 1.0   # move to breakeven after 1R in profit

# ── Trailing SL — ATR engine (replaces fixed-tick logic in _update_trailing_sl) ─
#
#   All distances below are expressed as multiples of ATR(SL_ATR_PERIOD).
#   This makes every buffer self-calibrating to current market volatility.
#
#   Example at BTC $90 000 with 5m ATR ≈ $120:
#     breakeven SL  = entry  ± $6     (0.05 × $120)
#     structural buf= $60             (0.50 × $120) below swing/OB anchor
#     min clearance = $120            (1.00 × $120) — SL always ≥ 1 ATR from price
#     min move gate = $12             (0.10 × $120) — suppresses micro-updates

SL_ATR_PERIOD               = 14    # Wilder's ATR lookback (5m candles)
#SL_BREAKEVEN_ATR_MULT       = 0.05  # BE SL = entry ± 0.05×ATR (tiny locked profit)
SL_ATR_BUFFER_MULT          = 0.75  # structural buffer beyond swing/OB anchor
SL_MIN_CLEARANCE_ATR_MULT   = 1.5  # SL must stay ≥ 1.5×ATR from current price
SL_MIN_IMPROVEMENT_ATR_MULT = 0.1   # min SL move per update (avoids exchange spam)
TRAIL_SWING_MAX_AGE_MS      = 14_400_000   # 4 h — discard structure older than this
#SL_BREAKEVEN_LOCK_RR      = 0.25   # fraction of initial risk to lock as profit at BE (was atr*0.05 ≈ $10)
TRAILING_SL_CHECK_INTERVAL     = 30     # seconds between trail evaluations


# ─────────────────────────────────────────────
# TAKE PROFIT — Structure-Based
# ─────────────────────────────────────────────
# TP targets opposing liquidity, OB, or FVG
# Single TP — no tranches
TP_STRUCTURE_BUFFER_PCT     = 0.001   # buffer inside structure target


# ─────────────────────────────────────────────
# ORDER EXECUTION
# ─────────────────────────────────────────────
TICK_SIZE                = 0.1
LIMIT_ORDER_OFFSET_TICKS = 5
ORDER_TIMEOUT_SECONDS    = 300
MAX_ORDER_RETRIES        = 2


# ─────────────────────────────────────────────
# RATE LIMITING
# ─────────────────────────────────────────────
GLOBAL_API_MIN_INTERVAL  = 3.0
RATE_LIMIT_ORDERS        = 15
REQUEST_TIMEOUT          = 30


# ─────────────────────────────────────────────
# REGIME ENGINE
# ─────────────────────────────────────────────
ADX_TREND_THRESHOLD      = 25.0
ADX_RANGE_THRESHOLD      = 20.0
EXPANSION_ATR_RATIO      = 1.8


# ─────────────────────────────────────────────
# HEALTH / SUPERVISOR
# ─────────────────────────────────────────────
WS_STALE_SECONDS                   = 35.0
HEALTH_CHECK_INTERVAL_SEC          = 12.0
BALANCE_CACHE_TTL_SEC              = 35.0
STRUCTURE_UPDATE_INTERVAL_SECONDS  = 30
ENTRY_EVALUATION_INTERVAL_SECONDS  = 5
ENTRY_PENDING_TIMEOUT_SECONDS      = 120


# ─────────────────────────────────────────────
# LOGGING / REPORTING
# ─────────────────────────────────────────────
LOG_LEVEL                    = "INFO"
TELEGRAM_REPORT_INTERVAL_SEC = 900.0
OUTLOOK_INTERVAL_SECONDS     = 900        # 15 min — single consolidated Telegram report

# ─────────────────────────────────────────────
# FEES
# ─────────────────────────────────────────────
# CoinSwitch taker fee per side (both entry and exit assumed taker).
# Update this to your exact tier rate if different.
COMMISSION_RATE = 0.00055   # 0.055% taker fee


# ─────────────────────────────────────────────
# STRUCTURE MAINTENANCE
# ─────────────────────────────────────────────
STRUCTURE_CLEANUP_DISTANCE_PCT = 5.0
