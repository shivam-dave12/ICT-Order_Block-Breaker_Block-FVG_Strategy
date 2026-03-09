"""
ICT Order Block Trading Bot v11
=================================
ICT + SMC + FVG + OB + Liquidity Institutional Engine

Single TP. Single SL. Structure-based trailing.
Sniper entry at OTE zones. ATR-based SL.
No fallbacks. No synthetic data. Pure market structure.
"""

import logging
import signal
import sys
import threading
import time
from typing import Optional

import config
from data_manager import ICTDataManager
from order_manager import OrderManager
from risk_manager import RiskManager
from strategy import AdvancedICTStrategy
from telegram_notifier import (
    install_global_telegram_log_handler,
    send_telegram_message,
    format_periodic_report,
)

logging.basicConfig(
    level=getattr(config, "LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("ict_bot.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

install_global_telegram_log_handler(level=logging.WARNING, throttle_seconds=5.0)


class ICTBot:
    def __init__(self) -> None:
        self.running               = False
        self.last_report_sec       = 0.0
        self.last_health_check_sec = 0.0

        self.data_manager:  Optional[ICTDataManager]      = None
        self.order_manager: Optional[OrderManager]        = None
        self.risk_manager:  Optional[RiskManager]         = None
        self.strategy:      Optional[AdvancedICTStrategy] = None

        self.trading_enabled      = True
        self.trading_pause_reason = ""

    # =================================================================
    # INITIALIZE
    # =================================================================

    def initialize(self) -> bool:
        try:
            logger.info("=" * 80)
            logger.info("🚀 ICT BOT v11 — INSTITUTIONAL ENGINE INITIALIZING")
            logger.info("   Sniper OTE Entry | ATR-Based SL | Structure Trailing")
            logger.info("   ICT + SMC + FVG + OB + Liquidity")
            logger.info("=" * 80)

            self.data_manager  = ICTDataManager()
            self.order_manager = OrderManager()
            self.risk_manager  = RiskManager(shared_api=self.order_manager.api)
            self.strategy      = AdvancedICTStrategy(self.order_manager)
            self.data_manager.register_strategy(self.strategy)

            features = [
                ("volume_analyzer",   "Volume Profile Analyzer"),
                ("absorption_model",  "Absorption Model"),
                ("regime_engine",     "Regime Engine"),
                ("_ndr",              "Nested Dealing Ranges (3-tier IPDA)"),
            ]
            for attr, label in features:
                if getattr(self.strategy, attr, None):
                    logger.info(f"✅ {label}: ENABLED")
                else:
                    logger.warning(f"⚠️  {label}: MISSING")

            logger.info("✅ Bot initialized successfully")
            return True

        except Exception:
            logger.exception("❌ Failed to initialize bot")
            return False

    # =================================================================
    # START
    # =================================================================

    def start(self) -> bool:
        try:
            if not all([self.order_manager, self.risk_manager,
                        self.data_manager, self.strategy]):
                logger.error("Bot components not initialized")
                return False

            # Set leverage
            logger.info("Setting leverage to %sx...", config.LEVERAGE)
            resp = self.order_manager.api.set_leverage(
                symbol=config.SYMBOL,
                exchange=config.EXCHANGE,
                leverage=int(config.LEVERAGE))
            if isinstance(resp, dict) and resp.get("error"):
                logger.warning("⚠️ Leverage warning (may already be set): %s", resp)

            balance_info = self.risk_manager.get_available_balance()
            if balance_info:
                logger.info("Initial Balance: %.2f USDT",
                            float(balance_info.get("available", 0.0)))

            logger.info("Starting data streams (WS + REST warmup)...")
            if not self.data_manager.start():
                logger.error("❌ Failed to start data streams")
                return False

            logger.info("Waiting for readiness...")
            ready = self.data_manager.wait_until_ready(
                timeout_sec=float(config.READY_TIMEOUT_SEC))
            if not ready:
                logger.error("❌ DataManager not ready within timeout")
                return False

            logger.info("✅ Data ready. Price: $%.2f", self.data_manager.get_last_price())
            self.running = True

            send_telegram_message(
                "🚀 <b>ICT BOT v11 STARTED</b>\n\n"
                "✅ WS + REST warmup OK\n"
                "✅ Sniper OTE Entry\n"
                "✅ ATR-Based SL (not fixed %)\n"
                "✅ Structure-based Trailing SL\n"
                "✅ Nested Dealing Ranges (3-tier IPDA)\n"
                "✅ Bias Conflict + Regime Gates\n"
                "✅ No Fallbacks — Pure Structure")
            logger.info("🚀 BOT RUNNING — v11 INSTITUTIONAL ENGINE")
            return True

        except Exception:
            logger.exception("❌ Error starting bot")
            return False

    # =================================================================
    # STREAM SUPERVISOR
    # =================================================================

    def maybe_supervise_streams(self) -> None:
        if not self.data_manager or not self.data_manager.ws:
            return

        now = time.time()
        interval = float(config.HEALTH_CHECK_INTERVAL_SEC)
        if now - self.last_health_check_sec < interval:
            return
        self.last_health_check_sec = now

        stale_sec = float(config.WS_STALE_SECONDS)
        ws_healthy = self.data_manager.ws.is_healthy(timeout_seconds=int(stale_sec))

        # Separate check: price may be frozen even when WS appears healthy.
        # On weekends/low-volume, orderbook pings keep the WS alive but
        # actual trade/candle price updates stop arriving.
        price_stale_sec = getattr(config, "PRICE_STALE_SECONDS", 90.0)
        price_fresh = self.data_manager.is_price_fresh(max_stale_seconds=price_stale_sec)

        if ws_healthy and price_fresh:
            return

        reason = []
        if not ws_healthy:
            reason.append(f"WS silent >{stale_sec:.0f}s")
        if not price_fresh:
            reason.append(f"Price frozen >{price_stale_sec:.0f}s")
        reason_str = " | ".join(reason)

        logger.warning("⚠️ Stream issue: %s — restarting...", reason_str)
        send_telegram_message(f"⚠️ STREAM ISSUE: {reason_str}\n🔄 Restarting streams...")

        ok = self.data_manager.restart_streams()
        if not ok:
            logger.error("❌ Stream restart failed. Entries gated.")
            return

        self.data_manager.wait_until_ready(timeout_sec=float(config.READY_TIMEOUT_SEC))

    # =================================================================
    # MAIN LOOP
    # =================================================================

    def run(self) -> None:
        if not all([self.strategy, self.data_manager,
                    self.order_manager, self.risk_manager]):
            logger.error("Bot components not initialized")
            return

        logger.info("📊 Main loop active (250ms tick)")

        while self.running:
            try:
                time.sleep(0.25)
                self.maybe_supervise_streams()

                pos = self.strategy.get_position() if self.strategy else None
                if not self.trading_enabled and not pos:
                    continue

                self.strategy.on_tick(
                    self.data_manager,
                    self.order_manager,
                    self.risk_manager,
                    int(time.time() * 1000))

                self.maybe_send_telegram_report()

            except KeyboardInterrupt:
                logger.info("Keyboard interrupt")
                break
            except Exception:
                logger.exception("❌ Main loop error")
                time.sleep(1.0)

        self.running = False

    # =================================================================
    # PERIODIC TELEGRAM REPORT
    # =================================================================

    def maybe_send_telegram_report(self) -> None:
        interval = float(config.TELEGRAM_REPORT_INTERVAL_SEC)
        if interval <= 0:
            return

        now = time.time()
        if now - self.last_report_sec < interval:
            return
        self.last_report_sec = now

        if not all([self.strategy, self.data_manager, self.risk_manager]):
            return

        try:
            last_price   = self.data_manager.get_last_price()
            balance_info = self.risk_manager.get_available_balance()
            strat        = self.strategy
            stats        = strat.get_strategy_stats()

            win_rate = stats.get("win_rate_pct", 0.0)

            # DR price strings
            ndr = strat._ndr
            dr_w = f"${ndr.weekly.low:,.0f}–${ndr.weekly.high:,.0f}" if ndr.weekly else "—"
            dr_d = f"${ndr.daily.low:,.0f}–${ndr.daily.high:,.0f}" if ndr.daily else "—"
            dr_i = f"${ndr.intraday.low:,.0f}–${ndr.intraday.high:,.0f}" if ndr.intraday else "—"

            # Regime context
            rs = strat.regime_engine.state
            regime_line = (
                f"Regime: {rs.regime} "
                f"ADX={rs.adx:.1f} "
                f"ATR×={rs.atr_ratio:.2f} "
                f"Size×={rs.size_multiplier:.2f}")

            msg = format_periodic_report(
                current_price=last_price,
                balance=(balance_info.get("available", 0.0)
                         if balance_info else 0.0),
                total_trades=stats.get("total_exits", 0),
                win_rate=win_rate,
                daily_pnl=stats.get("daily_pnl", 0.0),
                total_pnl=stats.get("total_pnl", 0.0),
                consecutive_losses=stats.get("consecutive_losses", 0),
                htf_bias=strat.htf_bias,
                htf_bias_strength=strat.htf_bias_strength,
                daily_bias=strat.daily_bias,
                session=strat.current_session,
                in_killzone=strat.in_killzone,
                amd_phase=strat.amd_phase,
                bot_state=strat.state,
                regime=rs.regime,
                regime_adx=rs.adx,
                position=strat.get_position(),
                current_sl=strat.current_sl_price,
                current_tp=strat.current_tp_price,
                entry_price=strat.initial_entry_price,
                breakeven_moved=strat.breakeven_moved,
                profit_locked_pct=strat.profit_locked_pct,
                bull_obs=len(strat.order_blocks_bull),
                bear_obs=len(strat.order_blocks_bear),
                bull_fvgs=len(strat.fvgs_bull),
                bear_fvgs=len(strat.fvgs_bear),
                liq_pools=len(strat.liquidity_pools),
                swing_h=len(strat.swing_highs),
                swing_l=len(strat.swing_lows),
                mss_count=len(strat.market_structures),
                dr_weekly_str=dr_w,
                dr_daily_str=dr_d,
                dr_intraday_str=dr_i,
                volume_delta=self.data_manager.get_volume_delta(lookback_seconds=300),
                extra_lines=[regime_line],
            )
            send_telegram_message(msg)

        except Exception:
            logger.exception("❌ Failed to send Telegram report")

    # =================================================================
    # STOP
    # =================================================================

    def stop(self) -> None:
        logger.info("Stopping ICT bot...")
        self.running = False

        stop_msg = "🛑 <b>ICT BOT v11 STOPPED</b>\nShut down gracefully"
        if self.strategy:
            pos = self.strategy.get_position()
            if pos:
                side  = pos.get("side", "?").upper()
                entry = pos.get("entry_price", 0)
                sl    = getattr(self.strategy, "current_sl_price", 0) or 0
                tp    = getattr(self.strategy, "current_tp_price", 0) or 0
                warn  = (
                    f"\n\n⚠️ ACTIVE POSITION LEFT OPEN\n"
                    f"Side: {side}  Entry: {entry:.2f}\n"
                    f"SL: {sl:.2f}  TP: {tp:.2f}\n"
                    f"Exchange SL/TP orders remain live.")
                logger.critical("Active position on shutdown: %s", warn)
                stop_msg += warn

        if self.data_manager:
            self.data_manager.stop()

        send_telegram_message(stop_msg)
        logger.info("ICT bot stopped")


# =====================================================================
# ENTRY POINT
# =====================================================================

def main() -> None:
    bot = ICTBot()

    if threading.current_thread() is threading.main_thread():
        def signal_handler(signum, frame):
            logger.info("Shutdown signal received")
            bot.stop()
            sys.exit(0)
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    if not bot.initialize():
        sys.exit(1)
    if not bot.start():
        sys.exit(1)

    try:
        bot.run()
    except Exception:
        logger.exception("Fatal error in main")
        bot.stop()
        sys.exit(1)


if __name__ == "__main__":
    main()
