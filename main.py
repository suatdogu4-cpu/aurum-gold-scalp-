"""AURUM GOLD SCALP PRO v1.2 — read-only gold scalp research terminal.

XAUUSDT: Binance USD-M Futures public market data only.
XAUUSD: OANDA v20 account-specific quote feed (optional, separate data).
No trading credentials, order placement, or artificial fallback quotes.
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import math
import os
import statistics
import time
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx
import websockets
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse

APP_NAME = "AURUM GOLD SCALP PRO"
VERSION = "1.2.0"
ROOT = Path(__file__).resolve().parent
BINANCE_API = "https://fapi.binance.com"
BITGET_API = "https://api.bitget.com"
BITGET_WS = "wss://ws.bitget.com/v3/ws/public"
BITGET_KEY = "XAUUSDT-BITGET"
BITGET_CAT = "USDT-FUTURES"
BINANCE_WS = (
    "wss://fstream.binance.com/stream?streams="
    "xauusdt@aggTrade/xauusdt@bookTicker/xauusdt@depth20@100ms/xauusdt@kline_1m"
)
OANDA_ACCOUNT_ID = os.getenv("OANDA_ACCOUNT_ID", "").strip()
OANDA_API_TOKEN = os.getenv("OANDA_API_TOKEN", "").strip()
OANDA_ENV = os.getenv("OANDA_ENV", "practice").strip().lower()
OANDA_BASE = "https://api-fxtrade.oanda.com" if OANDA_ENV == "live" else "https://api-fxpractice.oanda.com"
OANDA_READY = bool(OANDA_ACCOUNT_ID and OANDA_API_TOKEN)
TFs = ("1m", "3m", "5m", "15m", "30m", "1h", "4h", "1d")
SIGNAL_TFS = ("1m", "3m", "5m", "15m")
BITGET_INTERVALS = {"1m":"1m", "3m":"3m", "5m":"5m", "15m":"15m", "30m":"30m", "1h":"1H", "4h":"4H", "1d":"1D"}
OANDA_TFs = {"1m": "M1", "3m": "M3", "5m": "M5", "15m": "M15", "30m":"M30", "1h":"H1", "4h":"H4", "1d":"D"}
BINANCE_MS = {"1m": 60000, "3m": 180000, "5m": 300000, "15m": 900000, "30m":1800000, "1h":3600000, "4h":14400000, "1d":86400000}
LOG = logging.getLogger("aurum")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def nowms() -> int:
    return int(time.time() * 1000)


def num(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
        return result if math.isfinite(result) else default
    except (ValueError, TypeError, OverflowError):
        return default


def iso_ms(v: str) -> int:
    try:
        return int(datetime.fromisoformat(v.replace("Z", "+00:00")).timestamp() * 1000)
    except (ValueError, TypeError):
        return 0


def ema_series(values: list[float], period: int) -> list[float | None]:
    if len(values) < period:
        return [None] * len(values)
    out: list[float | None] = [None] * (period - 1)
    val = sum(values[:period]) / period
    out.append(val)
    alpha = 2 / (period + 1)
    for x in values[period:]:
        val += (x - val) * alpha
        out.append(val)
    return out


def rsi14(close: list[float]) -> float | None:
    """Wilder RSI(14), using the full confirmed-candle sequence."""
    if len(close) < 15:
        return None
    deltas = [b-a for a,b in zip(close, close[1:])]
    avg_gain = sum(max(d,0.0) for d in deltas[:14]) / 14
    avg_loss = sum(max(-d,0.0) for d in deltas[:14]) / 14
    for d in deltas[14:]:
        avg_gain = (avg_gain*13 + max(d,0.0)) / 14
        avg_loss = (avg_loss*13 + max(-d,0.0)) / 14
    if avg_loss == 0:
        return 100.0 if avg_gain > 0 else 50.0
    return 100 - 100 / (1 + avg_gain / avg_loss)


def atr14(c: list[dict[str, float]]) -> float | None:
    """Wilder ATR(14) from all confirmed candles."""
    if len(c) < 15:
        return None
    tr = [max(bar["h"]-bar["l"], abs(bar["h"]-prev["c"]), abs(bar["l"]-prev["c"]))
          for prev,bar in zip(c,c[1:])]
    value = sum(tr[:14])/14
    for x in tr[14:]:
        value = (value*13+x)/14
    return value


def indicators(candles: list[dict[str, float]]) -> dict[str, Any]:
    if len(candles) < 60:
        return {"ready": False, "bars": len(candles)}
    close = [x["c"] for x in candles]
    e20 = ema_series(close, 20)
    e50 = ema_series(close, 50)
    e12 = ema_series(close, 12)
    e26 = ema_series(close, 26)
    macd = [None if a is None or b is None else a - b for a, b in zip(e12, e26)]
    macd_vals = [x for x in macd if x is not None]
    macd_line = macd_vals[-1] if macd_vals else None
    macd_signal_list = ema_series(macd_vals, 9)
    macd_hist = (macd_line - macd_signal_list[-1]) if macd_line is not None and macd_signal_list[-1] is not None else None
    last = close[-1]
    trend = "LONG" if last > e20[-1] > e50[-1] else "SHORT" if last < e20[-1] < e50[-1] else "KARIŞIK"
    rsi = rsi14(close)
    atr = atr14(candles)
    vols = [max(bar["v"], 0.0) for bar in candles[-60:]]
    volsum = sum(vols)
    vwap60 = (sum(((bar["h"] + bar["l"] + bar["c"]) / 3) * vol for bar, vol in zip(candles[-60:], vols)) / volsum) if volsum else None
    basis = sum(close[-20:]) / 20
    std20 = statistics.pstdev(close[-20:])
    return {
        "ready": True,
        "bars": len(candles),
        "close": round(last, 5),
        "ema20": round(e20[-1], 5),
        "ema50": round(e50[-1], 5),
        "rsi14": round(rsi, 2) if rsi is not None else None,
        "atr14": round(atr, 5) if atr is not None else None,
        "macd_hist": round(macd_hist, 6) if macd_hist is not None else None,
        "vwap60": round(vwap60, 5) if vwap60 is not None else None,
        "bb_upper": round(basis + std20 * 2, 5),
        "bb_lower": round(basis - std20 * 2, 5),
        "trend": trend,
        "candle_open_ms": int(candles[-1]["t"]),
    }


def normalize_binance_klines(rows: list[Any], tf: str) -> list[dict[str, float]]:
    cur = nowms()
    out = []
    for x in rows:
        if not isinstance(x, (list, tuple)) or len(x) < 7:
            continue
        t = int(x[0]); ct = int(x[6])
        if ct >= cur:
            continue  # Never feed a forming candle into confirmed-candle signals.
        o,h,l,c,v = (num(z) for z in x[1:6])
        if min(o,h,l,c) <= 0 or l > min(o,c) or h < max(o,c):
            continue
        out.append({"t": t, "o": o, "h": h, "l": l, "c": c, "v": v})
    return sorted(out, key=lambda x: x["t"])[-450:]


def normalize_oanda_candles(rows: list[Any]) -> list[dict[str, float]]:
    out = []
    for b in rows:
        if not b.get("complete") or not b.get("mid"):
            continue
        mid = b["mid"]
        t = iso_ms(b.get("time", ""))
        o,h,l,c = (num(mid.get(k)) for k in ("o", "h", "l", "c"))
        if not t or min(o,h,l,c) <= 0 or l > min(o,c) or h < max(o,c):
            continue
        out.append({"t": t, "o": o, "h": h, "l": l, "c": c, "v": num(b.get("volume"))})
    return sorted(out, key=lambda x: x["t"])[-450:]


class Feed:
    def __init__(self, symbol: str, source: str) -> None:
        self.symbol = symbol
        self.source = source
        self.last_price = 0.0
        self.bid = 0.0
        self.ask = 0.0
        self.quote_ms = 0
        self.book_ms = 0
        self.trade_ms = 0
        self.connection = "BAĞLANIYOR"
        self.market_status = "BİLİNMİYOR"
        self.error = ""
        self.bids: list[list[float]] = []
        self.asks: list[list[float]] = []
        self.trades: deque[tuple[int, float, bool]] = deque(maxlen=10000)
        self.tape: deque[dict[str, Any]] = deque(maxlen=75)
        self.candles: dict[str, list[dict[str, float]]] = {tf: [] for tf in TFs}
        self.candle_checked_ms: dict[str, int] = {tf: 0 for tf in TFs}
        self.open_interest: float | None = None
        self.funding_rate: float | None = None
        self.extra_ms = 0
        self.tick_size: float | None = None
        self.live_bar: dict[str, float] | None = None
        self.day_change_pct: float | None = None
        self.day_volume: float | None = None
        self.day_high: float | None = None
        self.day_low: float | None = None
        self.mark_price: float | None = None
        self.index_price: float | None = None
        self.stats_checked_ms: int = 0

    def flow(self, t: int) -> dict[str, Any]:
        if self.symbol == "XAUUSD":
            return {"available": False, "reason": "OANDA fiyat kaynağında borsa emir defteri ve agresör işlem akışı yok."}
        cutoff = t - 60_000
        while self.trades and self.trades[0][0] < cutoff:
            self.trades.popleft()
        buy = sum(val for _, val, buyer in self.trades if buyer)
        sell = sum(val for _, val, buyer in self.trades if not buyer)
        total = buy + sell
        bidsum = sum(p*q for p,q in self.bids)
        asksum = sum(p*q for p,q in self.asks)
        depth_total = bidsum + asksum
        return {
            "available": bool(self.trade_ms and self.book_ms),
            "trade_count_60s": len(self.trades),
            "buy_usdt_60s": round(buy,2),
            "sell_usdt_60s": round(sell,2),
            "delta_usdt_60s": round(buy-sell,2),
            "aggressor_imbalance": round((buy-sell)/total,4) if total else None,
            "book_bid_usdt": round(bidsum,2),
            "book_ask_usdt": round(asksum,2),
            "book_imbalance": round((bidsum-asksum)/depth_total,4) if depth_total else None,
            "book_age_ms": t - self.book_ms if self.book_ms else None,
            "trade_age_ms": t - self.trade_ms if self.trade_ms else None,
        }

    def snapshot(self) -> dict[str, Any]:
        t = nowms()
        quote_age = max(0,t-self.quote_ms) if self.quote_ms else None
        bar_status = {tf: indicators(self.candles[tf]) for tf in SIGNAL_TFS}
        flow = self.flow(t)
        spread = max(self.ask-self.bid,0) if self.bid and self.ask else None
        spread_pct = (spread / ((self.bid+self.ask)/2))*100 if spread is not None else None
        freshness_limit_ms = 8_000 if self.symbol.startswith("XAUUSDT") else 10_000
        fresh = quote_age is not None and quote_age <= freshness_limit_ms
        last_bar_age = (t-(bar_status["1m"]["candle_open_ms"] + 60_000)) if bar_status["1m"].get("ready") else None
        veto = []
        if not fresh:
            veto.append("Canlı fiyat güncel değil veya veri yok")
        if self.connection != "BAĞLI":
            veto.append("Canlı kaynak bağlantısı aktif değil")
        if self.symbol == "XAUUSD" and not OANDA_READY:
            veto.append("OANDA API anahtarı ve hesap bilgisi yapılandırılmadı")
        if self.market_status not in ("TRADING", "tradeable"):
            veto.append("Kaynak piyasası işlem için açık olarak doğrulanamadı")
        if spread_pct is None:
            veto.append("Doğrulanmış alış/satış fiyatı yok")
        elif spread_pct > 0.08:
            veto.append(f"Spread yüksek (%{spread_pct:.3f})")
        if not all(k.get("ready") for k in bar_status.values()):
            veto.append("Tüm zaman dilimlerinde yeterli kapanmış mum yok")
        if last_bar_age is not None and last_bar_age > 210_000:
            veto.append("Son kapanmış 1dk mum bayat")
        if self.symbol.startswith("XAUUSDT"):
            if not flow.get("available") or (flow.get("book_age_ms") if flow.get("book_age_ms") is not None else 999999) > 8000:
                veto.append("Emir defteri güncel değil")
            if (flow.get("trade_age_ms") if flow.get("trade_age_ms") is not None else 999999) > 12000 or flow.get("trade_count_60s",0) < 8:
                veto.append("Son 60 saniyede yeterli canlı işlem akışı yok")

        decision = "BEKLE"
        alignment = 0
        reasons = []
        if all(bar_status[x].get("ready") for x in ("1m","3m","5m","15m")):
            m1,m3,m5,m15 = (bar_status[x] for x in ("1m","3m","5m","15m"))
            for side in ("LONG", "SHORT"):
                if any(x["trend"] != side for x in (m1,m3,m5)):
                    continue
                direction = 1 if side == "LONG" else -1
                checks: list[tuple[bool,int,str]] = [
                    (m15["trend"] == side, 2, "15dk üst zaman dilimi aynı yönde"),
                    ((m1["rsi14"]-50)*direction >= 2, 1, "RSI momentum uyumlu"),
                    ((m1["macd_hist"] or 0)*direction > 0, 1, "MACD histogramı uyumlu"),
                ]
                if self.symbol.startswith("XAUUSDT"):
                    checks += [
                        (m1["vwap60"] is not None and (m1["close"]-m1["vwap60"])*direction > 0, 1, "VWAP yönü uyumlu"),
                        ((flow.get("aggressor_imbalance") or 0)*direction >= 0.08,2,"Agresif işlemler yönü destekliyor"),
                        ((flow.get("book_imbalance") or 0)*direction >= 0.07,1,"Emir defteri yönü destekliyor"),
                    ]
                max_weight = 3 + sum(weight for _,weight,_ in checks)
                agree = 3 + sum(weight for ok,weight,_ in checks if ok)
                alignment = int(round(100*agree/max_weight))
                reasons = ["1/3/5 dakika EMA trendleri aynı yönde"] + [reason for ok,_,reason in checks if ok]
                reasons += ["Uyumsuz: " + reason for ok,_,reason in checks if not ok]
                critical = m15["trend"] == side and all(ok for ok,_,reason in checks if "Agresif" in reason or "Emir defteri" in reason)
                if critical and alignment >= 78 and not veto:
                    decision = side
                break
            else:
                reasons = ["1/3/5 dakika trendleri aynı yönde değil"]
        else:
            reasons = ["Kapanmış mumlar yükleniyor veya kaynak erişilemiyor"]
        if veto:
            decision = "BEKLE"
        if not reasons:
            reasons = ["Yeterli doğrulanmış eşleşme yok"]
        atr = bar_status["1m"].get("atr14")
        trade_plan = None
        if atr and self.bid and self.ask and self.last_price:
            stop_distance = max(atr*1.1, spread*3 if spread else 0)
            trade_plan = {
                "illustrative_only": True,
                "long_entry_ask": round(self.ask,5),
                "short_entry_bid": round(self.bid,5),
                "long_stop": round(self.ask-stop_distance,5),
                "long_target": round(self.ask+2*stop_distance,5),
                "short_stop": round(self.bid+stop_distance,5),
                "short_target": round(self.bid-2*stop_distance,5),
                "stop_distance": round(stop_distance,5),
                "rr_before_cost": 2.0,
            }
        return {
            "app": APP_NAME, "version": VERSION, "instrument": self.symbol,
            "source": self.source, "connected": self.connection == "BAĞLI",
            "connection": self.connection, "market_status": self.market_status,
            "price": round(self.last_price,5) if self.last_price else None,
            "bid": round(self.bid,5) if self.bid else None,
            "ask": round(self.ask,5) if self.ask else None,
            "spread": round(spread,5) if spread is not None else None,
            "spread_pct": round(spread_pct,5) if spread_pct is not None else None,
            "quote_age_ms": quote_age,
            "quote_time_ms": self.quote_ms or None,
            "fresh": fresh, "data_ok": not veto, "indicators": bar_status,
            "flow": flow, "volume_type": "Borsa işlem hacmi" if self.symbol.startswith("XAUUSDT") else "OANDA tick sayısı (işlem hacmi değil)", "open_interest": self.open_interest,
            "funding_rate": self.funding_rate, "extras_age_ms": t-self.extra_ms if self.extra_ms else None,
            "orderbook": {"bids":self.bids[:12], "asks":self.asks[:12]} if self.symbol.startswith("XAUUSDT") else None,
            "decision": decision, "alignment_score": alignment,
            "score_is_probability": False,
            "reasons": reasons, "veto": veto, "trade_plan": trade_plan,
            "tape": list(self.tape)[-18:] if self.symbol.startswith("XAUUSDT") else [],
            "tick_size": self.tick_size, "error": self.error[:140],
            "day_change_pct": self.day_change_pct if self.stats_checked_ms and t-self.stats_checked_ms<150000 else None,
            "day_volume": self.day_volume if self.stats_checked_ms and t-self.stats_checked_ms<150000 else None,
            "day_high": self.day_high if self.stats_checked_ms and t-self.stats_checked_ms<150000 else None,
            "day_low": self.day_low if self.stats_checked_ms and t-self.stats_checked_ms<150000 else None,
            "mark_price": self.mark_price if self.extra_ms and t-self.extra_ms<210000 else None,
            "index_price": self.index_price if self.extra_ms and t-self.extra_ms<210000 else None,
            "server_time_ms": t,
        }


feeds = {
    "XAUUSDT": Feed("XAUUSDT", "Binance Futures USD-M (gerçek kontrat verisi)"),
    "XAUUSD": Feed("XAUUSD", "OANDA v20 (hesaba özel bid/ask)"),
    BITGET_KEY: Feed(BITGET_KEY, "Bitget XAUUSDT USDT-M Perpetual (V3)")
}
http_client: httpx.AsyncClient | None = None


async def rest_json(url: str, *, params: dict | None = None, headers: dict | None = None) -> Any:
    if http_client is None:
        raise RuntimeError("HTTP istemcisi başlatılamadı")
    res = await http_client.get(url, params=params, headers=headers)
    res.raise_for_status()
    return res.json()


def oanda_headers() -> dict[str,str]:
    return {"Authorization": f"Bearer {OANDA_API_TOKEN}", "Accept-Datetime-Format": "RFC3339"}


async def binance_socket() -> None:
    f = feeds["XAUUSDT"]
    delay = 1.0
    while True:
        try:
            async with websockets.connect(BINANCE_WS, ping_interval=20, ping_timeout=20, max_size=4_000_000, open_timeout=12) as conn:
                f.connection = "BAĞLI"
                f.error = ""
                delay = 1.0
                async for raw in conn:
                    frame = json.loads(raw)
                    d = frame.get("data", frame)
                    ev = d.get("e")
                    stamp = nowms()
                    if ev == "aggTrade":
                        price = num(d.get("p")); qty = num(d.get("q")); ts = int(d.get("T") or stamp)
                        if price > 0:
                            f.last_price = price
                            f.quote_ms = stamp
                            f.trade_ms = stamp
                            if qty > 0:
                                f.trades.append((stamp, price*qty, not bool(d.get("m"))))
                                f.tape.append({"t":ts,"p":price,"q":qty,"side":"buy" if not d.get("m") else "sell"})
                    elif ev == "bookTicker":
                        bid,ask = num(d.get("b")),num(d.get("a"))
                        if 0 < bid <= ask:
                            f.bid,f.ask = bid,ask
                            f.quote_ms = stamp
                            if not f.last_price:
                                f.last_price = (bid+ask)/2
                    elif isinstance(d.get("b"),list) and isinstance(d.get("a"),list) and "depth" in frame.get("stream", ""):
                        bids = [[num(p),num(q)] for p,q in d["b"][:20]]
                        asks = [[num(p),num(q)] for p,q in d["a"][:20]]
                        f.bids = [v for v in bids if min(v) > 0]
                        f.asks = [v for v in asks if min(v) > 0]
                        f.book_ms = stamp
                    elif ev == "kline":
                        k = d.get("k") or {}
                        if not k.get("x") and k.get("i") == "1m":
                            f.live_bar = {"t":int(k["t"]),"o":num(k["o"]),"h":num(k["h"]),"l":num(k["l"]),"c":num(k["c"]),"v":num(k["v"])}
                        if k.get("x") and k.get("i") == "1m":
                            bar = {"t":int(k["t"]),"o":num(k["o"]),"h":num(k["h"]),"l":num(k["l"]),"c":num(k["c"]),"v":num(k["v"])}
                            f.live_bar = None
                            old = f.candles["1m"]
                            if not old or bar["t"] > old[-1]["t"]:
                                f.candles["1m"] = (old+[bar])[-450:]
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            f.connection = "KOPTU"
            f.error = f"Binance WS: {type(exc).__name__}: {exc}"
            LOG.warning(f.error)
            await asyncio.sleep(delay)
            delay = min(delay * 1.8,30)


async def binance_candles() -> None:
    f = feeds["XAUUSDT"]
    while True:
        for tf in TFs:
            if tf not in SIGNAL_TFS and f.candle_checked_ms[tf] and nowms()-f.candle_checked_ms[tf]<180000:
                continue
            try:
                data = await rest_json(BINANCE_API+"/fapi/v1/klines",params={"symbol":"XAUUSDT","interval":tf,"limit":300})
                bars = normalize_binance_klines(data,tf)
                if bars and (not f.candles[tf] or bars[-1]["t"] >= f.candles[tf][-1]["t"]):
                    f.candles[tf] = bars
                    f.candle_checked_ms[tf] = nowms()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                f.error = f"Binance mum verisi: {type(exc).__name__}: {str(exc)[:90]}"
            await asyncio.sleep(0.2)
        await asyncio.sleep(20)


async def binance_extras() -> None:
    f = feeds["XAUUSDT"]
    while True:
        try:
            info,oi,mark = await asyncio.gather(
                rest_json(BINANCE_API+"/fapi/v1/exchangeInfo",params={"symbol":"XAUUSDT"}),
                rest_json(BINANCE_API+"/fapi/v1/openInterest",params={"symbol":"XAUUSDT"}),
                rest_json(BINANCE_API+"/fapi/v1/premiumIndex",params={"symbol":"XAUUSDT"}),
            )
            items = info.get("symbols") or []
            if items:
                item = items[0]
                f.market_status = item.get("status","BİLİNMİYOR")
                price_filter = next((x for x in item.get("filters",[]) if x.get("filterType")=="PRICE_FILTER"),{})
                f.tick_size = num(price_filter.get("tickSize")) or None
            f.open_interest = num(oi.get("openInterest")) or None
            f.funding_rate = num(mark.get("lastFundingRate"))
            f.mark_price = num(mark.get("markPrice")) or None
            f.index_price = num(mark.get("indexPrice")) or None
            f.extra_ms = nowms()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            f.error = f"Binance ek veriler: {type(exc).__name__}: {str(exc)[:90]}"
        await asyncio.sleep(180)


async def binance_day_statistics() -> None:
    f = feeds["XAUUSDT"]
    while True:
        try:
            d = await rest_json(BINANCE_API+"/fapi/v1/ticker/24hr",params={"symbol":"XAUUSDT"})
            f.day_change_pct = num(d.get("priceChangePercent"))
            f.day_volume = num(d.get("volume"))
            f.day_high = num(d.get("highPrice"))
            f.day_low = num(d.get("lowPrice"))
            f.stats_checked_ms = nowms()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            LOG.warning("Binance 24h stats: %s",type(exc).__name__)
        await asyncio.sleep(70)


async def oanda_price_loop() -> None:
    f = feeds["XAUUSD"]
    if not OANDA_READY:
        f.connection = "YAPILANDIRILMADI"
        f.error = "OANDA_ACCOUNT_ID ve OANDA_API_TOKEN Render ortam değişkenlerine girilmeli."
        return
    path = f"/v3/accounts/{OANDA_ACCOUNT_ID}/pricing"
    while True:
        try:
            d = await rest_json(OANDA_BASE+path,params={"instruments":"XAU_USD"},headers=oanda_headers())
            prices = d.get("prices") or []
            p = next((v for v in prices if v.get("instrument")=="XAU_USD"), None)
            if not p:
                raise ValueError("XAU_USD hesabınızda bulunmuyor veya fiyat erişimi yok")
            bid = num((p.get("bids") or [{}])[0].get("price"))
            ask = num((p.get("asks") or [{}])[0].get("price"))
            if not (0 < bid <= ask):
                raise ValueError("OANDA bid/ask boş veya geçersiz")
            f.bid,f.ask = bid,ask
            f.last_price = (bid+ask)/2  # midpoint, NOT broker execution price
            f.quote_ms = iso_ms(p.get("time", ""))
            f.market_status = str(p.get("status") or "BİLİNMİYOR")
            f.connection = "BAĞLI"
            f.error = ""
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            f.connection = "HATA"
            f.error = f"OANDA fiyat: {type(exc).__name__}: {str(exc)[:105]}"
        await asyncio.sleep(2.5)


async def oanda_candle_loop() -> None:
    if not OANDA_READY:
        return
    f = feeds["XAUUSD"]
    endpoint = OANDA_BASE+"/v3/instruments/XAU_USD/candles"
    while True:
        for tf in TFs:
            if tf not in SIGNAL_TFS and f.candle_checked_ms[tf] and nowms()-f.candle_checked_ms[tf]<180000:
                continue
            try:
                rows = await rest_json(endpoint,params={"granularity":OANDA_TFs[tf],"count":300,"price":"M"},headers=oanda_headers())
                bars = normalize_oanda_candles(rows.get("candles",[]))
                if bars and (not f.candles[tf] or bars[-1]["t"] >= f.candles[tf][-1]["t"]):
                    f.candles[tf] = bars
                    f.candle_checked_ms[tf] = nowms()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                f.error = f"OANDA mum verisi: {type(exc).__name__}: {str(exc)[:95]}"
            await asyncio.sleep(0.2)
        await asyncio.sleep(20)




# === Bitget v3: public live gold futures (no trading key) ===
# The three exchanges/brokers remain SEPARATE feeds. Never overwrite Binance or OANDA.
bitget_meta: dict[str, Any] = {"symbol_status": "BİLİNMİYOR", "last_rest_error": "", "last_ws_error": "", "last_heartbeat_ms": None,
                               "day_change_pct": None, "day_volume": None, "mark": None, "index": None,
                               "next_funding_ms": None, "book_levels": 5, "ws_ack": [], "liquidations_60s": None, "long_short_5m": None,
                               "active_buy_sell_5m": None, "statistics_ms": None, "statistics_error": ""}
bitget_liquidations: deque[tuple[int,str,float]] = deque(maxlen=1000)
bitget_backfill_checked_ms: dict[str,int] = {tf: 0 for tf in TFs}
macro_data: dict[str, Any] = {"series": {}, "updated_ms": None, "error": "", "frequency": "Günlük; scalp sinyali için kullanılmaz"}


def bitget_payload(response: Any) -> Any:
    if not isinstance(response, dict) or str(response.get("code")) != "00000":
        raise ValueError("Bitget API hata kodu: " + str(response.get("code") if isinstance(response,dict) else "yanıt geçersiz"))
    return response.get("data")


def normalized_bitget_candles(rows: Any, tf: str) -> list[dict[str, float]]:
    interval_ms = BINANCE_MS[tf]
    t = nowms()
    out = []
    for x in rows if isinstance(rows,list) else []:
        if not isinstance(x,(list,tuple)) or len(x) < 6:
            continue
        start = int(x[0])
        # The REST endpoint can return an in-progress candle: exclude it.
        if start + interval_ms > t - 1500:
            continue
        o,h,l,c,v = (num(z) for z in x[1:6])
        if min(o,h,l,c) <= 0 or not(l<=min(o,c) and h>=max(o,c)):
            continue
        out.append({"t":start,"o":o,"h":h,"l":l,"c":c,"v":v})
    # deduplicate, preserve ascending time order
    return [v for _,v in sorted({int(x["t"]):x for x in out}.items())][-450:]


async def bitget_ping(conn: Any) -> None:
    while True:
        await asyncio.sleep(25)
        await conn.send("ping")


async def bitget_socket() -> None:
    f = feeds[BITGET_KEY]
    retry = 1.0
    topics = ["ticker", "books5", "publicTrade", "liquidation"]
    subscribe = {"op":"subscribe", "args":[{"instType":"usdt-futures", "topic":t, "symbol":"XAUUSDT"} for t in topics if t != "liquidation"] +
                 [{"instType":"usdt-futures", "topic":"liquidation"}]}
    while True:
        heartbeat = None
        try:
            async with websockets.connect(BITGET_WS, ping_interval=20, ping_timeout=20, max_size=4_000_000, open_timeout=15) as conn:
                # Source is considered connected only AFTER real market payloads have arrived.
                f.connection = "BAĞLANIYOR"
                f.error = ""
                f.quote_ms = 0
                f.trade_ms = 0
                f.book_ms = 0
                f.bids = []
                f.asks = []
                f.trades.clear()
                f.tape.clear()
                bitget_liquidations.clear()
                bitget_meta["ws_ack"] = []
                await conn.send(json.dumps(subscribe,separators=(",",":")))
                heartbeat = asyncio.create_task(bitget_ping(conn))
                retry = 1.0
                async for raw in conn:
                    if raw == "pong":
                        bitget_meta["last_heartbeat_ms"] = nowms()
                        continue
                    if not isinstance(raw,str):
                        continue
                    frame = json.loads(raw)
                    if frame.get("event") == "error":
                        raise ValueError("Bitget WS abone hatası: " + str(frame.get("msg") or frame.get("code")))
                    if frame.get("event") == "subscribe":
                        topic = (frame.get("arg") or {}).get("topic")
                        bitget_meta["ws_ack"].append(topic)
                        continue
                    topic = (frame.get("arg") or {}).get("topic")
                    if topic not in topics or (topic != "liquidation" and (frame.get("arg") or {}).get("symbol") != "XAUUSDT"):
                        continue
                    action = frame.get("action")
                    for d in frame.get("data") or []:
                        if not isinstance(d,dict):
                            continue
                        stamp = nowms()
                        if topic == "liquidation":
                            if d.get("symbol") == "XAUUSDT" and action == "update":
                                side = str(d.get("side","")).lower()
                                qty = num(d.get("amount"))
                                if side in ("buy","sell") and qty > 0:
                                    bitget_liquidations.append((stamp, side, qty))
                                    cutoff = stamp - 60_000
                                    while bitget_liquidations and bitget_liquidations[0][0] < cutoff:
                                        bitget_liquidations.popleft()
                                    # Bitget sends only the largest liquidation by side/sec, NOT full flow.
                                    bitget_meta["liquidations_60s"] = {
                                        "reported_count":len(bitget_liquidations),
                                        "reported_long_amount":round(sum(q for _,s,q in bitget_liquidations if s=="buy"),4),
                                        "reported_short_amount":round(sum(q for _,s,q in bitget_liquidations if s=="sell"),4),
                                        "exhaustive":False}
                        elif topic == "ticker":
                            p,b,a = num(d.get("lastPrice")),num(d.get("bid1Price")),num(d.get("ask1Price"))
                            if p > 0:
                                f.last_price = p
                            if 0 < b <= a:
                                f.bid, f.ask = b,a
                            if p>0 and 0 < b <= a:
                                f.quote_ms = stamp  # receive time; a live packet, not a REST poll
                                f.connection = "BAĞLI"
                            for fld,out in (("markPrice","mark"),("indexPrice","index"),
                                            ("price24hPcnt","day_change_pct"),("volume24h","day_volume")):
                                if d.get(fld) is not None:
                                    bitget_meta[out] = num(d[fld]) * (100.0 if fld=="price24hPcnt" else 1.0)
                            if d.get("openInterest") is not None:
                                f.open_interest = num(d["openInterest"])
                            if d.get("fundingRate") is not None:
                                f.funding_rate = num(d["fundingRate"])
                            if d.get("nextFundingTime") is not None:
                                bitget_meta["next_funding_ms"] = int(d["nextFundingTime"])
                            f.day_change_pct = bitget_meta.get("day_change_pct")
                            f.day_volume = bitget_meta.get("day_volume")
                            f.day_high = num(d.get("highPrice24h")) or None
                            f.day_low = num(d.get("lowPrice24h")) or None
                            f.mark_price = bitget_meta.get("mark")
                            f.index_price = bitget_meta.get("index")
                            f.stats_checked_ms = stamp
                            f.extra_ms = stamp
                        elif topic == "books5" and action == "snapshot":
                            bids = [[num(x[0]),num(x[1])] for x in d.get("b",[]) if len(x)>=2]
                            asks = [[num(x[0]),num(x[1])] for x in d.get("a",[]) if len(x)>=2]
                            bids = [row for row in bids if min(row)>0]
                            asks = [row for row in asks if min(row)>0]
                            if bids and asks and max(x[0] for x in bids)<min(x[0] for x in asks):
                                f.bids, f.asks = bids[:5], asks[:5]
                                f.book_ms = stamp
                        elif topic == "publicTrade" and action == "update":
                            p,qty = num(d.get("p")),num(d.get("v"))
                            side = str(d.get("S","")).lower()
                            if p>0 and qty>0 and side in ("buy","sell"):
                                f.trades.append((stamp, p*qty, side=="buy"))
                                f.tape.append({"t":int(d.get("T") or stamp),"p":p,"q":qty,"side":side})
                                f.trade_ms = stamp
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            bitget_meta["last_ws_error"] = type(exc).__name__ + ": " + str(exc)[:130]
            f.connection = "KOPTU"
            f.error = bitget_meta["last_ws_error"]
            LOG.warning("Bitget WS: %s",f.error)
            await asyncio.sleep(retry)
            retry = min(retry*1.8,35.0)
        finally:
            if heartbeat:
                heartbeat.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await heartbeat


async def bitget_candles() -> None:
    f = feeds[BITGET_KEY]
    while True:
        for tf in TFs:
            if tf not in SIGNAL_TFS and f.candle_checked_ms[tf] and nowms()-f.candle_checked_ms[tf]<180000:
                continue
            try:
                # Bitget V3 endpoint limit is 100 rows, NOT 300.
                # Backfill up to 300 closed candles only when needed; then merge 100 recent bars.
                params = {"category":BITGET_CAT,"symbol":"XAUUSDT",
                          "interval":BITGET_INTERVALS[tf],"limit":"100"}
                latest = bitget_payload(await rest_json(BITGET_API+"/api/v3/market/candles",params=params))
                merged_rows = list(latest) if isinstance(latest,list) else []
                if merged_rows and len(f.candles[tf]) < 240 and nowms()-bitget_backfill_checked_ms[tf] > 600000:
                    bitget_backfill_checked_ms[tf] = nowms()
                    earliest = min(int(r[0]) for r in merged_rows if isinstance(r,(list,tuple)) and r)
                    for _ in range(3):
                        older = bitget_payload(await rest_json(BITGET_API+"/api/v3/market/history-candles",
                            params={**params,"endTime":str(earliest-1)}))
                        if not isinstance(older,list) or not older:
                            break
                        past = [int(r[0]) for r in older if isinstance(r,(list,tuple)) and r]
                        if not past or min(past)>=earliest:
                            break
                        merged_rows.extend(older)
                        earliest = min(past)
                        if len(merged_rows)>=320:
                            break
                        await asyncio.sleep(.15)
                rows_map = {int(r[0]):r for r in merged_rows if isinstance(r,(list,tuple)) and len(r)>=6}
                for oldbar in f.candles[tf]:
                    if oldbar["t"] not in rows_map:
                        rows_map[oldbar["t"]]=[oldbar["t"],oldbar["o"],oldbar["h"],oldbar["l"],oldbar["c"],oldbar["v"]]
                bars = normalized_bitget_candles(list(rows_map.values()),tf)
                if bars and (not f.candles[tf] or bars[-1]["t"]>=f.candles[tf][-1]["t"]):
                    f.candles[tf] = bars[-350:]
                    f.candle_checked_ms[tf] = nowms()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                bitget_meta["last_rest_error"] = "Bitget mum: " + str(exc)[:110]
                f.error = bitget_meta["last_rest_error"]
            await asyncio.sleep(0.4)
        await asyncio.sleep(20)


async def bitget_instruments() -> None:
    f = feeds[BITGET_KEY]
    while True:
        try:
            data = bitget_payload(await rest_json(BITGET_API+"/api/v3/market/instruments",params={
                "category":BITGET_CAT, "symbol":"XAUUSDT"}))
            entry = next((x for x in data if x.get("symbol")=="XAUUSDT"),None) if isinstance(data,list) else None
            if not entry:
                f.market_status = "BİLİNMİYOR"
                raise ValueError("Bitget XAUUSDT kontrat kaydı bulunamadı")
            status = entry.get("status", "BİLİNMİYOR")
            bitget_meta["symbol_status"] = status
            # Only 'online' is a verified trading state; restricted API/maintenance vetoes.
            f.market_status = "TRADING" if status=="online" else status
            bitget_meta["symbol_type"] = entry.get("symbolType")
            bitget_meta["contract_type"] = entry.get("type")
            bitget_meta["price_multiplier"] = entry.get("priceMultiplier")
            price_precision = int(entry.get("pricePrecision",2))
            f.tick_size = num(entry.get("priceMultiplier")) or 10**(-max(0,min(price_precision,8)))
            bitget_meta["last_rest_error"] = ""
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            f.market_status = "BİLİNMİYOR"
            bitget_meta["last_rest_error"] = str(exc)[:130]
            f.error = "Bitget kontrat: " + str(exc)[:100]
        await asyncio.sleep(180)


async def bitget_statistics() -> None:
    # Separately requested 5-minute aggregates. Never mix with 60-second WS flow.
    while True:
        for field,endpoint in (("long_short_5m","futures-long-short"),
                               ("active_buy_sell_5m","futures-active-buy-sell")):
            try:
                data = bitget_payload(await rest_json(BITGET_API+"/api/v3/market/"+endpoint,
                    params={"symbol":"XAUUSDT","period":"5m"}))
                row = data[0] if isinstance(data,list) and data else None
                if row is None:
                    raise ValueError("Bu kontratta 5dk veri yok")
                if field=="long_short_5m":
                    bitget_meta[field] = {"long_ratio":num(row.get("longRatio")),"short_ratio":num(row.get("shortRatio")),
                                          "long_short_ratio":num(row.get("longShortRatio")),"timestamp_ms":int(row.get("ts") or 0)}
                else:
                    bitget_meta[field] = {"buy_volume":num(row.get("buyVolume")),"sell_volume":num(row.get("sellVolume")),
                                          "timestamp_ms":int(row.get("ts") or 0)}
                bitget_meta["statistics_ms"] = nowms()
                bitget_meta["statistics_error"] = ""
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                bitget_meta["statistics_error"] = "Bitget 5dk istatistik: " + str(exc)[:100]
            await asyncio.sleep(1.1)
        await asyncio.sleep(80)


async def fred_macro_loop() -> None:
    # Free daily research context, NEVER treated as live scalping order flow.
    import csv, io
    series = {"DFII10":"ABD 10Y reel faiz (%)", "DGS10":"ABD 10Y tahvil (%)", "T10YIE":"ABD enflasyon beklentisi (%)"}
    while True:
        for code,label in series.items():
            try:
                if http_client is None:
                    break
                res = await http_client.get("https://fred.stlouisfed.org/graph/fredgraph.csv",params={"id":code},timeout=15)
                res.raise_for_status()
                lines = list(csv.reader(io.StringIO(res.text)))
                valid = [(a,num(b,None)) for a,b,*_ in lines[1:] if b not in (".","",None)]
                valid = [(a,b) for a,b in valid if b is not None]
                if valid:
                    date,val = valid[-1]
                    macro_data["series"][code] = {"name":label,"date":date,"value":val,"source":"FRED"}
                    macro_data["updated_ms"] = nowms()
                    macro_data["error"] = ""
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                macro_data["error"] = "FRED erişimi: " + type(exc).__name__
            await asyncio.sleep(0.5)
        await asyncio.sleep(21600)


@asynccontextmanager
async def lifespan(_: FastAPI):
    global http_client
    http_client = httpx.AsyncClient(timeout=httpx.Timeout(12.0, connect=6.0), follow_redirects=False)
    tasks = [asyncio.create_task(fun(),name=fun.__name__) for fun in (
        binance_socket, binance_candles, binance_extras, binance_day_statistics, bitget_socket, bitget_candles, bitget_instruments, bitget_statistics, fred_macro_loop, oanda_price_loop, oanda_candle_loop
    )]
    try:
        yield
    finally:
        for task in tasks: task.cancel()
        for task in tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await task
        await http_client.aclose()
        http_client = None


app = FastAPI(title=APP_NAME, version=VERSION, lifespan=lifespan)

@app.get("/api/compare")
async def venue_compare():
    now = nowms()
    rows = {}
    for key in ("XAUUSDT",BITGET_KEY):
        f = feeds[key]
        fresh = bool(f.quote_ms and f.connection=="BAĞLI" and now-f.quote_ms<8000)
        rows[key] = {"source":f.source,"connected":f.connection,"quote_age_ms":now-f.quote_ms if f.quote_ms else None,
                     "fresh":fresh,"bid":f.bid if fresh else None,"ask":f.ask if fresh else None,
                     "last":f.last_price if fresh else None,"spread":round(f.ask-f.bid,5) if fresh and f.bid and f.ask else None}
    a,b = rows["XAUUSDT"], rows[BITGET_KEY]
    basis = round(((b["bid"]+b["ask"])-(a["bid"]+a["ask"]))/2,5) if a["fresh"] and b["fresh"] else None
    return {"pairs": rows,"bitget_minus_binance_mid":basis,"currency":"USDT",
            "valid":basis is not None,"note":"Borsa fiyat farkı arbitraj garantisi değildir. Ücret, kayma, transfer ve mark/index farkları hariçtir.","server_time_ms":now}


@app.get("/api/bitget")
async def bitget_details():
    f=feeds[BITGET_KEY]
    t = nowms()
    current = [x for x in bitget_liquidations if x[0]>=t-60000]
    liq_valid = "liquidation" in bitget_meta["ws_ack"] and f.connection == "BAĞLI"
    recent_liq = ({"reported_count":len(current),
                   "reported_long_amount":round(sum(q for _,side,q in current if side=="buy"),4),
                   "reported_short_amount":round(sum(q for _,side,q in current if side=="sell"),4),
                   "exhaustive":False,"last_age_ms":t-bitget_liquidations[-1][0] if bitget_liquidations else None}
                  if liq_valid else None)
    meta = {**bitget_meta, "liquidations_60s":recent_liq}
    if not f.extra_ms or t-f.extra_ms>12000:
        meta["mark"] = meta["index"] = meta["day_volume"] = meta["day_change_pct"] = None
    if not bitget_meta["statistics_ms"] or t-bitget_meta["statistics_ms"]>600000:
        meta["long_short_5m"] = meta["active_buy_sell_5m"] = None
    return {"symbol":"XAUUSDT","venue":"Bitget USDT-M","status":f.market_status,"connection":f.connection,
            "quote_age_ms":t-f.quote_ms if f.quote_ms else None, "flow":f.flow(t),
            "funding_rate":f.funding_rate if f.extra_ms and t-f.extra_ms<=12000 else None,
            "open_interest":f.open_interest if f.extra_ms and t-f.extra_ms<=12000 else None,
            "meta":meta,"source":"https://www.bitget.com/docs/uta/quick-start", "server_time_ms":t}


@app.get("/api/macro")
async def macro_snapshot():
    return {**macro_data,"note":"FRED verileri günlük/gecikmeli makro bağlamdır; anlık altın fiyatı veya al-sat sinyali değildir."}


@app.get("/api/sources")
async def sources():
    return {"active":[
        {"name":"Binance USD-M","mode":"WebSocket + REST","symbol":"XAUUSDT","url":"https://developers.binance.com/docs/derivatives"},
        {"name":"Bitget V3","mode":"WebSocket + REST","symbol":"XAUUSDT","url":"https://www.bitget.com/docs/uta/quick-start"},
        {"name":"OANDA v20","mode":"REST (isteğe bağlı hesap)","symbol":"XAU_USD","url":"https://developer.oanda.com/rest-live-v20/introduction/"},
        {"name":"FRED","mode":"Günlük makro CSV","symbol":"DFII10 / DGS10 / T10YIE","url":"https://fred.stlouisfed.org/"}
    ],"research_not_connected":[
        {"name":"Bitget CFD XAUUSD","mode":"Read-only API anahtarı ve CFD erişimi gerektirir; bağlı değil","url":"https://www.bitget.com/docs/catalog/cfd-market/cfd-market"},
        {"name":"CFTC COT","mode":"Haftalık altın vadeli pozisyon raporu","url":"https://www.cftc.gov/MarketReports/CommitmentsofTraders/index.htm"},
        {"name":"GDELT","mode":"Haber arşivi / metin analizi","url":"https://www.gdeltproject.org/"},
        {"name":"Federal Reserve","mode":"FOMC açıklamaları / resmi takvim","url":"https://www.federalreserve.gov/newsevents.htm"},
        {"name":"BLS","mode":"ABD TÜFE / tarım dışı istihdam açıklama takvimi","url":"https://www.bls.gov/schedule/news_release/"},
        {"name":"Reddit r/Forex","mode":"Topluluk görüşleri; doğrulanmış al-sat verisi değil","url":"https://www.reddit.com/r/Forex/"},
        {"name":"Reddit r/Daytrading","mode":"Strateji deneyimleri; otomatik sinyale dahil değil","url":"https://www.reddit.com/r/Daytrading/"},
        {"name":"Bitget resmî Python SDK","mode":"API örnekleri","url":"https://github.com/BitgetLimited/v3-bitget-api-sdk"},
        {"name":"CCXT","mode":"Çoklu borsa veri adaptörü (ağır WS akışı yerine resmî bağlantı kullanılır)","url":"https://github.com/ccxt/ccxt"},
        {"name":"backtesting.py","mode":"İşlem maliyetli strateji testi, lisans kontrolü gerekir","url":"https://github.com/kernc/backtesting.py"},
        {"name":"smartmoneyconcepts","mode":"SMC araştırma kütüphanesi","url":"https://github.com/joshyattridge/smart-money-concepts"},
        {"name":"vectorbt","mode":"Backtest araştırma kütüphanesi","url":"https://github.com/polakowo/vectorbt"},
        {"name":"pandas-ta-classic","mode":"Teknik göstergeler","url":"https://github.com/xgboosted/pandas-ta-classic"}
    ]}





@app.get("/")
async def homepage():
    return FileResponse(ROOT / "index.html", media_type="text/html")


@app.get("/api/health")
async def health():
    return {"name": APP_NAME, "version": VERSION, "mode":"READ_ONLY", "instruments":{
        key:{"connection":f.connection,"quote_age_ms":(nowms()-f.quote_ms) if f.quote_ms else None,"source":f.source,"error":f.error[:120],"market_status":f.market_status}
        for key,f in feeds.items()
    },"oanda_configured":OANDA_READY,"bitget_configured":True,"server_time_ms":nowms()}


@app.get("/api/snapshot/{instrument}")
async def snapshot(instrument: str):
    key = instrument.upper()
    if key not in feeds:
        raise HTTPException(404,"Desteklenen semboller: XAUUSD, XAUUSDT, XAUUSDT-BITGET")
    return feeds[key].snapshot()


@app.get("/api/chart/{instrument}/{tf}")
async def chart(instrument: str,tf: str):
    key = instrument.upper()
    if key not in feeds or tf not in TFs:
        raise HTTPException(404,"Sembol veya periyot desteklenmiyor")
    f = feeds[key]
    bars = f.candles[tf][-280:]
    close = [x["c"] for x in bars]
    return {"instrument":key,"tf":tf,"source":f.source,"confirmed_only":True,
            "bars":bars,"ema20":ema_series(close,20),"ema50":ema_series(close,50),
            "checked_ms":f.candle_checked_ms[tf] or None,
            "live_bar": f.live_bar if tf=="1m" and f.live_bar and f.connection=="BAĞLI" and nowms()-f.quote_ms<8000 else None,
            "server_time_ms":nowms()}


@app.websocket("/ws/{instrument}")
async def websocket_dashboard(ws: WebSocket, instrument: str):
    key = instrument.upper()
    if key not in feeds:
        await ws.close(code=1008)
        return
    await ws.accept()
    try:
        while True:
            await ws.send_json(feeds[key].snapshot())
            await asyncio.sleep(1.0)
    except (WebSocketDisconnect, RuntimeError):
        return


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT","8000")), reload=False)
