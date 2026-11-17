"""AURUM GOLD SCALP PRO v1.0 — read-only gold scalp research terminal.

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
VERSION = "1.0.0"
ROOT = Path(__file__).resolve().parent
BINANCE_API = "https://fapi.binance.com"
BINANCE_WS = (
    "wss://fstream.binance.com/stream?streams="
    "xauusdt@aggTrade/xauusdt@bookTicker/xauusdt@depth20@100ms/xauusdt@kline_1m"
)
OANDA_ACCOUNT_ID = os.getenv("OANDA_ACCOUNT_ID", "").strip()
OANDA_API_TOKEN = os.getenv("OANDA_API_TOKEN", "").strip()
OANDA_ENV = os.getenv("OANDA_ENV", "practice").strip().lower()
OANDA_BASE = "https://api-fxtrade.oanda.com" if OANDA_ENV == "live" else "https://api-fxpractice.oanda.com"
OANDA_READY = bool(OANDA_ACCOUNT_ID and OANDA_API_TOKEN)
TFs = ("1m", "3m", "5m", "15m")
OANDA_TFs = {"1m": "M1", "3m": "M3", "5m": "M5", "15m": "M15"}
BINANCE_MS = {"1m": 60000, "3m": 180000, "5m": 300000, "15m": 900000}
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
        self.candles: dict[str, list[dict[str, float]]] = {tf: [] for tf in TFs}
        self.candle_checked_ms: dict[str, int] = {tf: 0 for tf in TFs}
        self.open_interest: float | None = None
        self.funding_rate: float | None = None
        self.extra_ms = 0
        self.tick_size: float | None = None

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
        bar_status = {tf: indicators(self.candles[tf]) for tf in TFs}
        flow = self.flow(t)
        spread = max(self.ask-self.bid,0) if self.bid and self.ask else None
        spread_pct = (spread / ((self.bid+self.ask)/2))*100 if spread is not None else None
        freshness_limit_ms = 8_000 if self.symbol == "XAUUSDT" else 10_000
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
        if self.symbol == "XAUUSDT":
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
                if self.symbol == "XAUUSDT":
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
            "flow": flow, "volume_type": "Borsa işlem hacmi" if self.symbol == "XAUUSDT" else "OANDA tick sayısı (işlem hacmi değil)", "open_interest": self.open_interest,
            "funding_rate": self.funding_rate, "extras_age_ms": t-self.extra_ms if self.extra_ms else None,
            "orderbook": {"bids":self.bids[:12], "asks":self.asks[:12]} if self.symbol == "XAUUSDT" else None,
            "decision": decision, "alignment_score": alignment,
            "score_is_probability": False,
            "reasons": reasons, "veto": veto, "trade_plan": trade_plan,
            "tick_size": self.tick_size, "error": self.error[:140],
            "server_time_ms": t,
        }


feeds = {
    "XAUUSDT": Feed("XAUUSDT", "Binance Futures USD-M (gerçek kontrat verisi)"),
    "XAUUSD": Feed("XAUUSD", "OANDA v20 (hesaba özel bid/ask)")
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
                        if k.get("x") and k.get("i") == "1m":
                            bar = {"t":int(k["t"]),"o":num(k["o"]),"h":num(k["h"]),"l":num(k["l"]),"c":num(k["c"]),"v":num(k["v"])}
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
            f.extra_ms = nowms()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            f.error = f"Binance ek veriler: {type(exc).__name__}: {str(exc)[:90]}"
        await asyncio.sleep(180)


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


@asynccontextmanager
async def lifespan(_: FastAPI):
    global http_client
    http_client = httpx.AsyncClient(timeout=httpx.Timeout(12.0, connect=6.0), follow_redirects=False)
    tasks = [asyncio.create_task(fun(),name=fun.__name__) for fun in (
        binance_socket, binance_candles, binance_extras, oanda_price_loop, oanda_candle_loop
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


@app.get("/")
async def homepage():
    return FileResponse(ROOT / "index.html", media_type="text/html")


@app.get("/api/health")
async def health():
    return {"name": APP_NAME, "version": VERSION, "mode":"READ_ONLY", "instruments":{
        key:{"connection":f.connection,"quote_age_ms":(nowms()-f.quote_ms) if f.quote_ms else None,"source":f.source,"error":f.error[:120]}
        for key,f in feeds.items()
    },"oanda_configured":OANDA_READY,"server_time_ms":nowms()}


@app.get("/api/snapshot/{instrument}")
async def snapshot(instrument: str):
    key = instrument.upper()
    if key not in feeds:
        raise HTTPException(404,"Desteklenen semboller: XAUUSD, XAUUSDT")
    return feeds[key].snapshot()


@app.get("/api/chart/{instrument}/{tf}")
async def chart(instrument: str,tf: str):
    key = instrument.upper()
    if key not in feeds or tf not in TFs:
        raise HTTPException(404,"Sembol veya periyot desteklenmiyor")
    f = feeds[key]
    bars = f.candles[tf][-160:]
    close = [x["c"] for x in bars]
    return {"instrument":key,"tf":tf,"source":f.source,"confirmed_only":True,
            "bars":bars,"ema20":ema_series(close,20),"ema50":ema_series(close,50),
            "checked_ms":f.candle_checked_ms[tf] or None,"server_time_ms":nowms()}


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
