#!/usr/bin/env python3
"""Live Zerodha market-data collector through OpenAlgo.

Collects normalized WebSocket Quote ticks, builds 1-minute OHLCV bars, records
periodic NIFTY option-chain snapshots, and dynamically subscribes to ATM plus
nearby CE/PE contracts. Data is persisted in SQLite WAL mode and current state
is exported atomically to CSV/JSON.

No orders are placed.
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import signal
import sqlite3
import tempfile
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, time as dtime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

import requests
import websocket
from dotenv import load_dotenv

IST = ZoneInfo("Asia/Kolkata")
load_dotenv()


def now_ist() -> datetime:
    return datetime.now(IST)


def epoch_ms_to_dt(value: Any) -> datetime:
    if value is None:
        return now_ist()
    try:
        n = float(value)
        if n > 10_000_000_000:
            n /= 1000.0
        return datetime.fromtimestamp(n, tz=IST)
    except (TypeError, ValueError, OSError):
        return now_ist()


def derive_ws_url(host: str) -> str:
    parsed = urlparse(host)
    scheme = "wss" if parsed.scheme == "https" else "ws"
    hostname = parsed.hostname or "127.0.0.1"
    return f"{scheme}://{hostname}:8765"


@dataclass
class Config:
    host: str = os.getenv("OPENALGO_HOST", "http://127.0.0.1:5000").rstrip("/")
    ws_url: str = os.getenv("OPENALGO_WS_URL", "")
    api_key: str = os.getenv("OPENALGO_API_KEY", "")
    underlying: str = os.getenv("COLLECTOR_UNDERLYING", "NIFTY")
    underlying_exchange: str = os.getenv("COLLECTOR_UNDERLYING_EXCHANGE", "NSE_INDEX")
    option_exchange: str = os.getenv("COLLECTOR_OPTION_EXCHANGE", "NFO")
    strike_count: int = int(os.getenv("COLLECTOR_STRIKE_COUNT", "8"))
    option_offsets_each_side: int = int(os.getenv("COLLECTOR_OPTION_OFFSETS", "4"))
    chain_refresh_seconds: int = int(os.getenv("COLLECTOR_CHAIN_REFRESH", "60"))
    state_refresh_seconds: int = int(os.getenv("COLLECTOR_STATE_REFRESH", "5"))
    reconnect_seconds: int = int(os.getenv("COLLECTOR_RECONNECT_SECONDS", "5"))
    db_path: str = os.getenv("COLLECTOR_DB", "data/zerodha_live.sqlite3")
    state_csv: str = os.getenv("COLLECTOR_STATE_CSV", "data/live_collector_state.csv")
    state_json: str = os.getenv("COLLECTOR_STATE_JSON", "data/live_collector_state.json")
    market_start: dtime = field(default_factory=lambda: dtime(9, 0))
    market_end: dtime = field(default_factory=lambda: dtime(15, 35))

    def __post_init__(self) -> None:
        if not self.ws_url:
            self.ws_url = derive_ws_url(self.host)


class Store:
    def __init__(self, path: str):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.lock = threading.RLock()
        self.db = sqlite3.connect(self.path, check_same_thread=False, timeout=30)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=NORMAL")
        self.create_schema()

    def create_schema(self) -> None:
        with self.lock, self.db:
            self.db.executescript("""
            CREATE TABLE IF NOT EXISTS ticks (
              ts_ms INTEGER NOT NULL, ts_ist TEXT NOT NULL, exchange TEXT NOT NULL,
              symbol TEXT NOT NULL, mode INTEGER, ltp REAL, open REAL, high REAL,
              low REAL, close REAL, volume REAL, oi REAL, broker TEXT,
              raw_json TEXT NOT NULL,
              PRIMARY KEY(ts_ms, exchange, symbol)
            );
            CREATE INDEX IF NOT EXISTS ix_ticks_symbol_ts ON ticks(exchange, symbol, ts_ms);

            CREATE TABLE IF NOT EXISTS minute_bars (
              minute_ts TEXT NOT NULL, exchange TEXT NOT NULL, symbol TEXT NOT NULL,
              open REAL NOT NULL, high REAL NOT NULL, low REAL NOT NULL,
              close REAL NOT NULL, volume REAL, last_cum_volume REAL, oi REAL,
              tick_count INTEGER NOT NULL, updated_at TEXT NOT NULL,
              PRIMARY KEY(minute_ts, exchange, symbol)
            );
            CREATE INDEX IF NOT EXISTS ix_bars_symbol_ts ON minute_bars(exchange, symbol, minute_ts);

            CREATE TABLE IF NOT EXISTS option_chain_snapshots (
              snapshot_ts TEXT NOT NULL, expiry TEXT NOT NULL, underlying_ltp REAL,
              atm_strike REAL, strike REAL NOT NULL, option_type TEXT NOT NULL,
              label TEXT, symbol TEXT NOT NULL, ltp REAL, bid REAL, ask REAL,
              bid_qty REAL, ask_qty REAL, volume REAL, oi REAL, lotsize REAL,
              raw_json TEXT NOT NULL,
              PRIMARY KEY(snapshot_ts, symbol)
            );
            CREATE INDEX IF NOT EXISTS ix_chain_symbol_ts ON option_chain_snapshots(symbol, snapshot_ts);

            CREATE TABLE IF NOT EXISTS collector_events (
              event_ts TEXT NOT NULL, level TEXT NOT NULL, event TEXT NOT NULL,
              detail TEXT
            );
            """)

    def event(self, level: str, event: str, detail: Any = "") -> None:
        with self.lock, self.db:
            self.db.execute("INSERT INTO collector_events VALUES (?,?,?,?)",
                            (now_ist().isoformat(), level, event, json.dumps(detail, default=str)))

    def tick(self, wrapper: dict[str, Any]) -> None:
        data = wrapper.get("data") or wrapper
        symbol = str(wrapper.get("symbol") or data.get("symbol") or "")
        exchange = str(wrapper.get("exchange") or data.get("exchange") or "")
        if not symbol or not exchange:
            return
        dt = epoch_ms_to_dt(data.get("timestamp") or wrapper.get("timestamp"))
        ts_ms = int(dt.timestamp() * 1000)
        ltp = data.get("ltp")
        if ltp is None:
            return
        row = (ts_ms, dt.isoformat(), exchange, symbol, wrapper.get("mode"),
               ltp, data.get("open"), data.get("high"), data.get("low"),
               data.get("close"), data.get("volume"), data.get("oi"),
               wrapper.get("broker"), json.dumps(wrapper, separators=(",", ":"), default=str))
        minute = dt.replace(second=0, microsecond=0).isoformat()
        cum_volume = data.get("volume")
        with self.lock, self.db:
            self.db.execute("""INSERT OR IGNORE INTO ticks
              (ts_ms,ts_ist,exchange,symbol,mode,ltp,open,high,low,close,volume,oi,broker,raw_json)
              VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", row)
            existing = self.db.execute(
                "SELECT open,high,low,close,volume,last_cum_volume,oi,tick_count FROM minute_bars WHERE minute_ts=? AND exchange=? AND symbol=?",
                (minute, exchange, symbol)).fetchone()
            if existing is None:
                self.db.execute("""INSERT INTO minute_bars
                  (minute_ts,exchange,symbol,open,high,low,close,volume,last_cum_volume,oi,tick_count,updated_at)
                  VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                  (minute, exchange, symbol, ltp, ltp, ltp, ltp, 0, cum_volume, data.get("oi"), 1, now_ist().isoformat()))
            else:
                o, h, lo, _, bar_vol, last_cum, old_oi, count = existing
                incremental = 0.0
                if cum_volume is not None and last_cum is not None:
                    incremental = max(0.0, float(cum_volume) - float(last_cum))
                self.db.execute("""UPDATE minute_bars SET high=?,low=?,close=?,volume=?,last_cum_volume=?,oi=?,tick_count=?,updated_at=?
                  WHERE minute_ts=? AND exchange=? AND symbol=?""",
                  (max(float(h), float(ltp)), min(float(lo), float(ltp)), ltp,
                   float(bar_vol or 0) + incremental, cum_volume if cum_volume is not None else last_cum,
                   data.get("oi") if data.get("oi") is not None else old_oi, int(count)+1,
                   now_ist().isoformat(), minute, exchange, symbol))

    def chain_snapshot(self, response: dict[str, Any], expiry: str) -> set[tuple[str, str]]:
        snapshot = now_ist().isoformat()
        subscriptions: set[tuple[str, str]] = set()
        atm = float(response.get("atm_strike") or 0)
        candidates = sorted(response.get("chain", []), key=lambda r: abs(float(r.get("strike", 0)) - atm))
        keep = candidates[: 1 + 2 * max(1, int((len(candidates)-1)/2))]
        with self.lock, self.db:
            for strike_row in keep:
                strike = float(strike_row.get("strike", 0))
                for opt_type, key in (("CE", "ce"), ("PE", "pe")):
                    q = strike_row.get(key) or {}
                    symbol = q.get("symbol")
                    if not symbol:
                        continue
                    subscriptions.add(("NFO", str(symbol)))
                    self.db.execute("""INSERT OR REPLACE INTO option_chain_snapshots
                      (snapshot_ts,expiry,underlying_ltp,atm_strike,strike,option_type,label,symbol,ltp,bid,ask,bid_qty,ask_qty,volume,oi,lotsize,raw_json)
                      VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                      (snapshot, expiry, response.get("underlying_ltp"), atm, strike, opt_type,
                       q.get("label"), symbol, q.get("ltp"), q.get("bid"), q.get("ask"),
                       q.get("bid_qty"), q.get("ask_qty"), q.get("volume"), q.get("oi"),
                       q.get("lotsize"), json.dumps(q, separators=(",", ":"), default=str)))
        return subscriptions

    def latest_state(self) -> list[dict[str, Any]]:
        sql = """
        SELECT b.exchange,b.symbol,b.minute_ts,b.open,b.high,b.low,b.close,b.volume,b.oi,b.tick_count,b.updated_at,
               (SELECT MAX(t.ts_ist) FROM ticks t WHERE t.exchange=b.exchange AND t.symbol=b.symbol) last_tick
        FROM minute_bars b
        JOIN (SELECT exchange,symbol,MAX(minute_ts) minute_ts FROM minute_bars GROUP BY exchange,symbol) x
          ON b.exchange=x.exchange AND b.symbol=x.symbol AND b.minute_ts=x.minute_ts
        ORDER BY b.exchange,b.symbol
        """
        with self.lock:
            cur = self.db.execute(sql)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]

    def close(self) -> None:
        with self.lock:
            self.db.close()


class Collector:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.store = Store(cfg.db_path)
        self.http = requests.Session()
        self.ws: websocket.WebSocketApp | None = None
        self.connected = threading.Event()
        self.authenticated = threading.Event()
        self.stop = threading.Event()
        self.send_lock = threading.Lock()
        self.subscriptions: set[tuple[str, str]] = {(cfg.underlying_exchange, cfg.underlying)}
        self.expiry = ""
        self.last_message_at: str | None = None
        self.started_at = now_ist().isoformat()

    def post(self, endpoint: str, payload: dict[str, Any]) -> dict[str, Any]:
        r = self.http.post(f"{self.cfg.host}/api/v1/{endpoint}",
                           json={"apikey": self.cfg.api_key, **payload}, timeout=30)
        try:
            data = r.json()
        except ValueError as exc:
            raise RuntimeError(f"{endpoint}: non-JSON HTTP {r.status_code}") from exc
        if not r.ok or data.get("status") == "error":
            raise RuntimeError(f"{endpoint}: HTTP {r.status_code}: {data}")
        return data

    def nearest_expiry(self) -> str:
        data = self.post("expiry", {"symbol": self.cfg.underlying,
                                    "exchange": self.cfg.option_exchange,
                                    "instrumenttype": "options"}).get("data", [])
        if not data:
            raise RuntimeError("No option expiry returned")
        return str(data[0]).replace("-", "").upper()

    def refresh_chain(self) -> None:
        expiry = self.nearest_expiry()
        response = self.post("optionchain", {
            "underlying": self.cfg.underlying,
            "exchange": self.cfg.underlying_exchange,
            "expiry_date": expiry,
            "strike_count": self.cfg.strike_count,
        })
        all_options = self.store.chain_snapshot(response, expiry)
        atm = float(response.get("atm_strike") or 0)
        rows = sorted(response.get("chain", []), key=lambda r: abs(float(r.get("strike", 0)) - atm))
        selected_rows = rows[:1 + 2 * self.cfg.option_offsets_each_side]
        wanted = {(self.cfg.underlying_exchange, self.cfg.underlying)}
        for row in selected_rows:
            for key in ("ce", "pe"):
                q = row.get(key) or {}
                if q.get("symbol"):
                    wanted.add((self.cfg.option_exchange, str(q["symbol"])))
        self.expiry = expiry
        self.update_subscriptions(wanted)
        self.store.event("INFO", "CHAIN_REFRESH", {"expiry": expiry, "atm": atm, "symbols": len(wanted), "snapshot_contracts": len(all_options)})

    def send(self, obj: dict[str, Any]) -> None:
        if self.ws and self.connected.is_set():
            with self.send_lock:
                self.ws.send(json.dumps(obj))

    def update_subscriptions(self, wanted: set[tuple[str, str]]) -> None:
        added = wanted - self.subscriptions
        removed = self.subscriptions - wanted
        if self.authenticated.is_set():
            if added:
                self.send({"action": "subscribe", "mode": "Quote",
                           "symbols": [{"exchange": e, "symbol": s} for e, s in sorted(added)],
                           "request_id": f"add-{int(time.time())}"})
            if removed:
                self.send({"action": "unsubscribe",
                           "symbols": [{"exchange": e, "symbol": s, "mode": "Quote"} for e, s in sorted(removed)],
                           "request_id": f"remove-{int(time.time())}"})
        self.subscriptions = wanted

    def subscribe_all(self) -> None:
        self.send({"action": "subscribe", "mode": "Quote",
                   "symbols": [{"exchange": e, "symbol": s} for e, s in sorted(self.subscriptions)],
                   "request_id": f"all-{int(time.time())}"})

    def on_open(self, ws: websocket.WebSocketApp) -> None:
        self.connected.set()
        self.store.event("INFO", "WS_OPEN", self.cfg.ws_url)
        self.send({"action": "authenticate", "api_key": self.cfg.api_key})

    def on_message(self, ws: websocket.WebSocketApp, message: str) -> None:
        try:
            obj = json.loads(message)
            self.last_message_at = now_ist().isoformat()
            typ = obj.get("type")
            if typ == "auth" and obj.get("status") == "success":
                self.authenticated.set()
                self.store.event("INFO", "WS_AUTH", {"broker": obj.get("broker"), "features": obj.get("supported_features")})
                self.subscribe_all()
            elif typ in {"market_data", "quote"}:
                self.store.tick(obj)
            elif typ in {"error", "auth"} and obj.get("status") != "success":
                self.store.event("ERROR", "WS_MESSAGE", obj)
                logging.error("WebSocket response: %s", obj)
            elif typ in {"subscribe", "unsubscribe"}:
                self.store.event("INFO", f"WS_{typ.upper()}", obj)
        except Exception:
            logging.exception("Failed to process WebSocket message")

    def on_error(self, ws: websocket.WebSocketApp, error: Any) -> None:
        self.store.event("ERROR", "WS_ERROR", str(error))
        logging.error("WebSocket error: %s", error)

    def on_close(self, ws: websocket.WebSocketApp, code: Any, message: Any) -> None:
        self.connected.clear()
        self.authenticated.clear()
        self.store.event("WARNING", "WS_CLOSE", {"code": code, "message": message})

    def ws_loop(self) -> None:
        while not self.stop.is_set():
            self.ws = websocket.WebSocketApp(self.cfg.ws_url, on_open=self.on_open,
                                             on_message=self.on_message, on_error=self.on_error,
                                             on_close=self.on_close)
            self.ws.run_forever(ping_interval=20, ping_timeout=10)
            if not self.stop.is_set():
                time.sleep(self.cfg.reconnect_seconds)

    def chain_loop(self) -> None:
        while not self.stop.is_set():
            try:
                self.refresh_chain()
            except Exception as exc:
                self.store.event("ERROR", "CHAIN_REFRESH_FAILED", str(exc))
                logging.exception("Option-chain refresh failed")
            self.stop.wait(self.cfg.chain_refresh_seconds)

    def atomic_state(self) -> None:
        rows = self.store.latest_state()
        csv_path = Path(self.cfg.state_csv)
        json_path = Path(self.cfg.state_json)
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        state = {
            "generated_at": now_ist().isoformat(),
            "started_at": self.started_at,
            "ws_url": self.cfg.ws_url,
            "connected": self.connected.is_set(),
            "authenticated": self.authenticated.is_set(),
            "expiry": self.expiry,
            "subscriptions": len(self.subscriptions),
            "last_message_at": self.last_message_at,
            "database": str(Path(self.cfg.db_path).resolve()),
            "symbols": rows,
        }
        with tempfile.NamedTemporaryFile("w", delete=False, dir=csv_path.parent, suffix=".tmp", newline="", encoding="utf-8") as f:
            if rows:
                w = csv.DictWriter(f, fieldnames=list(rows[0]))
                w.writeheader(); w.writerows(rows)
            tmp = Path(f.name)
        tmp.replace(csv_path)
        with tempfile.NamedTemporaryFile("w", delete=False, dir=json_path.parent, suffix=".tmp", encoding="utf-8") as f:
            json.dump(state, f, indent=2, default=str)
            tmpj = Path(f.name)
        tmpj.replace(json_path)

    def state_loop(self) -> None:
        while not self.stop.is_set():
            try:
                self.atomic_state()
            except Exception:
                logging.exception("State export failed")
            self.stop.wait(self.cfg.state_refresh_seconds)

    def run(self) -> None:
        if not self.cfg.api_key:
            raise SystemExit("OPENALGO_API_KEY is missing")
        logging.info("Starting collector: REST=%s WebSocket=%s DB=%s", self.cfg.host, self.cfg.ws_url, self.cfg.db_path)
        threads = [
            threading.Thread(target=self.ws_loop, name="websocket", daemon=True),
            threading.Thread(target=self.chain_loop, name="option-chain", daemon=True),
            threading.Thread(target=self.state_loop, name="state-export", daemon=True),
        ]
        for t in threads: t.start()
        while not self.stop.wait(1):
            pass
        if self.ws:
            try:
                self.send({"action": "unsubscribe_all"})
                self.ws.close()
            except Exception:
                pass
        self.atomic_state()
        self.store.event("INFO", "COLLECTOR_STOPPED")
        self.store.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(threadName)s %(message)s")
    collector = Collector(Config())
    def shutdown(signum: int, frame: Any) -> None:
        logging.info("Shutdown requested")
        collector.stop.set()
    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)
    collector.run()
