#!/usr/bin/env python3
"""Historical NIFTY ORB signal backtester using OpenAlgo history API.

Research-only underlying signal test. It does not simulate option premiums.
Outputs:
  backtest_trades.csv   one row per completed trade
  living_state.csv     one row per evaluated 5-minute bar
  backtest_summary.json
  backtest_report.html

Use --watch to periodically refresh the current date range and atomically replace
all output files, making living_state.csv/report.html a living state table.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import tempfile
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import requests
from dotenv import load_dotenv

IST = ZoneInfo("Asia/Kolkata")
load_dotenv()


@dataclass
class Config:
    host: str
    api_key: str
    symbol: str = "NIFTY"
    exchange: str = "NSE_INDEX"
    interval: str = "5m"
    source: str = "api"
    start_date: str = "2023-01-01"
    end_date: str = datetime.now(IST).date().isoformat()
    orb_start: str = "09:15"
    orb_end: str = "09:45"
    entry_start: str = "09:45"
    entry_cutoff: str = "13:30"
    square_off: str = "15:15"
    rsi_min: float = 58.0
    rsi_max: float = 75.0
    max_vwap_extension: float = 0.006
    stop_pct: float = 0.0035
    target_pct: float = 0.0060
    max_hold_minutes: int = 60
    slippage_bps_each_side: float = 1.0
    cost_bps_round_trip: float = 2.0
    initial_capital: float = 100000.0
    risk_fraction: float = 0.01


class OpenAlgoHistory:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.session = requests.Session()

    def fetch_chunk(self, start: str, end: str) -> pd.DataFrame:
        url = f"{self.cfg.host.rstrip('/')}/api/v1/history"
        payload = {
            "apikey": self.cfg.api_key,
            "symbol": self.cfg.symbol,
            "exchange": self.cfg.exchange,
            "interval": self.cfg.interval,
            "start_date": start,
            "end_date": end,
            "source": self.cfg.source,
        }
        r = self.session.post(url, json=payload, timeout=60)
        try:
            data = r.json()
        except ValueError as exc:
            raise RuntimeError(f"History returned non-JSON HTTP {r.status_code}") from exc
        if not r.ok or data.get("status") == "error":
            raise RuntimeError(f"History failed HTTP {r.status_code}: {data}")
        rows = data.get("data", [])
        return pd.DataFrame(rows)

    def fetch(self, chunk_days: int = 80) -> pd.DataFrame:
        start = pd.Timestamp(self.cfg.start_date).date()
        end = pd.Timestamp(self.cfg.end_date).date()
        frames: list[pd.DataFrame] = []
        cursor = start
        while cursor <= end:
            chunk_end = min(cursor + timedelta(days=chunk_days - 1), end)
            print(f"Fetching {cursor} to {chunk_end} from {self.cfg.source}...")
            frame = self.fetch_chunk(cursor.isoformat(), chunk_end.isoformat())
            if not frame.empty:
                frames.append(frame)
            cursor = chunk_end + timedelta(days=1)
            if self.cfg.source == "api":
                time.sleep(0.4)
        if not frames:
            raise RuntimeError("No historical candles returned")
        df = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["timestamp"])
        ts = df["timestamp"]
        if pd.api.types.is_numeric_dtype(ts):
            unit = "ms" if float(ts.dropna().iloc[-1]) > 10_000_000_000 else "s"
            parsed = pd.to_datetime(ts, unit=unit, utc=True).dt.tz_convert(IST)
        else:
            parsed = pd.to_datetime(ts, errors="coerce")
            if parsed.dt.tz is None:
                parsed = parsed.dt.tz_localize(IST)
            else:
                parsed = parsed.dt.tz_convert(IST)
        df["timestamp"] = parsed
        for c in ["open", "high", "low", "close", "volume", "oi"]:
            if c not in df:
                df[c] = 0.0
            df[c] = pd.to_numeric(df[c], errors="coerce")
        return df.dropna(subset=["timestamp", "open", "high", "low", "close"]).sort_values("timestamp").reset_index(drop=True)


def ema(s: pd.Series, span: int) -> pd.Series:
    return s.ewm(span=span, adjust=False).mean()


def rsi(s: pd.Series, n: int = 14) -> pd.Series:
    d = s.diff()
    gain = d.clip(lower=0).ewm(alpha=1 / n, adjust=False).mean()
    loss = (-d.clip(upper=0)).ewm(alpha=1 / n, adjust=False).mean()
    rs = gain / loss.replace(0, np.nan)
    return (100 - 100 / (1 + rs)).fillna(100)


def prepare(df: pd.DataFrame) -> pd.DataFrame:
    x = df.copy()
    x["session"] = x["timestamp"].dt.date
    x["clock"] = x["timestamp"].dt.strftime("%H:%M")
    x["ema9"] = x.groupby("session")["close"].transform(lambda s: ema(s, 9))
    x["ema21"] = x.groupby("session")["close"].transform(lambda s: ema(s, 21))
    x["rsi14"] = x.groupby("session")["close"].transform(lambda s: rsi(s, 14))
    typical = (x["high"] + x["low"] + x["close"]) / 3
    pv = typical * x["volume"].replace(0, 1)
    x["vwap"] = pv.groupby(x["session"]).cumsum() / x["volume"].replace(0, 1).groupby(x["session"]).cumsum()
    x["bar_range"] = x["high"] - x["low"]
    x["body_ratio"] = (x["close"] - x["open"]).abs() / x["bar_range"].replace(0, np.nan)
    x["close_location"] = (x["close"] - x["low"]) / x["bar_range"].replace(0, np.nan)
    return x


def evaluate(cfg: Config, data: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    states: list[dict[str, Any]] = []
    trades: list[dict[str, Any]] = []
    trade_id = 0

    for session, day in data.groupby("session", sort=True):
        day = day.reset_index(drop=True)
        orb = day[(day["clock"] >= cfg.orb_start) & (day["clock"] < cfg.orb_end)]
        if orb.empty:
            continue
        orb_high = float(orb["high"].max())
        orb_low = float(orb["low"].min())
        position: dict[str, Any] | None = None
        traded = False

        for i, row in day.iterrows():
            checks = {
                "entry_window": cfg.entry_start <= row.clock <= cfg.entry_cutoff,
                "orb_breakout": row.close > orb_high,
                "above_vwap": row.close > row.vwap,
                "ema_trend": row.ema9 > row.ema21,
                "rsi_ok": cfg.rsi_min <= row.rsi14 <= cfg.rsi_max,
                "not_extended": row.close <= row.vwap * (1 + cfg.max_vwap_extension),
                "bullish_bar": row.close > row.open,
                "body_ok": row.body_ratio >= 0.50,
                "close_location_ok": row.close_location >= 0.75,
            }
            signal = all(checks.values()) and not traded and position is None
            action = "WAIT"
            exit_reason = ""

            if signal:
                # Enter at next bar open to avoid look-ahead. If no next bar, skip.
                if i + 1 < len(day):
                    nxt = day.iloc[i + 1]
                    raw_entry = float(nxt.open)
                    entry = raw_entry * (1 + cfg.slippage_bps_each_side / 10000)
                    position = {
                        "signal_time": row.timestamp,
                        "entry_time": nxt.timestamp,
                        "entry": entry,
                        "raw_entry": raw_entry,
                        "signal_price": float(row.close),
                        "stop": entry * (1 - cfg.stop_pct),
                        "target": entry * (1 + cfg.target_pct),
                        "mfe": 0.0,
                        "mae": 0.0,
                        "entry_index": i + 1,
                    }
                    action = "ENTER_NEXT_BAR"
                    traded = True
                    trade_id += 1

            if position is not None and i >= position["entry_index"]:
                position["mfe"] = max(position["mfe"], (float(row.high) / position["entry"]) - 1)
                position["mae"] = min(position["mae"], (float(row.low) / position["entry"]) - 1)
                held = int((row.timestamp - position["entry_time"]).total_seconds() // 60)
                raw_exit = None

                # Conservative same-bar rule: if both touched, stop is assumed first.
                if row.low <= position["stop"]:
                    raw_exit, exit_reason = position["stop"], "STOP"
                elif row.high >= position["target"]:
                    raw_exit, exit_reason = position["target"], "TARGET"
                elif row.close < row.vwap:
                    raw_exit, exit_reason = float(row.close), "BELOW_VWAP"
                elif row.ema9 < row.ema21:
                    raw_exit, exit_reason = float(row.close), "EMA_CROSS"
                elif held >= cfg.max_hold_minutes:
                    raw_exit, exit_reason = float(row.close), "MAX_HOLD"
                elif row.clock >= cfg.square_off:
                    raw_exit, exit_reason = float(row.close), "TIME_EXIT"

                if raw_exit is not None:
                    exit_price = raw_exit * (1 - cfg.slippage_bps_each_side / 10000)
                    gross_ret = exit_price / position["entry"] - 1
                    net_ret = gross_ret - cfg.cost_bps_round_trip / 10000
                    risk_per_unit = position["entry"] * cfg.stop_pct
                    units = math.floor((cfg.initial_capital * cfg.risk_fraction) / risk_per_unit) if risk_per_unit > 0 else 0
                    pnl = units * position["entry"] * net_ret
                    trades.append({
                        "trade_id": trade_id,
                        "date": str(session),
                        "signal_time": position["signal_time"],
                        "entry_time": position["entry_time"],
                        "exit_time": row.timestamp,
                        "signal_price": position["signal_price"],
                        "entry_price": position["entry"],
                        "exit_price": exit_price,
                        "stop_price": position["stop"],
                        "target_price": position["target"],
                        "exit_reason": exit_reason,
                        "holding_minutes": held,
                        "gross_return_pct": gross_ret * 100,
                        "net_return_pct": net_ret * 100,
                        "mfe_pct": position["mfe"] * 100,
                        "mae_pct": position["mae"] * 100,
                        "units_for_1pct_risk": units,
                        "model_pnl": pnl,
                        "orb_high": orb_high,
                        "orb_low": orb_low,
                    })
                    action = f"EXIT_{exit_reason}"
                    position = None

            states.append({
                "timestamp": row.timestamp,
                "date": str(session),
                "time": row.clock,
                "open": row.open,
                "high": row.high,
                "low": row.low,
                "close": row.close,
                "orb_high": orb_high,
                "orb_low": orb_low,
                "vwap": row.vwap,
                "ema9": row.ema9,
                "ema21": row.ema21,
                "rsi14": row.rsi14,
                **checks,
                "signal": signal,
                "action": action,
                "position_state": "OPEN" if position else "FLAT",
                "active_trade_id": trade_id if position else "",
                "current_stop": position["stop"] if position else np.nan,
                "current_target": position["target"] if position else np.nan,
                "exit_reason": exit_reason,
            })

    return pd.DataFrame(trades), pd.DataFrame(states)


def summary(trades: pd.DataFrame, cfg: Config) -> dict[str, Any]:
    if trades.empty:
        return {"generated_at": datetime.now(IST).isoformat(), "trades": 0, "config": asdict(cfg)}
    t = trades.copy()
    t["equity"] = cfg.initial_capital + t["model_pnl"].cumsum()
    peak = t["equity"].cummax()
    dd = t["equity"] / peak - 1
    wins = t[t["model_pnl"] > 0]
    losses = t[t["model_pnl"] < 0]
    gross_profit = wins["model_pnl"].sum()
    gross_loss = -losses["model_pnl"].sum()
    return {
        "generated_at": datetime.now(IST).isoformat(),
        "period": {"start": cfg.start_date, "end": cfg.end_date},
        "trades": int(len(t)),
        "win_rate_pct": float((t["model_pnl"] > 0).mean() * 100),
        "profit_factor": float(gross_profit / gross_loss) if gross_loss > 0 else None,
        "net_pnl": float(t["model_pnl"].sum()),
        "ending_equity": float(t["equity"].iloc[-1]),
        "max_drawdown_pct": float(dd.min() * 100),
        "avg_net_return_pct": float(t["net_return_pct"].mean()),
        "avg_win": float(wins["model_pnl"].mean()) if not wins.empty else 0.0,
        "avg_loss": float(losses["model_pnl"].mean()) if not losses.empty else 0.0,
        "avg_mfe_pct": float(t["mfe_pct"].mean()),
        "avg_mae_pct": float(t["mae_pct"].mean()),
        "best_trade": float(t["model_pnl"].max()),
        "worst_trade": float(t["model_pnl"].min()),
        "exit_reasons": {str(k): int(v) for k, v in t["exit_reason"].value_counts().items()},
        "config": asdict(cfg),
        "note": "Underlying NIFTY signal proxy only; option-spread premiums are not simulated.",
    }


def atomic_csv(df: pd.DataFrame, path: Path) -> None:
    with tempfile.NamedTemporaryFile("w", delete=False, dir=path.parent, suffix=".tmp", newline="", encoding="utf-8") as f:
        df.to_csv(f.name, index=False)
        tmp = Path(f.name)
    tmp.replace(path)


def write_outputs(trades: pd.DataFrame, states: pd.DataFrame, summ: dict[str, Any], outdir: Path) -> None:
    outdir.mkdir(parents=True, exist_ok=True)
    atomic_csv(trades, outdir / "backtest_trades.csv")
    atomic_csv(states, outdir / "living_state.csv")
    (outdir / "backtest_summary.json").write_text(json.dumps(summ, indent=2, default=str), encoding="utf-8")

    metrics = "".join(f"<tr><th>{k}</th><td>{v}</td></tr>" for k, v in summ.items() if k not in {"config", "exit_reasons"})
    recent_states = states.tail(100).to_html(index=False, classes="state", border=0)
    recent_trades = trades.tail(50).to_html(index=False, classes="state", border=0) if not trades.empty else "<p>No trades.</p>"
    html = f"""<!doctype html><html><head><meta charset='utf-8'><meta http-equiv='refresh' content='30'>
<title>OpenAlgo ORB Backtest</title><style>
body{{font-family:Segoe UI,Arial;margin:24px;color:#172033}}h1,h2{{color:#0f4c81}}
table{{border-collapse:collapse;width:100%;font-size:12px;margin-bottom:24px}}th,td{{border:1px solid #d8dee9;padding:6px;text-align:right}}th{{background:#eef4fa;position:sticky;top:0}}tr:nth-child(even){{background:#fafbfd}}.note{{padding:12px;background:#fff6df;border-left:5px solid #e5a100}}
</style></head><body><h1>NIFTY ORB Backtest and Living State</h1>
<p>Generated: {summ.get('generated_at')}</p><p class='note'>{summ.get('note','')}</p>
<h2>Summary</h2><table>{metrics}</table><h2>Recent Trades</h2>{recent_trades}<h2>Latest 100 State Rows</h2>{recent_states}</body></html>"""
    (outdir / "backtest_report.html").write_text(html, encoding="utf-8")


def run_once(cfg: Config, outdir: Path) -> None:
    df = prepare(OpenAlgoHistory(cfg).fetch())
    trades, states = evaluate(cfg, df)
    summ = summary(trades, cfg)
    write_outputs(trades, states, summ, outdir)
    print(json.dumps({k: v for k, v in summ.items() if k not in {"config"}}, indent=2, default=str))
    print(f"Outputs written to {outdir.resolve()}")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--start", default=os.getenv("BACKTEST_START", "2023-01-01"))
    p.add_argument("--end", default=os.getenv("BACKTEST_END", datetime.now(IST).date().isoformat()))
    p.add_argument("--source", choices=["api", "db"], default=os.getenv("HISTORY_SOURCE", "api"))
    p.add_argument("--outdir", default=os.getenv("OUTPUT_DIR", "."))
    p.add_argument("--watch", action="store_true", help="Refresh output repeatedly and maintain a living state table")
    p.add_argument("--refresh-minutes", type=int, default=5)
    args = p.parse_args()

    cfg = Config(
        host=os.getenv("OPENALGO_HOST", "http://127.0.0.1:5000"),
        api_key=os.getenv("OPENALGO_API_KEY", ""),
        start_date=args.start,
        end_date=args.end,
        source=args.source,
    )
    if not cfg.api_key:
        raise SystemExit("OPENALGO_API_KEY is missing")
    outdir = Path(args.outdir)
    while True:
        run_once(cfg, outdir)
        if not args.watch:
            break
        print(f"Refreshing in {args.refresh_minutes} minutes...")
        time.sleep(args.refresh_minutes * 60)


if __name__ == "__main__":
    main()
