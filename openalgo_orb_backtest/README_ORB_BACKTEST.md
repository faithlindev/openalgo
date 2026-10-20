# OpenAlgo NIFTY ORB Historical Backtest

This is an underlying-signal proxy backtest for the bullish ORB setup. It does not claim to reproduce historical ATM/OTM option-spread premiums.

## Install

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.backtest.txt
cp .env.backtest.example .env
```

Edit `.env` and set the OpenAlgo API key.

## Run using Zerodha through OpenAlgo

```bash
python openalgo_orb_backtest.py --start 2023-01-01 --end 2026-10-20 --source api --outdir results
```

## Run using candles already stored in Historify

```bash
python openalgo_orb_backtest.py --start 2023-01-01 --end 2026-10-20 --source db --outdir results
```

## Living state table

```bash
python openalgo_orb_backtest.py --start 2026-10-01 --end 2026-10-20 --source db --outdir results --watch --refresh-minutes 5
```

This atomically refreshes:

- `results/living_state.csv`: every evaluated candle, filter states, signal, action and position state
- `results/backtest_report.html`: auto-refreshing browser report
- `results/backtest_trades.csv`: completed trades
- `results/backtest_summary.json`: headline metrics

Open `results/backtest_report.html` in a browser.

## Model assumptions

- Signal candle must close above the opening-range high and satisfy VWAP, EMA, RSI and candle-quality filters.
- Entry occurs at the next 5-minute bar open to avoid look-ahead bias.
- The same-bar stop/target ambiguity is resolved conservatively in favour of the stop.
- The default underlying proxy stop is 0.35%; target is 0.60%; maximum hold is 60 minutes.
- Slippage and costs are deducted.
- Position size models 1% initial-capital risk, not an options lot.
- One trade is allowed per day.

Adjust parameters inside the `Config` dataclass only after retaining an untouched out-of-sample period.
