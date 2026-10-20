# OpenAlgo Zerodha Live Data Collector

This collector is read-only. It places no orders.

## What it stores

- Normalized OpenAlgo Quote WebSocket ticks for NIFTY and selected nearest-expiry options
- Locally aggregated 1-minute OHLCV/OI bars
- Periodic option-chain snapshots including bid, ask, OI, volume, labels and lot size
- Connection, authentication, subscription and error events
- A living CSV/JSON table showing the latest bar for every subscribed symbol

All persistent data is stored in `data/zerodha_live.sqlite3` using SQLite WAL mode.

## Install

```bash
python3 -m venv .venv-collector
source .venv-collector/bin/activate
pip install -r requirements.collector.txt
cp .env.collector.example .env
```

Set your OpenAlgo API key in `.env`. For a local deployment use:

```ini
OPENALGO_HOST=http://127.0.0.1:5000
OPENALGO_WS_URL=ws://127.0.0.1:8765
```

For a Codespaces HTTPS deployment proxied through Caddy, use the external HTTPS host and WSS route configured for OpenAlgo, for example:

```ini
OPENALGO_HOST=https://your-codespace-domain
OPENALGO_WS_URL=wss://your-codespace-domain/ws
```

## Run

```bash
python openalgo_zerodha_live_collector.py
```

Verbose diagnostics:

```bash
python openalgo_zerodha_live_collector.py --verbose
```

## Confirm collection

```bash
cat data/live_collector_state.json
column -s, -t < data/live_collector_state.csv | head -30
sqlite3 data/zerodha_live.sqlite3 'select exchange,symbol,count(*) from minute_bars group by exchange,symbol order by 3 desc;'
sqlite3 data/zerodha_live.sqlite3 'select event_ts,level,event,detail from collector_events order by rowid desc limit 20;'
```

A healthy state has `connected: true`, `authenticated: true`, a current nearest expiry, a recent `last_message_at`, and multiple NIFTY/NFO symbols.

## Tables

- `ticks`: normalized WebSocket quote events
- `minute_bars`: locally aggregated 1-minute bars
- `option_chain_snapshots`: periodic full chain observations
- `collector_events`: connection and error audit trail

## Run as a service

Update paths in `zerodha-live-collector.service`, then:

```bash
sudo cp zerodha-live-collector.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now zerodha-live-collector
sudo journalctl -u zerodha-live-collector -f
```

GitHub Codespaces can stop or sleep, so it is not suitable for guaranteed uninterrupted collection. Use an always-on VM for a durable research dataset.
