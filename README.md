# OKX Withdrawals Exporter (API v5)

Export full withdrawal history from OKX with deep pagination. Streams results to CSV (default) or JSONL and supports currency/time filters and address filtering.

## Setup

```
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
# Create .env with your keys
# OKX_KEY=...
# OKX_SECRET=...
# OKX_PASSPHRASE=...
```

Optional env:
- `OKX_BASE_URL` (default `https://www.okx.com`)
- `OKX_USE_DEMO=1` (adds `x-simulated-trading: 1`)

## Usage

Basic:
```
python okx.py --out withdrawals.csv
```

Currency and time window (UTC):
```
python okx.py --out withdrawals_usdt_2023.csv --ccy USDT --start 2023-01-01 --end 2023-12-31
```

Address filtering (write only rows containing any address from file):
```
python okx.py --out withdrawals_filtered.csv --addr-file wallets.txt
```

Replicate the exact run used in this repo (ETH, 2023‑11‑29..2023‑12‑03, with address filter):
```
python okx.py \
  --out withdrawals_eth_20231129_20231203_filtered.csv \
  --ccy ETH \
  --start "2023-11-29 00:00:00" \
  --end   "2023-12-03 23:59:59" \
  --addr-file wallets.txt
```

## Notes
- Deep pagination uses `after = min(ts_on_page) - 1` across pages.
- `--start` is applied client‑side (`ts >= start`); `--end` is sent as `before` only on the first request.
- Output format can be `csv` (default) or `jsonl` via `--fmt`.

## License
MIT
