# OKX History Tools

Utilities to export and search OKX account history via REST API v5.

Included scripts:
- okx_.py: Dump withdrawals (CSV/JSONL) with deep pagination; optional currency, time window, address filter.
- okx_check_txids.py: Check a list of txids across master and sub-accounts, deposits/withdrawals/bills.
- okx_addr_filter.py: Search deposits/withdrawals/bills by a batch of addresses across multiple API keys.

## Setup

```
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env  # then edit with your keys
```

Env variables (read from environment or .env):
- OKX_KEY
- OKX_SECRET
- OKX_PASSPHRASE

Optional:
- OKX_BASE_URL (default https://www.okx.com)
- OKX_USE_DEMO=1 (adds x-simulated-trading header)

## okx_.py (Withdrawals export)

```
python okx_.py --out withdrawals.csv [--fmt csv|jsonl] [--ccy USDT] [--start 2023-11-29] [--end 2023-12-03] [--addr-file wallets.txt]
```

- Deep pagination using after=min(ts)-1 across pages.
- start is applied client-side (ts >= start), end is sent as before only on the first request.
- When --addr-file is provided, only rows containing any of the addresses are written.

Example (replicates our exact run):
```
python okx_.py \
  --out withdrawals_eth_20231129_20231203_filtered.csv \
  --ccy ETH \
  --start "2023-11-29 00:00:00" \
  --end   "2023-12-03 23:59:59" \
  --addr-file wallets.txt
```

## okx_check_txids.py (TXID presence check)

```
python okx_check_txids.py --env-file okx1.env --txids-file txids.txt --all-subaccts --include-bills
```

- Scans deposits, withdrawals, and optionally bills for exact txid matches.
- Works across master and sub-accounts.

## okx_addr_filter.py (Batch address search)

```
python okx_addr_filter.py --addr-file wallets.txt --env-files okx1.env,okx2.env --all-subaccts --include-bills
```

- Streams newest->older using OKX cursors; writes matches to CSV.
- Supports multiple API keys and sub-accounts.

## License

MIT
