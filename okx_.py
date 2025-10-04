#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Export full withdrawal history from OKX API v5 with deep pagination.
Streams to CSV (default) or JSONL to avoid holding all data in memory.

Dependencies: requests, python-dotenv (optional)
Install:      pip install -r requirements.txt

Required environment variables:
  OKX_KEY
  OKX_SECRET
  OKX_PASSPHRASE

Optional:
  OKX_BASE_URL        (default https://www.okx.com)
  OKX_USE_DEMO=1      (adds header x-simulated-trading: 1)

Examples:
  # Everything (all currencies) -> CSV:
  export OKX_KEY=... OKX_SECRET=... OKX_PASSPHRASE=...
  python okx.py --out withdrawals.csv

  # Only USDT in 2023 -> JSONL:
  python okx.py --ccy USDT --start 2023-01-01 --end 2023-12-31 \
      --fmt jsonl --out withdrawals_2023.jsonl
"""
import argparse
import base64
import csv
import datetime as dt
import hashlib
import hmac
import json
import os
import sys
import time
import typing as t
import urllib.parse as up

import requests
from requests.adapters import HTTPAdapter
try:
    from urllib3.util.retry import Retry
except Exception:
    # старые версии urllib3
    from requests.packages.urllib3.util.retry import Retry  # type: ignore

try:
    # Load .env if present (optional)
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass


BASE_URL = os.environ.get("OKX_BASE_URL", "https://www.okx.com").rstrip("/")
ENDPOINT = "/api/v5/asset/withdrawal-history"
LIMIT = 100  # OKX maximum for this endpoint


def iso_utc_now_ms() -> str:
    """OK-ACCESS-TIMESTAMP in ISO8601 with milliseconds and 'Z'."""
    return dt.datetime.utcnow().isoformat(timespec="milliseconds") + "Z"


def sign(secret: str, timestamp: str, method: str, request_path: str, body: str = "") -> str:
    """
    OKX signature: Base64(HMAC_SHA256(timestamp + method + requestPath + body, secret))
    For GET include the query string (?a=b&...) in requestPath.
    """
    msg = f"{timestamp}{method}{request_path}{body}"
    digest = hmac.new(secret.encode("utf-8"), msg.encode("utf-8"), hashlib.sha256).digest()
    return base64.b64encode(digest).decode("utf-8")


def build_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=10,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"])
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update({"Content-Type": "application/json"})
    # demo environment if required
    if os.environ.get("OKX_USE_DEMO") in ("1", "true", "True"):
        s.headers["x-simulated-trading"] = "1"
    return s


def okx_get(session: requests.Session, key: str, secret: str, passphrase: str,
            path: str, params: t.Dict[str, t.Any]) -> t.Dict[str, t.Any]:
    # Build query first and reuse it in the signature
    query = up.urlencode({k: v for k, v in params.items() if v is not None}, doseq=True)
    request_path = path + (("?" + query) if query else "")
    url = BASE_URL + request_path

    ts = iso_utc_now_ms()
    signature = sign(secret, ts, "GET", request_path)

    headers = {
        "OK-ACCESS-KEY": key,
        "OK-ACCESS-SIGN": signature,
        "OK-ACCESS-TIMESTAMP": ts,
        "OK-ACCESS-PASSPHRASE": passphrase,
    }
    resp = session.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    if data.get("code") != "0":
        raise RuntimeError(f"OKX error: code={data.get('code')} msg={data.get('msg')}")
    return data


def parse_date_to_ms(s: str) -> int:
    """
    Accepts formats: YYYY-MM-DD[, HH:MM[:SS]]
    Returns Unix ms UTC.
    """
    s = s.strip()
    # Попробуем несколько шаблонов
    fmts = ["%Y-%m-%d", "%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S"]
    for f in fmts:
        try:
            dt_ = dt.datetime.strptime(s, f)
            return int(dt_.replace(tzinfo=dt.timezone.utc).timestamp() * 1000)
        except ValueError:
            pass
    raise ValueError(f"Cannot parse date: {s}. Supported: YYYY-MM-DD[, HH:MM[:SS]]")


def write_csv_row(writer: csv.DictWriter, row: dict, field_order: t.List[str]):
    # Ensure all fields exist
    normalized = {k: row.get(k, "") for k in field_order}
    writer.writerow(normalized)


def dump_withdrawals(
    out_path: str,
    fmt: str = "csv",
    ccy: t.Optional[str] = None,
    start_ms: t.Optional[int] = None,
    end_ms: t.Optional[int] = None,
    addr_set: t.Optional[t.Set[str]] = None,
) -> int:
    """
    Deep pagination until data is exhausted.
    - Start from the most recent page (no after/before).
    - Move backward: after = (min ts on page) - 1.
    - With start_ms / end_ms we constrain the window:
        * start_ms  -> client-side filter ts >= start_ms
        * end_ms    -> sent as before on the first request
    Returns number of saved records.
    """


    key = os.environ.get("OKX_KEY", "").strip()
    secret = os.environ.get("OKX_SECRET", "").strip()
    passphrase = os.environ.get("OKX_PASSPHRASE", "").strip()
    if not key or not secret or not passphrase:
        raise SystemExit("Missing env vars: OKX_KEY / OKX_SECRET / OKX_PASSPHRASE")

    session = build_session()

    # Подготовка файла
    saved = 0
    field_order: t.List[str] = []  # определим после первой страницы
    csv_writer: t.Optional[csv.DictWriter] = None
    f_out = None

    # Open output
    if fmt == "jsonl":
        f_out = open(out_path, "w", encoding="utf-8")
    elif fmt == "csv":
        f_out = open(out_path, "w", encoding="utf-8", newline="")
    else:
        raise SystemExit("Поддерживаемые форматы: csv, jsonl")

    try:
        after_cursor: t.Optional[int] = None
        page_idx = 0

        while True:
            # Build request params
            params: t.Dict[str, t.Any] = {"limit": str(LIMIT)}
            if ccy:
                params["ccy"] = ccy
            # Ограничиваем верхнюю границу только на ПЕРВОМ запросе,
            # далее пагинируем строго по after (ts-1), чтобы не получать пустые страницы
            if after_cursor is None and end_ms is not None:
                params["before"] = str(end_ms)
            if after_cursor is not None:
                params["after"] = str(after_cursor)

            data = okx_get(session, key, secret, passphrase, ENDPOINT, params)
            rows_all: t.List[dict] = data.get("data", []) or []

            if not rows_all:
                # no more pages
                break

            # OKX returns newest -> older. Apply start_ms filter if set.
            if start_ms is not None:
                rows = [r for r in rows_all if int(r.get("ts", "0")) >= start_ms]
            else:
                rows = rows_all

            # Фильтрация по адресам из addr_set (если задано)
            if addr_set:
                aset = {a.lower() for a in addr_set}
                def row_has_addr(r: dict) -> bool:
                    # Проверяем основные адресные поля и общий конкат
                    fields = [
                        str(r.get("to", "")),
                        str(r.get("toAddr", "")),
                        str(r.get("addr", "")),
                        str(r.get("from", "")),
                        str(r.get("memo", "")),
                        str(r.get("tag", "")),
                    ]
                    s_all = " ".join(fields).lower()
                    for a in aset:
                        if a and a in s_all:
                            return True
                    return False
                rows = [r for r in rows if row_has_addr(r)]

            # First page — prepare writer and field order
            if saved == 0:
                # union of keys + common keys in stable order
                base_keys = [
                    "wdId", "ts", "ccy", "amt", "state", "fee", "feeCcy",
                    "chain", "txId", "to", "toAddrType", "from", "areaCodeFrom",
                    "areaCodeTo", "nonTradableAsset", "clientId", "note", "tag",
                    "pmtId", "memo", "addrEx"
                ]
                # union
                all_keys = list(dict.fromkeys([k for r in rows for k in r.keys()] + base_keys))
                field_order = all_keys

                if fmt == "csv":
                    csv_writer = csv.DictWriter(f_out, fieldnames=field_order, dialect="excel")
                    csv_writer.writeheader()

            # Write
            if fmt == "csv":
                assert csv_writer is not None
                for r in rows:
                    # addrEx might be an object — stringify as JSON
                    if isinstance(r.get("addrEx"), (dict, list)):
                        r = {**r, "addrEx": json.dumps(r["addrEx"], ensure_ascii=False)}
                    write_csv_row(csv_writer, r, field_order)
            else:
                for r in rows:
                    f_out.write(json.dumps(r, ensure_ascii=False) + "\n")

            saved += len(rows)
            page_idx += 1

            # Move cursor: take min ts on UNFILTERED page and -1 ms
            min_ts = min(int(r.get("ts", "0")) for r in rows_all)
            after_cursor = min_ts - 1

            # Rate ≤ 6 rps; keep margin
            time.sleep(0.2)

            # Early stop if already paginated past start_ms
            if start_ms is not None and after_cursor < start_ms:
                break

            # Progress
            print(f"[page {page_idx}] +{len(rows)} (total {saved}), next after<{after_cursor}>", file=sys.stderr)

    finally:
        if f_out:
            f_out.flush()
            f_out.close()

    return saved


def main():
    p = argparse.ArgumentParser(description="Dump OKX withdrawal history to file (CSV/JSONL) with pagination.")
    p.add_argument("--out", required=True, help="Output file path (e.g. withdrawals.csv)")
    p.add_argument("--fmt", choices=["csv", "jsonl"], default="csv", help="Output format (csv|jsonl)")
    p.add_argument("--ccy", help="Currency filter (e.g. USDT)")
    p.add_argument("--start", help="Start (UTC), format YYYY-MM-DD[ HH:MM[:SS]]")
    p.add_argument("--end", help="End (UTC), format YYYY-MM-DD[ HH:MM[:SS]]")
    p.add_argument("--addr-file", help="Path to file with addresses (one per line) to filter records")
    args = p.parse_args()

    start_ms = parse_date_to_ms(args.start) if args.start else None
    end_ms = parse_date_to_ms(args.end) if args.end else None

    # Загрузим список адресов, если указан файл
    addr_set: t.Optional[t.Set[str]] = None
    if args.addr_file:
        with open(args.addr_file, "r", encoding="utf-8") as f:
            addr_set = {ln.strip().lower() for ln in f if ln.strip()}

    count = dump_withdrawals(
        out_path=args.out,
        fmt=args.fmt,
        ccy=args.ccy,
        start_ms=start_ms,
        end_ms=end_ms,
        addr_set=addr_set,
    )
    print(f"Done. Saved records: {count}")


if __name__ == "__main__":
    main()

