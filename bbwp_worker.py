#!/usr/bin/env python3
"""
BBWP compression screener worker.

Runs on a schedule, computes Bollinger Band Width Percentile across the top N
alts, and pushes state-change alerts to Telegram. Stateful: it only alerts when
a symbol CHANGES state, so a six-hour cadence does not produce six identical
messages about the same setup.

Env vars:
  TELEGRAM_BOT_TOKEN   from @BotFather
  TELEGRAM_CHAT_ID     your channel or DM id
  MESSARI_API_KEY      optional, enables the unlock filter
  UNIVERSE_SIZE        optional, default 50
  DRY_RUN              optional, "1" to print instead of send
"""

import json
import os
import sys
import time
import math
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

BB_LEN = 13
MULT = 2
LOOKBACK = 252
MA_LEN = 8
FREEZE = 15.0
COIL = 30.0
HOT = 85.0

STATE_PATH = Path(__file__).parent / "state.json"
UA = {"User-Agent": "bbwp-screener/1.0"}

STABLE = {
    "USDT", "USDC", "DAI", "TUSD", "FDUSD", "BUSD", "USDE", "PYUSD", "USDD",
    "EURT", "WBTC", "WETH", "STETH", "WSTETH", "WBETH",
}

# states that are worth waking you up for
ALERTABLE = {"FIRING", "WAKING"}


# ---------- http ----------

def get_json(url, headers=None, retries=3, backoff=1.5):
    last = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={**UA, **(headers or {})})
            with urllib.request.urlopen(req, timeout=20) as r:
                return json.loads(r.read().decode())
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(backoff ** attempt)
    raise RuntimeError(f"GET failed after {retries}: {url} :: {last}")


# ---------- data ----------

def universe(n):
    j = get_json("https://api.bybit.com/v5/market/tickers?category=spot")
    if j.get("retCode") != 0:
        raise RuntimeError(f"bybit tickers: {j.get('retMsg')}")
    rows = []
    for t in j["result"]["list"]:
        sym = t["symbol"]
        if not sym.endswith("USDT"):
            continue
        base = sym[:-4]
        if base in STABLE or base == "BTC":
            continue
        try:
            to = float(t.get("turnover24h") or 0)
        except ValueError:
            continue
        rows.append({"symbol": sym, "base": base, "turnover": to})
    rows.sort(key=lambda r: r["turnover"], reverse=True)
    return rows[:n]


def candles(symbol, limit=400):
    url = (
        "https://api.bybit.com/v5/market/kline"
        f"?category=spot&symbol={symbol}&interval=D&limit={limit}"
    )
    j = get_json(url)
    if j.get("retCode") != 0 or not j.get("result", {}).get("list"):
        raise RuntimeError(f"no klines for {symbol}")
    rows = [
        {"t": int(k[0]), "o": float(k[1]), "h": float(k[2]), "l": float(k[3]), "c": float(k[4])}
        for k in j["result"]["list"]
    ]
    rows.sort(key=lambda r: r["t"])
    return rows


# ---------- math ----------

def aggregate(rows, n_days):
    if n_days == 1:
        return rows
    out = []
    i = len(rows)
    while i > 0:
        chunk = rows[max(0, i - n_days):i]
        if chunk:
            out.insert(0, {
                "t": chunk[0]["t"],
                "o": chunk[0]["o"],
                "h": max(c["h"] for c in chunk),
                "l": min(c["l"] for c in chunk),
                "c": chunk[-1]["c"],
            })
        i -= n_days
    return out


def bbw_series(closes):
    out = [float("nan")] * len(closes)
    for i in range(len(closes)):
        if i < BB_LEN - 1:
            continue
        win = closes[i - BB_LEN + 1:i + 1]
        basis = sum(win) / BB_LEN
        if basis == 0:
            continue
        var = sum((x - basis) ** 2 for x in win) / BB_LEN
        sd = math.sqrt(var)
        out[i] = ((2 * MULT * sd) / basis) * 100
    return out


def bbwp_series(closes):
    bbw = bbw_series(closes)
    out = [float("nan")] * len(bbw)
    for i, cur in enumerate(bbw):
        if math.isnan(cur):
            continue
        start = max(0, i - LOOKBACK)
        prior = [x for x in bbw[start:i] if not math.isnan(x)]
        if len(prior) < 20:
            continue
        out[i] = (sum(1 for x in prior if x < cur) / len(prior)) * 100
    return out


def classify(bbwp):
    """Return (status, streak) from a BBWP series."""
    last = len(bbwp) - 1
    cur = bbwp[last]
    if math.isnan(cur):
        return None, 0
    prev = bbwp[last - 1] if last >= 1 else float("nan")
    prev2 = bbwp[last - 2] if last >= 2 else float("nan")

    streak = 0
    for i in range(last, -1, -1):
        v = bbwp[i]
        if not math.isnan(v) and v <= FREEZE:
            streak += 1
        elif streak > 0:
            break
        elif i < last - 3:
            break

    was_frozen = (not math.isnan(prev)) and prev <= FREEZE
    if cur >= HOT:
        status = "HOT"
    elif cur <= FREEZE:
        status = "COMPRESSED"
    elif was_frozen:
        status = "FIRING"
    elif (not math.isnan(prev2)) and prev2 <= FREEZE:
        status = "WAKING"
    elif cur <= COIL and (not math.isnan(prev)) and cur < prev:
        status = "COILING"
    else:
        status = "NEUTRAL"
    return status, streak


def analyze(base, rows, tf=3):
    agg = aggregate(rows, tf)
    if len(agg) < 40:
        return None
    closes = [c["c"] for c in agg]
    series = bbwp_series(closes)
    status, streak = classify(series)
    if status is None:
        return None

    cur = series[-1]
    ma_vals = [v for v in series[-MA_LEN:] if not math.isnan(v)]
    ma = sum(ma_vals) / len(ma_vals) if ma_vals else float("nan")

    win = agg[-max(streak, 10):]
    hi = max(c["h"] for c in win)
    lo = min(c["l"] for c in win)
    rng = ((hi - lo) / lo * 100) if lo > 0 else 0.0

    px = rows[-1]["c"]

    def chg(n):
        if len(rows) <= n:
            return float("nan")
        ref = rows[-1 - n]["c"]
        return (px - ref) / ref * 100 if ref else float("nan")

    # multi-timeframe compression check
    dots = []
    for n in (1, 3, 7):
        s = bbwp_series([c["c"] for c in aggregate(rows, n)])
        v = s[-1] if s else float("nan")
        dots.append(None if math.isnan(v) else round(v, 1))
    mtf = sum(1 for d in dots if d is not None and d <= FREEZE)

    return {
        "base": base,
        "status": status,
        "bbwp": round(cur, 1),
        "ma": round(ma, 1) if not math.isnan(ma) else None,
        "streak": streak,
        "range": round(rng, 1),
        "price": px,
        "w1": round(chg(7), 1) if not math.isnan(chg(7)) else None,
        "m1": round(chg(30), 1) if not math.isnan(chg(30)) else None,
        "dots": dots,
        "mtf": mtf,
        "bar": rows[-1]["t"],
    }


# ---------- messari (optional) ----------

def unlock_flag(base, key):
    """Return a short warning string if a cliff is near, else ''. Best effort."""
    if not key:
        return ""
    try:
        url = f"https://api.messari.io/api/v1/assets/{urllib.parse.quote(base.lower())}/metrics"
        j = get_json(url, headers={"x-messari-api-key": key}, retries=1)
        md = (j.get("data") or {}).get("supply") or {}
        circ = md.get("circulating")
        total = md.get("y_2050") or md.get("liquid")
        if circ and total and total > 0:
            float_pct = circ / total * 100
            if float_pct < 15:
                return f" ⚠ float {float_pct:.0f}%"
    except Exception:  # noqa: BLE001
        return ""
    return ""


# ---------- alerting ----------

def send(text, token, chat_id, dry):
    if dry or not token or not chat_id:
        print("[dry-run]\n" + text)
        return
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    body = urllib.parse.urlencode({
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": "true",
    }).encode()
    req = urllib.request.Request(url, data=body, headers=UA)
    with urllib.request.urlopen(req, timeout=20) as r:
        r.read()


def load_state():
    if STATE_PATH.exists():
        try:
            return json.loads(STATE_PATH.read_text())
        except Exception:  # noqa: BLE001
            pass
    return {"symbols": {}, "last_run": None}


def save_state(s):
    s["last_run"] = datetime.now(timezone.utc).isoformat()
    STATE_PATH.write_text(json.dumps(s, indent=2, sort_keys=True))


def fmt(r, unlock=""):
    dots = " ".join("●" if (d is not None and d <= FREEZE) else "○" for d in r["dots"])
    lines = [
        f"<b>${r['base']}</b>  {r['status']}{unlock}",
        f"bbwp {r['bbwp']}  ma{MA_LEN} {r['ma']}  streak {r['streak']} bars",
        f"range {r['range']}%  1w {r['w1']}%  1m {r['m1']}%",
        f"1D·3D·1W {dots}   px {r['price']:.6g}",
    ]
    return "\n".join(lines)



DATA_PATH = Path(__file__).parent / "data.json"


def write_dashboard(results, failures):
    """Emit a snapshot the static dashboard reads. No server required."""
    payload = {
        "generated": datetime.now(timezone.utc).isoformat(),
        "config": {
            "bb_len": BB_LEN, "mult": MULT, "lookback": LOOKBACK,
            "ma_len": MA_LEN, "freeze": FREEZE, "coil": COIL, "hot": HOT,
        },
        "counts": {},
        "failures": len(failures),
        "rows": sorted(
            results,
            key=lambda r: (
                {"FIRING": 0, "WAKING": 1, "COMPRESSED": 2, "COILING": 3, "HOT": 4, "NEUTRAL": 5}[r["status"]],
                -r["mtf"], -r["streak"],
            ),
        ),
    }
    for r in results:
        payload["counts"][r["status"]] = payload["counts"].get(r["status"], 0) + 1
    DATA_PATH.write_text(json.dumps(payload, indent=1))
    print(f"wrote data.json ({len(results)} rows)")


def main():
    token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    chat = os.getenv("TELEGRAM_CHAT_ID", "")
    mkey = os.getenv("MESSARI_API_KEY", "")
    n = int(os.getenv("UNIVERSE_SIZE", "50"))
    dry = os.getenv("DRY_RUN") == "1"

    state = load_state()
    prev = state.get("symbols", {})

    uni = universe(n)
    print(f"universe: {len(uni)} symbols")

    results, failures = [], []
    for u in uni:
        try:
            rows = candles(u["symbol"])
            r = analyze(u["base"], rows)
            if r:
                results.append(r)
        except Exception as e:  # noqa: BLE001
            failures.append(f"{u['base']}: {e}")
        time.sleep(0.12)  # stay well inside rate limits

    if not results:
        # Fail loudly. A screener that silently returns nothing looks exactly
        # like a quiet market, and that is how you stop trusting it.
        send("⚠️ BBWP worker: zero results. Data source may be down.", token, chat, dry)
        print("FATAL: no results", file=sys.stderr)
        print("\n".join(failures[:10]), file=sys.stderr)
        sys.exit(1)

    print(f"analyzed {len(results)}, failed {len(failures)}")
    write_dashboard(results, failures)

    # only alert on genuine state changes into an alertable status
    fired = []
    new_state = {}
    for r in results:
        key = r["base"]
        old = prev.get(key, {})
        new_state[key] = {"status": r["status"], "bar": r["bar"]}
        changed = old.get("status") != r["status"]
        if r["status"] in ALERTABLE and changed:
            fired.append(r)

    fired.sort(key=lambda r: (-r["mtf"], -r["streak"]))

    if fired:
        head = f"🧊→🔥 <b>{len(fired)} squeeze release{'s' if len(fired) > 1 else ''}</b>"
        blocks = [fmt(r, unlock_flag(r["base"], mkey)) for r in fired[:8]]
        tail = (
            "\nDirection not implied. Expansion resolves down as often as up. "
            "Check OI and sector before this becomes a plan."
        )
        send(head + "\n\n" + "\n\n".join(blocks) + tail, token, chat, dry)
    else:
        print("no state changes worth alerting")

    # daily digest of what is coiling, once per day at the 00 UTC run
    if datetime.now(timezone.utc).hour < 4:
        comp = sorted(
            [r for r in results if r["status"] in ("COMPRESSED", "COILING")],
            key=lambda r: (-r["mtf"], -r["streak"]),
        )[:10]
        if comp:
            body = "\n\n".join(fmt(r) for r in comp)
            send(f"❄️ <b>Watchlist: compressed</b>\n\n{body}", token, chat, dry)

    state["symbols"] = new_state
    save_state(state)
    print("done")


if __name__ == "__main__":
    main()
