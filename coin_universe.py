"""
OxyPulse — coin_universe.py
Unified futures universe and execution-aware ranking for all engines.

The runtime universe is the full offered OKX USDT perpetual swap set, filtered by:
  - tradable swap contracts only
  - bid/ask/last sanity
  - 24h volume floor
  - spread sanity
  - recent execution-admission cooldowns

All engines can evaluate the same broad universe. Candidate ordering then prefers
liquid, tight-spread, reliable, small-account-executable symbols.
"""
from __future__ import annotations

import logging
import asyncio
import os
import time

import httpx
from rest_rate_governor import process_rest_rate_governor, run_with_rate_admission

import execution_admission as _admission
import settings_store as _s
from liquidity_model import quote_volume_24h

logger = logging.getLogger("OxyPulse.CoinUniverse")

CATEGORIES: dict[str, list[str]] = {
    "L1": [
        "BTC", "ETH", "SOL", "BNB", "ADA", "AVAX", "DOT", "ATOM",
        "NEAR", "FTM", "ALGO", "EGLD", "HBAR", "ICP", "ONE",
    ],
    "L2": [
        "ARB", "OP", "MATIC", "IMX", "STRK", "METIS",
    ],
    "DeFi": [
        "UNI", "AAVE", "LINK", "CRV", "DYDX", "SNX", "COMP",
        "MKR", "LDO", "GMX", "1INCH",
    ],
    "Meme": [
        "DOGE", "SHIB", "PEPE", "FLOKI", "BONK", "WIF", "MEME",
        "TURBO", "BOME", "DOGS",
    ],
    "AI": [
        "FET", "AGIX", "OCEAN", "RNDR", "WLD", "TAO", "AKT", "GRT",
    ],
    "Gaming": [
        "AXS", "SAND", "MANA", "ENJ", "GALA", "BEAM", "MAGIC",
    ],
    "RWA_New": [
        "ONDO", "TIA", "SEI", "SUI", "APT", "INJ", "PYTH",
        "JTO", "JUP", "W",
    ],
    "Alts": [
        "XRP", "TRX", "TON", "LTC", "BCH", "XLM", "VET",
        "THETA", "FIL", "IOTA",
    ],
    "HiVol": [
        "TRUMP", "PNUT", "MOVE", "ACT", "DEEP", "IO",
        "ZK", "LISTA", "ZETA", "BLUR",
    ],
}

_STABLECOINS = {"USDT", "USDC", "BUSD", "DAI", "TUSD", "USDP", "FDUSD", "USDD", "UST", "LUSD", "USDE"}
_WRAPPED = {"WBTC", "WETH", "WBNB", "WMATIC", "WSOL"}
_EXCLUDE = _STABLECOINS | _WRAPPED

MIN_VOLUME_USD = float(os.getenv("MIN_COIN_VOL_USD", "50000"))

_last_top50: list[str] = []
_last_tradeable: list[str] = []
_last_refresh: float = 0.0
_volume_rank: dict[str, float] = {}
_spread_rank: dict[str, float] = {}

# Universe discovery is public market traffic.  Keep one bounded transport
# alive so each scan can reuse connections without sharing private auth state.
def _new_universe_http() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        timeout=httpx.Timeout(10.0, connect=5.0),
        limits=httpx.Limits(max_connections=4, max_keepalive_connections=2),
    )


_universe_http: httpx.AsyncClient | None = _new_universe_http()
_universe_http_gate = asyncio.Semaphore(2)


def _get_universe_http() -> httpx.AsyncClient:
    global _universe_http
    if _universe_http is None or _universe_http.is_closed:
        _universe_http = _new_universe_http()
    return _universe_http


async def close_public_http_client() -> None:
    """Close the reusable public transport; safe to call repeatedly."""
    global _universe_http
    client = _universe_http
    if client is None:
        return
    _universe_http = None
    try:
        await client.aclose()
    except Exception:
        logger.debug("Public universe HTTP client close failed", exc_info=True)

# Futures contract value estimates (ctVal × base units)
_CT_VAL_ESTIMATES: dict[str, float] = {
    "PEPE": 100_000_000, "BONK": 100_000_000,
    "FLOKI": 10_000_000, "BOME": 100_000_000,
    "TURBO": 100_000_000, "DOGS": 100_000_000,
    "SHIB": 1_000_000, "WIF": 10,
    "MEME": 1_000,
    "TRX": 1_000, "XLM": 1_000, "DOGE": 1_000,
    "ADA": 100, "XRP": 100, "MATIC": 100,
    "VET": 1_000, "ALGO": 1_000, "HBAR": 1_000,
    "ONE": 10_000, "FTM": 1_000, "SEI": 100,
    "APT": 10, "SUI": 100, "TIA": 10,
    "NEAR": 100, "ATOM": 10, "PYTH": 100,
    "JTO": 10, "W": 10,
    "LINK": 10, "UNI": 10, "ARB": 100, "OP": 10,
    "IMX": 10, "INJ": 10, "FET": 10, "AGIX": 10,
    "RNDR": 10, "GRT": 100, "LDO": 10,
    "GALA": 1_000, "SAND": 100, "MANA": 100, "AXS": 10,
    "BEAM": 1_000,
    "AVAX": 1, "DOT": 10, "SOL": 1, "BNB": 0.1,
    "ETH": 0.1, "BTC": 0.01,
}

TOP20_MCAP: list[str] = [
    "BTC", "ETH", "XRP", "BNB", "SOL", "ADA", "DOGE", "TRX",
    "LINK", "AVAX", "SUI", "DOT", "SHIB", "TON", "XLM",
    "HBAR", "BCH", "LTC", "UNI", "APT",
]

IZANAGI_LIQUID_UNIVERSE: list[str] = [
    "BTC", "ETH", "XRP", "BNB", "SOL", "ADA", "DOGE", "TRX",
    "LINK", "AVAX", "SUI", "DOT", "TON", "XLM", "HBAR",
    "BCH", "LTC", "APT",
]

NANO_GROWTH_COINS: list[str] = [
    "PEPE", "BONK", "FLOKI", "BOME", "TURBO", "DOGS",
    "SHIB", "WIF", "MEME", "TRX", "XLM", "DOGE",
    "ADA", "XRP", "MATIC", "ALGO", "VET", "NEAR",
    "SEI", "APT", "SUI", "TIA", "PYTH",
]


def _dedupe(items: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for coin in items:
        c = str(coin or "").upper()
        if not c or c in seen:
            continue
        seen.add(c)
        ordered.append(c)
    return ordered


def _volume_floor() -> float:
    if not bool(_s.get_val("dead_coin_suppression_enabled", True)):
        return 0.0
    return max(float(_s.get_val("universe_min_volume_usd_24h", 250000.0) or 250000.0), float(MIN_VOLUME_USD))


def _spread_cap_pct() -> float:
    if not bool(_s.get_val("spread_filter_enabled", True)):
        return 999.0
    return max(float(_s.get_val("universe_spread_max_pct", 1.0) or 1.0), 0.05)


def _spread_pct(bid: float, ask: float) -> float:
    if bid <= 0 or ask <= 0:
        return 999.0
    mid = (bid + ask) / 2.0
    if mid <= 0:
        return 999.0
    return abs(ask - bid) / mid * 100.0


def _fallback_universe() -> list[str]:
    static = [c for coins in CATEGORIES.values() for c in coins]
    return _dedupe(list(TOP20_MCAP) + static + list(NANO_GROWTH_COINS))


def _large_cap_bonus(coin: str, balance_usdt: float = 0.0) -> float:
    if not bool(_s.get_val("market_cap_bonus_enabled", True)):
        return 0.0
    if str(coin or "").upper() not in set(TOP20_MCAP):
        return 0.0
    small_cutoff = float(_s.get_val("large_cap_preference_balance_usdt", 100.0) or 100.0)
    if (
        bool(_s.get_val("small_account_large_cap_preference_enabled", True))
        and balance_usdt > 0
        and balance_usdt <= small_cutoff
    ):
        return float(_s.get_val("large_cap_preference_bonus_small_account", 8.0) or 8.0)
    return float(_s.get_val("large_cap_preference_bonus_default", 3.0) or 3.0)


async def _fetch_tradeable_universe() -> tuple[list[str], list[str], dict[str, float], dict[str, float]]:
    volume_floor = _volume_floor()
    spread_cap = _spread_cap_pct()
    resp, rate_bucket = await run_with_rate_admission(
        "GET", "/api/v5/market/tickers", params={"instType": "SWAP"}, priority="P2",
        pool_gate=_universe_http_gate,
        send=lambda: _get_universe_http().get(
            "https://www.okx.com/api/v5/market/tickers",
            params={"instType": "SWAP"},
        ),
    )
    if resp.status_code == 429:
        process_rest_rate_governor.record_http_429(rate_bucket)
    resp.raise_for_status()
    payload = resp.json()
    if str(payload.get("code", "")) == "50011":
        process_rest_rate_governor.record_business_50011(rate_bucket)
    data = payload.get("data", [])

    ranked_rows: list[tuple[str, float, float]] = []
    volume_map: dict[str, float] = {}
    spread_map: dict[str, float] = {}

    for row in data:
        inst = str(row.get("instId", "") or "")
        if not inst.endswith("-USDT-SWAP"):
            continue
        coin = inst.replace("-USDT-SWAP", "").upper()
        if coin in _EXCLUDE:
            continue
        last = float(row.get("last", 0) or 0)
        bid = float(row.get("bidPx", 0) or 0)
        ask = float(row.get("askPx", 0) or 0)
        vol = quote_volume_24h(row)
        if last <= 0 or bid <= 0 or ask <= 0:
            continue
        spread = _spread_pct(bid, ask)
        volume_map[coin] = vol
        spread_map[coin] = spread
        ranked_rows.append((coin, vol, spread))

    ranked_rows.sort(key=lambda item: item[1], reverse=True)
    top50 = [coin for coin, _vol, _spread in ranked_rows[:50]]
    tradeable = [
        coin for coin, vol, spread in ranked_rows
        if vol >= volume_floor and spread <= spread_cap
    ]
    return tradeable, top50, volume_map, spread_map


def get_min_order(coin: str) -> float:
    return 1.0


def is_nano_eligible(coin: str) -> bool:
    return True


def get_estimated_ct_val(coin: str) -> float:
    return _CT_VAL_ESTIMATES.get(str(coin or "").upper(), 1.0)


def get_nano_growth_coins() -> list[str]:
    return list(NANO_GROWTH_COINS)


def get_top20_mcap() -> list[str]:
    return list(TOP20_MCAP)


def get_izanagi_liquid_universe() -> list[str]:
    return list(IZANAGI_LIQUID_UNIVERSE)


def get_last_top10() -> list[str]:
    return list(_last_top50[:10])


def get_last_top30() -> list[str]:
    return list(_last_top50[:30])


def get_last_top50() -> list[str]:
    return list(_last_top50)


def get_volume_rank(coin: str) -> float:
    return float(_volume_rank.get(str(coin or "").upper(), 0.0) or 0.0)


def get_spread_pct(coin: str) -> float:
    return float(_spread_rank.get(str(coin or "").upper(), 0.0) or 0.0)


def get_category(coin: str) -> str:
    coin = str(coin or "").upper()
    for cat, coins in CATEGORIES.items():
        if coin in coins:
            return cat
    if coin in _last_top50:
        return "Top-50"
    return "Other"


def get_universe_preference(coin: str, balance_usdt: float = 0.0) -> dict[str, float | bool | str]:
    c = str(coin or "").upper()
    volume = float(_volume_rank.get(c, 0.0) or 0.0)
    spread = float(_spread_rank.get(c, 0.0) or 0.0)
    volume_floor = _volume_floor()
    spread_cap = _spread_cap_pct()
    has_stats = volume > 0 or spread > 0
    tradeable = True
    if has_stats:
        tradeable = volume >= volume_floor and spread <= spread_cap

    volume_quality = min(volume / max(volume_floor, 1.0), 40.0) if volume > 0 else 0.0
    spread_quality = 1.0
    if spread > 0 and spread_cap > 0:
        spread_quality = max(min((spread_cap - spread) / spread_cap, 1.0), -1.0)
    market_cap_bonus = _large_cap_bonus(c, balance_usdt)
    selection_penalty = float(_admission.get_selection_penalty(c) or 0.0)
    score = volume_quality * 12.0 + spread_quality * 20.0 + market_cap_bonus - selection_penalty

    return {
        "coin": c,
        "tradeable": tradeable,
        "volume_24h": volume,
        "spread_pct": spread,
        "volume_quality": volume_quality,
        "spread_quality": spread_quality,
        "market_cap_bonus": market_cap_bonus,
        "selection_penalty": selection_penalty,
        "score": score,
    }


def rank_tradeable_candidates(candidates: list[str], balance_usdt: float = 0.0) -> list[str]:
    ranked: list[dict] = []
    for coin in _dedupe([str(c or "").upper() for c in candidates]):
        cooled, _reason = _admission.is_symbol_cooled_down(coin)
        if cooled:
            continue
        snap = get_universe_preference(coin, balance_usdt=balance_usdt)
        if not bool(snap.get("tradeable", True)):
            continue
        ranked.append(snap)

    ranked.sort(
        key=lambda item: (
            -float(item.get("score", 0.0) or 0.0),
            -float(item.get("volume_24h", 0.0) or 0.0),
            float(item.get("spread_pct", 999.0) or 999.0),
            str(item.get("coin", "") or ""),
        )
    )
    return [str(item.get("coin", "") or "") for item in ranked]


def get_universe_stats() -> dict:
    age_s = int(time.monotonic() - _last_refresh) if _last_refresh else -1
    return {
        "total": len(_last_tradeable),
        "top50": len(_last_top50),
        "nano": len(NANO_GROWTH_COINS),
        "min_vol": _volume_floor(),
        "spread_max_pct": _spread_cap_pct(),
        "top5": list(_last_top50[:5]) if _last_top50 else ["BTC", "ETH", "SOL", "BNB", "XRP"],
        "cached_s": age_s,
        "categories": len(CATEGORIES),
    }


async def get_full_universe() -> set[str]:
    global _last_top50, _last_tradeable, _last_refresh, _volume_rank, _spread_rank
    try:
        tradeable, top50, vol_map, spread_map = await _fetch_tradeable_universe()
        _last_tradeable = _dedupe(tradeable)
        _last_top50 = _dedupe(top50)
        _volume_rank = vol_map
        _spread_rank = spread_map
        _last_refresh = time.monotonic()
    except Exception as exc:
        logger.warning("Unified futures universe refresh failed: %s", exc)
        if not _last_tradeable:
            fallback = _fallback_universe()
            _last_tradeable = fallback
            _last_top50 = fallback[:50]
            _last_refresh = time.monotonic()
    return set(_last_tradeable)


async def get_futures_universe() -> list[str]:
    await get_full_universe()
    return rank_tradeable_candidates(_last_tradeable, balance_usdt=0.0)


def is_liquid(coin: str) -> bool:
    snap = get_universe_preference(coin)
    return bool(snap.get("tradeable", True))


def get_volume(coin: str) -> float:
    """Return cached 24h quote-notional volume in USDT."""
    return float(_volume_rank.get(str(coin or "").upper(), 0.0) or 0.0)
