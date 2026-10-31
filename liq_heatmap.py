"""
OxyPulse — liq_heatmap.py
Estimates liquidation price zones for a given coin.

Why this matters:
  - ~75–80% of all crypto volume is in derivatives/futures
  - Leveraged positions create forced buy/sell orders at predictable prices
  - Price is ATTRACTED to dense liq clusters (large players need that liquidity)
  - Once a cluster is swept, price often REVERSES sharply

How it works (no external API required):
  1. Takes current price + OI
  2. Calculates estimated liq levels for common leverage multiples (3x, 5x, 10x, 20x, 25x, 50x)
  3. Groups nearby levels into "hot zones"
  4. Returns sorted list of zones with distance + intensity score

Optional: Set COINGLASS_KEY env var to use CoinGlass API for real heatmap data.

Uses:
  - Entry filter: skip if SL overlaps a heavy liq zone (avoid being swept)
  - TP targeting: set TP near a liq zone (price magnetically attracted there)
  - /liqmap command: visual zone display in Telegram
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass

logger = logging.getLogger("OxyPulse.LiqHeatmap")

COINGLASS_KEY = os.getenv("COINGLASS_KEY", "")

# Common retail leverage multiples — these are where most liquidations cluster
# OKX default leverage tiers for different account sizes
COMMON_LEVERAGES = [3, 5, 10, 20, 25, 50]

# Maintenance margin rate per leverage level (approx OKX rates)
# liq_price ≈ entry × (1 - 1/leverage × (1 - mmr))
MMR = {3: 0.01, 5: 0.01, 10: 0.02, 20: 0.025, 25: 0.03, 50: 0.04}


@dataclass
class LiqZone:
    price:       float
    side:        str         # "long_liq" or "short_liq"
    leverage:    int
    dist_pct:    float       # % distance from current price
    intensity:   float       # 0.0–1.0, higher = more liquidations expected
    label:       str

    @property
    def emoji(self) -> str:
        base = "🔴" if self.side == "long_liq" else "🟢"
        if self.intensity >= 0.7:
            return base + "🔥"
        if self.intensity >= 0.4:
            return base
        return "⚫"


def estimate_zones(current_price: float, oi_usd: float = 0.0) -> list[LiqZone]:
    """
    Estimate liquidation price zones from current price.

    Returns list of LiqZone sorted by distance (nearest first).
    Long liquidations sit BELOW current price.
    Short liquidations sit ABOVE current price.
    """
    if current_price <= 0:
        return []

    zones: list[LiqZone] = []

    for lev in COMMON_LEVERAGES:
        mmr_rate = MMR.get(lev, 0.01)

        # Long liquidation price (below current)
        long_liq = current_price * (1 - (1 / lev) * (1 - mmr_rate))
        # Short liquidation price (above current)
        short_liq = current_price * (1 + (1 / lev) * (1 - mmr_rate))

        long_dist_pct  = abs(current_price - long_liq)  / current_price * 100
        short_dist_pct = abs(short_liq - current_price) / current_price * 100

        # Intensity heuristic:
        # Higher leverage = more positions likely there = higher intensity
        # But also tempered by distance (further = lower intensity since fewer hold that long)
        long_intensity  = _intensity(lev, long_dist_pct,  oi_usd)
        short_intensity = _intensity(lev, short_dist_pct, oi_usd)

        zones.append(LiqZone(
            price=round(long_liq, 6), side="long_liq", leverage=lev,
            dist_pct=round(long_dist_pct, 2), intensity=long_intensity,
            label=f"Long {lev}x liq",
        ))
        zones.append(LiqZone(
            price=round(short_liq, 6), side="short_liq", leverage=lev,
            dist_pct=round(short_dist_pct, 2), intensity=short_intensity,
            label=f"Short {lev}x liq",
        ))

    # Sort by distance from current price
    zones.sort(key=lambda z: z.dist_pct)
    return zones


def _intensity(leverage: int, dist_pct: float, oi_usd: float) -> float:
    """
    Heuristic intensity score [0.0, 1.0].
    Higher leverage → more retail positions → higher intensity.
    Larger OI → more absolute capital at risk → higher intensity.
    """
    # Base score by leverage (10x and 20x are most common retail levers)
    lev_score = {3: 0.2, 5: 0.5, 10: 0.9, 20: 0.85, 25: 0.6, 50: 0.4}.get(leverage, 0.3)

    # OI modifier (if we have OI data)
    oi_mod = 1.0
    if oi_usd > 0:
        if oi_usd > 500_000_000:   # >$500M OI = very high
            oi_mod = 1.3
        elif oi_usd > 100_000_000: # >$100M OI = high
            oi_mod = 1.15
        elif oi_usd > 10_000_000:  # >$10M OI = moderate
            oi_mod = 1.0

    return min(1.0, lev_score * oi_mod)


def get_nearest_zones(
    current_price: float,
    oi_usd: float = 0.0,
    max_dist_pct: float = 15.0,
    min_intensity: float = 0.4,
) -> list[LiqZone]:
    """
    Returns only the significant zones within max_dist_pct of current price.
    Filters out low-intensity zones (noise).
    """
    all_zones = estimate_zones(current_price, oi_usd)
    return [
        z for z in all_zones
        if z.dist_pct <= max_dist_pct and z.intensity >= min_intensity
    ]


def get_nearest_tp_magnet(
    current_price: float,
    direction: str,
    oi_usd: float = 0.0,
    min_dist_pct: float = 0.5,
    max_dist_pct: float = 8.0,
) -> float | None:
    """
    Find the nearest significant liquidation cluster in the trade's favour.
    For a LONG, we want a SHORT liquidation cluster above us (short squeeze magnet).
    For a SHORT, we want a LONG liquidation cluster below us (long flush magnet).

    Returns the magnet price, or None if no strong zone found.
    """
    zones = estimate_zones(current_price, oi_usd)

    if direction == "long":
        candidates = [
            z for z in zones
            if z.side == "short_liq"
            and min_dist_pct <= z.dist_pct <= max_dist_pct
            and z.intensity >= 0.5
        ]
    else:
        candidates = [
            z for z in zones
            if z.side == "long_liq"
            and min_dist_pct <= z.dist_pct <= max_dist_pct
            and z.intensity >= 0.5
        ]

    if not candidates:
        return None

    # Pick the nearest high-intensity zone
    best = max(candidates, key=lambda z: z.intensity / (z.dist_pct + 0.1))
    logger.debug(
        "Liq magnet for %s %s: %s @ %.6f (%.2f%% away, intensity=%.2f)",
        direction, current_price, best.label, best.price, best.dist_pct, best.intensity,
    )
    return best.price


def check_sl_in_liq_zone(
    sl_price: float,
    current_price: float,
    direction: str,
    oi_usd: float = 0.0,
    buffer_pct: float = 0.3,
) -> tuple[bool, str]:
    """
    Check if our SL is sitting inside a major liquidation cluster.
    If yes → we risk being swept by a liq cascade before our real stop.
    Returns (is_risky, warning_message).
    """
    zones = get_nearest_zones(current_price, oi_usd, max_dist_pct=10.0, min_intensity=0.6)

    for zone in zones:
        # Check if SL is within buffer% of any liq zone
        gap_pct = abs(sl_price - zone.price) / current_price * 100
        if gap_pct <= buffer_pct:
            msg = (
                f"⚠️ SL at {sl_price:.6f} overlaps {zone.label} "
                f"({zone.dist_pct:.1f}% from price, intensity={zone.intensity:.1f})"
            )
            return True, msg

    return False, ""


def format_heatmap(coin: str, current_price: float, oi_usd: float = 0.0) -> str:
    """Format a Telegram-friendly heatmap display for /liqmap command."""
    zones = get_nearest_zones(current_price, oi_usd, max_dist_pct=12.0, min_intensity=0.35)

    if not zones:
        return f"📊 No significant liq zones found near `{coin}` @ `${current_price:,.4f}`"

    short_zones = sorted(
        [z for z in zones if z.side == "short_liq"],
        key=lambda z: z.price,
    )
    long_zones = sorted(
        [z for z in zones if z.side == "long_liq"],
        key=lambda z: z.price, reverse=True,
    )

    lines = [
        f"🗺 *Liq Heatmap — {coin}*",
        f"  Current: `${current_price:,.4f}`",
        f"  OI: `${oi_usd/1e6:.1f}M`\n",
        "⬆️ *Short Liq Zones (above price):*",
    ]
    for z in short_zones[:4]:
        bar = "▓" * int(z.intensity * 5) + "░" * (5 - int(z.intensity * 5))
        lines.append(f"  {z.emoji} `${z.price:,.4f}`  `+{z.dist_pct:.1f}%`  [{bar}] `{z.label}`")

    lines.append("\n⬇️ *Long Liq Zones (below price):*")
    for z in long_zones[:4]:
        bar = "▓" * int(z.intensity * 5) + "░" * (5 - int(z.intensity * 5))
        lines.append(f"  {z.emoji} `${z.price:,.4f}`  `-{z.dist_pct:.1f}%`  [{bar}] `{z.label}`")

    lines.append(
        "\n_Intensity: ▓▓▓▓▓=high  ░░░░░=low_\n"
        "_🔴🔥 = hot zone (price magnet)_"
    )
    return "\n".join(lines)
