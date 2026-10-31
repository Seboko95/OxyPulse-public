"""Compatibility entry point for the unified production capital report."""
from __future__ import annotations

import json
from pathlib import Path

from generate_live_capital_economic_frontier import generate as generate_frontier


def generate(
    *,
    metadata_path: Path = Path("data/instrument_metadata.json"),
    output_path: Path = Path("reports/live_minimum_capital_latest.md"),
) -> list[dict]:
    # The previous implementation modeled a global $5 floor with fixed fee,
    # stop, leverage, and exit-leg assumptions. Those values cannot answer
    # whether an actual candidate is viable. Retain this entry point for
    # operator tooling, but delegate to the unified candidate-aware model.
    del metadata_path  # The unified generator reads the authoritative metadata path.
    result = generate_frontier(output=output_path)
    return list(result.get("representative", []))


if __name__ == "__main__":
    generated = generate()
    print(json.dumps({"representative_candidates": len(generated)}, indent=2))
