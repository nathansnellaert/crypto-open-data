"""Transform DefiLlama stablecoin market data.

Produces:
- stablecoin_market: Stablecoin supply and market data
"""

import pyarrow as pa
from subsets_utils import load_raw_json, overwrite, validate, publish
from subsets_utils.testing import assert_positive


DATASET_ID = "stablecoin_market"

METADATA = {
    "id": DATASET_ID,
    "title": "Stablecoin Market Data",
    "description": "Stablecoin market data from DefiLlama. Includes supply, peg type, and circulating amounts across chains.",
    "column_descriptions": {
        "stablecoin_id": "DefiLlama stablecoin identifier",
        "name": "Stablecoin name",
        "symbol": "Token symbol (e.g., USDT, USDC)",
        "peg_type": "What the stablecoin is pegged to (peggedUSD, peggedEUR, etc.)",
        "peg_mechanism": "How the peg is maintained (fiat-backed, algorithmic, crypto-backed)",
        "circulating_usd": "Current circulating supply in USD equivalent",
        "circulating_prev_day": "Circulating supply previous day (USD)",
        "circulating_prev_week": "Circulating supply previous week (USD)",
        "circulating_prev_month": "Circulating supply previous month (USD)",
        "gecko_id": "CoinGecko token ID",
    }
}


def test(table: pa.Table) -> None:
    """Validate stablecoin_market output."""
    validate(table, {
        "columns": {
            "stablecoin_id": "string",
            "name": "string",
            "symbol": "string",
            "peg_type": "string",
            "peg_mechanism": "string",
            "circulating_usd": "double",
            "circulating_prev_day": "double",
            "circulating_prev_week": "double",
            "circulating_prev_month": "double",
            "gecko_id": "string",
        },
        "not_null": ["stablecoin_id", "name", "symbol"],
        "unique": ["stablecoin_id"],
        "min_rows": 50,
    })

    # Circulating supply should be positive where not null
    assert_positive(table, "circulating_usd", allow_zero=False)

    # Check for expected peg types
    peg_types = set(table.column("peg_type").to_pylist())
    assert "peggedUSD" in peg_types, "Expected peggedUSD in peg_types"

    # Check for expected peg mechanisms
    peg_mechanisms = set(p for p in table.column("peg_mechanism").to_pylist() if p)
    expected_mechanisms = {"fiat-backed", "crypto-backed", "algorithmic"}
    found = peg_mechanisms.intersection(expected_mechanisms)
    assert len(found) >= 2, f"Expected at least 2 of {expected_mechanisms}, found {found}"

    # Verify major stablecoins are present
    symbols = set(table.column("symbol").to_pylist())
    major_stablecoins = {"USDT", "USDC", "DAI"}
    found_major = symbols.intersection(major_stablecoins)
    assert len(found_major) >= 2, f"Expected at least 2 of {major_stablecoins}, found {found_major}"

    # Check that USDT has significant market cap (>$1B)
    rows = table.to_pydict()
    usdt_rows = [
        (rows["symbol"][i], rows["circulating_usd"][i])
        for i in range(len(rows["symbol"]))
        if rows["symbol"][i] == "USDT"
    ]
    assert len(usdt_rows) == 1, "Expected exactly one USDT entry"
    usdt_circ = usdt_rows[0][1]
    assert usdt_circ and usdt_circ > 1_000_000_000, f"USDT supply should be >$1B, got {usdt_circ}"

    print(f"  Validated {len(table):,} stablecoins")


def run():
    """Transform stablecoin market data."""
    print("\n--- Stablecoin Transform ---")
    print("  Transforming stablecoin data...")

    data = load_raw_json("defillama/stablecoins")
    stablecoins = data.get("stablecoins", [])

    if not stablecoins:
        raise ValueError("No stablecoin data found")

    rows = []
    for s in stablecoins:
        # Get circulating supply
        circulating = s.get("circulating", {})
        circulating_prev_day = s.get("circulatingPrevDay", {})
        circulating_prev_week = s.get("circulatingPrevWeek", {})
        circulating_prev_month = s.get("circulatingPrevMonth", {})

        # Extract USD values (most stablecoins are pegged to USD)
        peg_type = s.get("pegType", "peggedUSD")
        circ_key = peg_type  # Key is same as peg type

        circ_usd = circulating.get(circ_key) or circulating.get("peggedUSD")
        circ_prev_day = circulating_prev_day.get(circ_key) or circulating_prev_day.get("peggedUSD")
        circ_prev_week = circulating_prev_week.get(circ_key) or circulating_prev_week.get("peggedUSD")
        circ_prev_month = circulating_prev_month.get(circ_key) or circulating_prev_month.get("peggedUSD")

        rows.append({
            "stablecoin_id": str(s.get("id")),
            "name": s.get("name"),
            "symbol": s.get("symbol"),
            "peg_type": peg_type,
            "peg_mechanism": s.get("pegMechanism"),
            "circulating_usd": float(circ_usd) if circ_usd else None,
            "circulating_prev_day": float(circ_prev_day) if circ_prev_day else None,
            "circulating_prev_week": float(circ_prev_week) if circ_prev_week else None,
            "circulating_prev_month": float(circ_prev_month) if circ_prev_month else None,
            "gecko_id": s.get("gecko_id"),
        })

    print(f"  Processed {len(rows)} stablecoins")

    schema = pa.schema([
        ("stablecoin_id", pa.string()),
        ("name", pa.string()),
        ("symbol", pa.string()),
        ("peg_type", pa.string()),
        ("peg_mechanism", pa.string()),
        ("circulating_usd", pa.float64()),
        ("circulating_prev_day", pa.float64()),
        ("circulating_prev_week", pa.float64()),
        ("circulating_prev_month", pa.float64()),
        ("gecko_id", pa.string()),
    ])

    table = pa.Table.from_pylist(rows, schema=schema)

    test(table)

    overwrite(table, DATASET_ID)
    publish(DATASET_ID, METADATA)
from nodes.defillama import run as defillama_run

NODES = {
    run: [defillama_run],
}


if __name__ == "__main__":
    run()
