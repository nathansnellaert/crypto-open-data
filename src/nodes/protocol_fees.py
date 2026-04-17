"""Transform DefiLlama protocol fees and revenue data.

Produces:
- protocol_fees: Protocol fees by protocol (24h, 7d, 30d, all-time)
"""

import pyarrow as pa
from subsets_utils import load_raw_json, overwrite, validate, publish


DATASET_ID = "protocol_fees"

METADATA = {
    "id": DATASET_ID,
    "title": "DeFi Protocol Fees",
    "description": "Protocol fees and revenue from DefiLlama. Shows how much value each DeFi protocol captures through fees over various time periods.",
    "license": "MIT - DefiLlama (open source, redistribution allowed)",
    "column_descriptions": {
        "protocol_id": "DefiLlama protocol slug identifier",
        "name": "Protocol name",
        "display_name": "Display name",
        "category": "Protocol category (Lending, DEX, Stablecoin Issuer, etc.)",
        "chain": "Primary blockchain",
        "chains": "All supported chains (comma-separated)",
        "fees_24h": "Total fees in last 24 hours (USD)",
        "fees_7d": "Total fees in last 7 days (USD)",
        "fees_30d": "Total fees in last 30 days (USD)",
        "fees_all_time": "Total all-time fees (USD)",
        "fees_change_1d": "Fees change from previous day (percentage)",
        "logo_url": "Protocol logo URL",
    }
}


def test(table: pa.Table) -> None:
    """Validate protocol_fees output."""
    validate(table, {
        "columns": {
            "protocol_id": "string",
            "name": "string",
            "display_name": "string",
            "category": "string",
            "chain": "string",
            "chains": "string",
            "fees_24h": "double",
            "fees_7d": "double",
            "fees_30d": "double",
            "fees_all_time": "double",
            "fees_change_1d": "double",
            "logo_url": "string",
        },
        "not_null": ["protocol_id", "name"],
        "unique": ["protocol_id"],
        "min_rows": 100,
    })

    # Most fees should be non-negative (some protocols report negative due to corrections)
    fees_24h = [f for f in table.column("fees_24h").to_pylist() if f is not None]
    negative = [f for f in fees_24h if f < 0]
    assert len(negative) < len(fees_24h) * 0.05, (
        f"Too many negative fees: {len(negative)}/{len(fees_24h)}"
    )

    # Check for expected categories
    categories = set(table.column("category").to_pylist())
    expected = {"Lending", "Dexes", "Derivatives"}
    found = categories.intersection(expected)
    assert len(found) >= 2, f"Expected at least 2 of {expected}, found {found}"

    # Check for major protocols
    names = set(n.lower() for n in table.column("name").to_pylist() if n)
    major = {"aave v3", "uniswap v3", "lido"}
    found_major = sum(1 for n in names if any(m in n for m in major))
    assert found_major >= 1, f"Expected at least one of {major} in fee data"

    # Verify we have protocols with actual fee data
    fees = [f for f in table.column("fees_24h").to_pylist() if f and f > 0]
    assert len(fees) >= 50, f"Expected at least 50 protocols with 24h fees, got {len(fees)}"

    print(f"  Validated {len(table):,} fee protocols")


def run():
    """Transform protocol fees data."""
    print("\n--- Protocol Fees Transform ---")
    print("  Transforming fee data...")

    data = load_raw_json("defillama/fees")
    protocols = data.get("protocols", [])

    if not protocols:
        raise ValueError("No fee protocol data found")

    rows = []
    for p in protocols:
        protocol_id = p.get("slug") or p.get("module") or str(p.get("defillamaId", ""))
        if not protocol_id:
            continue

        chains = p.get("chains", [])
        chains_str = ",".join(chains) if chains else None
        chain = chains[0] if chains else None

        rows.append({
            "protocol_id": protocol_id,
            "name": p.get("name"),
            "display_name": p.get("displayName") or p.get("name"),
            "category": p.get("category"),
            "chain": chain,
            "chains": chains_str,
            "fees_24h": p.get("total24h"),
            "fees_7d": p.get("total7d"),
            "fees_30d": p.get("total30d"),
            "fees_all_time": p.get("totalAllTime"),
            "fees_change_1d": p.get("change_1d"),
            "logo_url": p.get("logo"),
        })

    print(f"  Processed {len(rows)} fee protocols")

    schema = pa.schema([
        ("protocol_id", pa.string()),
        ("name", pa.string()),
        ("display_name", pa.string()),
        ("category", pa.string()),
        ("chain", pa.string()),
        ("chains", pa.string()),
        ("fees_24h", pa.float64()),
        ("fees_7d", pa.float64()),
        ("fees_30d", pa.float64()),
        ("fees_all_time", pa.float64()),
        ("fees_change_1d", pa.float64()),
        ("logo_url", pa.string()),
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
