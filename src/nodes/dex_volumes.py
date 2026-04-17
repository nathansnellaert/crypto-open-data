"""Transform DefiLlama DEX trading volume data.

Produces:
- dex_trading_volumes: DEX trading volumes by protocol
"""

import pyarrow as pa
from subsets_utils import load_raw_json, overwrite, validate, publish
from subsets_utils.testing import assert_positive


DATASET_ID = "dex_trading_volumes"

METADATA = {
    "id": DATASET_ID,
    "title": "DEX Trading Volumes",
    "description": "Decentralized exchange (DEX) trading volumes from DefiLlama. Shows daily volume and historical data for major DEX protocols.",
    "license": "MIT - DefiLlama (open source, redistribution allowed)",
    "column_descriptions": {
        "protocol_id": "Protocol identifier",
        "name": "DEX protocol name",
        "display_name": "Display name",
        "chain": "Primary blockchain",
        "chains": "All supported chains (comma-separated)",
        "category": "Protocol category",
        "volume_24h": "24-hour trading volume in USD",
        "volume_change_1d": "Volume change from previous day (percentage)",
        "total_volume_7d": "Total volume over last 7 days (USD)",
        "total_volume_30d": "Total volume over last 30 days (USD)",
        "logo_url": "Protocol logo URL",
    }
}


def test(table: pa.Table) -> None:
    """Validate dex_trading_volumes output."""
    validate(table, {
        "columns": {
            "protocol_id": "string",
            "name": "string",
            "display_name": "string",
            "chain": "string",
            "chains": "string",
            "category": "string",
            "volume_24h": "double",
            "volume_change_1d": "double",
            "total_volume_7d": "double",
            "total_volume_30d": "double",
            "logo_url": "string",
        },
        "not_null": ["protocol_id", "name"],
        "min_rows": 50,
    })

    # Volume should be positive where not null
    assert_positive(table, "volume_24h", allow_zero=True)
    assert_positive(table, "total_volume_7d", allow_zero=True)

    # Check for major DEX protocols
    names = [n.lower() if n else "" for n in table.column("name").to_pylist()]
    major_dexes = {"uniswap", "pancakeswap", "curve"}
    found = sum(1 for n in names if any(d in n for d in major_dexes))
    assert found >= 1, f"Expected at least one of {major_dexes} in names"

    # Verify we have protocols with actual volume
    volumes = [v for v in table.column("volume_24h").to_pylist() if v and v > 0]
    assert len(volumes) >= 20, f"Expected at least 20 protocols with volume, got {len(volumes)}"

    print(f"  Validated {len(table):,} DEX protocols")


def run():
    """Transform DEX volume data."""
    print("\n--- DEX Volumes Transform ---")
    print("  Transforming DEX volume data...")

    data = load_raw_json("defillama/dex_volumes")
    protocols = data.get("protocols", [])

    if not protocols:
        raise ValueError("No DEX protocol data found")

    rows = []
    for p in protocols:
        # Get chains list
        chains = p.get("chains", [])
        chains_str = ",".join(chains) if chains else None

        # Primary chain is first in list or explicit
        chain = p.get("chain", chains[0] if chains else None)

        rows.append({
            "protocol_id": str(p.get("defillamaId") or p.get("module") or p.get("name", "").lower().replace(" ", "-")),
            "name": p.get("name"),
            "display_name": p.get("displayName") or p.get("name"),
            "chain": chain,
            "chains": chains_str,
            "category": p.get("category"),
            "volume_24h": p.get("total24h"),
            "volume_change_1d": p.get("change_1d"),
            "total_volume_7d": p.get("total7d"),
            "total_volume_30d": p.get("total30d"),
            "logo_url": p.get("logo"),
        })

    print(f"  Processed {len(rows)} DEX protocols")

    schema = pa.schema([
        ("protocol_id", pa.string()),
        ("name", pa.string()),
        ("display_name", pa.string()),
        ("chain", pa.string()),
        ("chains", pa.string()),
        ("category", pa.string()),
        ("volume_24h", pa.float64()),
        ("volume_change_1d", pa.float64()),
        ("total_volume_7d", pa.float64()),
        ("total_volume_30d", pa.float64()),
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
