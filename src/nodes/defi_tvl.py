"""Transform DefiLlama protocol and chain TVL data.

Produces two datasets:
- defi_protocol_tvl: TVL by protocol (name, chain, tvl, category)
- defi_chain_tvl: TVL by blockchain
"""

import pyarrow as pa
from subsets_utils import load_raw_json, overwrite, publish, validate
from subsets_utils.testing import assert_positive


PROTOCOL_DATASET_ID = "defi_protocol_tvl"
CHAIN_DATASET_ID = "defi_chain_tvl"

PROTOCOL_METADATA = {
    "id": PROTOCOL_DATASET_ID,
    "title": "DeFi Protocol TVL",
    "description": "Total Value Locked (TVL) for DeFi protocols from DefiLlama. Includes protocol name, category, chains supported, and current TVL in USD.",
    "license": "MIT - DefiLlama (open source, redistribution allowed)",
    "column_descriptions": {
        "protocol_id": "DefiLlama protocol identifier",
        "name": "Protocol name",
        "category": "Protocol category (DEX, Lending, Bridge, etc.)",
        "chain": "Primary blockchain",
        "chains": "All supported chains (comma-separated)",
        "tvl_usd": "Total Value Locked in USD",
        "tvl_change_1d": "TVL change in last 24 hours (percentage)",
        "tvl_change_7d": "TVL change in last 7 days (percentage)",
        "logo_url": "Protocol logo URL",
    }
}

CHAIN_METADATA = {
    "id": CHAIN_DATASET_ID,
    "title": "DeFi Chain TVL",
    "description": "Total Value Locked (TVL) by blockchain from DefiLlama. Shows how DeFi value is distributed across different blockchains.",
    "license": "MIT - DefiLlama (open source, redistribution allowed)",
    "column_descriptions": {
        "chain": "Blockchain name",
        "tvl_usd": "Total Value Locked in USD",
        "token_symbol": "Native token symbol",
        "gecko_id": "CoinGecko token ID",
        "chain_id": "EVM chain ID (if applicable)",
    }
}


def test_protocols(table: pa.Table) -> None:
    """Validate defi_protocol_tvl output."""
    validate(table, {
        "columns": {
            "protocol_id": "string",
            "name": "string",
            "category": "string",
            "chain": "string",
            "chains": "string",
            "tvl_usd": "double",
            "tvl_change_1d": "double",
            "tvl_change_7d": "double",
            "logo_url": "string",
        },
        "not_null": ["protocol_id", "name", "tvl_usd"],
        "unique": ["protocol_id"],
        "min_rows": 100,
    })

    # TVL should be positive
    assert_positive(table, "tvl_usd", allow_zero=False)

    # Check for expected categories
    categories = set(table.column("category").to_pylist())
    expected_categories = {"Dexes", "Lending", "Bridge"}
    found = categories.intersection(expected_categories)
    assert len(found) >= 2, f"Expected at least 2 of {expected_categories}, found {found}"

    # Verify we have major protocols
    names = set(table.column("name").to_pylist())
    major_protocols = {"Lido", "AAVE", "Uniswap"}
    found_major = names.intersection(major_protocols)
    assert len(found_major) >= 1, f"Expected at least one of {major_protocols}, found {found_major}"

    print(f"  Validated {len(table):,} protocols")


def test_chains(table: pa.Table) -> None:
    """Validate defi_chain_tvl output."""
    validate(table, {
        "columns": {
            "chain": "string",
            "tvl_usd": "double",
            "token_symbol": "string",
            "gecko_id": "string",
            "chain_id": "int",
        },
        "not_null": ["chain", "tvl_usd"],
        "unique": ["chain"],
        "min_rows": 20,
    })

    # TVL should be positive
    assert_positive(table, "tvl_usd", allow_zero=False)

    # Check for major chains
    chains = set(table.column("chain").to_pylist())
    major_chains = {"Ethereum", "Solana", "Arbitrum", "Base"}
    found_major = chains.intersection(major_chains)
    assert len(found_major) >= 2, f"Expected at least 2 of {major_chains}, found {found_major}"

    print(f"  Validated {len(table):,} chains")


def transform_protocols():
    """Transform protocol TVL data."""
    print("\n  Transforming protocol TVL...")
    data = load_raw_json("defillama/protocols")
    protocols = data.get("protocols", [])

    if not protocols:
        raise ValueError("No protocol data found")

    rows = []
    for p in protocols:
        # Skip if no TVL data, zero, or negative TVL
        tvl = p.get("tvl")
        if not tvl or tvl <= 0:  # Skips None, 0, negative, and empty values
            continue

        # Get chains list
        chains = p.get("chains", [])
        chains_str = ",".join(chains) if chains else None

        # Determine primary chain
        chain = p.get("chain", chains[0] if chains else None)

        # Use slug as protocol_id (more stable than numeric id)
        protocol_id = p.get("slug") or str(p.get("id", ""))

        # Sanitize change percentages (some can be absurdly large due to data issues)
        def sanitize_change(val):
            if val is None:
                return None
            if isinstance(val, (int, float)) and abs(val) > 1e10:
                return None  # Treat as invalid data
            return float(val)

        rows.append({
            "protocol_id": protocol_id,
            "name": p.get("name"),
            "category": p.get("category"),
            "chain": chain,
            "chains": chains_str,
            "tvl_usd": float(tvl) if tvl else None,
            "tvl_change_1d": sanitize_change(p.get("change_1d")),
            "tvl_change_7d": sanitize_change(p.get("change_7d")),
            "logo_url": p.get("logo"),
        })

    print(f"  Processed {len(rows)} protocols with TVL data")

    schema = pa.schema([
        ("protocol_id", pa.string()),
        ("name", pa.string()),
        ("category", pa.string()),
        ("chain", pa.string()),
        ("chains", pa.string()),
        ("tvl_usd", pa.float64()),
        ("tvl_change_1d", pa.float64()),
        ("tvl_change_7d", pa.float64()),
        ("logo_url", pa.string()),
    ])

    table = pa.Table.from_pylist(rows, schema=schema)

    test_protocols(table)

    overwrite(table, PROTOCOL_DATASET_ID)
    publish(PROTOCOL_DATASET_ID, PROTOCOL_METADATA)

def transform_chains():
    """Transform chain TVL data."""
    print("\n  Transforming chain TVL...")
    data = load_raw_json("defillama/chains")
    chains = data.get("chains", [])

    if not chains:
        raise ValueError("No chain data found")

    rows = []
    for c in chains:
        # Skip if no TVL data or zero TVL
        tvl = c.get("tvl")
        if not tvl:  # Skips None, 0, and empty values
            continue

        # Handle chainId - can be int, string, or None
        chain_id = c.get("chainId")
        if chain_id is not None:
            try:
                chain_id = int(chain_id)
            except (ValueError, TypeError):
                chain_id = None

        rows.append({
            "chain": c.get("name"),
            "tvl_usd": float(tvl) if tvl else None,
            "token_symbol": c.get("tokenSymbol"),
            "gecko_id": c.get("gecko_id"),
            "chain_id": chain_id,
        })

    print(f"  Processed {len(rows)} chains with TVL data")

    schema = pa.schema([
        ("chain", pa.string()),
        ("tvl_usd", pa.float64()),
        ("token_symbol", pa.string()),
        ("gecko_id", pa.string()),
        ("chain_id", pa.int64()),
    ])

    table = pa.Table.from_pylist(rows, schema=schema)

    test_chains(table)

    overwrite(table, CHAIN_DATASET_ID)
    publish(CHAIN_DATASET_ID, CHAIN_METADATA)

def run():
    """Run all DeFi TVL transforms."""
    print("\n--- DeFi TVL Transform ---")
    transform_protocols()
    transform_chains()


from nodes.defillama import run as defillama_run

NODES = {
    run: [defillama_run],
}


if __name__ == "__main__":
    run()
