"""Transform DefiLlama lending/yield pool data.

Produces:
- defi_lending_rates: Borrow/supply APYs by protocol and pool
"""

import pyarrow as pa
from subsets_utils import load_raw_json, overwrite, validate, publish
from subsets_utils.testing import assert_positive


DATASET_ID = "defi_lending_rates"

METADATA = {
    "id": DATASET_ID,
    "title": "DeFi Lending Rates",
    "description": "DeFi lending and yield rates from DefiLlama. Shows APY for various pools across lending protocols, staking, and liquidity provision.",
    "column_descriptions": {
        "pool_id": "Unique pool identifier",
        "chain": "Blockchain name",
        "project": "Protocol/project name",
        "symbol": "Token symbol(s) for the pool",
        "tvl_usd": "Total Value Locked in the pool (USD)",
        "apy_base": "Base APY (from protocol rewards/fees)",
        "apy_reward": "Reward APY (from token incentives)",
        "apy": "Total APY (base + reward)",
        "apy_pct_1d": "APY change over 1 day",
        "apy_pct_7d": "APY change over 7 days",
        "apy_pct_30d": "APY change over 30 days",
        "stablecoin": "Whether this is a stablecoin pool",
        "il_risk": "Impermanent loss risk level",
        "exposure": "Exposure type (single, multi)",
        "reward_tokens": "Reward token addresses (comma-separated)",
    }
}


def test(table: pa.Table) -> None:
    """Validate defi_lending_rates output."""
    validate(table, {
        "columns": {
            "pool_id": "string",
            "chain": "string",
            "project": "string",
            "symbol": "string",
            "tvl_usd": "double",
            "apy_base": "double",
            "apy_reward": "double",
            "apy": "double",
            "apy_pct_1d": "double",
            "apy_pct_7d": "double",
            "apy_pct_30d": "double",
            "stablecoin": "bool",
            "il_risk": "string",
            "exposure": "string",
            "reward_tokens": "string",
        },
        "not_null": ["pool_id", "chain", "project"],
        "unique": ["pool_id"],
        "min_rows": 1000,
    })

    # TVL should be positive where not null
    assert_positive(table, "tvl_usd", allow_zero=True)

    # APY can be negative (losing pools) but check we have positive ones
    apys = [a for a in table.column("apy").to_pylist() if a is not None and a > 0]
    assert len(apys) >= 100, f"Expected at least 100 pools with positive APY, got {len(apys)}"

    # Check for major lending protocols
    projects = set(table.column("project").to_pylist())
    major_projects = {"lido", "aave-v3", "compound-v3", "uniswap-v3"}
    found = projects.intersection(major_projects)
    assert len(found) >= 2, f"Expected at least 2 of {major_projects}, found {found}"

    # Check for major chains
    chains = set(table.column("chain").to_pylist())
    major_chains = {"Ethereum", "Arbitrum", "Base", "Polygon"}
    found_chains = chains.intersection(major_chains)
    assert len(found_chains) >= 2, f"Expected at least 2 of {major_chains}, found {found_chains}"

    # Verify we have both stablecoin and non-stablecoin pools
    stablecoin_values = table.column("stablecoin").to_pylist()
    has_stable = any(s is True for s in stablecoin_values)
    has_non_stable = any(s is False for s in stablecoin_values)
    assert has_stable, "Expected some stablecoin pools"
    assert has_non_stable, "Expected some non-stablecoin pools"

    print(f"  Validated {len(table):,} yield pools")


def run():
    """Transform lending rates data."""
    print("\n--- Lending Rates Transform ---")
    print("  Transforming yield pool data...")

    data = load_raw_json("defillama/yields")
    pools = data.get("pools", [])

    if not pools:
        raise ValueError("No yield pool data found")

    rows = []
    for p in pools:
        # Get reward tokens as comma-separated string, filtering out None values
        reward_tokens = p.get("rewardTokens")
        if reward_tokens:
            valid_tokens = [t for t in reward_tokens if t is not None]
            reward_str = ",".join(valid_tokens) if valid_tokens else None
        else:
            reward_str = None

        rows.append({
            "pool_id": p.get("pool"),
            "chain": p.get("chain"),
            "project": p.get("project"),
            "symbol": p.get("symbol"),
            "tvl_usd": p.get("tvlUsd"),
            "apy_base": p.get("apyBase"),
            "apy_reward": p.get("apyReward"),
            "apy": p.get("apy"),
            "apy_pct_1d": p.get("apyPct1D"),
            "apy_pct_7d": p.get("apyPct7D"),
            "apy_pct_30d": p.get("apyPct30D"),
            "stablecoin": p.get("stablecoin"),
            "il_risk": p.get("ilRisk"),
            "exposure": p.get("exposure"),
            "reward_tokens": reward_str,
        })

    print(f"  Processed {len(rows)} yield pools")

    schema = pa.schema([
        ("pool_id", pa.string()),
        ("chain", pa.string()),
        ("project", pa.string()),
        ("symbol", pa.string()),
        ("tvl_usd", pa.float64()),
        ("apy_base", pa.float64()),
        ("apy_reward", pa.float64()),
        ("apy", pa.float64()),
        ("apy_pct_1d", pa.float64()),
        ("apy_pct_7d", pa.float64()),
        ("apy_pct_30d", pa.float64()),
        ("stablecoin", pa.bool_()),
        ("il_risk", pa.string()),
        ("exposure", pa.string()),
        ("reward_tokens", pa.string()),
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
