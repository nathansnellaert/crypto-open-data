"""Ingest DeFi data from DefiLlama APIs.

DefiLlama is open source and allows redistribution.
API docs: https://defillama.com/docs/api

Endpoints used:
- /protocols - All DeFi protocols with TVL
- /v2/chains - TVL by blockchain
- /stablecoins - Stablecoin market data
- /overview/dexs - DEX volumes
- /overview/fees - Protocol fees and revenue
- /yields/pools - Lending/yield rates
"""

from datetime import datetime, timezone
from subsets_utils import get, save_raw_json


# Base URLs for different DefiLlama services
LLAMA_API = "https://api.llama.fi"
STABLECOINS_API = "https://stablecoins.llama.fi"
YIELDS_API = "https://yields.llama.fi"


def fetch_protocols():
    """Fetch all DeFi protocols with TVL data."""
    print("  Fetching DeFi protocols...")
    response = get(f"{LLAMA_API}/protocols")
    response.raise_for_status()
    protocols = response.json()
    print(f"  Downloaded {len(protocols)} protocols")

    save_raw_json({
        "protocols": protocols,
        "fetched_at": datetime.now(timezone.utc).isoformat()
    }, "defillama/protocols")


def fetch_chains():
    """Fetch TVL by blockchain."""
    print("  Fetching chain TVL data...")
    response = get(f"{LLAMA_API}/v2/chains")
    response.raise_for_status()
    chains = response.json()
    print(f"  Downloaded TVL for {len(chains)} chains")

    save_raw_json({
        "chains": chains,
        "fetched_at": datetime.now(timezone.utc).isoformat()
    }, "defillama/chains")


def fetch_stablecoins():
    """Fetch stablecoin market data."""
    print("  Fetching stablecoin data...")
    response = get(f"{STABLECOINS_API}/stablecoins")
    response.raise_for_status()
    data = response.json()
    stablecoins = data.get("peggedAssets", [])
    print(f"  Downloaded {len(stablecoins)} stablecoins")

    save_raw_json({
        "stablecoins": stablecoins,
        "fetched_at": datetime.now(timezone.utc).isoformat()
    }, "defillama/stablecoins")


def fetch_dex_volumes():
    """Fetch DEX trading volumes."""
    print("  Fetching DEX volumes...")
    response = get(f"{LLAMA_API}/overview/dexs")
    response.raise_for_status()
    data = response.json()

    # Extract protocol-level data
    protocols = data.get("protocols", [])
    total_data_chart = data.get("totalDataChart", [])

    print(f"  Downloaded {len(protocols)} DEX protocols")

    save_raw_json({
        "protocols": protocols,
        "total_data_chart": total_data_chart,
        "fetched_at": datetime.now(timezone.utc).isoformat()
    }, "defillama/dex_volumes")


def fetch_fees():
    """Fetch protocol fees and revenue data."""
    print("  Fetching protocol fees...")
    response = get(f"{LLAMA_API}/overview/fees")
    response.raise_for_status()
    data = response.json()

    protocols = data.get("protocols", [])
    print(f"  Downloaded {len(protocols)} fee protocols")

    save_raw_json({
        "protocols": protocols,
        "fetched_at": datetime.now(timezone.utc).isoformat()
    }, "defillama/fees")


def fetch_yields():
    """Fetch lending/yield pool data."""
    print("  Fetching yield pools...")
    response = get(f"{YIELDS_API}/pools")
    response.raise_for_status()
    data = response.json()

    pools = data.get("data", [])
    print(f"  Downloaded {len(pools)} yield pools")

    save_raw_json({
        "pools": pools,
        "fetched_at": datetime.now(timezone.utc).isoformat()
    }, "defillama/yields")


def run():
    """Run all DefiLlama ingestion."""
    print("\n--- DefiLlama Ingestion ---")
    fetch_protocols()
    fetch_chains()
    fetch_stablecoins()
    fetch_dex_volumes()
    fetch_fees()
    fetch_yields()
    print("  DefiLlama ingestion complete")


NODES = {
    run: [],
}


if __name__ == "__main__":
    run()
