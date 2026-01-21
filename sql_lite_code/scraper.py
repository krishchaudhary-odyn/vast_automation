import requests
import sqlite3
import datetime
from typing import Dict, List

# -----------------------
# CONFIG
# -----------------------
VAST_URL = "https://vast.ai/api/v0/bundles"

PARAMS = {
    "verified": "true",
    "rentable": "true",
    "limit": 10000,
}

DB_PATH = "gpu_marketplace.db"

# -----------------------
# FETCH
# -----------------------
def fetch_vast() -> List[Dict]:
    r = requests.get(VAST_URL, params=PARAMS, timeout=30)
    r.raise_for_status()
    data = r.json()

    if isinstance(data, dict) and "offers" in data:
        return data["offers"]

    if isinstance(data, list):
        return data

    raise RuntimeError(f"Unexpected Vast response format: {data}")


# -----------------------
# NORMALIZE
# -----------------------
def canonicalize(o: Dict) -> Dict:
    return {
        "provider": "vast",
        "offer_id": o["id"],

        # GPU
        "gpu_model": o.get("gpu_name"),
        "gpu_count": o.get("num_gpus"),
        "gpu_memory_gb": (
            o["gpu_ram"] / 1024 if o.get("gpu_ram") else None
        ),
        "gpu_total_memory_gb": (
            o["gpu_total_ram"] / 1024 if o.get("gpu_total_ram") else None
        ),
        "cuda_capability": o.get("cuda_max_good"),

        # System
        "cpu_cores": o.get("cpu_cores_effective"),
        "ram_gb": (
            o["cpu_ram"] / 1024 if o.get("cpu_ram") else None
        ),
        "disk_gb": o.get("disk_space"),

        # Networking
        "inet_up_mbps": o.get("inet_up"),
        "inet_down_mbps": o.get("inet_down"),

        # Pricing (canonical)
        "price_per_hour": o.get("search", {}).get("totalHour"),
        "price_per_gpu_hour": o.get("search", {}).get("gpuCostPerHour"),

        # Reliability
        "reliability": o.get("reliability2"),
        "verified": o.get("verification") == "verified",

        # Location
        "location": o.get("geolocation"),

        # Spot / bid
        "spot": o.get("is_bid"),

        # Metadata
        "source_timestamp": datetime.datetime.isoformat(),
    }


# -----------------------
# DATABASE
# -----------------------
def init_db(conn: sqlite3.Connection):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS gpu_offers (
            provider TEXT,
            offer_id INTEGER,
            gpu_model TEXT,
            gpu_count INTEGER,
            gpu_memory_gb REAL,
            gpu_total_memory_gb REAL,
            cuda_capability REAL,
            cpu_cores REAL,
            ram_gb REAL,
            disk_gb REAL,
            inet_up_mbps REAL,
            inet_down_mbps REAL,
            price_per_hour REAL,
            price_per_gpu_hour REAL,
            reliability REAL,
            verified BOOLEAN,
            location TEXT,
            spot BOOLEAN,
            source_timestamp TEXT,
            PRIMARY KEY (provider, offer_id)
        )
    """)


def upsert_offers(conn: sqlite3.Connection, rows: List[Dict]):
    conn.executemany("""
        INSERT OR REPLACE INTO gpu_offers VALUES (
            :provider,
            :offer_id,
            :gpu_model,
            :gpu_count,
            :gpu_memory_gb,
            :gpu_total_memory_gb,
            :cuda_capability,
            :cpu_cores,
            :ram_gb,
            :disk_gb,
            :inet_up_mbps,
            :inet_down_mbps,
            :price_per_hour,
            :price_per_gpu_hour,
            :reliability,
            :verified,
            :location,
            :spot,
            :source_timestamp
        )
    """, rows)
    conn.commit()


def main():
    print("Fetching Vast.ai offers...")
    offers = fetch_vast()
    print(f"Fetched {len(offers)} raw offers")

    rows = []
    for o in offers:
        try:
            rows.append(canonicalize(o))
        except Exception as e:
            # Defensive: skip malformed rows
            continue

    print(f"Canonicalized {len(rows)} offers")

    conn = sqlite3.connect(DB_PATH)
    init_db(conn)
    upsert_offers(conn, rows)
    conn.close()

    print(f"Stored {len(rows)} offers in {DB_PATH}")

if __name__ == "__main__":
    main()
