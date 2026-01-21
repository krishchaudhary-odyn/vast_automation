import requests
import psycopg2
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

PG_CONFIG = {
    "dbname": "gpu_marketplace",
    "host": "localhost",
    "port": 5432,
}

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

    raise RuntimeError("Unexpected Vast response format")

# -----------------------
# TRANSFORM
# -----------------------
def canonicalize(o: Dict) -> Dict:
    return {
        "provider": "vast",
        "offer_id": o["id"],

        "gpu_model": o.get("gpu_name"),
        "gpu_count": o.get("num_gpus"),
        "gpu_memory_gb": o["gpu_ram"] / 1024 if o.get("gpu_ram") else None,
        "gpu_total_memory_gb": o["gpu_total_ram"] / 1024 if o.get("gpu_total_ram") else None,
        "cuda_capability": o.get("cuda_max_good"),

        "cpu_cores": o.get("cpu_cores_effective"),
        "ram_gb": o["cpu_ram"] / 1024 if o.get("cpu_ram") else None,
        "disk_gb": o.get("disk_space"),

        "inet_up_mbps": o.get("inet_up"),
        "inet_down_mbps": o.get("inet_down"),

        "price_per_hour": o.get("search", {}).get("totalHour"),
        "price_per_gpu_hour": o.get("search", {}).get("gpuCostPerHour"),

        "reliability": o.get("reliability2"),
        "verified": o.get("verification") == "verified",

        "location": o.get("geolocation"),
        "spot": o.get("is_bid"),

        "source_timestamp": datetime.datetime.utcnow(),
    }

# -----------------------
# LOAD
# -----------------------
UPSERT_SQL = """
INSERT INTO gpu_offers_current VALUES (
    %(provider)s, %(offer_id)s,
    %(gpu_model)s, %(gpu_count)s,
    %(gpu_memory_gb)s, %(gpu_total_memory_gb)s,
    %(cuda_capability)s,
    %(cpu_cores)s, %(ram_gb)s, %(disk_gb)s,
    %(inet_up_mbps)s, %(inet_down_mbps)s,
    %(price_per_hour)s, %(price_per_gpu_hour)s,
    %(reliability)s, %(verified)s,
    %(location)s, %(spot)s,
    %(source_timestamp)s
)
ON CONFLICT (provider, offer_id) DO UPDATE SET
    price_per_hour = EXCLUDED.price_per_hour,
    price_per_gpu_hour = EXCLUDED.price_per_gpu_hour,
    reliability = EXCLUDED.reliability,
    source_timestamp = EXCLUDED.source_timestamp;
"""

HISTORY_SQL = """
INSERT INTO gpu_offers_history VALUES (
    %(provider)s, %(offer_id)s,
    %(gpu_model)s, %(gpu_count)s,
    %(gpu_memory_gb)s, %(gpu_total_memory_gb)s,
    %(cuda_capability)s,
    %(cpu_cores)s, %(ram_gb)s, %(disk_gb)s,
    %(inet_up_mbps)s, %(inet_down_mbps)s,
    %(price_per_hour)s, %(price_per_gpu_hour)s,
    %(reliability)s, %(verified)s,
    %(location)s, %(spot)s,
    %(source_timestamp)s
);
"""

def store(rows: List[Dict]):
    conn = psycopg2.connect(**PG_CONFIG)
    cur = conn.cursor()

    for r in rows:
        cur.execute(UPSERT_SQL, r)
        cur.execute(HISTORY_SQL, r)

    conn.commit()
    cur.close()
    conn.close()

# -----------------------
# MAIN
# -----------------------
def main():
    offers = fetch_vast()
    rows = [canonicalize(o) for o in offers]
    store(rows)
    print(f"Ingested {len(rows)} offers at {datetime.datetime.utcnow()}")

if __name__ == "__main__":
    main()

