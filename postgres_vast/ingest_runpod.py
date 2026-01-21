import requests
import psycopg2
import datetime
from typing import Dict, List

# -----------------------
# CONFIG
# -----------------------
RUNPOD_URL = "https://api.runpod.io/graphql"

GRAPHQL_QUERY = """
query {
  gpuTypes {
    id
    displayName
    memoryInGb
    securePrice
    communityPrice
    maxGpuCount
  }
}
"""

PG_CONFIG = {
    "dbname": "gpu_marketplace",
    "host": "localhost",
    "port": 5432,
}

# -----------------------
# FETCH
# -----------------------
def fetch_runpod() -> List[Dict]:
    r = requests.post(
        RUNPOD_URL,
        json={"query": GRAPHQL_QUERY},
        timeout=30,
    )
    r.raise_for_status()
    data = r.json()

    if "data" not in data or "gpuTypes" not in data["data"]:
        raise RuntimeError("Unexpected RunPod response format")

    return data["data"]["gpuTypes"]

# -----------------------
# TRANSFORM
# -----------------------
def canonicalize(o: Dict) -> Dict:
    return {
        "provider": "runpod",
        "offer_id": f"runpod-{o['id']}",

        "gpu_model": o.get("displayName"),
        "gpu_count": o.get("maxGpuCount"),
        "gpu_memory_gb": o.get("memoryInGb"),
        "gpu_total_memory_gb": (
            o.get("memoryInGb") * o.get("maxGpuCount")
            if o.get("memoryInGb") and o.get("maxGpuCount")
            else None
        ),
        "cuda_capability": None,

        "cpu_cores": None,
        "ram_gb": None,
        "disk_gb": None,

        "inet_up_mbps": None,
        "inet_down_mbps": None,

        # Prefer secure price, fall back to community
        "price_per_hour": o.get("securePrice") or o.get("communityPrice"),
        "price_per_gpu_hour": o.get("securePrice") or o.get("communityPrice"),

        "reliability": None,
        "verified": True,

        "location": "global",
        "spot": False,

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
    offers = fetch_runpod()
    rows = [canonicalize(o) for o in offers]
    store(rows)
    print(f"Ingested {len(rows)} RunPod offers at {datetime.datetime.utcnow()}")

if __name__ == "__main__":
    main()
