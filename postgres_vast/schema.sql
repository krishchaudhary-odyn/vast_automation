CREATE TABLE IF NOT EXISTS gpu_offers_current (
    provider TEXT NOT NULL,
    offer_id BIGINT NOT NULL,

    gpu_model TEXT,
    gpu_count INT,
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

    source_timestamp TIMESTAMP,

    PRIMARY KEY (provider, offer_id)
);

CREATE TABLE IF NOT EXISTS gpu_offers_history (
    provider TEXT,
    offer_id BIGINT,

    gpu_model TEXT,
    gpu_count INT,
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

    source_timestamp TIMESTAMP
);
