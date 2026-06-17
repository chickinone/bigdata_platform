import os


class Config:
    # Database connection
    PG_HOST = os.getenv("PG_HOST", "postgres")
    PG_PORT = int(os.getenv("PG_PORT", "5432"))
    PG_DB = os.getenv("PG_DB", "bankdb")
    PG_USER = os.getenv("PG_USER", "bankapp")
    PG_PASSWORD = os.getenv("PG_PASSWORD", "")

    # Throughput
    TARGET_RPS = int(os.getenv("TARGET_RPS", "150"))      # sustained
    PEAK_RPS = int(os.getenv("PEAK_RPS", "800"))          # burst
    DURATION_SEC = int(os.getenv("DURATION_SEC", "900"))  # 15 phút default

    # Burst pattern
    # Mỗi giây có ~1.5% chance trigger burst → trung bình ~1 burst/70s
    BURST_PROBABILITY = float(os.getenv("BURST_PROBABILITY", "0.015"))
    BURST_DURATION_MAX = float(os.getenv("BURST_DURATION_MAX", "5.0"))

    # Transaction mix
    PROB_TRANSFER = float(os.getenv("PROB_TRANSFER", "0.20"))   
    PROB_FAILURE = float(os.getenv("PROB_FAILURE", "0.05"))     

    # Transfer lifecycle
    TRANSFER_DELAY_MIN = float(os.getenv("TRANSFER_DELAY_MIN", "1.0"))
    TRANSFER_DELAY_MAX = float(os.getenv("TRANSFER_DELAY_MAX", "5.0"))

    # Logging
    STATS_INTERVAL_SEC = int(os.getenv("STATS_INTERVAL_SEC", "10"))
