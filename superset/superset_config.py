import os

# Keep secrets and environment wiring in .env to avoid hardcoding values.
SECRET_KEY = os.getenv("SUPERSET_SECRET_KEY", "change-me-to-a-long-random-string")

# JWT Secret for Global Async Queries (must be >= 32 bytes).
GLOBAL_ASYNC_QUERIES_JWT_SECRET = os.getenv("GLOBAL_ASYNC_QUERIES_JWT_SECRET", "change-me-to-a-long-random-string-that-is-at-least-32-bytes")

# Point SQLAlchemy to the Postgres service from docker-compose.
SQLALCHEMY_DATABASE_URI = os.getenv(
    "SUPERSET_DATABASE_URI",
    "postgresql+psycopg2://superset:superset@superset_db:5432/superset",
)

# Cache + async query results go through Redis.
REDIS_HOST = os.getenv("REDIS_HOST", "superset_redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
    "CACHE_KEY_PREFIX": "superset_cache_",
    "CACHE_REDIS_HOST": REDIS_HOST,
    "CACHE_REDIS_PORT": REDIS_PORT,
}

# Use Redis for async query event streams.
GLOBAL_ASYNC_QUERIES_CACHE_BACKEND = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
    "CACHE_KEY_PREFIX": "superset_async_",
    "CACHE_REDIS_HOST": REDIS_HOST,
    "CACHE_REDIS_PORT": REDIS_PORT,
}

# Align data cache with Redis to avoid localhost defaults.
DATA_CACHE_CONFIG = CACHE_CONFIG

RESULTS_BACKEND = {
    "backend": "superset.extensions.results_backend.RedisCacheBackend",
    "host": REDIS_HOST,
    "port": REDIS_PORT,
    "key_prefix": "superset_results_",
}

# Async events (chart data jobs) use Redis; avoid localhost defaults.
ASYNC_EVENT_CACHE_URL = f"redis://{REDIS_HOST}:{REDIS_PORT}/0"

FEATURE_FLAGS = {
    "DASHBOARD_CROSS_FILTERS": True,
    "GLOBAL_ASYNC_QUERIES": True,
}

# Avoid default sqlite Celery backend (not writable in container).
CELERY_CONFIG = {
    "broker_url": f"redis://{REDIS_HOST}:{REDIS_PORT}/0",
    "result_backend": f"redis://{REDIS_HOST}:{REDIS_PORT}/0",
    "task_acks_late": False,
    "worker_prefetch_multiplier": 1,
}
