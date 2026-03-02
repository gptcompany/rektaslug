"""Central runtime settings for the primary application workflow."""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path


def _read_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _read_csv(name: str, default: tuple[str, ...]) -> tuple[str, ...]:
    raw = os.getenv(name)
    if not raw:
        return default
    values = tuple(item.strip() for item in raw.split(",") if item.strip())
    return values or default


@dataclass(frozen=True)
class AppSettings:
    """Runtime settings shared by the main API and core helper scripts."""

    host: str
    port: int
    api_url: str
    project_root: Path
    data_dir: Path
    db_path: Path
    symbols: tuple[str, ...]
    cors_allowed_origins: tuple[str, ...]
    rate_limit_enabled: bool
    rate_limit_rpm: int
    cache_ttl: int
    cache_max_size: int

    @classmethod
    def from_env(cls) -> "AppSettings":
        host = os.getenv("HEATMAP_HOST", "0.0.0.0")
        port = int(os.getenv("HEATMAP_PORT", "8002"))
        return cls(
            host=host,
            port=port,
            api_url=os.getenv("HEATMAP_API_URL", f"http://127.0.0.1:{port}"),
            project_root=Path(
                os.getenv("HEATMAP_PROJECT_ROOT", "/media/sam/1TB/LiquidationHeatmap")
            ),
            data_dir=Path(
                os.getenv(
                    "HEATMAP_DATA_DIR",
                    "/media/sam/3TB-WDC/binance-history-data-downloader/data",
                )
            ),
            db_path=Path(
                os.getenv(
                    "HEATMAP_DB_PATH",
                    "/media/sam/2TB-NVMe/liquidationheatmap_db/liquidations.duckdb",
                )
            ),
            symbols=_read_csv("HEATMAP_SYMBOLS", ("BTCUSDT", "ETHUSDT")),
            cors_allowed_origins=_read_csv("CORS_ALLOWED_ORIGINS", ("*",)),
            rate_limit_enabled=_read_bool("RATE_LIMIT_ENABLED", True),
            rate_limit_rpm=int(os.getenv("RATE_LIMIT_RPM", "120")),
            cache_ttl=int(os.getenv("LH_CACHE_TTL", "300")),
            cache_max_size=int(os.getenv("LH_CACHE_MAX_SIZE", "100")),
        )


@lru_cache(maxsize=1)
def get_settings() -> AppSettings:
    """Return the cached runtime settings singleton."""

    return AppSettings.from_env()
