"""Ajustes de ejecución del crawler, cargados del JSON de configuración."""
from __future__ import annotations

from dataclasses import dataclass, fields


@dataclass(frozen=True)
class CrawlerSettings:
    url: str
    period: str = "todos"
    connect_timeout_s: int = 15
    read_timeout_s: int = 30
    delay_min_s: float = 1.5
    delay_max_s: float = 3.0
    long_pause_every_pages: int = 200
    long_pause_min_s: int = 20
    long_pause_max_s: int = 40
    retry_count: int = 4
    backoff_base_s: float = 2.0
    max_consecutive_errors: int = 5
    reset_session_every_pages: int = 30
    http_proxy: str = ""
    https_proxy: str = ""

    @classmethod
    def from_config(cls, config_env: dict) -> "CrawlerSettings":
        """Toma del bloque del entorno sólo las claves que conoce."""
        known = {f.name for f in fields(cls)}
        return cls(**{k: v for k, v in config_env.items() if k in known})
