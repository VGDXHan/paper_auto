from __future__ import annotations

import hashlib
import time
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse, urlunparse


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def normalize_url(href: str, base: str) -> str:
    u = urljoin(base, href)
    p = urlparse(u)
    p = p._replace(fragment="")
    return urlunparse(p)


def clean_text(s: str | None) -> str | None:
    if s is None:
        return None
    s = " ".join(s.split())
    return s or None


class RateLimiter:
    def __init__(self, rate_per_sec: float):
        self._min_interval = 0.0 if rate_per_sec <= 0 else 1.0 / rate_per_sec
        self._last = 0.0
        import asyncio

        self._lock = asyncio.Lock()

    async def wait(self) -> None:
        if self._min_interval <= 0:
            return
        async with self._lock:
            now = time.monotonic()
            sleep_s = self._min_interval - (now - self._last)
            if sleep_s > 0:
                import asyncio

                await asyncio.sleep(sleep_s)
            self._last = time.monotonic()
