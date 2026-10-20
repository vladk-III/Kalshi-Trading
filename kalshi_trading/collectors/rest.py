"""Minimal async Kalshi REST client (market discovery only, for now)."""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator
from urllib.parse import urlparse

import httpx

from kalshi_trading.common.auth import KalshiSigner


class RateLimiter:
    """Token bucket: at most `rate` requests per second, bursting up to `burst`."""

    def __init__(self, rate: float, burst: float | None = None):
        self.rate = rate
        self.capacity = burst if burst is not None else max(1.0, rate)
        self._tokens = self.capacity
        self._last = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            while True:
                now = time.monotonic()
                self._tokens = min(self.capacity, self._tokens + (now - self._last) * self.rate)
                self._last = now
                if self._tokens >= 1:
                    self._tokens -= 1
                    return
                await asyncio.sleep((1 - self._tokens) / self.rate)


class KalshiRest:
    def __init__(
        self,
        base_url: str,
        signer: KalshiSigner,
        reads_per_s: float = 10.0,
        client: httpx.AsyncClient | None = None,
    ):
        self.base_url = base_url.rstrip("/")
        self._base_path = urlparse(self.base_url).path
        self.signer = signer
        self.limiter = RateLimiter(reads_per_s)
        self._client = client or httpx.AsyncClient(timeout=15.0)

    async def aclose(self) -> None:
        await self._client.aclose()

    async def get(self, path: str, params: dict | None = None, retries: int = 4) -> dict:
        delay = 1.0
        for attempt in range(retries + 1):
            await self.limiter.acquire()
            headers = self.signer.headers("GET", self._base_path + path)
            try:
                resp = await self._client.get(self.base_url + path, params=params, headers=headers)
            except httpx.TransportError:
                if attempt == retries:
                    raise
            else:
                if resp.status_code == 429 or resp.status_code >= 500:
                    if attempt == retries:
                        resp.raise_for_status()
                else:
                    resp.raise_for_status()
                    return resp.json()
            await asyncio.sleep(delay)
            delay = min(delay * 2, 16.0)
        raise AssertionError("unreachable")

    async def iter_markets(
        self, series_ticker: str, status: str, max_pages: int = 10
    ) -> AsyncIterator[dict]:
        cursor = None
        for _ in range(max_pages):
            params = {"series_ticker": series_ticker, "status": status, "limit": 1000}
            if cursor:
                params["cursor"] = cursor
            data = await self.get("/markets", params)
            for market in data.get("markets", []):
                yield market
            cursor = data.get("cursor")
            if not cursor:
                return
