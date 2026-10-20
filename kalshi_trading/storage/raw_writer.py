"""Append-only, hourly-rotated, zstd-compressed NDJSON log of raw messages.

Layout:  <root>/<stream>/<YYYY-MM-DD>/<HH>_<start_ms>.ndjson.zst
Each line is an envelope around the original message, spliced in verbatim:
    {"rt": <local receive time, ns>, "h": "<host label>", "m": <original JSON>}

The open file carries a ".part" suffix and is renamed when the hour closes, so an
uploader can safely copy everything without that suffix. Data is flushed as a
complete zstd block every `flush()` call, so a crash loses at most one flush
interval and the partial file stays decodable up to the last flush.
"""

from __future__ import annotations

import os
import time
from datetime import UTC, datetime
from pathlib import Path

import orjson
import zstandard

PART_SUFFIX = ".part"
FILE_SUFFIX = ".ndjson.zst"


def hour_key(recv_ns: int) -> tuple[str, str]:
    dt = datetime.fromtimestamp(recv_ns / 1e9, tz=UTC)
    return dt.strftime("%Y-%m-%d"), dt.strftime("%H")


def make_envelope(recv_ns: int, host: str, payload: bytes) -> bytes:
    """Wrap a raw JSON message. Payloads containing newlines are re-serialised compactly."""
    if b"\n" in payload or b"\r" in payload:
        payload = orjson.dumps(orjson.loads(payload))
    return b'{"rt":%d,"h":"%s","m":%s}\n' % (recv_ns, host.encode(), payload)


class RawWriter:
    def __init__(self, root: Path, stream: str, host: str = "a", level: int = 6):
        self.root = Path(root)
        self.stream = stream
        self.host = host
        self._cctx = zstandard.ZstdCompressor(level=level)
        self._key: tuple[str, str] | None = None
        self._path: Path | None = None
        self._fh = None
        self._zw = None
        self.lines_written = 0
        self.bytes_in = 0

    def write(self, recv_ns: int, payload: bytes) -> None:
        key = hour_key(recv_ns)
        if key != self._key:
            self._rotate(key)
        line = make_envelope(recv_ns, self.host, payload)
        self._zw.write(line)
        self.lines_written += 1
        self.bytes_in += len(line)

    def write_obj(self, recv_ns: int, obj: dict) -> None:
        self.write(recv_ns, orjson.dumps(obj))

    def flush(self, fsync: bool = True) -> None:
        if self._zw is None:
            return
        self._zw.flush(zstandard.FLUSH_BLOCK)
        self._fh.flush()
        if fsync:
            os.fsync(self._fh.fileno())

    def close(self) -> None:
        if self._zw is None:
            return
        self._zw.close()  # writes the frame epilogue and closes the file
        final = self._path.with_name(self._path.name.removesuffix(PART_SUFFIX))
        os.replace(self._path, final)
        self._zw = self._fh = self._path = self._key = None

    def _rotate(self, key: tuple[str, str]) -> None:
        self.close()
        day, hour = key
        directory = self.root / self.stream / day
        directory.mkdir(parents=True, exist_ok=True)
        self._path = directory / f"{hour}_{time.time_ns() // 1_000_000}{FILE_SUFFIX}{PART_SUFFIX}"
        self._fh = open(self._path, "wb")
        self._zw = self._cctx.stream_writer(self._fh, closefd=True)
        self._key = key


def recover_partials(root: Path) -> list[Path]:
    """Rename `.part` files left behind by a crash so they get uploaded and read."""
    recovered = []
    for part in Path(root).rglob(f"*{FILE_SUFFIX}{PART_SUFFIX}"):
        final = part.with_name(part.name.removesuffix(PART_SUFFIX))
        os.replace(part, final)
        recovered.append(final)
    return recovered
