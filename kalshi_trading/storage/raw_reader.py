"""Read raw recorder logs, tolerating files truncated by a crash."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Iterator
from pathlib import Path

import orjson
import zstandard


def iter_lines(path: Path) -> Iterator[bytes]:
    """Yield complete lines; stop quietly at a truncated final block."""
    dctx = zstandard.ZstdDecompressor()
    buf = b""
    with open(path, "rb") as fh:
        reader = dctx.stream_reader(fh, read_across_frames=True)
        while True:
            try:
                chunk = reader.read(1 << 16)
            except zstandard.ZstdError:
                break
            if not chunk:
                break
            buf += chunk
            *lines, buf = buf.split(b"\n")
            yield from lines


def iter_envelopes(path: Path) -> Iterator[dict]:
    for line in iter_lines(path):
        if line:
            yield orjson.loads(line)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Print raw recorder envelopes as JSON lines")
    parser.add_argument("files", nargs="+", type=Path)
    parser.add_argument("--type", help="only messages whose m.type equals this")
    args = parser.parse_args(argv)
    out = sys.stdout.buffer
    for path in args.files:
        for env in iter_envelopes(path):
            if args.type and env.get("m", {}).get("type") != args.type:
                continue
            out.write(orjson.dumps(env) + b"\n")


if __name__ == "__main__":
    main()
