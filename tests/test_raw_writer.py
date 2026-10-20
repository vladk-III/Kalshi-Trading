from datetime import UTC, datetime

import orjson

from kalshi_trading.storage.raw_reader import iter_envelopes
from kalshi_trading.storage.raw_writer import RawWriter, recover_partials


def ns(*args):
    return int(datetime(*args, tzinfo=UTC).timestamp() * 1e9)


def read_all(root, stream):
    files = sorted((root / stream).rglob("*.ndjson.zst"))
    return files, [e for f in files for e in iter_envelopes(f)]


def test_rotates_hourly_and_round_trips(tmp_path):
    w = RawWriter(tmp_path, "s", host="a")
    w.write(ns(2026, 10, 20, 13, 59, 59), b'{"type":"x","n":1}')
    w.write(ns(2026, 10, 20, 14, 0, 0), b'{"type":"x","n":2}')
    w.close()
    files, envs = read_all(tmp_path, "s")
    assert [f.parent.name for f in files] == ["2026-10-20", "2026-10-20"]
    assert [f.name[:2] for f in files] == ["13", "14"]
    assert [e["m"]["n"] for e in envs] == [1, 2]
    assert envs[0]["h"] == "a" and envs[0]["rt"] == ns(2026, 10, 20, 13, 59, 59)


def test_open_file_is_part_until_closed(tmp_path):
    w = RawWriter(tmp_path, "s")
    w.write(ns(2026, 10, 20, 1), b"{}")
    assert list(tmp_path.rglob("*.part"))
    w.close()
    assert not list(tmp_path.rglob("*.part"))


def test_newlines_in_payload_are_reserialised(tmp_path):
    w = RawWriter(tmp_path, "s")
    w.write(ns(2026, 10, 20, 1), b'{"a":\n 1}')
    w.close()
    _, envs = read_all(tmp_path, "s")
    assert envs == [{"rt": ns(2026, 10, 20, 1), "h": "a", "m": {"a": 1}}]


def test_crash_leaves_readable_partial_up_to_last_flush(tmp_path):
    w = RawWriter(tmp_path, "s")
    for i in range(100):
        w.write(ns(2026, 10, 20, 1), orjson.dumps({"i": i}))
    w.flush()
    w.write(ns(2026, 10, 20, 1), b'{"i":"lost"}')  # never flushed
    w._fh.flush()  # simulate a crash: no frame epilogue, no rename
    recovered = recover_partials(tmp_path)
    assert len(recovered) == 1
    _, envs = read_all(tmp_path, "s")
    assert [e["m"]["i"] for e in envs][:100] == list(range(100))
