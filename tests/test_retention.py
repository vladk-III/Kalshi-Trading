from datetime import date
from pathlib import Path

from kalshi_trading.storage.retention import RawFile, plan_deletions, scan


def f(day, size=1):
    return RawFile(Path(f"/x/{day}/00_1.ndjson.zst"), date.fromisoformat(day), size)


def test_deletes_files_older_than_keep_days():
    files = [f("2026-01-01"), f("2026-10-01"), f("2026-10-19")]
    plan = plan_deletions(files, date(2026, 10, 20), keep_days=30, free_bytes=100, min_free_bytes=0)
    assert plan == [files[0]]


def test_deletes_oldest_until_free_space_restored():
    files = [f("2026-10-10", 5), f("2026-10-11", 5), f("2026-10-12", 5)]
    plan = plan_deletions(files, date(2026, 10, 20), keep_days=300, free_bytes=2, min_free_bytes=10)
    assert plan == files[:2]


def test_scan_skips_open_part_files(tmp_path):
    d = tmp_path / "s" / "2026-10-20"
    d.mkdir(parents=True)
    (d / "01_1.ndjson.zst").write_bytes(b"x")
    (d / "02_1.ndjson.zst.part").write_bytes(b"x")
    assert [r.path.name for r in scan(tmp_path)] == ["01_1.ndjson.zst"]
