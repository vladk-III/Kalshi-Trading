"""Free local disk: delete the oldest closed raw files.

Deletes files older than --keep-days, then keeps deleting oldest-first while free
space is below --min-free-gb. Open `.part` files are never touched. Run it daily
(see deploy/systemd/kalshi-retention.timer) after the off-site backup has run.
"""

from __future__ import annotations

import argparse
import shutil
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

from kalshi_trading.storage.raw_writer import FILE_SUFFIX


@dataclass(frozen=True)
class RawFile:
    path: Path
    day: date
    size: int


def scan(root: Path) -> list[RawFile]:
    files = []
    for path in Path(root).rglob(f"*{FILE_SUFFIX}"):
        try:
            day = date.fromisoformat(path.parent.name)
        except ValueError:
            continue
        files.append(RawFile(path, day, path.stat().st_size))
    return sorted(files, key=lambda f: (f.day, f.path.name))


def plan_deletions(
    files: list[RawFile], today: date, keep_days: int, free_bytes: int, min_free_bytes: int
) -> list[RawFile]:
    cutoff = today - timedelta(days=keep_days)
    plan = []
    for f in sorted(files, key=lambda f: (f.day, f.path.name)):
        if f.day < cutoff or free_bytes < min_free_bytes:
            plan.append(f)
            free_bytes += f.size
        else:
            break
    return plan


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--keep-days", type=int, default=300)
    parser.add_argument("--min-free-gb", type=float, default=20.0)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    free = shutil.disk_usage(args.root).free
    plan = plan_deletions(
        scan(args.root), date.today(), args.keep_days, free, int(args.min_free_gb * 1e9)
    )
    for f in plan:
        print(f"{'would delete' if args.dry_run else 'delete'} {f.path} ({f.size} bytes)")
        if not args.dry_run:
            f.path.unlink(missing_ok=True)
    print(f"{len(plan)} files, {sum(f.size for f in plan) / 1e9:.2f} GB")


if __name__ == "__main__":
    main()
