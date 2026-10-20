from datetime import UTC, datetime, timedelta

from kalshi_trading.collectors.registry import MarketRegistry, MarketWindow, is_wanted
from kalshi_trading.common.config import SeriesRule

NOW = datetime(2026, 10, 20, 18, 0, tzinfo=UTC)


def window(open_min, close_min):
    return MarketWindow(
        "T", "S", NOW + timedelta(minutes=open_min), NOW + timedelta(minutes=close_min)
    )


def test_15m_window_rules():
    rule = SeriesRule("S", lookahead_min=20, linger_min=3)
    assert is_wanted(window(0, 15), rule, NOW)  # open now
    assert is_wanted(window(15, 30), rule, NOW)  # next window, inside lookahead
    assert not is_wanted(window(30, 45), rule, NOW)  # too far ahead
    assert is_wanted(window(-15, -2), rule, NOW)  # just closed, lingering
    assert not is_wanted(window(-20, -5), rule, NOW)  # closed too long ago


def test_ladder_horizon():
    rule = SeriesRule("S", lookahead_min=1440, horizon_hours=3)
    assert is_wanted(window(-600, 120), rule, NOW)
    assert not is_wanted(window(-600, 240), rule, NOW)


def iso(dt):
    return dt.isoformat().replace("+00:00", "Z")


class FakeRest:
    def __init__(self, markets):
        self.markets = markets

    async def iter_markets(self, series, status):
        for m in self.markets:
            if m["ticker"].startswith(series) and m["status"] == status:
                yield m


def market(ticker, open_min, close_min, status="open", **extra):
    return {
        "ticker": ticker,
        "status": status,
        "open_time": iso(NOW + timedelta(minutes=open_min)),
        "close_time": iso(NOW + timedelta(minutes=close_min)),
        **extra,
    }


async def test_refresh_reports_new_and_changed_markets_only():
    now = [NOW]
    rest = FakeRest(
        [
            market("KXBTC15M-A", -5, 10, yes_bid_dollars="0.40"),
            market("KXBTC15M-B", 10, 25, status="unopened"),
            market("KXBTC15M-C", 60, 75, status="unopened"),
        ]
    )
    reg = MarketRegistry(rest, (SeriesRule("KXBTC15M"),), now=lambda: now[0])
    changed = await reg.refresh()
    assert {m["ticker"] for m in changed} == {"KXBTC15M-A", "KXBTC15M-B", "KXBTC15M-C"}
    assert reg.wanted() == {"KXBTC15M-A", "KXBTC15M-B"}

    rest.markets[0]["yes_bid_dollars"] = "0.55"  # price moves are not contract changes
    assert await reg.refresh() == []
    rest.markets[0]["floor_strike"] = 68000.12
    assert [m["ticker"] for m in await reg.refresh()] == ["KXBTC15M-A"]

    now[0] = NOW + timedelta(minutes=12)  # A closed at +10 and lingers 3 minutes
    assert "KXBTC15M-A" in reg.wanted()
    now[0] = NOW + timedelta(minutes=14)
    assert "KXBTC15M-A" not in reg.wanted()
    rest.markets.pop(0)
    await reg.refresh()
    assert "KXBTC15M-A" not in reg.markets
