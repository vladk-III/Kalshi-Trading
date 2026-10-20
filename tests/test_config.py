import pytest

from kalshi_trading.common.config import load_config

ENV = {"KALSHI_API_KEY_ID": "id", "KALSHI_PRIVATE_KEY_PATH": "/k.pem"}


def test_loads_repo_config():
    cfg = load_config("configs/recorder.yaml", ENV)
    assert cfg.ws_url == "wss://external-api-ws.kalshi.com/trade-api/ws/v2"
    assert [s.ticker for s in cfg.series][:4] == ["KXBTC15M", "KXETH15M", "KXSOL15M", "KXXRP15M"]
    assert cfg.series[4].horizon_hours == 3
    assert "BRTI" in cfg.index.ids


def test_demo_env_override(tmp_path):
    p = tmp_path / "c.yaml"
    p.write_text("series: [KXBTC15M]\n")
    cfg = load_config(p, {**ENV, "KALSHI_ENV": "demo"})
    assert "demo" in cfg.ws_url and "demo" in cfg.rest_base


def test_missing_credentials(tmp_path):
    p = tmp_path / "c.yaml"
    p.write_text("series: [KXBTC15M]\n")
    with pytest.raises(ValueError, match="KALSHI_API_KEY_ID"):
        load_config(p, {})
