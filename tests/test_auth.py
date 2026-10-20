import base64

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from kalshi_trading.common.auth import KalshiSigner, load_private_key


def test_rsa_signature_verifies_over_timestamp_method_path(rsa_key, signer):
    h = signer.headers(
        "get", "/trade-api/v2/markets?series_ticker=KXBTC15M", timestamp_ms=1703123456789
    )
    assert h["KALSHI-ACCESS-KEY"] == "test-key-id"
    assert h["KALSHI-ACCESS-TIMESTAMP"] == "1703123456789"
    rsa_key.public_key().verify(
        base64.b64decode(h["KALSHI-ACCESS-SIGNATURE"]),
        b"1703123456789GET/trade-api/v2/markets",  # query string is not signed
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
        hashes.SHA256(),
    )


def test_ed25519_signature_verifies():
    key = Ed25519PrivateKey.generate()
    h = KalshiSigner("k", key).headers("GET", "/trade-api/ws/v2", timestamp_ms=5)
    key.public_key().verify(base64.b64decode(h["KALSHI-ACCESS-SIGNATURE"]), b"5GET/trade-api/ws/v2")


def test_load_pkcs1_rsa_pem(tmp_path, rsa_key):
    pem = rsa_key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.TraditionalOpenSSL,  # "BEGIN RSA PRIVATE KEY", as Kalshi issues
        serialization.NoEncryption(),
    )
    path = tmp_path / "k.pem"
    path.write_bytes(pem)
    assert load_private_key(path).key_size == 2048
