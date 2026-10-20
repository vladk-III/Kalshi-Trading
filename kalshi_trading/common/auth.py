"""Kalshi API request signing.

Kalshi signs `timestamp_ms + METHOD + path` (path without query string) with the
account's private key: RSA-PSS/SHA-256 for RSA keys, plain Ed25519 for Ed25519 keys.
The signature is sent base64-encoded alongside the key id and timestamp headers.
"""

from __future__ import annotations

import base64
import time
from pathlib import Path

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey


def load_private_key(path: str | Path) -> RSAPrivateKey | Ed25519PrivateKey:
    key = serialization.load_pem_private_key(Path(path).read_bytes(), password=None)
    if not isinstance(key, RSAPrivateKey | Ed25519PrivateKey):
        raise TypeError(f"unsupported key type {type(key).__name__}; expected RSA or Ed25519")
    return key


def sign_text(private_key: RSAPrivateKey | Ed25519PrivateKey, text: str) -> str:
    message = text.encode("utf-8")
    if isinstance(private_key, Ed25519PrivateKey):
        signature = private_key.sign(message)
    else:
        signature = private_key.sign(
            message,
            padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
            hashes.SHA256(),
        )
    return base64.b64encode(signature).decode("ascii")


class KalshiSigner:
    def __init__(self, api_key_id: str, private_key: RSAPrivateKey | Ed25519PrivateKey):
        self.api_key_id = api_key_id
        self._key = private_key

    @classmethod
    def from_file(cls, api_key_id: str, path: str | Path) -> KalshiSigner:
        return cls(api_key_id, load_private_key(path))

    def headers(self, method: str, path: str, timestamp_ms: int | None = None) -> dict[str, str]:
        ts = str(int(time.time() * 1000) if timestamp_ms is None else timestamp_ms)
        signature = sign_text(self._key, ts + method.upper() + path.split("?")[0])
        return {
            "KALSHI-ACCESS-KEY": self.api_key_id,
            "KALSHI-ACCESS-SIGNATURE": signature,
            "KALSHI-ACCESS-TIMESTAMP": ts,
        }
