from __future__ import annotations

import base64
import os
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives.kdf.scrypt import Scrypt

from dna_insights.core.settings import AppSettings


def generate_salt() -> bytes:
    return os.urandom(16)


def derive_key(passphrase: str, salt: bytes) -> bytes:
    kdf = Scrypt(salt=salt, length=32, n=2**14, r=8, p=1)
    key = kdf.derive(passphrase.encode("utf-8"))
    return base64.urlsafe_b64encode(key)


class EncryptionManager:
    def __init__(self, settings: AppSettings) -> None:
        self.settings = settings
        self._key: bytes | None = None

    def is_enabled(self) -> bool:
        return self.settings.encryption_enabled

    def has_key(self) -> bool:
        return self._key is not None

    def unlock(self, passphrase: str) -> None:
        if not self.settings.encryption_salt:
            salt = generate_salt()
            self.settings.encryption_salt = base64.b64encode(salt).decode("ascii")
        else:
            salt = base64.b64decode(self.settings.encryption_salt.encode("ascii"))
        self._key = derive_key(passphrase, salt)

    def lock(self) -> None:
        self._key = None

    def encrypt_bytes(self, data: bytes) -> bytes:
        if not self.is_enabled():
            return data
        if self._key is None:
            raise RuntimeError("Encryption is enabled but not unlocked.")
        return Fernet(self._key).encrypt(data)

    def decrypt_bytes(self, data: bytes) -> bytes:
        if not self.is_enabled():
            return data
        if self._key is None:
            raise RuntimeError("Encryption is enabled but not unlocked.")
        return Fernet(self._key).decrypt(data)
