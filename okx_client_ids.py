"""Canonical client identifiers for OKX mutation fields."""

from __future__ import annotations

import hashlib
import re


OKX_CLIENT_ID_MAX_LENGTH = 32
_ALPHANUMERIC = re.compile(r"^[A-Za-z0-9]+$")


class InvalidOKXClientID(ValueError):
    """Raised when a client identifier cannot be sent to OKX."""


def validate_client_id(value: object, *, field: str = "client_id") -> str:
    """Validate an already-formatted OKX client identifier."""
    text = str(value or "")
    if not text:
        raise InvalidOKXClientID(f"{field} must not be empty")
    if len(text) > OKX_CLIENT_ID_MAX_LENGTH:
        raise InvalidOKXClientID(f"{field} exceeds {OKX_CLIENT_ID_MAX_LENGTH} characters")
    if _ALPHANUMERIC.fullmatch(text) is None:
        raise InvalidOKXClientID(f"{field} must be alphanumeric")
    return text


def canonical_client_id(prefix: str, logical_identity: object) -> str:
    """Encode a logical identity into a deterministic valid OKX identifier."""
    prefix_text = str(prefix or "")
    if not prefix_text or _ALPHANUMERIC.fullmatch(prefix_text) is None:
        raise InvalidOKXClientID("client identifier prefix must be alphanumeric")
    digest = hashlib.sha256(str(logical_identity).encode("utf-8")).hexdigest()
    token_length = OKX_CLIENT_ID_MAX_LENGTH - len(prefix_text)
    if token_length <= 0:
        raise InvalidOKXClientID("client identifier prefix leaves no identity space")
    return validate_client_id(prefix_text + digest[:token_length])


def canonical_entry_client_id(intent_id: object, suffix: str = "E") -> str:
    return canonical_client_id(f"OXY{suffix}", intent_id)


def canonical_protection_algo_client_id(logical_identity: object) -> str:
    return canonical_client_id("OXYA", logical_identity)
