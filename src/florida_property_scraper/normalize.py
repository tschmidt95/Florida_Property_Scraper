import re
from typing import Optional


_WHITESPACE_RE = re.compile(r"\s+")
_PUNCT_RE = re.compile(r"[^\w\s]", re.UNICODE)


def normalize_text(value: Optional[str]) -> str:
    if value is None:
        return ""
    cleaned = _WHITESPACE_RE.sub(" ", str(value)).strip()
    return cleaned.casefold()


def normalize_address(value: Optional[str]) -> str:
    cleaned = normalize_text(value)
    cleaned = _PUNCT_RE.sub("", cleaned)
    cleaned = _WHITESPACE_RE.sub(" ", cleaned).strip()
    return cleaned
