"""Text normalisation helpers for Italian place names."""

from __future__ import annotations

import unicodedata


def normalise(text: str) -> str:
    """Lowercase, strip, and remove accents/diacritics."""
    text = text.lower().strip()
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(c for c in nfkd if not unicodedata.combining(c))
