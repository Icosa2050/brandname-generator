from __future__ import annotations

import re
import unicodedata


SPECIAL_ASCII_MAP = str.maketrans(
    {
        "Æ": "AE",
        "æ": "ae",
        "Ø": "OE",
        "ø": "oe",
        "Å": "AA",
        "å": "aa",
        "Œ": "OE",
        "œ": "oe",
        "Ð": "D",
        "ð": "d",
        "Þ": "TH",
        "þ": "th",
    }
)


def fold_brand_text(raw: str) -> str:
    transliterated = str(raw or "").translate(SPECIAL_ASCII_MAP)
    normalized = unicodedata.normalize("NFKD", transliterated)
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def normalize_brand_token(raw: str) -> str:
    plain = fold_brand_text(raw).lower()
    return re.sub(r"[^a-z0-9]", "", plain)
