from __future__ import annotations

from dataclasses import dataclass
import re

from .models import CandidateResult, ResultStatus
from .taste import normalize_name


VOWELS = frozenset("aeiouy")
SYLLABLE_LIKE_RE = re.compile(r"[^aeiouy]*[aeiouy]+[^aeiouy]*")
PLEASANT_ENDINGS: tuple[str, ...] = (
    "a",
    "an",
    "ar",
    "el",
    "en",
    "er",
    "la",
    "ra",
    "ta",
)
HARSH_CHARS: tuple[str, ...] = ("x", "z", "q", "j")
LITERAL_SIGNAL_FRAGMENTS: tuple[str, ...] = ("clar", "civic", "trust", "legal", "secur")
GENERIC_SAFE_OPENINGS: tuple[str, ...] = ("pre", "prec", "prim", "cora", "stati")
LIQUIDS = frozenset("lrmn")
CONSONANT_RUN_RE = re.compile(r"[^aeiouy]+")
LEADING_HARSH_RE = re.compile(r"^[qxzj]")


@dataclass(frozen=True)
class AttractivenessAssessment:
    score_delta: float
    status: str
    reasons: tuple[str, ...]


def _vowel_ratio(name: str) -> float:
    if not name:
        return 0.0
    return sum(1 for char in name if char in VOWELS) / len(name)


def _open_syllable_ratio_proxy(name: str) -> float:
    chunks = SYLLABLE_LIKE_RE.findall(name)
    if not chunks:
        return 0.0
    openish = 0
    for chunk in chunks:
        tail = re.sub(r"^.*?[aeiouy]+", "", chunk)
        tail_letters = re.sub(r"[^a-z]", "", tail)
        if len(tail_letters) <= 1:
            openish += 1
    return openish / len(chunks)


def _pleasant_ending(name: str) -> str:
    lowered = str(name or "").strip().lower()
    for ending in sorted(PLEASANT_ENDINGS, key=len, reverse=True):
        if lowered.endswith(ending):
            return ending
    return ""


def _has_heavy_consonant_run(name: str) -> bool:
    return any(len(run) >= 3 for run in CONSONANT_RUN_RE.findall(name))


def _looks_lexical_seam(name: str) -> bool:
    chunks = [re.sub(r"[^a-z]", "", chunk) for chunk in SYLLABLE_LIKE_RE.findall(name)]
    if len(chunks) != 3:
        return False
    first, middle, _last = chunks
    if not (4 <= len(first) <= 5):
        return False
    if middle not in {"al", "el"}:
        return False
    return bool(_pleasant_ending(name))


def _generic_safe_opening(name: str) -> str:
    for opening in GENERIC_SAFE_OPENINGS:
        if name.startswith(opening) and len(name) - len(opening) >= 2:
            return opening
    return ""


def score_name_attractiveness(raw_name: str) -> AttractivenessAssessment:
    name = normalize_name(raw_name)
    if not name:
        return AttractivenessAssessment(score_delta=-12.0, status="warn", reasons=("invalid_name",))

    score = 0.0
    reasons: list[str] = []
    length = len(name)
    if 7 <= length <= 9:
        score += 6.0
        reasons.append("length_sweet_spot")
    elif length in {6, 10}:
        score += 3.0
        reasons.append("length_acceptable")
    else:
        score -= 4.0
        reasons.append("length_awkward")

    vowel_ratio = _vowel_ratio(name)
    if 0.35 <= vowel_ratio <= 0.56:
        score += 5.0
        reasons.append("vowel_balance")
    elif 0.30 <= vowel_ratio <= 0.60:
        score += 2.0
        reasons.append("vowel_balance_soft")
    else:
        score -= 5.0
        reasons.append("vowel_balance_off")

    open_ratio = _open_syllable_ratio_proxy(name)
    if open_ratio >= 0.55:
        score += 4.0
        reasons.append("open_syllables")
    elif open_ratio >= 0.40:
        score += 1.5
        reasons.append("open_syllables_soft")
    else:
        score -= 3.0
        reasons.append("closed_syllables_heavy")

    liquid_count = sum(1 for char in name if char in LIQUIDS)
    if 1 <= liquid_count <= 3:
        score += 3.0
        reasons.append("liquid_support")
    elif liquid_count == 0:
        score -= 2.0
        reasons.append("liquid_absent")

    harsh_count = sum(1 for char in name if char in HARSH_CHARS)
    if harsh_count:
        score -= 4.0 * harsh_count
        reasons.append("harsh_letters")
    if LEADING_HARSH_RE.search(name):
        score -= 3.0
        reasons.append("leading_harsh")

    if "v" in name:
        score -= 1.0
        reasons.append("sharp_v")

    if _has_heavy_consonant_run(name):
        score -= 4.0
        reasons.append("dense_consonant_run")

    ending = _pleasant_ending(name)
    if ending:
        score += 2.0
        reasons.append("pleasant_ending")

    if _looks_lexical_seam(name):
        score -= 4.0
        reasons.append("lexical_seam")

    if any(fragment in name for fragment in LITERAL_SIGNAL_FRAGMENTS):
        score -= 5.0
        reasons.append("literal_signal_fragment")

    generic_opening = _generic_safe_opening(name)
    if generic_opening:
        score -= 5.0
        reasons.append("generic_safe_opening")

    heavy_shape = "closed_syllables_heavy" in reasons or "dense_consonant_run" in reasons
    open_support = "open_syllables" in reasons or "open_syllables_soft" in reasons
    pleasant_support = "pleasant_ending" in reasons
    forced_warn = heavy_shape and not pleasant_support and not open_support
    if "dense_consonant_run" in reasons and score < 12.0:
        forced_warn = True
    if "vowel_balance_off" in reasons and score < 10.0:
        forced_warn = True
    if "lexical_seam" in reasons and score < 18.0:
        forced_warn = True
    if "literal_signal_fragment" in reasons:
        forced_warn = True
    if "generic_safe_opening" in reasons:
        forced_warn = True
    if "leading_harsh" in reasons:
        forced_warn = True
    if "harsh_letters" in reasons and score < 15.0:
        forced_warn = True

    status = "pass" if score >= 7.5 and not forced_warn else "warn"
    return AttractivenessAssessment(
        score_delta=round(score, 2),
        status=status,
        reasons=tuple(reasons),
    )


def build_attractiveness_result(raw_name: str) -> CandidateResult:
    assessment = score_name_attractiveness(raw_name)
    return CandidateResult(
        check_name="attractiveness",
        status=ResultStatus.PASS if assessment.status == "pass" else ResultStatus.WARN,
        score_delta=float(assessment.score_delta),
        reason=f"attractiveness_{assessment.status}",
        details={"reasons": list(assessment.reasons)},
    )
