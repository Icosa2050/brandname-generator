from __future__ import annotations

from dataclasses import dataclass
import re

from .models import CandidateResult, ResultStatus
from .naming_policy import AttractivenessPolicy, DEFAULT_NAMING_POLICY, NamingPolicy
from .taste import normalize_name


VOWELS = frozenset("aeiouy")
SYLLABLE_LIKE_RE = re.compile(r"[^aeiouy]*[aeiouy]+[^aeiouy]*")
CONSONANT_RUN_RE = re.compile(r"[^aeiouy]+")


@dataclass(frozen=True)
class AttractivenessAssessment:
    score_delta: float
    status: str
    reasons: tuple[str, ...]


def _resolved_policy(policy: NamingPolicy | None) -> AttractivenessPolicy:
    return (policy or DEFAULT_NAMING_POLICY).attractiveness


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


def _pleasant_ending(name: str, policy: AttractivenessPolicy) -> str:
    lowered = str(name or "").strip().lower()
    for ending in sorted(policy.pleasant_endings, key=len, reverse=True):
        if lowered.endswith(ending):
            return ending
    return ""


def _has_heavy_consonant_run(name: str) -> bool:
    return any(len(run) >= 3 for run in CONSONANT_RUN_RE.findall(name))


def _looks_lexical_seam(name: str, policy: AttractivenessPolicy) -> bool:
    chunks = [re.sub(r"[^a-z]", "", chunk) for chunk in SYLLABLE_LIKE_RE.findall(name)]
    if len(chunks) != 3:
        return False
    first, middle, _last = chunks
    if not (4 <= len(first) <= 5):
        return False
    if middle not in {"al", "el"}:
        return False
    return bool(_pleasant_ending(name, policy))


def _generic_safe_opening(name: str, policy: AttractivenessPolicy) -> str:
    for opening in policy.generic_safe_openings:
        if name.startswith(opening) and len(name) - len(opening) >= 2:
            return opening
    return ""


def score_name_attractiveness(
    raw_name: str,
    *,
    policy: NamingPolicy | None = None,
) -> AttractivenessAssessment:
    active_policy = _resolved_policy(policy)
    name = normalize_name(raw_name)
    if not name:
        return AttractivenessAssessment(score_delta=-12.0, status="warn", reasons=("invalid_name",))

    score = 0.0
    reasons: list[str] = []
    length = len(name)
    if int(active_policy.sweet_spot_min_length) <= length <= int(active_policy.sweet_spot_max_length):
        score += float(active_policy.length_sweet_bonus)
        reasons.append("length_sweet_spot")
    elif length in set(active_policy.acceptable_lengths):
        score += float(active_policy.length_ok_bonus)
        reasons.append("length_acceptable")
    else:
        score -= float(active_policy.length_penalty)
        reasons.append("length_awkward")

    vowel_ratio = _vowel_ratio(name)
    if float(active_policy.vowel_balance_min) <= vowel_ratio <= float(active_policy.vowel_balance_max):
        score += float(active_policy.vowel_balance_bonus)
        reasons.append("vowel_balance")
    elif float(active_policy.vowel_balance_soft_min) <= vowel_ratio <= float(active_policy.vowel_balance_soft_max):
        score += float(active_policy.vowel_balance_soft_bonus)
        reasons.append("vowel_balance_soft")
    else:
        score -= float(active_policy.vowel_balance_penalty)
        reasons.append("vowel_balance_off")

    open_ratio = _open_syllable_ratio_proxy(name)
    if open_ratio >= float(active_policy.open_syllable_strong_min):
        score += float(active_policy.open_syllable_bonus)
        reasons.append("open_syllables")
    elif open_ratio >= float(active_policy.open_syllable_soft_min):
        score += float(active_policy.open_syllable_soft_bonus)
        reasons.append("open_syllables_soft")
    else:
        score -= float(active_policy.open_syllable_penalty)
        reasons.append("closed_syllables_heavy")

    liquids = frozenset(str(active_policy.liquids))
    liquid_count = sum(1 for char in name if char in liquids)
    if int(active_policy.liquid_support_min) <= liquid_count <= int(active_policy.liquid_support_max):
        score += float(active_policy.liquid_support_bonus)
        reasons.append("liquid_support")
    elif liquid_count == 0:
        score -= float(active_policy.liquid_absent_penalty)
        reasons.append("liquid_absent")

    harsh_chars = set(active_policy.harsh_chars)
    harsh_count = sum(1 for char in name if char in harsh_chars)
    if harsh_count:
        score -= float(active_policy.harsh_penalty_per_char) * harsh_count
        reasons.append("harsh_letters")
    if name[:1] in harsh_chars:
        score -= float(active_policy.leading_harsh_penalty)
        reasons.append("leading_harsh")

    if "v" in name:
        score -= float(active_policy.sharp_v_penalty)
        reasons.append("sharp_v")

    if _has_heavy_consonant_run(name):
        score -= float(active_policy.dense_consonant_run_penalty)
        reasons.append("dense_consonant_run")

    ending = _pleasant_ending(name, active_policy)
    if ending:
        score += float(active_policy.pleasant_ending_bonus)
        reasons.append("pleasant_ending")

    if _looks_lexical_seam(name, active_policy):
        score -= float(active_policy.lexical_seam_penalty)
        reasons.append("lexical_seam")

    if any(fragment in name for fragment in active_policy.literal_signal_fragments):
        score -= float(active_policy.literal_signal_penalty)
        reasons.append("literal_signal_fragment")

    generic_opening = _generic_safe_opening(name, active_policy)
    if generic_opening:
        score -= float(active_policy.generic_opening_penalty)
        reasons.append("generic_safe_opening")

    heavy_shape = "closed_syllables_heavy" in reasons or "dense_consonant_run" in reasons
    open_support = "open_syllables" in reasons or "open_syllables_soft" in reasons
    pleasant_support = "pleasant_ending" in reasons
    forced_warn = heavy_shape and not pleasant_support and not open_support
    if "dense_consonant_run" in reasons and score < float(active_policy.dense_run_warn_below):
        forced_warn = True
    if "vowel_balance_off" in reasons and score < float(active_policy.vowel_balance_warn_below):
        forced_warn = True
    if "lexical_seam" in reasons and score < float(active_policy.lexical_seam_warn_below):
        forced_warn = True
    if "literal_signal_fragment" in reasons:
        forced_warn = True
    if "generic_safe_opening" in reasons:
        forced_warn = True
    if "leading_harsh" in reasons:
        forced_warn = True
    if "harsh_letters" in reasons and score < float(active_policy.harsh_letters_warn_below):
        forced_warn = True

    status = "pass" if score >= float(active_policy.pass_threshold) and not forced_warn else "warn"
    return AttractivenessAssessment(
        score_delta=round(score, 2),
        status=status,
        reasons=tuple(reasons),
    )


def build_attractiveness_result(
    raw_name: str,
    *,
    policy: NamingPolicy | None = None,
) -> CandidateResult:
    assessment = score_name_attractiveness(raw_name, policy=policy)
    return CandidateResult(
        check_name="attractiveness",
        status=ResultStatus.PASS if assessment.status == "pass" else ResultStatus.WARN,
        score_delta=float(assessment.score_delta),
        reason=f"attractiveness_{assessment.status}",
        details={"reasons": list(assessment.reasons)},
    )
