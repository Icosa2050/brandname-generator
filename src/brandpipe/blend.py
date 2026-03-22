from __future__ import annotations

import re


VOWELS = frozenset("aeiouy")
VALID_BLEND_RE = re.compile(r"^[a-z]{6,14}$")


def normalize_blend_word(raw: str) -> str:
    return re.sub(r"[^a-z]", "", str(raw or "").strip().lower())


def _vowel_groups(name: str) -> int:
    return max(1, len(re.findall(r"[aeiouy]+", name)))


def _has_harsh_cluster(name: str) -> bool:
    return bool(re.search(r"[^aeiouy]{4,}", name)) or bool(re.search(r"(.)\1\1", name))


def _soften_seam(name: str) -> str:
    if len(name) < 2:
        return name
    if name[-1] == name[-2]:
        return name[:-1]
    return name


def _retains_literal_edges(
    candidate: str,
    left: str,
    right: str,
    *,
    overlap: int,
    left_keep: int,
    right_piece_len: int,
) -> bool:
    left_literal = left_keep >= 4 and candidate.startswith(left[:4])
    right_literal = right_piece_len >= 4 and candidate.endswith(right[-4:])
    if left_literal and right_literal:
        return True
    if overlap == 0 and len(left) >= 7 and left_keep >= 5:
        return True
    if overlap == 0 and len(right) >= 7 and right_piece_len >= 5:
        return True
    return False


def _score_candidate(
    candidate: str,
    left: str,
    right: str,
    overlap: int,
    *,
    left_keep: int,
    right_piece_len: int,
) -> float:
    score = 0.0
    length = len(candidate)
    vowel_groups = _vowel_groups(candidate)
    if 6 <= length <= 12:
        score += 2.0
    elif 5 <= length <= 13:
        score += 1.0
    else:
        score -= 1.0
    if 2 <= vowel_groups <= 4:
        score += 1.2
    if overlap >= 2:
        score += 1.4 + (0.1 * overlap)
    if left_keep >= 4:
        score += 0.25 * min(3, left_keep - 3)
    if right_piece_len >= 3:
        score += 0.2 * min(3, right_piece_len - 2)
    if left_keep >= 4 and right_piece_len >= 4:
        score -= 3.0
    elif overlap == 0 and (left_keep >= 4 or right_piece_len >= 4):
        score -= 1.5
    ideal_left = max(3, min(5, len(left) // 2 + 1))
    ideal_right = max(3, min(5, len(right) // 2))
    score -= 0.25 * abs(left_keep - ideal_left)
    score -= 0.25 * abs(right_piece_len - ideal_right)
    if candidate[0] in VOWELS:
        score -= 0.2
    if candidate[-1] in VOWELS:
        score += 0.2
    if _has_harsh_cluster(candidate):
        score -= 2.5
    if candidate.startswith(left[:2]) and candidate.endswith(right[-2:]):
        score += 0.3
    if candidate == left or candidate == right:
        score -= 3.0
    return round(score, 4)


def _build_exact_overlap_candidates(left: str, right: str) -> list[tuple[str, int, int, int]]:
    candidates: list[tuple[str, int, int, int]] = []
    max_overlap = min(4, len(left), len(right))
    for overlap in range(max_overlap, 1, -1):
        if left.endswith(right[:overlap]):
            candidate = _soften_seam(left[:-overlap] + right)
            candidates.append((candidate, overlap, len(left) - overlap, len(right)))
    return candidates


def _build_seam_candidates(left: str, right: str) -> list[tuple[str, int, int, int]]:
    candidates: list[tuple[str, int, int, int]] = []
    left_min = max(3, len(left) - 4)
    left_max = max(3, min(len(left) - 2, len(left) - 1))
    right_max = min(4, len(right) - 3)
    if right_max < 1:
        return candidates
    for left_keep in range(left_min, left_max + 1):
        left_piece = left[:left_keep]
        if not left_piece:
            continue
        for right_drop in range(1, right_max + 1):
            right_piece = right[right_drop:]
            if len(right_piece) < 3:
                continue
            candidate = _soften_seam(left_piece + right_piece)
            candidates.append((candidate, 0, left_keep, len(right_piece)))
    return candidates


def blend_candidates(left: str, right: str, *, limit: int = 5) -> list[str]:
    left_norm = normalize_blend_word(left)
    right_norm = normalize_blend_word(right)
    if not left_norm or not right_norm:
        return []
    if left_norm == right_norm:
        return []

    raw_candidates: list[tuple[str, int, int, int]] = []
    raw_candidates.extend(_build_exact_overlap_candidates(left_norm, right_norm))
    raw_candidates.extend(_build_seam_candidates(left_norm, right_norm))

    scored: list[tuple[float, str]] = []
    seen: set[str] = set()
    for candidate, overlap, left_keep, right_piece_len in raw_candidates:
        normalized = normalize_blend_word(candidate)
        if not normalized or normalized in seen:
            continue
        if not VALID_BLEND_RE.fullmatch(normalized):
            continue
        if _has_harsh_cluster(normalized):
            continue
        if _retains_literal_edges(
            normalized,
            left_norm,
            right_norm,
            overlap=overlap,
            left_keep=left_keep,
            right_piece_len=right_piece_len,
        ):
            continue
        seen.add(normalized)
        scored.append(
            (
                _score_candidate(
                    normalized,
                    left_norm,
                    right_norm,
                    overlap,
                    left_keep=left_keep,
                    right_piece_len=right_piece_len,
                ),
                normalized,
            )
        )

    scored.sort(key=lambda item: (-item[0], item[1]))
    return [candidate for _score, candidate in scored[: max(1, limit)]]


def best_blend(left: str, right: str) -> str | None:
    candidates = blend_candidates(left, right, limit=1)
    return candidates[0] if candidates else None
