from __future__ import annotations

from collections import defaultdict

from .models import CandidateResult, RankedCandidate, ResultStatus


def rank_candidates(results_by_name: dict[str, list[CandidateResult]]) -> list[RankedCandidate]:
    ranked: list[RankedCandidate] = []
    for name, results in results_by_name.items():
        total_score = 100.0
        blocker_count = 0
        unavailable_count = 0
        unsupported_count = 0
        warning_count = 0
        for result in results:
            total_score += float(result.score_delta)
            if result.status == ResultStatus.FAIL:
                blocker_count += 1
            elif result.status == ResultStatus.UNAVAILABLE:
                unavailable_count += 1
            elif result.status == ResultStatus.UNSUPPORTED:
                unsupported_count += 1
            elif result.status == ResultStatus.WARN:
                warning_count += 1
        if blocker_count > 0:
            decision = "blocked"
        elif unavailable_count > 0:
            decision = "degraded"
        elif unsupported_count > 0:
            decision = "partial"
        elif warning_count > 0:
            decision = "watch"
        else:
            decision = "candidate"
        ranked.append(
            RankedCandidate(
                name=name,
                total_score=round(total_score, 2),
                blocker_count=blocker_count,
                unavailable_count=unavailable_count,
                unsupported_count=unsupported_count,
                warning_count=warning_count,
                decision=decision,
            )
        )
    ranked.sort(
        key=lambda item: (
            item.blocker_count,
            item.unavailable_count,
            item.unsupported_count,
            item.warning_count,
            -item.total_score,
            item.name,
        )
    )
    return ranked


def group_results(rows: list[tuple[str, CandidateResult]]) -> dict[str, list[CandidateResult]]:
    grouped: dict[str, list[CandidateResult]] = defaultdict(list)
    for name, result in rows:
        grouped[name].append(result)
    return grouped
