from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path


class RunStatus(StrEnum):
    CREATED = "created"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class ResultStatus(StrEnum):
    PASS = "pass"
    WARN = "warn"
    FAIL = "fail"
    UNAVAILABLE = "unavailable"
    UNSUPPORTED = "unsupported"
    SKIPPED = "skipped"


@dataclass(frozen=True)
class Brief:
    product_core: str = ""
    target_users: list[str] = field(default_factory=list)
    trust_signals: list[str] = field(default_factory=list)
    forbidden_directions: list[str] = field(default_factory=list)
    language_market: str = ""
    notes: str = ""


@dataclass(frozen=True)
class LexiconBundle:
    core_terms: tuple[str, ...] = ()
    modifiers: tuple[str, ...] = ()
    avoid_terms: tuple[str, ...] = ()
    associative_terms: tuple[str, ...] = ()
    morphemes: tuple[str, ...] = ()
    language_bias: str = "neutral"


@dataclass(frozen=True)
class SeedCandidate:
    name: str
    archetype: str
    ingredients: tuple[str, ...] = field(default_factory=tuple)
    source_score: float = 0.0
    taste_penalty: float = 0.0
    taste_reasons: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class TasteRuleHit:
    code: str
    details: dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class TasteDecision:
    accepted: bool
    penalty: float = 0.0
    reasons: tuple[str, ...] = field(default_factory=tuple)
    hits: tuple[TasteRuleHit, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class IdeationRoleConfig:
    model: str
    role: str = "creative_divergence"
    temperature: float = 0.8
    weight: int = 1


@dataclass(frozen=True)
class PseudowordConfig:
    language_plugin: str = "orthographic_english"
    language_plugins: tuple[str, ...] = ()
    seed_count: int = 18
    rare_seed_count: int = 0
    rare_profile: str = "off"


@dataclass(frozen=True)
class IdeationConfig:
    provider: str
    model: str = ""
    rounds: int = 1
    candidates_per_round: int = 12
    overgenerate_factor: float = 2.0
    round_seed_min: int = 3
    round_seed_max: int = 6
    seed_pool_multiplier: int = 8
    seed_saturation_limit: int = 1
    per_family_cap: int = 2
    lexicon_core_limit: int = 6
    lexicon_modifier_limit: int = 6
    lexicon_associative_limit: int = 6
    lexicon_morpheme_limit: int = 8
    local_filter_saturation_limit: int = 1
    local_filter_lead_fragment_limit: int = 0
    local_filter_lead_fragment_length: int = 4
    local_filter_lead_skeleton_limit: int = 0
    temperature: float = 0.8
    timeout_ms: int = 60000
    strict_json: bool = True
    prompt_template_file: Path | None = None
    fixture_input: Path | None = None
    openai_base_url: str = "http://127.0.0.1:1234/v1"
    api_key_env: str = "OPENROUTER_API_KEY"
    input_price_per_1k: float = 0.0
    output_price_per_1k: float = 0.0
    pseudoword: PseudowordConfig | None = None
    roles: tuple[IdeationRoleConfig, ...] = ()


@dataclass(frozen=True)
class ValidationConfig:
    checks: list[str] = field(default_factory=list)
    parallel_workers: int = 1
    scope: str = "global"
    required_domain_tlds: str = ""
    store_countries: str = "de,ch,it"
    timeout_s: float = 8.0
    company_top: int = 8
    social_unavailable_fail_threshold: int = 3
    web_search_order: str = "brave,google_cse,duckduckgo"
    web_brave_top: int = 8
    web_brave_api_env: str = "BRAVE_API_KEY"
    web_brave_country: str = "DE"
    web_brave_search_lang: str = "en"
    web_google_top: int = 8
    web_google_api_env: str = "GOOGLE_CSE_API_KEY"
    web_google_cx_env: str = "GOOGLE_CSE_CX"
    web_google_gl: str = "de"
    web_google_hl: str = "en"
    web_browser_profile_dir: str = ""
    web_browser_chrome_executable: str = ""
    web_retry_attempts: int = 2
    web_retry_backoff_s: float = 1.0
    tm_registry_top: int = 12
    tmview_profile_dir: str = ""
    tmview_chrome_executable: str = ""


@dataclass(frozen=True)
class ExportConfig:
    out_csv: Path | None = None
    top_n: int = 25


@dataclass(frozen=True)
class RunConfig:
    db_path: Path
    title: str
    brief: Brief
    ideation: IdeationConfig
    validation: ValidationConfig
    export: ExportConfig


@dataclass(frozen=True)
class CandidateResult:
    check_name: str
    status: ResultStatus
    score_delta: float
    reason: str
    details: dict[str, object]


@dataclass(frozen=True)
class RankedCandidate:
    name: str
    total_score: float
    blocker_count: int
    unavailable_count: int
    unsupported_count: int
    warning_count: int
    decision: str
