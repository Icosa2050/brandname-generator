from __future__ import annotations

from dataclasses import dataclass, field


DEFAULT_FAMILY_ORDER: tuple[str, ...] = (
    "literal_tld_hack",
    "smooth_blend",
    "mascot_mutation",
    "runic_forge",
    "contrarian_dictionary",
    "brutalist_utility",
)


@dataclass(frozen=True)
class NameShapePolicy:
    min_length: int = 6
    max_length: int = 14
    allow_digits: bool = False
    require_letter: bool = False
    max_consonant_run: int = 3
    repeated_char_run_length: int = 3
    reject_repeated_char_run: bool = True
    banned_triple_clusters: tuple[str, ...] = ("ptr", "rbl", "rth", "thv")
    safe_triple_clusters: tuple[str, ...] = ("sch", "scr", "shr", "spl", "spr", "squ", "str", "thr")
    harsh_letters: tuple[str, ...] = ("q", "x", "z", "j")
    disallow_terminal_o: bool = False


@dataclass(frozen=True)
class TastePolicy:
    banned_suffix_families: tuple[str, ...] = ("venix", "trix", "trex")
    banned_morphemes: tuple[str, ...] = (
        "parcl",
        "prec",
        "priva",
        "vex",
        "xen",
        "trix",
        "trex",
        "splint",
        "kest",
    )
    direct_domain_fragment_roots: tuple[str, ...] = (
        "arrears",
        "billing",
        "cashflow",
        "clar",
        "civic",
        "deposit",
        "invoice",
        "landlord",
        "ledger",
        "legal",
        "lease",
        "owner",
        "parcel",
        "payout",
        "portfolio",
        "private",
        "property",
        "reconcile",
        "report",
        "rent",
        "secur",
        "settlement",
        "tenant",
        "tenure",
        "trust",
        "utility",
    )
    generic_safe_openings: tuple[str, ...] = ("pre", "prec", "prim", "cora", "stati")
    exact_generic_words: tuple[str, ...] = ("render", "renders", "string", "strings")
    min_vowel_ratio: float = 0.28
    low_vowel_penalty: float = 0.25
    min_open_syllable_ratio: float = 0.34
    low_open_syllable_penalty: float = 0.2
    reject_penalty_threshold: float = 0.5
    reject_codes: tuple[str, ...] = (
        "banned_morpheme",
        "repeated_char_run",
        "cluster_overload",
        "direct_domain_fragment",
        "clipped_literal_fragment",
        "generic_safe_opening",
        "exact_generic_word",
    )


@dataclass(frozen=True)
class AttractivenessPolicy:
    pleasant_endings: tuple[str, ...] = ("a", "an", "ar", "el", "en", "er", "la", "ra", "ta")
    harsh_chars: tuple[str, ...] = ("x", "z", "q", "j")
    literal_signal_fragments: tuple[str, ...] = ("clar", "civic", "trust", "legal", "secur")
    generic_safe_openings: tuple[str, ...] = ("pre", "prec", "prim", "cora", "stati")
    liquids: str = "lrmn"
    sweet_spot_min_length: int = 7
    sweet_spot_max_length: int = 9
    acceptable_lengths: tuple[int, ...] = (6, 10)
    length_sweet_bonus: float = 6.0
    length_ok_bonus: float = 3.0
    length_penalty: float = 4.0
    vowel_balance_min: float = 0.35
    vowel_balance_max: float = 0.56
    vowel_balance_bonus: float = 5.0
    vowel_balance_soft_min: float = 0.30
    vowel_balance_soft_max: float = 0.60
    vowel_balance_soft_bonus: float = 2.0
    vowel_balance_penalty: float = 5.0
    open_syllable_strong_min: float = 0.55
    open_syllable_soft_min: float = 0.40
    open_syllable_bonus: float = 4.0
    open_syllable_soft_bonus: float = 1.5
    open_syllable_penalty: float = 3.0
    liquid_support_min: int = 1
    liquid_support_max: int = 3
    liquid_support_bonus: float = 3.0
    liquid_absent_penalty: float = 2.0
    harsh_penalty_per_char: float = 4.0
    leading_harsh_penalty: float = 3.0
    sharp_v_penalty: float = 1.0
    dense_consonant_run_penalty: float = 4.0
    pleasant_ending_bonus: float = 2.0
    lexical_seam_penalty: float = 4.0
    literal_signal_penalty: float = 5.0
    generic_opening_penalty: float = 5.0
    pass_threshold: float = 7.5
    dense_run_warn_below: float = 12.0
    vowel_balance_warn_below: float = 10.0
    lexical_seam_warn_below: float = 18.0
    harsh_letters_warn_below: float = 15.0


@dataclass(frozen=True)
class LocalCollisionPolicy:
    brand_suffixes: tuple[str, ...] = (
        "ability",
        "ation",
        "ingly",
        "ingly",
        "ify",
        "ness",
        "tion",
        "able",
        "core",
        "flow",
        "hub",
        "labs",
        "lab",
        "line",
        "logic",
        "loop",
        "nova",
        "pilot",
        "scope",
        "stack",
        "sync",
        "ware",
        "wise",
        "ly",
        "io",
        "iq",
        "sy",
        "er",
        "ai",
        "x",
    )
    terminal_bigram_quota: int = 2
    trigram_threshold: float = 0.62
    salvage_keep_count: int = 1


@dataclass(frozen=True)
class PromptScheme:
    phonetic: str
    morphology: str
    semantic: str
    label: str
    preferred_endings: str
    structure: str


@dataclass(frozen=True)
class PromptPolicy:
    round_schemes: tuple[PromptScheme, ...] = (
        PromptScheme(
            phonetic="smooth",
            morphology="blend",
            semantic="trust",
            label="rounded-open",
            preferred_endings="a, o, u, oo, el",
            structure="2-3 syllables, open endings, rounded vowels, liquid consonants",
        ),
        PromptScheme(
            phonetic="crisp",
            morphology="coined",
            semantic="precision",
            label="stop-spark",
            preferred_endings="e, o, um, et, ix",
            structure="firmer stops, contrasty rhythm, less default enterprise polish",
        ),
        PromptScheme(
            phonetic="bright",
            morphology="hybrid",
            semantic="clarity",
            label="bright-lilt",
            preferred_endings="i, o, ar, il, en",
            structure="clean stems, light vowels, sharper finish, wider ending range",
        ),
        PromptScheme(
            phonetic="grounded",
            morphology="coined",
            semantic="stability",
            label="odd-familiar",
            preferred_endings="o, a, er, um, en",
            structure="almost-familiar forms, asymmetry welcome, avoid obvious dictionary drift",
        ),
        PromptScheme(
            phonetic="balanced",
            morphology="blend",
            semantic="fairness",
            label="cross-current",
            preferred_endings="al, or, ou, ar, el",
            structure="mixed cadences, less latinate sameness, push opening diversity",
        ),
        PromptScheme(
            phonetic="resonant",
            morphology="hybrid",
            semantic="lift",
            label="wildcard-open",
            preferred_endings="a, o, u, is, on",
            structure="surprising phonetic turns, rounded or clipped exits, pronounceability retained",
        ),
    )
    role_hints: dict[str, str] = field(
        default_factory=lambda: {
            "creative_divergence": "Push away from the existing-company center of gravity; favor non-obvious sound-shapes and structural variety while staying pronounceable.",
            "recombinator": "Use the seed pool and morphemes as launch points, then splice and mutate them into less ordinary phonetic territory.",
            "contrarian": "Refuse the most literal naming path and search for angled, unexpected directions instead of safe B2B polish.",
            "phonetic_explorer": "Search for fresh openings, rhythm shifts, and less crowded sound-shapes without defaulting to near-real dictionary comfort.",
            "morpheme_hybridizer": "Fuse lexicon atoms into names that feel ownable and less namespace-crowded than direct near-real-word transmutations.",
            "ending_diversifier": "Actively explore endings and cadences that widen the batch instead of returning another safe enterprise family remix.",
        }
    )
    role_scheme_offsets: dict[str, int] = field(
        default_factory=lambda: {
            "creative_divergence": 5,
            "recombinator": 3,
            "contrarian": 4,
            "phonetic_explorer": 1,
            "morpheme_hybridizer": 2,
            "ending_diversifier": 0,
        }
    )
    ending_family_rules: tuple[tuple[str, str], ...] = (
        ("aria", "aria"),
        ("eria", "eria"),
        ("ia", "ia"),
        ("ea", "ea"),
        ("en", "en"),
        ("er", "er"),
        ("el", "el"),
        ("et", "et"),
        ("is", "is"),
        ("il", "il"),
        ("in", "in"),
        ("ix", "ix"),
        ("ex", "ex"),
        ("um", "um"),
        ("an", "an"),
        ("ar", "ar"),
        ("a", "a"),
        ("e", "e"),
        ("i", "i"),
        ("n", "n"),
        ("r", "r"),
        ("l", "l"),
        ("s", "s"),
        ("x", "x"),
    )


@dataclass(frozen=True)
class SurfaceGenerationPolicy:
    family_order: tuple[str, ...] = DEFAULT_FAMILY_ORDER
    fusion_family_order: tuple[str, ...] = DEFAULT_FAMILY_ORDER
    stopwords: tuple[str, ...] = (
        "and",
        "for",
        "the",
        "with",
        "from",
        "into",
        "your",
        "their",
        "software",
        "platform",
        "system",
        "utility",
        "manager",
        "managers",
        "cost",
    )
    tld_suffixes: tuple[str, ...] = (".io", ".app", ".hq", ".cloud", "-hq", "-app", "-io")
    dictionary_words: tuple[str, ...] = (
        "discord",
        "signal",
        "beacon",
        "vector",
        "anchor",
        "lattice",
        "rally",
        "harbor",
        "murmur",
        "parley",
        "temper",
        "fable",
        "orbit",
        "native",
        "current",
    )
    mascot_bases: tuple[str, ...] = ("llama", "otter", "panda", "koala", "manta", "orca", "gecko", "yak", "lynx", "ibis")
    brutalist_suffixes: tuple[str, ...] = ("TSX", "MP", "DX", "OS", "HQ", "RX")
    runic_fallbacks: tuple[str, ...] = (
        "VÆRMON",
        "KÆDRIN",
        "SØLKRIN",
        "TRÆNVOR",
        "FYRNDAL",
        "KYLMOR",
        "VYRKON",
        "SYLQAR",
        "ZYLFRAN",
        "QYLMAR",
    )
    mascot_hints: tuple[str, ...] = ("llama", "otter", "orca", "panda", "koala", "gecko", "lynx", "manta", "koi", "yak")
    contrarian_hints: tuple[str, ...] = ("signal", "vector", "anchor", "rally", "forge", "discord", "pulse", "beacon", "arc")
    tld_hints: tuple[str, ...] = (".io", ".app", ".hq", ".cloud", ".ai")
    runic_forge_good_endings: tuple[str, ...] = ("MON", "RIN", "KRIN", "VOR", "DAL", "MOR", "KON", "QAR", "FRAN")
    hardstop_endings: tuple[str, ...] = ("r", "n", "l", "m", "s")
    source_unit_endings: tuple[str, ...] = ("ability", "ibility", "ity", "ment", "tion", "ness", "ance", "ence", "ship", "ward")
    transmute_common_endings: tuple[str, ...] = (
        "ity",
        "tion",
        "ment",
        "ness",
        "able",
        "ance",
        "ence",
        "ward",
        "ship",
        "ing",
        "ers",
        "er",
        "or",
        "al",
        "el",
        "en",
        "ia",
        "us",
    )
    transmute_retarget_endings: tuple[str, ...] = (
        "a",
        "an",
        "ara",
        "ela",
        "el",
        "en",
        "era",
        "ia",
        "ien",
        "ine",
        "ora",
        "o",
        "u",
        "ou",
        "oo",
        "io",
        "is",
        "on",
        "or",
        "um",
    )
    roleish_terms: tuple[str, ...] = (
        "customer",
        "customers",
        "landlord",
        "landlords",
        "manager",
        "managers",
        "owner",
        "owners",
        "tenant",
        "tenants",
        "user",
        "users",
    )
    anti_corporate_tokens: tuple[str, ...] = (
        "solution",
        "solutions",
        "connect",
        "nexus",
        "core",
        "hub",
        "bridge",
        "sync",
        "flow",
        "cloud",
        "smart",
        "meta",
        "verse",
        "suite",
    )
    runic_crowded_patterns: tuple[str, ...] = ("naq", "qel", "qal", "qil")
    runic_bad_tails: tuple[str, ...] = ("rax", "gn")


@dataclass(frozen=True)
class NamingPolicy:
    shape: NameShapePolicy = field(default_factory=NameShapePolicy)
    taste: TastePolicy = field(default_factory=TastePolicy)
    attractiveness: AttractivenessPolicy = field(default_factory=AttractivenessPolicy)
    local_collision: LocalCollisionPolicy = field(default_factory=LocalCollisionPolicy)
    prompts: PromptPolicy = field(default_factory=PromptPolicy)
    surface: SurfaceGenerationPolicy = field(default_factory=SurfaceGenerationPolicy)


DEFAULT_NAMING_POLICY = NamingPolicy()
DEFAULT_VALIDATION_NAME_SHAPE_POLICY = NameShapePolicy(
    min_length=6,
    max_length=14,
    allow_digits=True,
    require_letter=True,
)


def _string_tuple(raw: object, default: tuple[str, ...]) -> tuple[str, ...]:
    if raw is None:
        return default
    if isinstance(raw, (list, tuple)):
        return tuple(str(item).strip() for item in raw if str(item).strip())
    if isinstance(raw, str):
        values = tuple(part.strip() for part in raw.split(",") if part.strip())
        return values or default
    return default


def _int_tuple(raw: object, default: tuple[int, ...]) -> tuple[int, ...]:
    if raw is None:
        return default
    if isinstance(raw, (list, tuple)):
        return tuple(int(item) for item in raw)
    if isinstance(raw, str):
        values = tuple(int(part.strip()) for part in raw.split(",") if part.strip())
        return values or default
    return default


def _string_map(raw: object, default: dict[str, str]) -> dict[str, str]:
    if not isinstance(raw, dict):
        return dict(default)
    return {str(key).strip(): str(value).strip() for key, value in raw.items() if str(key).strip() and str(value).strip()}


def _int_map(raw: object, default: dict[str, int]) -> dict[str, int]:
    if not isinstance(raw, dict):
        return dict(default)
    return {str(key).strip(): int(value) for key, value in raw.items() if str(key).strip()}


def _shape_policy(raw: object, default: NameShapePolicy) -> NameShapePolicy:
    payload = raw if isinstance(raw, dict) else {}
    return NameShapePolicy(
        min_length=max(1, int(payload.get("min_length", default.min_length))),
        max_length=max(1, int(payload.get("max_length", default.max_length))),
        allow_digits=bool(payload.get("allow_digits", default.allow_digits)),
        require_letter=bool(payload.get("require_letter", default.require_letter)),
        max_consonant_run=max(1, int(payload.get("max_consonant_run", default.max_consonant_run))),
        repeated_char_run_length=max(2, int(payload.get("repeated_char_run_length", default.repeated_char_run_length))),
        reject_repeated_char_run=bool(payload.get("reject_repeated_char_run", default.reject_repeated_char_run)),
        banned_triple_clusters=_string_tuple(payload.get("banned_triple_clusters"), default.banned_triple_clusters),
        safe_triple_clusters=_string_tuple(payload.get("safe_triple_clusters"), default.safe_triple_clusters),
        harsh_letters=_string_tuple(payload.get("harsh_letters"), default.harsh_letters),
        disallow_terminal_o=bool(payload.get("disallow_terminal_o", default.disallow_terminal_o)),
    )


def _taste_policy(raw: object, default: TastePolicy) -> TastePolicy:
    payload = raw if isinstance(raw, dict) else {}
    return TastePolicy(
        banned_suffix_families=_string_tuple(payload.get("banned_suffix_families"), default.banned_suffix_families),
        banned_morphemes=_string_tuple(payload.get("banned_morphemes"), default.banned_morphemes),
        direct_domain_fragment_roots=_string_tuple(payload.get("direct_domain_fragment_roots"), default.direct_domain_fragment_roots),
        generic_safe_openings=_string_tuple(payload.get("generic_safe_openings"), default.generic_safe_openings),
        exact_generic_words=_string_tuple(payload.get("exact_generic_words"), default.exact_generic_words),
        min_vowel_ratio=float(payload.get("min_vowel_ratio", default.min_vowel_ratio)),
        low_vowel_penalty=float(payload.get("low_vowel_penalty", default.low_vowel_penalty)),
        min_open_syllable_ratio=float(payload.get("min_open_syllable_ratio", default.min_open_syllable_ratio)),
        low_open_syllable_penalty=float(payload.get("low_open_syllable_penalty", default.low_open_syllable_penalty)),
        reject_penalty_threshold=float(payload.get("reject_penalty_threshold", default.reject_penalty_threshold)),
        reject_codes=_string_tuple(payload.get("reject_codes"), default.reject_codes),
    )


def _attractiveness_policy(raw: object, default: AttractivenessPolicy) -> AttractivenessPolicy:
    payload = raw if isinstance(raw, dict) else {}
    return AttractivenessPolicy(
        pleasant_endings=_string_tuple(payload.get("pleasant_endings"), default.pleasant_endings),
        harsh_chars=_string_tuple(payload.get("harsh_chars"), default.harsh_chars),
        literal_signal_fragments=_string_tuple(payload.get("literal_signal_fragments"), default.literal_signal_fragments),
        generic_safe_openings=_string_tuple(payload.get("generic_safe_openings"), default.generic_safe_openings),
        liquids=str(payload.get("liquids", default.liquids)),
        sweet_spot_min_length=max(1, int(payload.get("sweet_spot_min_length", default.sweet_spot_min_length))),
        sweet_spot_max_length=max(1, int(payload.get("sweet_spot_max_length", default.sweet_spot_max_length))),
        acceptable_lengths=_int_tuple(payload.get("acceptable_lengths"), default.acceptable_lengths),
        length_sweet_bonus=float(payload.get("length_sweet_bonus", default.length_sweet_bonus)),
        length_ok_bonus=float(payload.get("length_ok_bonus", default.length_ok_bonus)),
        length_penalty=float(payload.get("length_penalty", default.length_penalty)),
        vowel_balance_min=float(payload.get("vowel_balance_min", default.vowel_balance_min)),
        vowel_balance_max=float(payload.get("vowel_balance_max", default.vowel_balance_max)),
        vowel_balance_bonus=float(payload.get("vowel_balance_bonus", default.vowel_balance_bonus)),
        vowel_balance_soft_min=float(payload.get("vowel_balance_soft_min", default.vowel_balance_soft_min)),
        vowel_balance_soft_max=float(payload.get("vowel_balance_soft_max", default.vowel_balance_soft_max)),
        vowel_balance_soft_bonus=float(payload.get("vowel_balance_soft_bonus", default.vowel_balance_soft_bonus)),
        vowel_balance_penalty=float(payload.get("vowel_balance_penalty", default.vowel_balance_penalty)),
        open_syllable_strong_min=float(payload.get("open_syllable_strong_min", default.open_syllable_strong_min)),
        open_syllable_soft_min=float(payload.get("open_syllable_soft_min", default.open_syllable_soft_min)),
        open_syllable_bonus=float(payload.get("open_syllable_bonus", default.open_syllable_bonus)),
        open_syllable_soft_bonus=float(payload.get("open_syllable_soft_bonus", default.open_syllable_soft_bonus)),
        open_syllable_penalty=float(payload.get("open_syllable_penalty", default.open_syllable_penalty)),
        liquid_support_min=int(payload.get("liquid_support_min", default.liquid_support_min)),
        liquid_support_max=int(payload.get("liquid_support_max", default.liquid_support_max)),
        liquid_support_bonus=float(payload.get("liquid_support_bonus", default.liquid_support_bonus)),
        liquid_absent_penalty=float(payload.get("liquid_absent_penalty", default.liquid_absent_penalty)),
        harsh_penalty_per_char=float(payload.get("harsh_penalty_per_char", default.harsh_penalty_per_char)),
        leading_harsh_penalty=float(payload.get("leading_harsh_penalty", default.leading_harsh_penalty)),
        sharp_v_penalty=float(payload.get("sharp_v_penalty", default.sharp_v_penalty)),
        dense_consonant_run_penalty=float(payload.get("dense_consonant_run_penalty", default.dense_consonant_run_penalty)),
        pleasant_ending_bonus=float(payload.get("pleasant_ending_bonus", default.pleasant_ending_bonus)),
        lexical_seam_penalty=float(payload.get("lexical_seam_penalty", default.lexical_seam_penalty)),
        literal_signal_penalty=float(payload.get("literal_signal_penalty", default.literal_signal_penalty)),
        generic_opening_penalty=float(payload.get("generic_opening_penalty", default.generic_opening_penalty)),
        pass_threshold=float(payload.get("pass_threshold", default.pass_threshold)),
        dense_run_warn_below=float(payload.get("dense_run_warn_below", default.dense_run_warn_below)),
        vowel_balance_warn_below=float(payload.get("vowel_balance_warn_below", default.vowel_balance_warn_below)),
        lexical_seam_warn_below=float(payload.get("lexical_seam_warn_below", default.lexical_seam_warn_below)),
        harsh_letters_warn_below=float(payload.get("harsh_letters_warn_below", default.harsh_letters_warn_below)),
    )


def _local_collision_policy(raw: object, default: LocalCollisionPolicy) -> LocalCollisionPolicy:
    payload = raw if isinstance(raw, dict) else {}
    return LocalCollisionPolicy(
        brand_suffixes=_string_tuple(payload.get("brand_suffixes"), default.brand_suffixes),
        terminal_bigram_quota=max(1, int(payload.get("terminal_bigram_quota", default.terminal_bigram_quota))),
        trigram_threshold=float(payload.get("trigram_threshold", default.trigram_threshold)),
        salvage_keep_count=max(0, int(payload.get("salvage_keep_count", default.salvage_keep_count))),
    )


def _prompt_schemes(raw: object, default: tuple[PromptScheme, ...]) -> tuple[PromptScheme, ...]:
    if not isinstance(raw, list):
        return default
    parsed: list[PromptScheme] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        parsed.append(
            PromptScheme(
                phonetic=str(item.get("phonetic", "")).strip(),
                morphology=str(item.get("morphology", "")).strip(),
                semantic=str(item.get("semantic", "")).strip(),
                label=str(item.get("label", "")).strip(),
                preferred_endings=str(item.get("preferred_endings", "")).strip(),
                structure=str(item.get("structure", "")).strip(),
            )
        )
    return tuple(parsed)


def _prompt_policy(raw: object, default: PromptPolicy) -> PromptPolicy:
    payload = raw if isinstance(raw, dict) else {}
    return PromptPolicy(
        round_schemes=_prompt_schemes(payload.get("round_schemes"), default.round_schemes),
        role_hints=_string_map(payload.get("role_hints"), default.role_hints) if "role_hints" in payload else dict(default.role_hints),
        role_scheme_offsets=_int_map(payload.get("role_scheme_offsets"), default.role_scheme_offsets)
        if "role_scheme_offsets" in payload
        else dict(default.role_scheme_offsets),
        ending_family_rules=tuple(
            (str(left).strip(), str(right).strip())
            for left, right in (
                payload.get("ending_family_rules", default.ending_family_rules)
                if isinstance(payload.get("ending_family_rules"), list)
                else default.ending_family_rules
            )
            if str(left).strip() and str(right).strip()
        )
        if "ending_family_rules" in payload
        else default.ending_family_rules,
    )


def _surface_policy(raw: object, default: SurfaceGenerationPolicy) -> SurfaceGenerationPolicy:
    payload = raw if isinstance(raw, dict) else {}
    return SurfaceGenerationPolicy(
        family_order=_string_tuple(payload.get("family_order"), default.family_order),
        fusion_family_order=_string_tuple(payload.get("fusion_family_order"), default.fusion_family_order),
        stopwords=_string_tuple(payload.get("stopwords"), default.stopwords),
        tld_suffixes=_string_tuple(payload.get("tld_suffixes"), default.tld_suffixes),
        dictionary_words=_string_tuple(payload.get("dictionary_words"), default.dictionary_words),
        mascot_bases=_string_tuple(payload.get("mascot_bases"), default.mascot_bases),
        brutalist_suffixes=_string_tuple(payload.get("brutalist_suffixes"), default.brutalist_suffixes),
        runic_fallbacks=_string_tuple(payload.get("runic_fallbacks"), default.runic_fallbacks),
        mascot_hints=_string_tuple(payload.get("mascot_hints"), default.mascot_hints),
        contrarian_hints=_string_tuple(payload.get("contrarian_hints"), default.contrarian_hints),
        tld_hints=_string_tuple(payload.get("tld_hints"), default.tld_hints),
        runic_forge_good_endings=_string_tuple(payload.get("runic_forge_good_endings"), default.runic_forge_good_endings),
        hardstop_endings=_string_tuple(payload.get("hardstop_endings"), default.hardstop_endings),
        source_unit_endings=_string_tuple(payload.get("source_unit_endings"), default.source_unit_endings),
        transmute_common_endings=_string_tuple(payload.get("transmute_common_endings"), default.transmute_common_endings),
        transmute_retarget_endings=_string_tuple(payload.get("transmute_retarget_endings"), default.transmute_retarget_endings),
        roleish_terms=_string_tuple(payload.get("roleish_terms"), default.roleish_terms),
        anti_corporate_tokens=_string_tuple(payload.get("anti_corporate_tokens"), default.anti_corporate_tokens),
        runic_crowded_patterns=_string_tuple(payload.get("runic_crowded_patterns"), default.runic_crowded_patterns),
        runic_bad_tails=_string_tuple(payload.get("runic_bad_tails"), default.runic_bad_tails),
    )


def build_naming_policy(raw: object, *, default: NamingPolicy | None = None) -> NamingPolicy:
    base = default or DEFAULT_NAMING_POLICY
    payload = raw if isinstance(raw, dict) else {}
    return NamingPolicy(
        shape=_shape_policy(payload.get("shape"), base.shape),
        taste=_taste_policy(payload.get("taste"), base.taste),
        attractiveness=_attractiveness_policy(payload.get("attractiveness"), base.attractiveness),
        local_collision=_local_collision_policy(payload.get("local_collision"), base.local_collision),
        prompts=_prompt_policy(payload.get("prompts"), base.prompts),
        surface=_surface_policy(payload.get("surface"), base.surface),
    )


def build_validation_name_shape_policy(raw: object, *, default: NameShapePolicy | None = None) -> NameShapePolicy:
    return _shape_policy(raw, default or DEFAULT_VALIDATION_NAME_SHAPE_POLICY)
