from __future__ import annotations

import argparse
import json
import re
import unicodedata
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

try:
    from rapidfuzz import fuzz

    FUZZY_BACKEND = "rapidfuzz"

    def similarity_score(left: str, right: str) -> float:
        """Return a 0-100 fuzzy similarity score using rapidfuzz."""
        return float(fuzz.ratio(left, right))

except Exception:
    from difflib import SequenceMatcher

    FUZZY_BACKEND = "difflib"

    def similarity_score(left: str, right: str) -> float:
        """Return a 0-100 fuzzy similarity score using the standard library."""
        return SequenceMatcher(None, left, right).ratio() * 100


NOTICE_COLUMNS = [
    "notice_id",
    "publication_date",
    "notice_type",
    "country_codes",
    "buyer_name",
    "winner_name",
    "total_award_amount",
    "award_currency",
]
ORGANIZATION_COLUMNS = ["notice_id", "org_id", "name", "role", "country", "town"]
CPV_COLUMNS = ["notice_id", "cpv_code", "cpv_division"]
LOT_COLUMNS = ["notice_id", "lot_id", "title", "lot_amount", "lot_currency"]

NOTICES_CLEAN_COLUMNS = NOTICE_COLUMNS + [
    "publication_year",
    "publication_month",
    "has_buyer",
    "has_cpv",
    "is_awarded",
]
ORGANIZATIONS_CLEAN_COLUMNS = ORGANIZATION_COLUMNS + ["org_name_clean"]
BUYER_MASTER_COLUMNS = [
    "buyer_group_id",
    "buyer_group_name",
    "buyer_name_clean",
    "buyer_country",
    "buyer_town",
    "raw_name_examples",
    "notice_count",
    "first_seen_date",
    "last_seen_date",
    "total_award_amount_eur",
    "entity_resolution_method",
]
BUYER_NOTICE_MAP_COLUMNS = [
    "notice_id",
    "buyer_group_id",
    "buyer_name_raw",
    "buyer_group_name",
    "buyer_country",
    "buyer_town",
]
SEMANTIC_COLUMNS = [
    "notice_id",
    "publication_date",
    "buyer_group_id",
    "buyer_group_name",
    "buyer_country",
    "cpv_codes",
    "cpv_divisions",
    "microsoft_category",
    "microsoft_relevance_score",
    "is_microsoft_relevant",
    "relevance_reason",
]

MISSING_STRINGS = {
    "",
    "unknown",
    "unk",
    "n/a",
    "na",
    "nan",
    "none",
    "null",
    "not available",
    "missing",
    "-",
    "--",
}
LEGAL_SUFFIXES = {
    "ltd",
    "limited",
    "gmbh",
    "sarl",
    "sa",
    "spa",
    "sl",
    "plc",
    "inc",
    "corporation",
    "corp",
    "company",
    "co",
}

MICROSOFT_CATEGORIES = [
    "Cloud & Infrastructure",
    "Cybersecurity",
    "Software Licenses",
    "Data & AI",
    "IT Consulting & Services",
    "Hardware & Devices",
    "Telecom & Networking",
    "Non-Relevant / Other",
]

KEYWORD_RULES: dict[str, list[str]] = {
    "Cloud & Infrastructure": [
        "cloud",
        "hosting",
        "datacenter",
        "data center",
        "infrastructure as a service",
        "paas",
        "iaas",
        "azure",
    ],
    "Cybersecurity": [
        "cyber",
        "cybersecurity",
        "security software",
        "identity",
        "threat",
        "firewall",
        "soc",
        "siem",
        "endpoint protection",
    ],
    "Software Licenses": [
        "software",
        "licenses",
        "licence",
        "microsoft 365",
        "office",
        "enterprise software",
        "erp",
        "crm",
    ],
    "Data & AI": [
        "data platform",
        "analytics",
        "artificial intelligence",
        "machine learning",
        "ai",
        "business intelligence",
        "big data",
    ],
    "IT Consulting & Services": [
        "it services",
        "digital transformation",
        "implementation",
        "systems integration",
        "managed services",
        "consulting",
    ],
    "Hardware & Devices": [
        "laptops",
        "computers",
        "servers",
        "tablets",
        "workstations",
        "devices",
    ],
    "Telecom & Networking": [
        "network",
        "telecommunications",
        "connectivity",
        "broadband",
        "routers",
        "switches",
    ],
}

CPV_RULES: dict[str, list[tuple[str, int, str]]] = {
    "48": [("Software Licenses", 60, "CPV 48 software evidence")],
    "72": [("IT Consulting & Services", 60, "CPV 72 IT services evidence")],
    "32": [
        ("Telecom & Networking", 45, "CPV 32 telecom/networking evidence"),
        ("Hardware & Devices", 20, "CPV 32 hardware/device evidence"),
    ],
    "30": [("Hardware & Devices", 45, "CPV 30 hardware/device evidence")],
}


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments for the S2 Silver Layer workflow."""
    parser = argparse.ArgumentParser(
        description="Build S2 Silver cleaning, buyer resolution, semantic tags, and data-quality reports."
    )
    parser.add_argument("--input-dir", type=Path, default=Path("data/silver"))
    parser.add_argument("--output-dir", type=Path, default=Path("data/silver"))
    parser.add_argument("--reports-dir", type=Path, default=Path("data/reports"))
    parser.add_argument("--fuzzy-threshold", type=int, default=90)
    return parser.parse_args()


def is_missing_value(value: Any) -> bool:
    """Return True when a scalar value should be treated as missing."""
    if value is None:
        return True
    try:
        if pd.isna(value):
            return True
    except TypeError:
        pass
    if isinstance(value, str):
        return value.strip().lower() in MISSING_STRINGS
    return False


def present_mask(series: pd.Series) -> pd.Series:
    """Return a boolean mask for values that are not obvious missing values."""
    return ~series.map(is_missing_value)


def ensure_columns(df: pd.DataFrame, columns: Iterable[str]) -> pd.DataFrame:
    """Add expected columns that are missing from a DataFrame."""
    for column in columns:
        if column not in df.columns:
            df[column] = pd.NA
    return df


def load_csv(path: Path, columns: list[str]) -> pd.DataFrame:
    """Load a CSV as strings, returning an empty schema-compatible frame if missing."""
    if not path.exists():
        print(f"Input missing, continuing with empty data: {path}")
        return pd.DataFrame(columns=columns)
    try:
        df = pd.read_csv(path, dtype=str, keep_default_na=False)
    except pd.errors.EmptyDataError:
        print(f"Input empty, continuing with empty data: {path}")
        return pd.DataFrame(columns=columns)
    return ensure_columns(df, columns)


def clean_string_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Strip whitespace and convert obvious missing string values to nulls."""
    cleaned = df.copy()
    for column in cleaned.columns:
        if cleaned[column].dtype == object or pd.api.types.is_string_dtype(cleaned[column]):
            cleaned[column] = cleaned[column].map(
                lambda value: value.strip() if isinstance(value, str) else value
            )
            cleaned[column] = cleaned[column].mask(cleaned[column].map(is_missing_value), pd.NA)
    return cleaned


def normalize_org_name(name: str) -> str:
    """Normalize organization names for deterministic buyer entity resolution."""
    if is_missing_value(name):
        return ""
    text = str(name).lower().strip()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"\bs\.?\s*a\.?\b", " sa ", text)
    text = re.sub(r"\bs\.?\s*l\.?\b", " sl ", text)
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    tokens = [token for token in text.split() if token not in LEGAL_SUFFIXES]
    return " ".join(tokens)


def normalize_town(value: Any) -> Any:
    """Normalize town capitalization while preserving missing values."""
    if is_missing_value(value):
        return pd.NA
    return re.sub(r"\s+", " ", str(value).strip()).title()


def unique_join(values: Iterable[Any], sep: str = " | ", limit: int | None = None) -> str:
    """Join unique, non-missing values in deterministic order."""
    cleaned = sorted({str(value).strip() for value in values if not is_missing_value(value)})
    if limit is not None:
        cleaned = cleaned[:limit]
    return sep.join(cleaned)


def most_common_value(values: Iterable[Any]) -> Any:
    """Return the most common non-missing value, using lexical order for ties."""
    items = [str(value).strip() for value in values if not is_missing_value(value)]
    if not items:
        return pd.NA
    counts = Counter(items)
    return sorted(counts.items(), key=lambda item: (-item[1], item[0]))[0][0]


def clean_notices(notices: pd.DataFrame, cpv_codes: pd.DataFrame) -> pd.DataFrame:
    """Clean notices and add S2 convenience flags."""
    df = ensure_columns(clean_string_columns(notices), NOTICE_COLUMNS)

    parsed_dates = pd.to_datetime(df["publication_date"], errors="coerce")
    df["publication_date"] = parsed_dates.dt.strftime("%Y-%m-%d")
    df["publication_date"] = df["publication_date"].mask(parsed_dates.isna(), pd.NA)
    df["publication_year"] = parsed_dates.dt.year.astype("Int64")
    df["publication_month"] = parsed_dates.dt.month.astype("Int64")

    df["total_award_amount"] = pd.to_numeric(df["total_award_amount"], errors="coerce")
    df["award_currency"] = df["award_currency"].map(
        lambda value: str(value).strip().upper() if not is_missing_value(value) else pd.NA
    )

    cpv_notice_ids = set()
    if not cpv_codes.empty and "notice_id" in cpv_codes.columns:
        cpv_notice_ids = set(cpv_codes.loc[present_mask(cpv_codes["notice_id"]), "notice_id"])

    df["has_buyer"] = present_mask(df["buyer_name"])
    df["has_cpv"] = df["notice_id"].isin(cpv_notice_ids)
    df["is_awarded"] = present_mask(df["winner_name"])
    return df[NOTICES_CLEAN_COLUMNS]


def clean_organizations(organizations: pd.DataFrame) -> pd.DataFrame:
    """Clean organizations and add normalized organization names."""
    df = ensure_columns(clean_string_columns(organizations), ORGANIZATION_COLUMNS)
    df["role"] = df["role"].map(
        lambda value: str(value).strip().upper() if not is_missing_value(value) else pd.NA
    )
    df["country"] = df["country"].map(
        lambda value: str(value).strip().upper() if not is_missing_value(value) else pd.NA
    )
    df["town"] = df["town"].map(normalize_town)
    df["org_name_clean"] = df["name"].map(normalize_org_name)
    df["org_name_clean"] = df["org_name_clean"].mask(df["org_name_clean"] == "", pd.NA)
    return df[ORGANIZATIONS_CLEAN_COLUMNS]


class UnionFind:
    """Small deterministic union-find for grouping fuzzy-matched names."""

    def __init__(self, values: Iterable[str]) -> None:
        self.parent = {value: value for value in values}

    def find(self, value: str) -> str:
        """Return the representative for a value."""
        parent = self.parent[value]
        if parent != value:
            self.parent[value] = self.find(parent)
        return self.parent[value]

    def union(self, left: str, right: str) -> None:
        """Union two values, keeping the lexical representative for determinism."""
        root_left = self.find(left)
        root_right = self.find(right)
        if root_left == root_right:
            return
        keep, move = sorted([root_left, root_right])
        self.parent[move] = keep


def fuzzy_blocks(names: Iterable[str]) -> dict[tuple[str, int], list[str]]:
    """Create simple candidate blocks for high-threshold fuzzy matching."""
    blocks: dict[tuple[str, int], list[str]] = defaultdict(list)
    for name in names:
        tokens = name.split()
        first_token = tokens[0] if tokens else ""
        length_bucket = len(name) // 10
        blocks[(first_token[:8], length_bucket)].append(name)
    return blocks


def build_name_components(
    buyers: pd.DataFrame,
    fuzzy_threshold: int,
) -> list[dict[str, Any]]:
    """Resolve normalized buyer names by country with exact and fuzzy matching."""
    components: list[dict[str, Any]] = []
    unique_names = (
        buyers[["org_name_clean", "buyer_country_key"]]
        .dropna(subset=["org_name_clean"])
        .drop_duplicates()
        .sort_values(["buyer_country_key", "org_name_clean"])
    )

    for country, country_names_df in unique_names.groupby("buyer_country_key", dropna=False):
        names = sorted(country_names_df["org_name_clean"].dropna().astype(str).unique())
        if not names:
            continue
        union_find = UnionFind(names)
        for block_names in fuzzy_blocks(names).values():
            block_names = sorted(block_names)
            for i, left in enumerate(block_names):
                for right in block_names[i + 1 :]:
                    max_len = max(len(left), len(right))
                    if abs(len(left) - len(right)) > max(5, int(max_len * 0.20)):
                        continue
                    if similarity_score(left, right) >= fuzzy_threshold:
                        union_find.union(left, right)

        grouped_names: dict[str, list[str]] = defaultdict(list)
        for name in names:
            grouped_names[union_find.find(name)].append(name)
        for names_in_group in grouped_names.values():
            group_rows = buyers[
                (buyers["buyer_country_key"] == country)
                & (buyers["org_name_clean"].isin(names_in_group))
            ]
            canonical_clean = most_common_value(group_rows["org_name_clean"])
            components.append(
                {
                    "country": country,
                    "names": sorted(names_in_group),
                    "buyer_name_clean": canonical_clean,
                    "method": (
                        "within-country fuzzy match"
                        if len(set(names_in_group)) > 1
                        else "exact normalized name + country"
                    ),
                }
            )
    return sorted(
        components,
        key=lambda row: (
            str(row["country"] or ""),
            str(row["buyer_name_clean"] or ""),
            "|".join(row["names"]),
        ),
    )


def build_buyer_outputs(
    organizations_clean: pd.DataFrame,
    notices_clean: pd.DataFrame,
    fuzzy_threshold: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Build buyer master and buyer-notice mapping tables."""
    empty_master = pd.DataFrame(columns=BUYER_MASTER_COLUMNS)
    empty_map = pd.DataFrame(columns=BUYER_NOTICE_MAP_COLUMNS)
    if organizations_clean.empty:
        return empty_master, empty_map

    buyers = organizations_clean[organizations_clean["role"] == "BUYER"].copy()
    buyers = buyers.dropna(subset=["org_name_clean"])
    if buyers.empty:
        return empty_master, empty_map

    buyers["buyer_country_key"] = buyers["country"].fillna("").astype(str)
    components = build_name_components(buyers, fuzzy_threshold)
    assignment: dict[tuple[str, str], str] = {}
    component_by_id: dict[str, dict[str, Any]] = {}
    for index, component in enumerate(components, start=1):
        group_id = f"BUYER_{index:06d}"
        component["buyer_group_id"] = group_id
        component_by_id[group_id] = component
        for name in component["names"]:
            assignment[(component["country"], name)] = group_id

    buyers["buyer_group_id"] = buyers.apply(
        lambda row: assignment.get((row["buyer_country_key"], row["org_name_clean"])),
        axis=1,
    )
    buyers = buyers.dropna(subset=["buyer_group_id"])

    notices_for_join = notices_clean[
        ["notice_id", "publication_date", "total_award_amount", "award_currency"]
    ].copy()
    buyer_notice_amounts = (
        buyers[["buyer_group_id", "notice_id"]]
        .drop_duplicates()
        .merge(notices_for_join, on="notice_id", how="left")
    )

    master_rows: list[dict[str, Any]] = []
    for group_id in sorted(component_by_id):
        component = component_by_id[group_id]
        group_buyers = buyers[buyers["buyer_group_id"] == group_id]
        group_notices = buyer_notice_amounts[buyer_notice_amounts["buyer_group_id"] == group_id]
        date_values = pd.to_datetime(group_notices["publication_date"], errors="coerce")
        eur_amounts = group_notices.loc[
            group_notices["award_currency"].fillna("").str.upper() == "EUR",
            "total_award_amount",
        ]
        master_rows.append(
            {
                "buyer_group_id": group_id,
                "buyer_group_name": most_common_value(group_buyers["name"]),
                "buyer_name_clean": component["buyer_name_clean"],
                "buyer_country": most_common_value(group_buyers["country"]),
                "buyer_town": most_common_value(group_buyers["town"]),
                "raw_name_examples": unique_join(group_buyers["name"], limit=5),
                "notice_count": int(group_notices["notice_id"].nunique()),
                "first_seen_date": (
                    date_values.min().strftime("%Y-%m-%d") if date_values.notna().any() else pd.NA
                ),
                "last_seen_date": (
                    date_values.max().strftime("%Y-%m-%d") if date_values.notna().any() else pd.NA
                ),
                "total_award_amount_eur": float(pd.to_numeric(eur_amounts, errors="coerce").sum()),
                "entity_resolution_method": component["method"],
            }
        )

    buyer_master = pd.DataFrame(master_rows, columns=BUYER_MASTER_COLUMNS)
    map_rows = buyers[
        ["notice_id", "buyer_group_id", "name", "country", "town"]
    ].rename(columns={"name": "buyer_name_raw"})
    map_rows = map_rows.merge(
        buyer_master[["buyer_group_id", "buyer_group_name", "buyer_country", "buyer_town"]],
        on="buyer_group_id",
        how="left",
    )
    map_rows["buyer_country"] = map_rows["buyer_country"].combine_first(map_rows["country"])
    map_rows["buyer_town"] = map_rows["buyer_town"].combine_first(map_rows["town"])
    buyer_notice_map = (
        map_rows[BUYER_NOTICE_MAP_COLUMNS]
        .sort_values(["notice_id", "buyer_group_id", "buyer_name_raw"], na_position="last")
        .drop_duplicates(["notice_id", "buyer_group_id"])
        .reset_index(drop=True)
    )
    return buyer_master, buyer_notice_map


def split_joined(value: Any) -> list[str]:
    """Split pipe-delimited strings and remove missing values."""
    if is_missing_value(value):
        return []
    return [part.strip() for part in str(value).split("|") if part.strip()]


def aggregate_cpv(cpv_codes: pd.DataFrame) -> pd.DataFrame:
    """Aggregate CPV codes and divisions to one row per notice."""
    if cpv_codes.empty:
        return pd.DataFrame(columns=["notice_id", "cpv_codes", "cpv_divisions"])
    cpv = ensure_columns(clean_string_columns(cpv_codes), CPV_COLUMNS)
    cpv["cpv_code"] = cpv["cpv_code"].map(
        lambda value: str(value).strip() if not is_missing_value(value) else pd.NA
    )
    cpv["cpv_division"] = cpv.apply(
        lambda row: (
            str(row["cpv_division"]).strip()
            if not is_missing_value(row["cpv_division"])
            else str(row["cpv_code"])[:2]
            if not is_missing_value(row["cpv_code"])
            else pd.NA
        ),
        axis=1,
    )
    cpv = cpv.dropna(subset=["notice_id"])
    if cpv.empty:
        return pd.DataFrame(columns=["notice_id", "cpv_codes", "cpv_divisions"])
    return (
        cpv.groupby("notice_id", dropna=False)
        .agg(
            cpv_codes=("cpv_code", lambda values: unique_join(values, sep="|")),
            cpv_divisions=("cpv_division", lambda values: unique_join(values, sep="|")),
        )
        .reset_index()
    )


def aggregate_buyers(buyer_notice_map: pd.DataFrame) -> pd.DataFrame:
    """Aggregate buyer mappings to one row per notice."""
    if buyer_notice_map.empty:
        return pd.DataFrame(columns=["notice_id", "buyer_group_id", "buyer_group_name", "buyer_country"])
    return (
        buyer_notice_map.groupby("notice_id", dropna=False)
        .agg(
            buyer_group_id=("buyer_group_id", lambda values: unique_join(values)),
            buyer_group_name=("buyer_group_name", lambda values: unique_join(values)),
            buyer_country=("buyer_country", lambda values: unique_join(values)),
        )
        .reset_index()
    )


def aggregate_lot_text(lots: pd.DataFrame) -> pd.DataFrame:
    """Aggregate optional lot titles to support semantic tagging."""
    if lots.empty:
        return pd.DataFrame(columns=["notice_id", "lot_text"])
    clean_lots = ensure_columns(clean_string_columns(lots), LOT_COLUMNS)
    title_columns = [column for column in clean_lots.columns if "title" in column.lower()]
    if not title_columns:
        return pd.DataFrame(columns=["notice_id", "lot_text"])
    clean_lots["lot_text"] = clean_lots[title_columns].apply(
        lambda row: " ".join(str(value) for value in row if not is_missing_value(value)),
        axis=1,
    )
    return (
        clean_lots.groupby("notice_id", dropna=False)
        .agg(lot_text=("lot_text", lambda values: unique_join(values, sep=" ")))
        .reset_index()
    )


def keyword_in_text(keyword: str, text: str) -> bool:
    """Return True if a keyword or phrase appears in normalized text."""
    keyword = keyword.lower()
    if " " in keyword:
        return keyword in text
    if len(keyword) <= 3:
        return re.search(rf"\b{re.escape(keyword)}\b", text) is not None
    return keyword in text


def classify_microsoft_relevance(cpv_divisions: Any, text: str) -> tuple[str, int, bool, str]:
    """Classify a notice into a Microsoft-relevant category using CPV and keywords."""
    category_scores: Counter[str] = Counter()
    reasons: dict[str, list[str]] = defaultdict(list)

    for division in split_joined(cpv_divisions):
        normalized_division = str(division).strip()[:2]
        for category, points, reason in CPV_RULES.get(normalized_division, []):
            category_scores[category] += points
            reasons[category].append(reason)

    normalized_text = re.sub(r"\s+", " ", text.lower())
    for category, keywords in KEYWORD_RULES.items():
        matched = [keyword for keyword in keywords if keyword_in_text(keyword, normalized_text)]
        for keyword_index, keyword in enumerate(matched):
            category_scores[category] += 30 if keyword_index == 0 else 15
            reasons[category].append(f"keyword: {keyword}")

    if not category_scores:
        return (
            "Non-Relevant / Other",
            0,
            False,
            "No Microsoft-relevant CPV or keyword evidence",
        )

    category = sorted(
        category_scores.items(),
        key=lambda item: (-item[1], MICROSOFT_CATEGORIES.index(item[0]), item[0]),
    )[0][0]
    score = min(100, int(category_scores[category]))
    is_relevant = score >= 40
    if not is_relevant:
        return (
            "Non-Relevant / Other",
            score,
            False,
            "Weak Microsoft-relevant evidence below threshold",
        )
    reason = " + ".join(reasons[category][:3])
    return category, score, True, reason


def choose_text_columns(df: pd.DataFrame) -> list[str]:
    """Choose available tender metadata columns for keyword-based semantic tagging."""
    patterns = ("title", "description", "name", "type", "subject", "objective")
    excluded = {"notice_id", "publication_date", "award_currency"}
    return [
        column
        for column in df.columns
        if column not in excluded and any(pattern in column.lower() for pattern in patterns)
    ]


def build_semantic_layer(
    notices_clean: pd.DataFrame,
    cpv_codes: pd.DataFrame,
    buyer_notice_map: pd.DataFrame,
    lots: pd.DataFrame,
) -> pd.DataFrame:
    """Create one Microsoft procurement semantic row per notice."""
    if notices_clean.empty:
        return pd.DataFrame(columns=SEMANTIC_COLUMNS)

    cpv_agg = aggregate_cpv(cpv_codes)
    buyer_agg = aggregate_buyers(buyer_notice_map)
    lot_text = aggregate_lot_text(lots)

    semantic = notices_clean.copy()
    semantic = semantic.merge(cpv_agg, on="notice_id", how="left")
    semantic = semantic.merge(buyer_agg, on="notice_id", how="left")
    semantic = semantic.merge(lot_text, on="notice_id", how="left")

    text_columns = choose_text_columns(semantic)
    if "lot_text" in semantic.columns:
        text_columns.append("lot_text")

    classifications = []
    for _, row in semantic.iterrows():
        text = " ".join(
            str(row[column])
            for column in text_columns
            if column in row.index and not is_missing_value(row[column])
        )
        classifications.append(classify_microsoft_relevance(row.get("cpv_divisions"), text))

    semantic[
        [
            "microsoft_category",
            "microsoft_relevance_score",
            "is_microsoft_relevant",
            "relevance_reason",
        ]
    ] = pd.DataFrame(classifications, index=semantic.index)

    for column in ["buyer_group_id", "buyer_group_name", "buyer_country", "cpv_codes", "cpv_divisions"]:
        if column not in semantic.columns:
            semantic[column] = pd.NA
    return semantic[SEMANTIC_COLUMNS].sort_values("notice_id").reset_index(drop=True)


def pct(numerator: float, denominator: float) -> float:
    """Return a rounded percentage with safe zero handling."""
    if denominator == 0:
        return 0
    return round((numerator / denominator) * 100, 2)


def explode_counts(series: pd.Series, limit: int = 10) -> dict[str, int]:
    """Count values in normal or pipe-delimited columns."""
    counter: Counter[str] = Counter()
    for value in series:
        values = split_joined(value)
        counter.update(values or ([] if is_missing_value(value) else [str(value)]))
    return {key: int(value) for key, value in counter.most_common(limit)}


def build_quality_metrics(
    notices: pd.DataFrame,
    organizations: pd.DataFrame,
    cpv_codes: pd.DataFrame,
    notices_clean: pd.DataFrame,
    organizations_clean: pd.DataFrame,
    buyer_master: pd.DataFrame,
    buyer_notice_map: pd.DataFrame,
    semantic_layer: pd.DataFrame,
    fuzzy_threshold: int,
) -> dict[str, Any]:
    """Build the S2 data-quality metric payload."""
    missing_buyer = (
        int((~present_mask(notices_clean["buyer_name"])).sum()) if "buyer_name" in notices_clean else 0
    )
    missing_country = (
        int((~present_mask(organizations_clean["country"])).sum())
        if "country" in organizations_clean
        else 0
    )
    missing_cpv = (
        int((~notices_clean["has_cpv"].fillna(False).astype(bool)).sum())
        if "has_cpv" in notices_clean
        else 0
    )
    relevant_count = (
        int(semantic_layer["is_microsoft_relevant"].fillna(False).astype(bool).sum())
        if "is_microsoft_relevant" in semantic_layer
        else 0
    )

    return {
        "input_rows": {
            "notices": int(len(notices)),
            "organizations": int(len(organizations)),
            "cpv_codes": int(len(cpv_codes)),
        },
        "output_rows": {
            "notices_clean": int(len(notices_clean)),
            "organizations_clean": int(len(organizations_clean)),
            "buyer_master": int(len(buyer_master)),
            "buyer_notice_map": int(len(buyer_notice_map)),
            "procurement_semantic_layer": int(len(semantic_layer)),
        },
        "quality": {
            "missing_buyer_name_pct": pct(missing_buyer, len(notices_clean)),
            "missing_country_pct": pct(missing_country, len(organizations_clean)),
            "missing_cpv_pct": pct(missing_cpv, len(notices_clean)),
            "duplicate_notice_id_count": (
                int(notices_clean["notice_id"].duplicated().sum())
                if "notice_id" in notices_clean
                else 0
            ),
            "unique_buyer_groups": int(len(buyer_master)),
            "microsoft_relevant_notice_count": relevant_count,
            "microsoft_relevant_notice_pct": pct(relevant_count, len(semantic_layer)),
        },
        "top_countries": (
            explode_counts(buyer_notice_map["buyer_country"])
            if "buyer_country" in buyer_notice_map
            else {}
        ),
        "top_cpv_divisions": (
            explode_counts(semantic_layer["cpv_divisions"])
            if "cpv_divisions" in semantic_layer
            else {}
        ),
        "top_microsoft_categories": (
            {key: int(value) for key, value in Counter(semantic_layer["microsoft_category"]).most_common(10)}
            if "microsoft_category" in semantic_layer
            else {}
        ),
        "entity_resolution": {
            "fuzzy_threshold": int(fuzzy_threshold),
            "method": "exact normalized name + within-country fuzzy matching",
        },
    }


def markdown_table_from_dict(values: dict[str, int]) -> str:
    """Format a dictionary as a two-column Markdown table."""
    if not values:
        return "| Value | Count |\n|---|---|\n| No data | 0 |"
    lines = ["| Value | Count |", "|---|---|"]
    lines.extend(f"| {key} | {value} |" for key, value in values.items())
    return "\n".join(lines)


def write_quality_report(
    report_path: Path,
    metrics: dict[str, Any],
    input_files: list[Path],
    output_files: list[Path],
) -> None:
    """Write the Markdown S2 data-quality report."""
    quality = metrics["quality"]
    missingness_table = "\n".join(
        [
            "| Field | Missing % |",
            "|---|---:|",
            f"| Buyer name | {quality['missing_buyer_name_pct']} |",
            f"| Organization country | {quality['missing_country_pct']} |",
            f"| CPV on notice | {quality['missing_cpv_pct']} |",
        ]
    )
    report = f"""# S2 Silver Layer Data Quality Report

Generated at: {datetime.now(timezone.utc).isoformat()}

## Executive summary

The S2 layer prepares reliable buyer-level entities and Microsoft-relevant procurement tags for later Gold feature engineering and ML modelling. This run produced {metrics['output_rows']['buyer_master']} buyer groups and tagged {quality['microsoft_relevant_notice_count']} Microsoft-relevant notices.

## What the script does

- Cleans notices and organization records from the exploratory TED ETL.
- Standardizes buyer organization names for entity resolution.
- Builds `buyer_master.csv` and `buyer_notice_map.csv`.
- Creates a rule-based Microsoft procurement semantic layer from CPV divisions and available tender metadata.
- Writes reproducible S2 data-quality metrics and this Markdown report.

## Input files used

{chr(10).join(f'- `{path.as_posix()}`' for path in input_files)}

## Output files created

{chr(10).join(f'- `{path.as_posix()}`' for path in output_files)}

## Key metrics

- Input notices: {metrics['input_rows']['notices']}
- Input organizations: {metrics['input_rows']['organizations']}
- Input CPV rows: {metrics['input_rows']['cpv_codes']}
- Clean notices: {metrics['output_rows']['notices_clean']}
- Clean organizations: {metrics['output_rows']['organizations_clean']}
- Buyer groups: {quality['unique_buyer_groups']}
- Duplicate notice IDs: {quality['duplicate_notice_id_count']}
- Microsoft-relevant notices: {quality['microsoft_relevant_notice_count']} ({quality['microsoft_relevant_notice_pct']}%)

## Missingness table

{missingness_table}

## Top countries

{markdown_table_from_dict(metrics['top_countries'])}

## Top CPV divisions

{markdown_table_from_dict(metrics['top_cpv_divisions'])}

## Microsoft relevance category distribution

{markdown_table_from_dict(metrics['top_microsoft_categories'])}

## Entity-resolution methodology

Buyer entities are grouped first by exact normalized organization name and country. The script then applies high-threshold fuzzy matching only within the same country. Buyer group IDs are assigned deterministically as `BUYER_000001`, `BUYER_000002`, and so on. This run used a fuzzy threshold of {metrics['entity_resolution']['fuzzy_threshold']} with the `{FUZZY_BACKEND}` backend.

## Limitations

At this stage, Microsoft relevance is estimated using CPV divisions and available tender metadata. Future iterations should extract richer tender titles and descriptions from XML to improve semantic classification.

Fuzzy matching is intentionally conservative and never links buyers across countries. This reduces false positives but may leave some true buyer aliases unresolved.

## Next steps for S3/S4 ML team

- Use `buyer_master.csv` as the canonical buyer entity table for Gold buyer-level features.
- Use `buyer_notice_map.csv` to connect resolved buyers to notices.
- Use `procurement_semantic_layer.csv` as a first rule-based Microsoft relevance signal.
- Improve semantic classification when richer notice titles and descriptions become available.
"""
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(report, encoding="utf-8")


def write_csv(df: pd.DataFrame, path: Path) -> None:
    """Write a DataFrame to CSV with parent directory creation."""
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def main() -> None:
    """Run the full S2 Silver Layer workflow."""
    args = parse_args()
    input_dir = args.input_dir
    output_dir = args.output_dir
    reports_dir = args.reports_dir

    notices = load_csv(input_dir / "notices.csv", NOTICE_COLUMNS)
    organizations = load_csv(input_dir / "organizations.csv", ORGANIZATION_COLUMNS)
    cpv_codes = load_csv(input_dir / "cpv_codes.csv", CPV_COLUMNS)
    lots = load_csv(input_dir / "lots.csv", LOT_COLUMNS)

    notices_clean = clean_notices(notices, cpv_codes)
    organizations_clean = clean_organizations(organizations)
    buyer_master, buyer_notice_map = build_buyer_outputs(
        organizations_clean,
        notices_clean,
        args.fuzzy_threshold,
    )
    semantic_layer = build_semantic_layer(notices_clean, cpv_codes, buyer_notice_map, lots)

    output_files = [
        output_dir / "notices_clean.csv",
        output_dir / "organizations_clean.csv",
        output_dir / "buyer_master.csv",
        output_dir / "buyer_notice_map.csv",
        output_dir / "procurement_semantic_layer.csv",
        reports_dir / "s2_data_quality_report.md",
        reports_dir / "s2_data_quality_metrics.json",
    ]

    write_csv(notices_clean, output_files[0])
    write_csv(organizations_clean, output_files[1])
    write_csv(buyer_master, output_files[2])
    write_csv(buyer_notice_map, output_files[3])
    write_csv(semantic_layer, output_files[4])

    metrics = build_quality_metrics(
        notices,
        organizations,
        cpv_codes,
        notices_clean,
        organizations_clean,
        buyer_master,
        buyer_notice_map,
        semantic_layer,
        args.fuzzy_threshold,
    )
    output_files[6].parent.mkdir(parents=True, exist_ok=True)
    output_files[6].write_text(json.dumps(metrics, indent=2), encoding="utf-8")
    write_quality_report(
        output_files[5],
        metrics,
        [
            input_dir / "notices.csv",
            input_dir / "organizations.csv",
            input_dir / "cpv_codes.csv",
            input_dir / "lots.csv",
        ],
        output_files,
    )

    print("S2 Silver Layer complete")
    print(f"  notices_clean rows: {len(notices_clean)}")
    print(f"  organizations_clean rows: {len(organizations_clean)}")
    print(f"  buyer_master rows: {len(buyer_master)}")
    print(f"  buyer_notice_map rows: {len(buyer_notice_map)}")
    print(f"  procurement_semantic_layer rows: {len(semantic_layer)}")
    print(f"  Microsoft-relevant notices: {metrics['quality']['microsoft_relevant_notice_count']}")
    print(f"  Reports written to: {reports_dir}")


if __name__ == "__main__":
    main()
