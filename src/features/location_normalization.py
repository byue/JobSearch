"""Location normalization helpers for the features service."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from functools import lru_cache
from typing import Any

import country_converter as coco
import geonamescache
import pycountry
from thefuzz import process

_FIRST_LEVEL_TYPES = {
    "state",
    "province",
    "territory",
    "region",
    "prefecture",
    "autonomous community",
    "canton",
    "land",
    "emirate",
    "governorate",
}
_COUNTRY_PRIORITY = ("US", "CA", "AU", "GB", "DE", "JP", "FR", "IN")
_STRIP_PREFIXES = re.compile(
    r"^(remote\s*[-–]?\s*|hybrid\s*[-–]?\s*|onsite\s*[-–]?\s*|greater\s+|metro\s+|downtown\s+)",
    re.IGNORECASE,
)
_REMOTE_TOKENS = frozenset({"remote", "hybrid", "onsite"})


@dataclass(frozen=True)
class _ClassifiedToken:
    token_type: str
    value: str
    confidence: float
    meta: dict[str, Any]


@lru_cache(maxsize=1)
def _country_converter() -> coco.CountryConverter:
    return coco.CountryConverter()


@lru_cache(maxsize=1)
def _geonames_cache() -> geonamescache.GeonamesCache:
    return geonamescache.GeonamesCache()


@lru_cache(maxsize=1)
def _city_lookup() -> dict[str, list[dict[str, Any]]]:
    lookup: dict[str, list[dict[str, Any]]] = {}
    for value in _geonames_cache().get_cities().values():
        key = str(value.get("name") or "").lower()
        if not key:
            continue
        lookup.setdefault(key, []).append(value)
    return lookup


@lru_cache(maxsize=1)
def _subdivision_names() -> dict[str, Any]:
    names: dict[str, Any] = {}
    for subdivision in pycountry.subdivisions:
        subtype = str(getattr(subdivision, "type", "") or "").lower()
        if subtype not in _FIRST_LEVEL_TYPES:
            continue
        names[str(subdivision.name).lower()] = subdivision
    return names


@lru_cache(maxsize=1)
def _subdivision_codes() -> dict[str, list[Any]]:
    codes: dict[str, list[Any]] = {}
    for subdivision in pycountry.subdivisions:
        subtype = str(getattr(subdivision, "type", "") or "").lower()
        if subtype not in _FIRST_LEVEL_TYPES:
            continue
        code = str(subdivision.code).split("-")[-1].lower()
        codes.setdefault(code, []).append(subdivision)
    return codes


@lru_cache(maxsize=1)
def _subdivision_name_keys() -> list[str]:
    return list(_subdivision_names().keys())


@lru_cache(maxsize=1)
def _city_name_keys() -> list[str]:
    return list(_city_lookup().keys())


def _country_priority_index(value: str) -> int:
    try:
        return _COUNTRY_PRIORITY.index(value)
    except ValueError:
        return len(_COUNTRY_PRIORITY)


def _resolve_city_candidate(
    candidates: list[dict[str, Any]],
    *,
    country: str | None = None,
    state_code: str | None = None,
    require_constraints: bool = False,
) -> dict[str, Any] | None:
    if not candidates:
        return None

    matches = candidates
    if country:
        country_matches = [candidate for candidate in matches if str(candidate.get("countrycode") or "") == country]
        if country_matches:
            matches = country_matches
        elif require_constraints:
            return None

    if country and state_code:
        state_matches = [
            candidate
            for candidate in matches
            if str(candidate.get("countrycode") or "") == country and str(candidate.get("admin1code") or "") == state_code
        ]
        if state_matches:
            matches = state_matches
        elif require_constraints:
            return None

    return max(
        matches,
        key=lambda candidate: (
            int(candidate.get("population") or 0),
            -_country_priority_index(str(candidate.get("countrycode") or "")),
        ),
    )


def _country_candidate(token: str) -> tuple[str, str] | None:
    country_name = _normalize_country_name(token)
    if not country_name:
        return None
    iso2 = _normalize_country_iso2(token)
    if not iso2:
        return None
    return country_name, iso2


def _state_candidates(token: str) -> list[Any]:
    normalized = _preprocess_token(token).lower()
    if not normalized:
        return []
    exact = _subdivision_names().get(normalized)
    if exact is not None:
        return [exact]
    return list(_subdivision_codes().get(normalized) or [])


def _city_candidates(token: str) -> list[dict[str, Any]]:
    normalized = _preprocess_token(token).lower()
    if not normalized:
        return []
    return list(_city_lookup().get(normalized) or [])


def _resolve_two_token_interpretation(raw_tokens: list[str]) -> tuple[str | None, str | None, str | None] | None:
    if len(raw_tokens) != 2:
        return None

    left, right = raw_tokens
    left_cities = _city_candidates(left)
    right_cities = _city_candidates(right)
    left_states = _state_candidates(left)
    right_states = _state_candidates(right)
    left_country = _country_candidate(left)
    right_country = _country_candidate(right)

    scored: list[tuple[int, tuple[str | None, str | None, str | None]]] = []

    if right_country is not None and left_cities:
        country_name, iso2 = right_country
        city = _resolve_city_candidate(left_cities, country=iso2, require_constraints=True)
        if city is not None:
            scored.append((30, (str(city["name"]), None, country_name)))

    if left_country is not None and right_cities:
        country_name, iso2 = left_country
        city = _resolve_city_candidate(right_cities, country=iso2, require_constraints=True)
        if city is not None:
            scored.append((30, (str(city["name"]), None, country_name)))

    for subdivision in right_states:
        city = _resolve_city_candidate(
            left_cities,
            country=str(subdivision.country_code),
            state_code=str(subdivision.code).split("-")[-1],
            require_constraints=True,
        )
        if city is not None:
            country_name = _normalize_country_name(str(subdivision.country_code) or "")
            if country_name:
                scored.append((40, (str(city["name"]), str(subdivision.name), country_name)))

    for subdivision in left_states:
        city = _resolve_city_candidate(
            right_cities,
            country=str(subdivision.country_code),
            state_code=str(subdivision.code).split("-")[-1],
            require_constraints=True,
        )
        if city is not None:
            country_name = _normalize_country_name(str(subdivision.country_code) or "")
            if country_name:
                scored.append((25, (str(city["name"]), str(subdivision.name), country_name)))

    if right_country is not None:
        country_name, iso2 = right_country
        for subdivision in left_states:
            if str(subdivision.country_code) == iso2:
                scored.append((35, (None, str(subdivision.name), country_name)))

    if left_country is not None:
        country_name, iso2 = left_country
        for subdivision in right_states:
            if str(subdivision.country_code) == iso2:
                scored.append((35, (None, str(subdivision.name), country_name)))

    if not scored:
        return None

    return max(scored, key=lambda item: item[0])[1]


def _resolve_three_token_interpretation(raw_tokens: list[str]) -> tuple[str | None, str | None, str | None] | None:
    if len(raw_tokens) != 3:
        return None

    first, second, third = raw_tokens
    first_cities = _city_candidates(first)
    second_cities = _city_candidates(second)
    third_cities = _city_candidates(third)
    first_states = _state_candidates(first)
    second_states = _state_candidates(second)
    third_states = _state_candidates(third)
    first_country = _country_candidate(first)
    third_country = _country_candidate(third)

    scored: list[tuple[int, tuple[str | None, str | None, str | None]]] = []

    if third_country is not None:
        country_name, iso2 = third_country

        for subdivision in second_states:
            if str(subdivision.country_code) != iso2:
                continue
            city = _resolve_city_candidate(
                first_cities,
                country=iso2,
                state_code=str(subdivision.code).split("-")[-1],
                require_constraints=True,
            )
            if city is not None:
                scored.append((50, (str(city["name"]), str(subdivision.name), country_name)))

        for subdivision in first_states:
            if str(subdivision.country_code) != iso2:
                continue
            city = _resolve_city_candidate(
                second_cities,
                country=iso2,
                state_code=str(subdivision.code).split("-")[-1],
                require_constraints=True,
            )
            if city is not None:
                scored.append((40, (str(city["name"]), str(subdivision.name), country_name)))

    if first_country is not None:
        country_name, iso2 = first_country

        for subdivision in second_states:
            if str(subdivision.country_code) != iso2:
                continue
            city = _resolve_city_candidate(
                third_cities,
                country=iso2,
                state_code=str(subdivision.code).split("-")[-1],
                require_constraints=True,
            )
            if city is not None:
                scored.append((48, (str(city["name"]), str(subdivision.name), country_name)))

        for subdivision in third_states:
            if str(subdivision.country_code) != iso2:
                continue
            city = _resolve_city_candidate(
                second_cities,
                country=iso2,
                state_code=str(subdivision.code).split("-")[-1],
                require_constraints=True,
            )
            if city is not None:
                scored.append((38, (str(city["name"]), str(subdivision.name), country_name)))

    if not scored:
        return None

    return max(scored, key=lambda item: item[0])[1]


def _preprocess_token(token: str) -> str:
    normalized = _STRIP_PREFIXES.sub("", token).strip()
    return normalized


def _normalize_country_name(value: str) -> str | None:
    normalized = str(value or "").strip()
    if not normalized:
        return None
    converted = _country_converter().convert(normalized, to="name")
    return None if converted == "not found" else str(converted)


def _normalize_country_iso2(value: str) -> str | None:
    normalized = str(value or "").strip()
    if not normalized:
        return None
    converted = _country_converter().convert(normalized, to="ISO2")
    return None if converted == "not found" else str(converted)


def _classify_token(token: str) -> _ClassifiedToken:
    normalized = _preprocess_token(token)
    key = normalized.lower()
    if not key:
        return _ClassifiedToken(token_type="unknown", value="", confidence=0.0, meta={})

    country_name = _normalize_country_name(normalized)
    if country_name:
        iso2 = _country_converter().convert(normalized, to="ISO2")
        return _ClassifiedToken(
            token_type="country",
            value=country_name,
            confidence=1.0,
            meta={"iso2": None if iso2 == "not found" else str(iso2)},
        )

    subdivision = _subdivision_names().get(key)
    if subdivision is not None:
        return _ClassifiedToken(
            token_type="state",
            value=str(subdivision.name),
            confidence=1.0,
            meta={"code": str(subdivision.code).split("-")[-1], "country": str(subdivision.country_code)},
        )

    code_matches = _subdivision_codes().get(key)
    if code_matches:
        subdivision = next(
            (match for country_code in _COUNTRY_PRIORITY for match in code_matches if str(match.country_code) == country_code),
            code_matches[0],
        )
        return _ClassifiedToken(
            token_type="state",
            value=str(subdivision.name),
            confidence=1.0 if len(code_matches) == 1 else 0.7,
            meta={
                "code": str(subdivision.code).split("-")[-1],
                "country": str(subdivision.country_code),
            },
        )

    city_candidates = _city_lookup().get(key)
    if city_candidates:
        city_entry = _resolve_city_candidate(city_candidates)
        return _ClassifiedToken(
            token_type="city",
            value=str(city_entry["name"]),
            confidence=1.0,
            meta={
                "country": str(city_entry.get("countrycode") or ""),
                "admin1": str(city_entry.get("admin1code") or ""),
                "candidates": city_candidates,
            },
        )

    subdivision_match = process.extractOne(key, _subdivision_name_keys())
    if subdivision_match:
        match_name, score = subdivision_match
        if score >= 88:
            subdivision = _subdivision_names().get(match_name)
            if subdivision is not None:
                return _ClassifiedToken(
                    token_type="state",
                    value=str(subdivision.name),
                    confidence=round(score / 100, 2),
                    meta={"code": str(subdivision.code).split("-")[-1], "country": str(subdivision.country_code)},
                )

    city_match = process.extractOne(key, _city_name_keys())
    if city_match:
        match_name, score = city_match
        if score >= 88:
            city_candidates = _city_lookup().get(match_name)
            if city_candidates:
                city_entry = _resolve_city_candidate(city_candidates)
                return _ClassifiedToken(
                    token_type="city",
                    value=str(city_entry["name"]),
                    confidence=round(score / 100, 2),
                    meta={
                        "country": str(city_entry.get("countrycode") or ""),
                        "admin1": str(city_entry.get("admin1code") or ""),
                        "candidates": city_candidates,
                    },
                )

    return _ClassifiedToken(token_type="unknown", value=normalized, confidence=0.0, meta={})


def _disambiguate_with_context(
    raw_tokens: list[str],
    classified_tokens: list[_ClassifiedToken],
) -> list[_ClassifiedToken]:
    if len(raw_tokens) != 2 or len(classified_tokens) != 2:
        return classified_tokens

    updated = list(classified_tokens)
    for city_index in range(2):
        other_index = 1 - city_index
        city_token = updated[city_index]
        city_candidates = city_token.meta.get("candidates")
        if city_token.token_type != "city" or not isinstance(city_candidates, list):
            continue

        subdivision_matches = _subdivision_codes().get(_preprocess_token(raw_tokens[other_index]).lower())
        if not subdivision_matches:
            continue

        matching_subdivision = next(
            (
                subdivision
                for subdivision in subdivision_matches
                for candidate in city_candidates
                if str(candidate.get("countrycode") or "") == str(subdivision.country_code)
                and str(candidate.get("admin1code") or "") == str(subdivision.code).split("-")[-1]
            ),
            None,
        )
        if matching_subdivision is None:
            continue

        resolved_city = _resolve_city_candidate(
            city_candidates,
            country=str(matching_subdivision.country_code),
            state_code=str(matching_subdivision.code).split("-")[-1],
            require_constraints=True,
        )
        if resolved_city is None:
            continue

        updated[city_index] = _ClassifiedToken(
            token_type="city",
            value=str(resolved_city["name"]),
            confidence=city_token.confidence,
            meta={
                "country": str(resolved_city.get("countrycode") or ""),
                "admin1": str(resolved_city.get("admin1code") or ""),
                "candidates": city_candidates,
            },
        )
        updated[other_index] = _ClassifiedToken(
            token_type="state",
            value=str(matching_subdivision.name),
            confidence=1.0 if len(subdivision_matches) == 1 else 0.9,
            meta={
                "code": str(matching_subdivision.code).split("-")[-1],
                "country": str(matching_subdivision.country_code),
            },
        )
        return updated

    return updated


def _assemble(tokens: list[_ClassifiedToken]) -> tuple[str | None, str | None, str | None]:
    country = state = city = None
    meta: dict[str, dict[str, Any]] = {}

    for token in tokens:
        if token.token_type == "country" and country is None:
            country = token.value
            meta["country"] = token.meta
        elif token.token_type == "state" and state is None:
            state = token.value
            meta["state"] = token.meta
        elif token.token_type == "city" and city is None:
            city = token.value
            meta["city"] = token.meta

    state_country = str(meta.get("state", {}).get("country") or "")
    state_code = str(meta.get("state", {}).get("code") or "")
    city_candidates = meta.get("city", {}).get("candidates")
    if isinstance(city_candidates, list) and city:
        effective_country = state_country or str(meta.get("country", {}).get("iso2") or "") or None
        effective_state_code = state_code or None
        resolved_city = _resolve_city_candidate(
            city_candidates,
            country=effective_country,
            state_code=effective_state_code,
            require_constraints=bool(effective_country or effective_state_code),
        )
        if resolved_city is not None:
            city = str(resolved_city["name"])
            meta["city"] = {
                "country": str(resolved_city.get("countrycode") or ""),
                "admin1": str(resolved_city.get("admin1code") or ""),
                "candidates": city_candidates,
            }

    if city and not country:
        country_name = _normalize_country_name(str(meta.get("city", {}).get("country") or ""))
        if country_name:
            country = country_name

    if state and not country:
        country_name = _normalize_country_name(str(meta.get("state", {}).get("country") or ""))
        if country_name:
            country = country_name

    return city, state, country


def _fill_missing_city_from_three_tokens(
    raw_tokens: list[str],
    classified_tokens: list[_ClassifiedToken],
    city: str | None,
    state: str | None,
    country: str | None,
) -> tuple[str | None, str | None, str | None]:
    if city is not None or state is None or country is None or len(raw_tokens) != 3 or len(classified_tokens) != 3:
        return city, state, country

    if classified_tokens[0].token_type == "country":
        candidate_city = _preprocess_token(raw_tokens[2])
        if candidate_city:
            return candidate_city, state, country

    if classified_tokens[2].token_type == "country":
        candidate_city = _preprocess_token(raw_tokens[0])
        if candidate_city:
            return candidate_city, state, country

    for raw_token, classified_token in reversed(list(zip(raw_tokens, classified_tokens))):
        if classified_token.token_type == "unknown":
            candidate_city = _preprocess_token(raw_token)
            if candidate_city:
                return candidate_city, state, country
    return city, state, country


def normalize_location(raw: str) -> tuple[str | None, str | None, str | None]:
    normalized_raw = str(raw or "").strip()
    if not normalized_raw:
        return None, None, None

    if normalized_raw.lower() in _REMOTE_TOKENS:
        return None, None, None

    if normalized_raw.startswith("{"):
        try:
            payload = json.loads(normalized_raw)
        except json.JSONDecodeError:
            payload = None
        if isinstance(payload, dict):
            city = payload.get("normalizedCityName") or payload.get("city")
            state = payload.get("normalizedStateName") or payload.get("region")
            country = payload.get("normalizedCountryName") or payload.get("countryIso2a")
            parts = [str(part).strip() for part in (city, state, country) if str(part).strip()]
            if parts:
                return normalize_location(", ".join(parts))

    raw_tokens = [token.strip() for token in normalized_raw.split(",") if token.strip()]
    three_token_result = _resolve_three_token_interpretation(raw_tokens)
    if three_token_result is not None:
        return three_token_result
    two_token_result = _resolve_two_token_interpretation(raw_tokens)
    if two_token_result is not None:
        return two_token_result
    classified = [_classify_token(token) for token in raw_tokens]
    classified = _disambiguate_with_context(raw_tokens, classified)
    city, state, country = _assemble(classified)
    city, state, country = _fill_missing_city_from_three_tokens(raw_tokens, classified, city, state, country)
    return city, state, country
