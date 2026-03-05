"""Compensation extraction helpers shared by company clients."""

from __future__ import annotations

import html
import re

from web.backend.schemas import PayDetails, PayRange

_PAY_RANGE_PATTERN = re.compile(
    r"(?P<code1>\b[A-Z]{2,3}\b)?\s*"
    r"(?P<sym1>[$€£¥])?\s*"
    r"(?P<min>\d{1,3}(?:,\d{3})+|\d{4,})"
    r"\s*(?:-|–|to)\s*"
    r"(?P<code2>\b[A-Z]{2,3}\b)?\s*"
    r"(?P<sym2>[$€£¥])?\s*"
    r"(?P<max>\d{1,3}(?:,\d{3})+|\d{4,})",
    re.IGNORECASE,
)
_INTERVAL_PATTERN = re.compile(r"(?:per|/)\s*(year|month|week|day|hour)\b", re.IGNORECASE)
_PAY_CONTEXT_KEYWORDS = (
    "salary",
    "base pay",
    "base salary",
    "pay range",
    "compensation",
)
_PAY_NOTE_KEYWORDS = (
    "salary",
    "pay",
    "compensation",
    "bonus",
    "equity",
    "benefits",
)
_SYMBOL_TO_CURRENCY = {
    "$": "USD",
    "€": "EUR",
    "£": "GBP",
    "¥": "JPY",
}
_ALIASED_CURRENCY_CODES = {
    "US": "USD",
    "CA": "CAD",
    "AU": "AUD",
    "HK": "HKD",
    "SG": "SGD",
}


def extract_pay_details_from_description(value: str | None) -> PayDetails | None:
    """Parse compensation ranges/notes from a job description payload."""
    if not isinstance(value, str):
        return None

    cleaned = _clean_html_fragment(value)
    if not cleaned:
        return None

    ranges = _extract_pay_ranges(cleaned)
    if not ranges:
        return None

    notes = _extract_pay_notes(cleaned, ranges)
    return PayDetails(ranges=ranges, notes=notes)


def _extract_pay_ranges(text: str) -> list[PayRange]:
    out: list[PayRange] = []
    seen: set[tuple[int, int, str | None, str | None, str]] = set()

    for match in _PAY_RANGE_PATTERN.finditer(text):
        min_amount = _parse_amount(match.group("min"))
        max_amount = _parse_amount(match.group("max"))
        if min_amount is None or max_amount is None:
            continue
        if min_amount < 1000 and max_amount < 1000:
            continue

        context = _extract_sentence(text=text, start=match.start(), end=match.end())
        context_lower = context.lower()
        has_currency = any(
            (
                bool(match.group("code1")),
                bool(match.group("sym1")),
                bool(match.group("code2")),
                bool(match.group("sym2")),
            )
        )
        if not has_currency and not any(keyword in context_lower for keyword in _PAY_CONTEXT_KEYWORDS):
            continue

        trailing = text[match.end() : match.end() + 64]
        currency = _resolve_currency(
            code1=match.group("code1"),
            code2=match.group("code2"),
            sym1=match.group("sym1"),
            sym2=match.group("sym2"),
            context=context,
        )
        interval = _resolve_interval(context=context, trailing=trailing)

        dedupe_key = (min_amount, max_amount, currency, interval, context)
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        out.append(
            PayRange(
                minAmount=min_amount,
                maxAmount=max_amount,
                currency=currency,
                interval=interval,
                context=context or None,
            )
        )

    return out


def _extract_pay_notes(text: str, ranges: list[PayRange]) -> list[str]:
    contexts = {item.context for item in ranges if item.context}
    candidates = re.split(r"(?:\n+|(?<=[.!?])\s+)", text)

    out: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        normalized = re.sub(r"\s+", " ", candidate).strip()
        if not normalized or normalized in contexts:
            continue

        lowered = normalized.lower()
        if not any(keyword in lowered for keyword in _PAY_NOTE_KEYWORDS):
            continue
        if normalized in seen:
            continue
        seen.add(normalized)
        out.append(normalized)

    return out


def _clean_html_fragment(value: str) -> str:
    normalized = (
        value.replace("\r\n", "\n")
        .replace("\r", "\n")
        .replace("<br/>", "\n")
        .replace("<br />", "\n")
        .replace("<br>", "\n")
        .replace("</p>", "\n")
        .replace("</li>", "\n")
    )
    normalized = re.sub(r"(?is)<[^>]+>", " ", normalized)
    normalized = html.unescape(normalized)
    normalized = re.sub(r"[ \t]+", " ", normalized)
    normalized = re.sub(r"\n{2,}", "\n", normalized)
    return normalized.strip()


def _parse_amount(value: str | None) -> int | None:
    if not isinstance(value, str):
        return None
    normalized = value.replace(",", "").strip()
    if not normalized.isdigit():
        return None
    return int(normalized)


def _resolve_currency(
    *,
    code1: str | None,
    code2: str | None,
    sym1: str | None,
    sym2: str | None,
    context: str,
) -> str | None:
    for code in (code1, code2):
        normalized_code = _normalize_currency_code(code)
        if normalized_code:
            return normalized_code

    symbol = sym1 or sym2
    if not symbol:
        return None

    if symbol == "$":
        context_lower = context.lower()
        if any(token in context_lower for token in ("u.s.", "us base", "united states", "usd")):
            return "USD"
    return _SYMBOL_TO_CURRENCY.get(symbol, symbol)


def _normalize_currency_code(value: str | None) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip().upper()
    if not normalized:
        return None
    if len(normalized) == 2:
        return _ALIASED_CURRENCY_CODES.get(normalized)
    if len(normalized) == 3:
        return normalized
    return None


def _resolve_interval(*, context: str, trailing: str) -> str | None:
    combined = f"{context} {trailing}".lower()
    interval_match = _INTERVAL_PATTERN.search(combined)
    if interval_match:
        return interval_match.group(1).lower()
    if "hourly" in combined:
        return "hour"
    if any(
        keyword in combined
        for keyword in ("base salary range", "base pay range", "salary range", "annual", "per annum")
    ):
        return "year"
    return None


def _extract_sentence(*, text: str, start: int, end: int) -> str:
    left_delimiters = [text.rfind(token, 0, start) for token in (".", "!", "?", "\n")]
    left = max(left_delimiters)

    right_candidates = [text.find(token, end) for token in (".", "!", "?", "\n")]
    right_positions = [pos for pos in right_candidates if pos >= 0]
    right = min(right_positions) if right_positions else len(text)

    if right < len(text):
        sentence = text[left + 1 : right + 1]
    else:
        sentence = text[left + 1 : right]
    return re.sub(r"\s+", " ", sentence).strip()
