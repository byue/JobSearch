"""Extract technology skills from job descriptions using repo-owned sources."""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any


DEFAULT_SPACY_MODEL = "en_core_web_sm"
_DEFAULT_TECHNICAL_PATH = Path(__file__).resolve().parent / "data" / "technical_skills.csv"
_DEFAULT_KEYWORD_PATH = Path(__file__).resolve().parent / "data" / "tech_keywords.csv"


class SkillExtractor:
    def __init__(
        self,
        technical_filepath: str | Path | None = None,
        keyword_filepath: str | Path | None = None,
        *,
        spacy_model: str = DEFAULT_SPACY_MODEL,
    ) -> None:
        self.technical_filepath = (
            Path(technical_filepath) if technical_filepath is not None else _DEFAULT_TECHNICAL_PATH
        )
        self.keyword_filepath = Path(keyword_filepath) if keyword_filepath is not None else _DEFAULT_KEYWORD_PATH
        self.nlp = self._load_nlp(spacy_model)
        self.matcher = self._phrase_matcher(self.nlp, attr="LOWER")
        self.lemma_matcher = self._phrase_matcher(self.nlp, attr="LEMMA") if self._supports_lemma_matching() else None
        self.skills: set[str] = set()
        self._canonical_by_key: dict[str, str] = {}
        self._canonical_by_lemma_key: dict[str, str] = {}
        self._load_skill_rows(self.technical_filepath, required=True)
        self._load_keyword_rows(self.keyword_filepath, required=False)

    def _load_nlp(self, spacy_model: str) -> Any:
        import spacy

        try:
            return spacy.load(spacy_model)
        except OSError:
            return spacy.blank("en")

    def _phrase_matcher(self, nlp: Any, *, attr: str) -> Any:
        from spacy.matcher import PhraseMatcher

        return PhraseMatcher(nlp.vocab, attr=attr)

    def _supports_lemma_matching(self) -> bool:
        return "lemmatizer" in getattr(self.nlp, "pipe_names", [])

    @staticmethod
    def _lemma_key(doc: Any) -> str:
        return " ".join((getattr(token, "lemma_", "") or token.text).lower() for token in doc).strip()

    def _add_skills(self, skills: list[str]) -> None:
        new_skills: list[str] = []
        new_lemma_docs: list[Any] = []
        for skill in skills:
            canonical = str(skill or "").strip()
            if not canonical:
                continue
            key = canonical.lower()
            if key in self._canonical_by_key:
                continue
            self.skills.add(canonical)
            self._canonical_by_key[key] = canonical
            new_skills.append(canonical)
            if self.lemma_matcher is not None:
                lemma_doc = self.nlp(canonical)
                lemma_key = self._lemma_key(lemma_doc)
                if lemma_key and lemma_key not in self._canonical_by_lemma_key:
                    self._canonical_by_lemma_key[lemma_key] = canonical
                    new_lemma_docs.append(lemma_doc)
        if new_skills:
            self.matcher.add("TECH_SKILL", [self.nlp.make_doc(name) for name in new_skills])
        if self.lemma_matcher is not None and new_lemma_docs:
            self.lemma_matcher.add("TECH_SKILL", new_lemma_docs)

    def _load_skill_rows(self, filepath: Path, *, required: bool) -> None:
        if not filepath.exists():
            if required:
                raise FileNotFoundError(f"Skills file not found: {filepath}")
            return

        delimiter = "\t" if filepath.suffix.lower() in {".txt", ".tsv"} else ","
        with filepath.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle, delimiter=delimiter)
            skills: list[str] = []
            for row in reader:
                skill = str(
                    row.get("Example", "")
                    or row.get("Skill Name", "")
                    or row.get("preferredLabel", "")
                    or row.get("title", "")
                    or ""
                ).strip()
                if skill:
                    skills.append(skill)
        self._add_skills(skills)

    def _load_keyword_rows(self, filepath: Path, *, required: bool) -> None:
        if not filepath.exists():
            if required:
                raise FileNotFoundError(f"KEYWORD skills file not found: {filepath}")
            return

        skills: list[str] = []
        with filepath.open("r", encoding="utf-8-sig") as handle:
            for line in handle:
                skill = str(line or "").strip()
                if skill:
                    skills.append(skill)
        self._add_skills(skills)

    def extract(self, text: str) -> list[str]:
        if not text.strip():
            return []

        doc = self.nlp(text)

        seen: set[str] = set()
        results: list[str] = []
        for _, start, end in self.matcher(doc):
            span = doc[start:end]
            key = span.text.lower()
            canonical = self._canonical_by_key.get(key)
            if canonical is None:
                continue
            canonical_key = canonical.lower()
            if canonical_key in seen:
                continue
            seen.add(canonical_key)
            results.append(canonical)
        if self.lemma_matcher is not None:
            for _, start, end in self.lemma_matcher(doc):
                span = doc[start:end]
                canonical = self._canonical_by_lemma_key.get(self._lemma_key(span))
                if canonical is None:
                    continue
                canonical_key = canonical.lower()
                if canonical_key in seen:
                    continue
                seen.add(canonical_key)
                results.append(canonical)
        return results
