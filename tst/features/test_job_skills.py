from __future__ import annotations

import importlib
import sys
import tempfile
import types
import unittest
from pathlib import Path


class _FakeSpan:
    def __init__(self, tokens) -> None:
        self._tokens = tokens
        self.text = " ".join(token.text for token in tokens)

    def __iter__(self):
        return iter(self._tokens)


class _FakeToken:
    def __init__(self, text: str) -> None:
        self.text = text
        self.lemma_ = self._lemma(text)

    @staticmethod
    def _lemma(text: str) -> str:
        value = text.lower()
        if len(value) > 3 and value.endswith("s"):
            return value[:-1]
        return value


class _FakeDoc:
    def __init__(self, text: str) -> None:
        self.tokens = [_FakeToken(token) for token in text.split()]

    def __getitem__(self, item):
        if isinstance(item, slice):
            return _FakeSpan(self.tokens[item.start:item.stop])
        raise TypeError("unsupported index")

    def __iter__(self):
        return iter(self.tokens)


class _FakeNLP:
    def __init__(self, *, with_lemma: bool) -> None:
        self.vocab = object()
        self.pipe_names = ["lemmatizer"] if with_lemma else []

    def make_doc(self, text: str) -> str:
        return text

    def __call__(self, text: str) -> _FakeDoc:
        return _FakeDoc(text)


class _FakePhraseMatcher:
    def __init__(self, vocab: object, attr: str = "LOWER") -> None:
        _ = vocab
        self._attr = attr
        self._patterns: list[str] = []

    def add(self, name: str, patterns: list[str]) -> None:
        _ = name
        normalized_patterns: list[str] = []
        for pattern in patterns:
            if hasattr(pattern, "text"):
                normalized_patterns.append(pattern.text)
            elif hasattr(pattern, "tokens"):
                normalized_patterns.append(" ".join(token.text for token in pattern.tokens))
            else:
                normalized_patterns.append(str(pattern))
        self._patterns.extend(normalized_patterns)

    def __call__(self, doc: _FakeDoc) -> list[tuple[int, int, int]]:
        lowered_tokens = [
            (token.lemma_ if self._attr == "LEMMA" else token.text.lower())
            for token in doc.tokens
        ]
        matches: list[tuple[int, int, int]] = []
        for index, pattern in enumerate(self._patterns):
            pattern_tokens = [
                _FakeToken(token).lemma_ if self._attr == "LEMMA" else token.lower()
                for token in pattern.split()
            ]
            token_count = len(pattern_tokens)
            for start in range(len(lowered_tokens) - token_count + 1):
                if lowered_tokens[start:start + token_count] == pattern_tokens:
                    matches.append((index, start, start + token_count))
        return matches


class JobSkillsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._module_names = [
            "spacy",
            "spacy.matcher",
            "features.job_skills",
        ]
        cls._saved_modules = {name: sys.modules.get(name) for name in cls._module_names}

        spacy_mod = types.ModuleType("spacy")
        spacy_matcher_mod = types.ModuleType("spacy.matcher")

        def _load(model: str) -> _FakeNLP:
            if model == "missing_model":
                raise OSError("missing")
            return _FakeNLP(with_lemma=True)

        def _blank(lang: str) -> _FakeNLP:
            _ = lang
            return _FakeNLP(with_lemma=False)

        spacy_mod.load = _load
        spacy_mod.blank = _blank
        spacy_matcher_mod.PhraseMatcher = _FakePhraseMatcher

        sys.modules["spacy"] = spacy_mod
        sys.modules["spacy.matcher"] = spacy_matcher_mod
        sys.modules.pop("features.job_skills", None)

        cls.mod = importlib.import_module("features.job_skills")

    @classmethod
    def tearDownClass(cls) -> None:
        for name in cls._module_names:
            if cls._saved_modules[name] is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = cls._saved_modules[name]

    def test_extract_skills_from_csv(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            technical_path = Path(tmpdir) / "technical_skills.csv"
            keyword_path = Path(tmpdir) / "tech_keywords.csv"
            technical_path.write_text(
                "Skill ID,Skill Name,Category\n"
                "1,Python,Programming language\n"
                "2,Atlassian JIRA,Project management software\n"
                "3,Python,Programming language\n",
                encoding="utf-8",
            )

            extractor = self.mod.SkillExtractor(
                technical_filepath=technical_path,
                keyword_filepath=keyword_path,
            )
            out = extractor.extract(
                "Experience with Python and Atlassian JIRA and Python preferred"
            )

        self.assertEqual(
            out,
            ["Python", "Atlassian JIRA"],
        )

    def test_extract_skills_from_tsv(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            technical_path = Path(tmpdir) / "technical_skills.csv"
            keyword_path = Path(tmpdir) / "tech_keywords.csv"
            technical_path.write_text(
                "Skill ID,Skill Name,Category\n"
                "1,Docker,Container software\n",
                encoding="utf-8",
            )

            extractor = self.mod.SkillExtractor(
                technical_filepath=technical_path,
                keyword_filepath=keyword_path,
            )
            out = extractor.extract("Docker experience required")

        self.assertEqual(
            out,
            ["Docker"],
        )

    def test_extract_uses_blank_model_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            technical_path = Path(tmpdir) / "technical_skills.csv"
            keyword_path = Path(tmpdir) / "tech_keywords.csv"
            technical_path.write_text(
                "Skill ID,Skill Name,Category\n"
                "1,Kubernetes,Container orchestration\n",
                encoding="utf-8",
            )

            extractor = self.mod.SkillExtractor(
                technical_filepath=technical_path,
                keyword_filepath=keyword_path,
                spacy_model="missing_model",
            )
            out = extractor.extract("Kubernetes platform experience")

        self.assertEqual(out[0], "Kubernetes")

    def test_extract_empty_text_returns_empty_list(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            technical_path = Path(tmpdir) / "technical_skills.csv"
            keyword_path = Path(tmpdir) / "tech_keywords.csv"
            technical_path.write_text(
                "Skill ID,Skill Name,Category\n"
                "1,Git,Version control\n",
                encoding="utf-8",
            )

            extractor = self.mod.SkillExtractor(
                technical_filepath=technical_path,
                keyword_filepath=keyword_path,
            )
            self.assertEqual(extractor.extract("   "), [])

    def test_init_raises_when_technical_file_missing(self) -> None:
        missing_path = Path("/tmp/does-not-exist-technical-skills.csv")
        with self.assertRaises(FileNotFoundError):
            self.mod.SkillExtractor(technical_filepath=missing_path)

    def test_load_technical_skips_blank_skill_name_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            technical_path = Path(tmpdir) / "technical_skills.csv"
            keyword_path = Path(tmpdir) / "tech_keywords.csv"
            technical_path.write_text(
                "Skill ID,Skill Name,Category\n"
                "1,,Programming language\n"
                "2,Python,Programming language\n",
                encoding="utf-8",
            )

            extractor = self.mod.SkillExtractor(
                technical_filepath=technical_path,
                keyword_filepath=keyword_path,
            )

        self.assertEqual(extractor.skills, {"Python"})

    def test_add_skills_skips_blank_values(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            technical_path = Path(tmpdir) / "technical_skills.csv"
            keyword_path = Path(tmpdir) / "tech_keywords.csv"
            technical_path.write_text(
                "Skill ID,Skill Name,Category\n"
                "1,Python,Programming language\n",
                encoding="utf-8",
            )

            extractor = self.mod.SkillExtractor(
                technical_filepath=technical_path,
                keyword_filepath=keyword_path,
            )
            extractor._add_skills(["", "   ", "Python"])

        self.assertEqual(extractor.skills, {"Python"})

    def test_load_technical_returns_when_optional_file_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            technical_path = Path(tmpdir) / "technical_skills.csv"
            keyword_path = Path(tmpdir) / "tech_keywords.csv"
            technical_path.write_text(
                "Skill ID,Skill Name,Category\n"
                "1,Python,Programming language\n",
                encoding="utf-8",
            )

            extractor = self.mod.SkillExtractor(
                technical_filepath=technical_path,
                keyword_filepath=keyword_path,
            )

            extractor._load_skill_rows(Path(tmpdir) / "missing.tsv", required=False)

        self.assertEqual(extractor.skills, {"Python"})

    def test_load_keywords_raises_when_required_file_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            technical_path = Path(tmpdir) / "technical_skills.csv"
            technical_path.write_text(
                "Skill ID,Skill Name,Category\n"
                "1,Python,Programming language\n",
                encoding="utf-8",
            )
            extractor = self.mod.SkillExtractor(
                technical_filepath=technical_path,
                keyword_filepath=Path(tmpdir) / "tech_keywords.csv",
            )

            with self.assertRaises(FileNotFoundError):
                extractor._load_keyword_rows(Path(tmpdir) / "missing.txt", required=True)

    def test_extract_skips_match_without_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            technical_path = Path(tmpdir) / "technical_skills.csv"
            keyword_path = Path(tmpdir) / "tech_keywords.csv"
            technical_path.write_text(
                "Skill ID,Skill Name,Category\n"
                "1,Python,Programming language\n",
                encoding="utf-8",
            )

            extractor = self.mod.SkillExtractor(
                technical_filepath=technical_path,
                keyword_filepath=keyword_path,
            )
            extractor.matcher = lambda doc: [(0, 0, 1)]
            extractor.lemma_matcher = lambda doc: []
            extractor._canonical_by_key.clear()
            extractor._canonical_by_lemma_key.clear()
            extractor.skills.clear()

            out = extractor.extract("Python")

        self.assertEqual(out, [])

    def test_extract_skips_lemma_match_without_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            technical_path = Path(tmpdir) / "technical_skills.csv"
            keyword_path = Path(tmpdir) / "tech_keywords.csv"
            technical_path.write_text(
                "Skill ID,Skill Name,Category\n"
                "1,Kubernetes,Container orchestration\n",
                encoding="utf-8",
            )

            extractor = self.mod.SkillExtractor(
                technical_filepath=technical_path,
                keyword_filepath=keyword_path,
            )
            extractor.matcher = lambda doc: []
            extractor.lemma_matcher = lambda doc: [(0, 0, 1)]
            extractor._canonical_by_lemma_key.clear()

            out = extractor.extract("Kubernetes")

        self.assertEqual(out, [])

    def test_extract_merges_onet_and_custom_sources(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            technical_path = Path(tmpdir) / "technical_skills.csv"
            keyword_path = Path(tmpdir) / "tech_keywords.csv"
            technical_path.write_text(
                "Skill ID,Skill Name,Category\n"
                "1,Python,Programming language\n"
                "2,SQL,Databases\n",
                encoding="utf-8",
            )

            extractor = self.mod.SkillExtractor(
                technical_filepath=technical_path,
                keyword_filepath=keyword_path,
            )
            out = extractor.extract("Python and SQL experience required")

        self.assertEqual(
            out,
            ["Python", "SQL"],
        )

    def test_extract_uses_custom_technical_skills_second_column(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            technical_path = Path(tmpdir) / "technical_skills.csv"
            keyword_path = Path(tmpdir) / "tech_keywords.csv"
            technical_path.write_text(
                "Skill ID,Skill Name,Category\n"
                "1,Terraform,DevOps\n",
                encoding="utf-8",
            )

            extractor = self.mod.SkillExtractor(
                technical_filepath=technical_path,
                keyword_filepath=keyword_path,
            )
            out = extractor.extract("Experience with Terraform required")

        self.assertEqual(
            out,
            ["Terraform"],
        )

    def test_extract_uses_newline_keyword_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            technical_path = Path(tmpdir) / "technical_skills.csv"
            keyword_path = Path(tmpdir) / "tech_keywords.csv"
            technical_path.write_text("Skill ID,Skill Name,Category\n", encoding="utf-8")
            keyword_path.write_text("BitBake\nJSON\n", encoding="utf-8")

            extractor = self.mod.SkillExtractor(
                technical_filepath=technical_path,
                keyword_filepath=keyword_path,
            )
            out = extractor.extract("BitBake and JSON experience required")

        self.assertEqual(
            out,
            ["BitBake", "JSON"],
        )

    def test_extract_uses_lemma_matching_for_plural(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            technical_path = Path(tmpdir) / "technical_skills.csv"
            keyword_path = Path(tmpdir) / "tech_keywords.csv"
            technical_path.write_text("Skill ID,Skill Name,Category\n", encoding="utf-8")
            keyword_path.write_text("Software Engineer\n", encoding="utf-8")

            extractor = self.mod.SkillExtractor(
                technical_filepath=technical_path,
                keyword_filepath=keyword_path,
            )
            out = extractor.extract("Software Engineers build systems")

        self.assertEqual(out, ["Software Engineer"])


if __name__ == "__main__":
    unittest.main()
