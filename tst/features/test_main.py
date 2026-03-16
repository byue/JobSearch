from __future__ import annotations

import os
import sys
import types
import unittest
from unittest.mock import patch

try:
    import features.main as main_mod
    from fastapi.testclient import TestClient
    from features.main import app
except ModuleNotFoundError:  # pragma: no cover - depends on local env
    main_mod = None
    TestClient = None
    app = None


@unittest.skipIf(main_mod is None, "fastapi test dependencies are not installed")
class FeaturesMainHelpersTest(unittest.TestCase):
    def test_env_helpers_fallback_and_override(self) -> None:
        with patch.dict(os.environ, {}, clear=False):
            self.assertEqual(main_mod._technical_filepath(), main_mod._DEFAULT_TECHNICAL_PATH)
            self.assertEqual(main_mod._keyword_filepath(), main_mod._DEFAULT_KEYWORD_PATH)
            self.assertEqual(main_mod._spacy_model(), main_mod.DEFAULT_SPACY_MODEL)
            self.assertEqual(main_mod._embedding_model_name(), main_mod._DEFAULT_EMBEDDING_MODEL)

        with patch.dict(
            os.environ,
            {
                "JOBSEARCH_FEATURES_TECHNICAL_PATH": " /tmp/technical.csv ",
                "JOBSEARCH_FEATURES_KEYWORD_PATH": " /tmp/keywords.csv ",
                "JOBSEARCH_FEATURES_SPACY_MODEL": " custom-model ",
                "JOBSEARCH_FEATURES_EMBEDDING_MODEL": " custom-embedding ",
            },
            clear=False,
        ):
            self.assertEqual(main_mod._technical_filepath(), "/tmp/technical.csv")
            self.assertEqual(main_mod._keyword_filepath(), "/tmp/keywords.csv")
            self.assertEqual(main_mod._spacy_model(), "custom-model")
            self.assertEqual(main_mod._embedding_model_name(), "custom-embedding")

    def test_skill_extractor_is_cached(self) -> None:
        main_mod._skill_extractor.cache_clear()
        main_mod._embedding_model.cache_clear()
        extractor = object()
        embedding_model = object()
        with patch.object(main_mod, "SkillExtractor", return_value=extractor) as factory, patch.dict(
            os.environ,
            {},
            clear=False,
        ), patch.object(main_mod, "_text_embedding_class", return_value=lambda **kwargs: embedding_model) as embedding_factory:
            self.assertIs(main_mod._skill_extractor(), extractor)
            self.assertIs(main_mod._skill_extractor(), extractor)
            self.assertIs(main_mod._embedding_model(), embedding_model)
            self.assertIs(main_mod._embedding_model(), embedding_model)
        factory.assert_called_once_with(
            technical_filepath=main_mod._DEFAULT_TECHNICAL_PATH,
            keyword_filepath=main_mod._DEFAULT_KEYWORD_PATH,
            spacy_model=main_mod.DEFAULT_SPACY_MODEL,
        )
        embedding_factory.assert_called_once_with()
        main_mod._skill_extractor.cache_clear()
        main_mod._embedding_model.cache_clear()

    def test_text_embedding_class_imports_fastembed(self) -> None:
        main_mod._text_embedding_class.cache_clear()
        saved_fastembed = sys.modules.get("fastembed")
        fastembed_mod = types.ModuleType("fastembed")

        class _FakeTextEmbedding:
            pass

        fastembed_mod.TextEmbedding = _FakeTextEmbedding
        sys.modules["fastembed"] = fastembed_mod
        try:
            self.assertIs(main_mod._text_embedding_class(), _FakeTextEmbedding)
        finally:
            main_mod._text_embedding_class.cache_clear()
            if saved_fastembed is None:
                sys.modules.pop("fastembed", None)
            else:
                sys.modules["fastembed"] = saved_fastembed

    def test_extract_embedding_returns_empty_when_model_returns_no_vectors(self) -> None:
        fake_embedding_model = type(
            "FakeEmbeddingModel",
            (),
            {
                "embed": lambda self, texts: [],
            },
        )()
        with patch.object(main_mod, "_embedding_model", return_value=fake_embedding_model):
            self.assertEqual(main_mod._extract_embedding("Need Python"), [])

    def test_extract_embedding_uses_tolist_when_available(self) -> None:
        class _FakeVector:
            def tolist(self) -> list[float]:
                return [0.1, 2, -0.3]

        fake_embedding_model = type(
            "FakeEmbeddingModel",
            (),
            {
                "embed": lambda self, texts: [_FakeVector()],
            },
        )()
        with patch.object(main_mod, "_embedding_model", return_value=fake_embedding_model):
            self.assertEqual(main_mod._extract_embedding("Need Python"), [0.1, 2.0, -0.3])

    def test_normalize_location_helper(self) -> None:
        city, state, country = main_mod.normalize_location("Seattle, WA, USA")
        self.assertEqual((city, state, country), ("Seattle", "Washington", "United States"))

        city, state, country = main_mod.normalize_location("London, UK")
        self.assertEqual((city, state, country), ("London", None, "United Kingdom"))

        city, state, country = main_mod.normalize_location("Remote")
        self.assertEqual((city, state, country), (None, None, None))


@unittest.skipIf(TestClient is None or app is None, "fastapi test dependencies are not installed")
class FeaturesBackendTest(unittest.TestCase):
    def test_get_job_skills_endpoint(self) -> None:
        fake_extractor = type(
            "FakeExtractor",
            (),
            {
                "technical_filepath": "/tmp/technical_skills.csv",
                "keyword_filepath": "/tmp/tech_keywords.csv",
                "skills": {"Python"},
                "extract": lambda self, text: ["Python"],
            },
        )()
        fake_embedding_model = type(
            "FakeEmbeddingModel",
            (),
            {
                "embed": lambda self, texts: [[0.1, -0.2]],
            },
        )()

        with patch("features.main._skill_extractor", return_value=fake_extractor), patch(
            "features.main._embedding_model", return_value=fake_embedding_model
        ):
            with TestClient(app) as client:
                response = client.post("/job_skills", json={"text": "Need Python"})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json(),
            {
                "status": 200,
                "error": None,
                "skills": ["Python"],
                "embedding": [0.1, -0.2],
            },
        )

    def test_get_job_skills_endpoint_validates_text(self) -> None:
        fake_extractor = type(
            "FakeExtractor",
            (),
            {
                "technical_filepath": "/tmp/technical_skills.csv",
                "keyword_filepath": "/tmp/tech_keywords.csv",
                "skills": set(),
                "extract": lambda self, text: [],
            },
        )()
        fake_embedding_model = type(
            "FakeEmbeddingModel",
            (),
            {
                "embed": lambda self, texts: [[]],
            },
        )()
        with patch("features.main._skill_extractor", return_value=fake_extractor), patch(
            "features.main._embedding_model", return_value=fake_embedding_model
        ):
            with TestClient(app) as client:
                response = client.post("/job_skills", json={"text": ""})
        self.assertEqual(response.status_code, 422)

    def test_get_query_embedding_endpoint(self) -> None:
        fake_extractor = type(
            "FakeExtractor",
            (),
            {
                "technical_filepath": "/tmp/technical_skills.csv",
                "keyword_filepath": "/tmp/tech_keywords.csv",
                "skills": set(),
                "extract": lambda self, text: [],
            },
        )()
        fake_embedding_model = type(
            "FakeEmbeddingModel",
            (),
            {
                "embed": lambda self, texts: [[0.1, -0.2]],
            },
        )()
        with patch("features.main._skill_extractor", return_value=fake_extractor), patch(
            "features.main._embedding_model", return_value=fake_embedding_model
        ):
            with TestClient(app) as client:
                response = client.post("/query_embedding", json={"text": "Need Python"})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json(),
            {
                "status": 200,
                "error": None,
                "embedding": [0.1, -0.2],
            },
        )

    def test_normalize_locations_endpoint(self) -> None:
        fake_extractor = type(
            "FakeExtractor",
            (),
            {
                "technical_filepath": "/tmp/technical_skills.csv",
                "keyword_filepath": "/tmp/tech_keywords.csv",
                "skills": set(),
                "extract": lambda self, text: [],
            },
        )()
        fake_embedding_model = type(
            "FakeEmbeddingModel",
            (),
            {
                "embed": lambda self, texts: [[]],
            },
        )()
        with patch("features.main._skill_extractor", return_value=fake_extractor), patch(
            "features.main._embedding_model", return_value=fake_embedding_model
        ):
            with TestClient(app) as client:
                response = client.post(
                    "/normalize_locations",
                    json={"locations": ["Seattle, WA, USA", "London, UK", "Remote"]},
                )

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["status"], 200)
        self.assertEqual(body["error"], None)
        self.assertEqual(
            body["locations"],
            [
                {"city": "Seattle", "region": "Washington", "country": "United States"},
                {"city": "London", "region": None, "country": "United Kingdom"},
                {"city": None, "region": None, "country": None},
            ],
        )


if __name__ == "__main__":
    unittest.main()
