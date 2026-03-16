from __future__ import annotations

import unittest

try:
    from pydantic import ValidationError
    from features.schemas import (
        ExtractJobSkillsRequest,
        ExtractJobSkillsResponse,
        ExtractQueryEmbeddingRequest,
        ExtractQueryEmbeddingResponse,
        NormalizedLocation,
        NormalizeLocationsRequest,
        NormalizeLocationsResponse,
    )
except ModuleNotFoundError:  # pragma: no cover - depends on local env
    ValidationError = None


@unittest.skipIf(ValidationError is None, "pydantic is not installed")
class FeaturesSchemasTest(unittest.TestCase):
    def test_request_models_validate(self) -> None:
        self.assertEqual(ExtractJobSkillsRequest(text="x").text, "x")
        self.assertEqual(ExtractQueryEmbeddingRequest(text="y").text, "y")
        self.assertEqual(NormalizeLocationsRequest(locations=["Seattle, WA, USA"]).locations, ["Seattle, WA, USA"])
        with self.assertRaises(ValidationError):
            ExtractJobSkillsRequest(text="")
        with self.assertRaises(ValidationError):
            ExtractQueryEmbeddingRequest(text="")

    def test_response_models_defaults(self) -> None:
        skills_response = ExtractJobSkillsResponse(skills=["Python"])
        self.assertEqual(skills_response.status, 200)
        self.assertEqual(skills_response.embedding, [])

        embedding_response = ExtractQueryEmbeddingResponse()
        self.assertEqual(embedding_response.embedding, [])

        normalized = NormalizedLocation(city="Seattle", region="Washington", country="United States")
        response = NormalizeLocationsResponse(locations=[normalized])
        self.assertEqual(response.status, 200)
        self.assertEqual(response.locations[0].city, "Seattle")
        self.assertEqual(response.locations[0].region, "Washington")
        self.assertEqual(response.locations[0].country, "United States")


if __name__ == "__main__":
    unittest.main()
