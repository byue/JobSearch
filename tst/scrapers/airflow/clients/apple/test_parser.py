import unittest
from unittest.mock import patch
import json

from scrapers.airflow.clients.apple import parser


class AppleParserTest(unittest.TestCase):
    def test_extract_hydration_payload(self) -> None:
        html_payload = 'window.__staticRouterHydrationData = JSON.parse("{\\"loaderData\\":{\\"search\\":{\\"searchResults\\":[]}}}");'
        out = parser.extract_hydration_payload(html_payload=html_payload, context="search")
        self.assertIn("loaderData", out)
        with self.assertRaises(ValueError):
            parser.extract_hydration_payload(html_payload="nope", context="search")
        with self.assertRaises(ValueError):
            parser.require_mapping([], context="x")
        bad_unicode_escape = 'window.__staticRouterHydrationData = JSON.parse("\\\\uZZZZ");'
        with self.assertRaises(Exception):
            parser.extract_hydration_payload(html_payload=bad_unicode_escape, context="search")

    def test_parse_job_metadata_and_details(self) -> None:
        metadata = parser.parse_job_metadata(
            payload={
                "positionId": "1",
                "postingTitle": "Engineer",
                "transformedPostingTitle": "engineer",
                "locations": [{"city": "Cupertino", "stateProvince": "CA", "countryName": "USA"}],
                "postDateInGMT": "2024-01-01T00:00:00Z",
            },
            base_url="https://jobs.apple.com",
            locale="en-us",
        )
        self.assertEqual(metadata.id, "1")
        self.assertEqual(metadata.company, "apple")

        details = parser.parse_job_details(
            payload={
                "jobSummary": "Summary",
                "description": "Description",
                "minimumQualifications": "<li>Min</li>",
                "preferredQualifications": "<li>Pref</li>",
                "responsibilities": "<li>Resp</li>",
            }
        )
        self.assertIn("Min", details.minimumQualifications)
        self.assertIn("Pref", details.preferredQualifications)
        self.assertIn("Resp", details.responsibilities)

    def test_misc_helpers(self) -> None:
        self.assertEqual(parser.slugify_title("Hello World!"), "hello-world")
        self.assertEqual(parser.slugify_title("!!!"), "job")
        self.assertIsNotNone(parser.parse_posting_date("Jan 01, 2024"))
        self.assertIsNone(parser.parse_posting_date("bad"))
        self.assertIsNotNone(parser.parse_posted_ts("2024-01-01T00:00:00Z"))
        self.assertIsNone(parser.parse_posted_ts("bad"))
        self.assertEqual(
            parser.build_details_url(base_url="https://jobs.apple.com", locale="en-us", job_id="1", transformed_title="engineer"),
            "https://jobs.apple.com/en-us/details/1/engineer",
        )
        self.assertEqual(parser.extract_locations("bad"), [])
        self.assertEqual(parser.coerce_detail_list(None), [])

    def test_additional_branches(self) -> None:
        self.assertEqual(parser.to_int("-10"), -10)
        self.assertIsNone(parser.to_int([]))
        self.assertEqual(parser.dedupe(["a", "a", "b"]), ["a", "b"])
        with self.assertRaises(ValueError):
            parser.parse_job_metadata(payload={"positionId": " "}, base_url="https://jobs.apple.com", locale="en-us")

        with_fallback = parser.extract_locations([{"name": "United States"}])
        self.assertEqual(with_fallback[0].country, "United States")
        self.assertEqual(parser.extract_locations([1, {"city": "X"}])[0].city, "X")
        html_payload = 'window.__staticRouterHydrationData = JSON.parse("{\\"a\\":1}");'
        with patch(
            "scrapers.airflow.clients.apple.parser.json.loads",
            side_effect=[json.JSONDecodeError("x", '"\\"', 0), {"a": 1}],
        ):
            out = parser.extract_hydration_payload(html_payload=html_payload, context="x")
            self.assertEqual(out["a"], 1)


if __name__ == "__main__":
    unittest.main()
