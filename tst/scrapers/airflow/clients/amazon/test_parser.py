import unittest

from scrapers.airflow.clients.amazon import parser


class AmazonParserTest(unittest.TestCase):
    def test_scalar_helpers(self) -> None:
        self.assertEqual(parser.to_optional_str(" a "), "a")
        self.assertIsNone(parser.to_optional_str(1))
        self.assertEqual(parser.to_int("12"), 12)
        self.assertIsNone(parser.to_int("x"))
        self.assertEqual(parser.dedupe(["a", "a", "b"]), ["a", "b"])

    def test_urls_and_dates(self) -> None:
        self.assertEqual(
            parser.build_details_url(job_path="/en/jobs/1", job_id="1", base_url="https://x"),
            "https://x/en/jobs/1",
        )
        self.assertEqual(
            parser.build_apply_url(job_id="1", raw_apply_url=None, base_url="https://x"),
            "https://x/applicant/jobs/1/apply",
        )
        self.assertIsNotNone(parser.parse_posted_ts("January 01, 2024"))
        self.assertIsNone(parser.parse_posted_ts("bad-date"))

    def test_extract_locations_and_lists(self) -> None:
        payload = {"locations": [{"city": "Seattle", "state": "WA", "countryIso3a": "USA"}]}
        locations = parser.extract_locations(payload)
        self.assertEqual(len(locations), 1)
        self.assertEqual(locations[0].city, "Seattle")
        payload_json_locations = {"locations": ['{"city":"SF","state":"CA","countryIso3a":"USA"}']}
        locations_json = parser.extract_locations(payload_json_locations)
        self.assertEqual(locations_json[0].city, "SF")
        fallback = parser.extract_locations({"location": "Seattle, WA, USA"})
        self.assertEqual(fallback[0].country, "USA")

        details = parser.coerce_detail_list("<li>a</li><li>b</li>")
        self.assertEqual(details, ["a", "b"])
        self.assertEqual(parser.coerce_detail_list(["<li>a</li>", "<li>b</li>"]), ["a", "b"])
        self.assertEqual(parser.coerce_detail_list(None), [])
        self.assertEqual(parser.extract_section("<h2>Key job responsibilities</h2><p>x</p>", heading="Key job responsibilities"), "<p>x</p>")
        self.assertIsNone(parser.extract_section("abc", heading="x"))

    def test_parse_job_metadata_and_details(self) -> None:
        metadata = parser.parse_job_metadata(
            payload={
                "id_icims": "1",
                "title": "SWE",
                "job_path": "/en/jobs/1",
                "url_next_step": "/applicant/jobs/1/apply",
                "posted_date": "January 01, 2024",
                "location": "Seattle, WA, USA",
            },
            base_url="https://www.amazon.jobs",
        )
        self.assertEqual(metadata.id, "1")
        self.assertEqual(metadata.company, "amazon")
        self.assertIn("/en/jobs/1", metadata.detailsUrl)

        details = parser.parse_job_details(
            payload={
                "description": "<h2>Key job responsibilities</h2><li>Do things</li>",
                "basic_qualifications": "<li>Min 1</li>",
                "preferred_qualifications": "<li>Pref 1</li>",
            }
        )
        self.assertIn("Min 1", details.minimumQualifications)
        self.assertIn("Pref 1", details.preferredQualifications)
        self.assertIn("Do things", details.responsibilities)
        self.assertEqual(parser.clean_html_fragment("<p>a</p>"), "a")

    def test_apply_url_variants(self) -> None:
        base = "https://www.amazon.jobs"
        self.assertEqual(
            parser.build_apply_url(job_id="1", raw_apply_url="/applicant/jobs/1/apply", base_url=base),
            "https://www.amazon.jobs/applicant/jobs/1/apply",
        )
        self.assertEqual(
            parser.build_apply_url(job_id="1", raw_apply_url="http://x", base_url=base),
            "http://x",
        )
        self.assertEqual(
            parser.build_apply_url(job_id="1", raw_apply_url="/jobs/1/apply", base_url=base),
            "https://www.amazon.jobs/applicant/jobs/1/apply",
        )

    def test_additional_edge_paths(self) -> None:
        with self.assertRaises(ValueError):
            parser.parse_job_metadata(payload={"id_icims": " "}, base_url="https://x")
        self.assertEqual(
            parser.build_details_url(job_path="jobs/1", job_id="1", base_url="https://x"),
            "https://x/jobs/1",
        )
        self.assertEqual(
            parser.build_details_url(job_path="https://x/jobs/1", job_id="1", base_url="https://x"),
            "https://x/jobs/1",
        )
        self.assertEqual(
            parser.build_apply_url(job_id="1", raw_apply_url="/raw/path", base_url="https://x"),
            "https://x/raw/path",
        )
        self.assertEqual(
            parser.build_apply_url(
                job_id="1",
                raw_apply_url="https://account.amazon.com/jobs/1/apply",
                base_url="https://x",
            ),
            "https://x/applicant/jobs/1/apply",
        )
        self.assertEqual(
            parser.build_apply_url(job_id="1", raw_apply_url="relative/path", base_url="https://x"),
            "relative/path",
        )
        self.assertIsNone(parser.build_apply_url(job_id=" ", raw_apply_url=None, base_url="https://x"))
        self.assertIsNotNone(parser.parse_posted_ts("Jan 01, 2024"))

        parsed_from_fallback = parser.extract_locations({"location": "USA, Seattle"})
        self.assertEqual(parsed_from_fallback[0].country, "USA")
        self.assertEqual(parsed_from_fallback[0].city, "Seattle")
        self.assertEqual(parser.extract_locations({"location": "USA"}), [parser.Location(city="", state="", country="USA")])
        self.assertEqual(parser.extract_locations({"locations": ["bad-json"]}), [])

        self.assertEqual(parser.coerce_detail_list("- a\n- b"), ["a", "b"])
        self.assertEqual(parser.coerce_detail_list("   "), [])
        self.assertEqual(parser.coerce_detail_list("<li> </li><li>x</li>"), ["x"])
        self.assertIsNone(parser.extract_section(None, heading="x"))


if __name__ == "__main__":
    unittest.main()
