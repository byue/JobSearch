import unittest
from unittest.mock import patch

from scrapers.airflow.clients.amazon import parser


class AmazonParserTest(unittest.TestCase):
    def test_scalar_helpers(self) -> None:
        self.assertEqual(parser.to_optional_str(" a "), "a")
        self.assertIsNone(parser.to_optional_str(1))
        self.assertEqual(parser.to_int(12), 12)
        self.assertEqual(parser.to_int("12"), 12)
        self.assertIsNone(parser.to_int("x"))
        self.assertEqual(parser.dedupe(["a", "a", "b"]), ["a", "b"])

    def test_urls_and_dates(self) -> None:
        self.assertEqual(
            parser.build_details_path(job_path="/en/jobs/1", job_id="1"),
            "/en/jobs/1",
        )
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
        self.assertIsNone(parser.parse_posted_ts(" "))

    def test_extract_location_strings(self) -> None:
        payload = {
            "locations": [
                '{"normalizedLocation":"Seattle, Washington, USA"}',
                '{"normalizedLocation":"New York, New York, USA"}',
            ]
        }
        self.assertEqual(
            parser.extract_location_strings(payload),
            ["Seattle, Washington, USA", "New York, New York, USA"],
        )
        self.assertEqual(
            parser.extract_location_strings({"locations": [1, '{"normalizedLocation":"San Francisco, California, USA"}']}),
            ["San Francisco, California, USA"],
        )
        self.assertEqual(parser.extract_location_strings({"location": "Seattle, WA, USA"}), [])

    def test_parse_job_metadata_and_details(self) -> None:
        metadata = parser.parse_job_metadata(
            payload={
                "id_icims": "1",
                "title": "SWE",
                "job_category": "Software Development",
                "job_path": "/en/jobs/1",
                "url_next_step": "/applicant/jobs/1/apply",
                "posted_date": "January 01, 2024",
            },
            base_url="https://www.amazon.jobs",
            locations=[parser.Location(city="Seattle", state="Washington", country="United States")],
        )
        self.assertEqual(metadata.id, "1")
        self.assertEqual(metadata.company, "amazon")
        self.assertIsNone(metadata.jobCategory)
        self.assertIn("/en/jobs/1", metadata.detailsUrl)

        mle_metadata = parser.parse_job_metadata(
            payload={
                "id_icims": "2",
                "title": "Machine Learning Engineer II",
                "job_category": "Software Development",
            },
            base_url="https://www.amazon.jobs",
        )
        self.assertEqual(mle_metadata.jobCategory, "machine_learning_engineer")

        details = parser.parse_job_details(
            payload={
                "description": "<h2>Key job responsibilities</h2><li>Do things</li>",
                "basic_qualifications": "<li>Min 1</li>",
                "preferred_qualifications": "<li>Pref 1</li>",
            }
        )
        self.assertEqual(parser.clean_html_fragment("<p>a</p>"), "a")
        self.assertIn("Key job responsibilities", details.jobDescription or "")
        self.assertIn("Do things", details.jobDescription or "")
        self.assertNotIn("Basic Qualifications", details.jobDescription or "")

        details_from_br = parser.parse_job_details(
            payload={
                "description": "Intro<br/><br/>Key job responsibilities<br/> - Do things<br/> - Ship code",
                "basic_qualifications": "<li>Min 1</li>",
                "preferred_qualifications": "<li>Pref 1</li>",
            }
        )
        self.assertIn("Intro", details_from_br.jobDescription or "")
        self.assertIn("Key job responsibilities", details_from_br.jobDescription or "")
        self.assertIn("- Do things", details_from_br.jobDescription or "")
        self.assertIn("- Ship code", details_from_br.jobDescription or "")

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
            parser.build_details_path(job_path="jobs/1", job_id="1"),
            "/jobs/1",
        )
        self.assertEqual(
            parser.build_details_path(job_path="https://x/jobs/1", job_id="1"),
            "/jobs/1",
        )
        self.assertEqual(
            parser.build_details_path(job_path="https://x/jobs/1?locale=en_US", job_id="1"),
            "/jobs/1?locale=en_US",
        )
        self.assertEqual(
            parser.build_details_path(job_path=None, job_id="1"),
            "/en/jobs/1",
        )
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

        self.assertEqual(parser.extract_location_strings({"location": "USA, Seattle"}), [])
        self.assertEqual(parser.extract_location_strings({"location": "USA"}), [])
        self.assertEqual(parser.extract_location_strings({"locations": ["bad-json"]}), [])
        self.assertEqual(parser.extract_location_strings({"locations": ['["not-a-mapping"]']}), [])

        self.assertEqual(parser.clean_html_fragment("   "), "")

    def test_render_job_description_helper_branches(self) -> None:
        self.assertIsNone(parser.render_job_description("<<<"))
        with patch.object(parser.lxml_html, "fromstring", side_effect=ValueError("bad html")):
            self.assertIsNone(parser.render_job_description("<html></html>"))
        self.assertIsNone(parser.render_job_description("<html><body><h1 class='title'>Role</h1></body></html>"))

        heading_only = parser.render_job_description(
            "<html><body><h1 class='title'>Role</h1><div id='job-detail-body'><div class='content'><div class='section'><h2>Only Heading</h2></div></div></div></body></html>"
        )
        self.assertEqual(heading_only, "Role\n\nOnly Heading")

        body_only = parser.render_job_description(
            "<html><body><div id='job-detail-body'><div class='content'><div class='section'><p>Only Body</p></div></div></div></body></html>"
        )
        self.assertEqual(body_only, "Only Body")
        heading_and_body = parser.render_job_description(
            "<html><body><div id='job-detail-body'><div class='content'><div class='section'><h2>Heading</h2><p>Body</p></div></div></div></body></html>"
        )
        self.assertEqual(heading_and_body, "Heading\nBody")
        skip_empty = parser.render_job_description(
            "<html><body><div id='job-detail-body'><div class='content'><div class='section'></div><div class='section'><p>Body</p></div></div></div></body></html>"
        )
        self.assertEqual(skip_empty, "Body")

        self.assertIsNone(parser._first_text([object()]))
        self.assertIsNone(parser._render_section_body(object()))
        self.assertIsNone(parser._render_child_block(object()))
        section = parser.lxml_html.fromstring("<div><p>Body</p><!--c--></div>")
        self.assertEqual(parser._render_section_body(section), "Body")
        self.assertIsNone(parser._render_child_block(parser.lxml_html.fromstring("<ul></ul>")))

        rendered = parser._render_child_block(
            parser.lxml_html.fromstring("<p>Line 1<br/><br/>Line 2</p>")
        )
        self.assertEqual(rendered, "Line 1\n\nLine 2")


if __name__ == "__main__":
    unittest.main()
