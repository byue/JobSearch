import unittest

from scrapers.airflow.clients.microsoft import parser


class MicrosoftParserTest(unittest.TestCase):
    def test_scalar_helpers(self) -> None:
        self.assertEqual(parser.string_list([" a ", "", 1]), ["a"])
        self.assertEqual(parser.string_list(" a "), ["a"])
        self.assertEqual(parser.string_list(None), [])
        self.assertEqual(parser.to_optional_str(" x "), "x")
        self.assertEqual(parser.to_int("10"), 10)
        self.assertIsNone(parser.to_int("bad"))
        self.assertEqual(parser.normalize_url("http://x"), "https://x")
        self.assertEqual(parser.dedupe(["a", "a"]), ["a"])
        with self.assertRaises(ValueError):
            parser.require_non_empty_string_field({}, field="name", context="x")

    def test_parse_job_metadata_and_details(self) -> None:
        metadata = parser.parse_job_metadata(
            payload={
                "id": "1",
                "name": "Engineer",
                "postedTs": 1700000000,
                "standardizedLocations": ["Seattle, WA, USA"],
                "positionUrl": "/careers/job/1",
            },
            base_url="https://apply.careers.microsoft.com",
        )
        self.assertEqual(metadata.id, "1")
        self.assertEqual(metadata.company, "microsoft")

        details = parser.parse_job_details(
            payload={
                "jobDescription": "desc",
                "minimumQualifications": "<li>Min</li>",
                "preferredQualifications": "<li>Pref</li>",
                "responsibilities": "<li>Resp</li>",
            }
        )
        self.assertIn("Min", details.minimumQualifications)
        self.assertIn("Pref", details.preferredQualifications)
        self.assertIn("Resp", details.responsibilities)

    def test_misc_helpers(self) -> None:
        locs = parser.to_locations(["Seattle, WA, USA"])
        self.assertEqual(locs[0].city, "Seattle")
        self.assertEqual(parser.build_apply_url(job_id="1", base_url="https://x"), "https://x/careers/apply?pid=1")
        self.assertIsNone(parser.build_apply_url(job_id=" ", base_url="https://x"))
        self.assertEqual(
            parser.build_details_url(position_url="/careers/job/1", base_url="https://x"),
            "https://x/careers/job/1",
        )
        self.assertEqual(
            parser.build_details_url(public_url="https://public", base_url="https://x"),
            "https://public",
        )
        self.assertIsNone(parser.build_details_url(position_url=None, public_url=None, job_id=" ", base_url="https://x"))
        self.assertEqual(parser.coerce_detail_list("<li>a</li><li>b</li>"), ["a", "b"])
        self.assertEqual(parser.coerce_detail_list(["<li>a</li>", "<li>b</li>"]), ["a", "b"])
        self.assertEqual(parser.clean_html_fragment("<p>a</p>"), "a")

    def test_additional_branches(self) -> None:
        self.assertIsNone(parser.to_optional_str(None))
        self.assertEqual(parser.to_locations(["State, Country"])[0].state, "State")
        self.assertEqual(parser.to_locations(["Country"])[0].country, "Country")
        with self.assertRaises(ValueError):
            parser.parse_job_metadata(payload={"id": " ", "name": "n", "postedTs": 1}, base_url="https://x")
        self.assertEqual(
            parser.build_details_url(position_url="careers/job/1", base_url="https://x"),
            "https://x/careers/job/1",
        )
        self.assertEqual(
            parser.build_details_url(position_url="https://x/careers/job/1", base_url="https://x"),
            "https://x/careers/job/1",
        )
        self.assertEqual(
            parser.build_details_url(position_url=None, public_url="http://public", base_url="https://x"),
            "https://public",
        )
        with self.assertRaises(ValueError):
            parser.parse_job_metadata(payload={"id": "1", "name": "n", "postedTs": "bad"}, base_url="https://x")
        self.assertEqual(parser.coerce_detail_list("- a\n• b"), ["- a", "b"])
        self.assertEqual(parser.coerce_detail_list(" "), [])
        self.assertEqual(parser.coerce_detail_list("<li> </li><li>a</li><li>a</li>"), ["a"])


if __name__ == "__main__":
    unittest.main()
