import unittest

from scrapers.airflow.clients.google import parser


class GoogleParserTest(unittest.TestCase):
    def test_extract_ds_payloads(self) -> None:
        html_payload = "AF_initDataCallback({key: 'ds:1', hash: 'x', data:[[[]],null,1,10], sideChannel: {}});"
        rows, total, page_size = parser.extract_rows(html_payload)
        self.assertEqual(rows, [[]])
        self.assertEqual(total, 1)
        self.assertEqual(page_size, 10)
        with self.assertRaises(ValueError):
            parser.extract_ds1_payload("no ds1")

        ds0_html = "AF_initDataCallback({key: 'ds:0', hash: 'x', data:[[\"job1\"]], sideChannel: {}});"
        self.assertEqual(parser.extract_row_from_ds0(ds0_html), ["job1"])
        self.assertIsNone(parser.extract_row_from_ds0("no ds0"))
        self.assertIsNone(parser.extract_row_from_ds0("AF_initDataCallback({key: 'ds:0', hash: 'x', data:[], sideChannel: {}});"))
        self.assertIsNone(
            parser.extract_row_from_ds0("AF_initDataCallback({key: 'ds:0', hash: 'x', data:bad-json, sideChannel: {}});")
        )

    def test_parse_job_metadata_and_details(self) -> None:
        row = ["id1", "Engineer", "https://apply", None, None, None, None, None, None, ["Seattle, WA, USA"], None, None, 1700000000]
        metadata = parser.parse_job_metadata(row=row, page=1, base_url="https://google.com", results_path="/about/careers/jobs/results/")
        self.assertEqual(metadata.id, "id1")
        self.assertEqual(metadata.company, "google")

        details_row = [
            "id1",
            "Engineer",
            "https://apply",
            "<li>Build</li>",
            "<h2>Minimum Qualifications</h2><li>Min</li><h2>Preferred Qualifications</h2><li>Pref</li>",
            None,
            None,
            None,
            None,
            None,
            "<p>Description</p>",
            None,
            1700000000,
        ]
        details = parser.parse_job_details(row=details_row)
        self.assertEqual(
            details.jobDescription,
            "Engineer\n\nAbout the job\nDescription\n\nMinimum Qualifications\nMin\n\nPreferred Qualifications\nPref\n\nResponsibilities\nBuild",
        )

    def test_misc_helpers(self) -> None:
        self.assertEqual(parser.as_str(1), "1")
        self.assertEqual(parser.as_str("x"), "x")
        self.assertEqual(parser.as_str(None), "")
        self.assertEqual(parser.extract_locations(["a", ["b"]]), ["a", "b"])
        self.assertEqual(parser.extract_locations("bad"), [])
        self.assertEqual(parser.extract_ts_seconds([10]), 10)
        self.assertIsNone(parser.extract_ts_seconds("bad"))
        self.assertEqual(parser.extract_html_text([None, "<p>x</p>"]), "<p>x</p>")
        self.assertEqual(parser.extract_html_text("x"), "x")
        self.assertEqual(parser.clean_html_fragment("<p>a</p>"), "a")
        self.assertTrue(parser.has_next_page(page=1, jobs_count=10, total_results=20, page_size=10))
        self.assertFalse(parser.has_next_page(page=1, jobs_count=0, total_results=None, page_size=None))
        self.assertEqual(parser.extract_rows("AF_initDataCallback({key: 'ds:1', hash: 'x', data:[\"bad\"], sideChannel: {}});")[0], [])
        self.assertEqual(parser.extract_rows("AF_initDataCallback({key: 'ds:1', hash: 'x', data:[[[]],\"x\",\"y\",\"z\"], sideChannel: {}});")[1:], (None, None))
        self.assertIn("?page=2", parser.build_public_url(job_id="1", name="A B", page=2, base_url="https://x", results_path="/r/"))
        self.assertEqual(parser.to_locations(["OnlyCountry"])[0].country, "OnlyCountry")
        self.assertEqual(parser.to_locations(["State, Country"])[0].state, "State")
        self.assertFalse(parser.has_next_page(page=1, jobs_count=5, total_results=None, page_size=10))

    def test_qualification_and_list_formatting_helpers(self) -> None:
        self.assertIsNone(parser._format_section("About the job", None))
        self.assertIsNone(parser._format_qualifications(None))
        self.assertIsNone(parser._format_qualifications("   "))
        self.assertIsNone(parser._format_qualifications("\n \n"))
        self.assertIsNone(parser._format_qualifications("\n\t\n"))
        self.assertEqual(
            parser._format_qualifications("Minimum qualifications:\nMin\nPreferred qualifications:\nPref"),
            "Minimum Qualifications\nMin\n\nPreferred Qualifications\nPref",
        )
        self.assertEqual(
            parser._format_qualifications("Some qualification text"),
            "Minimum Qualifications\nSome qualification text",
        )
        self.assertIsNone(parser._strip_list_markers(None))
        self.assertEqual(parser._strip_list_markers("- A\n* B\n• C\nD"), "A\nB\nC\nD")
        self.assertIsNone(parser.get([1], 2))
        self.assertIsNone(parser.extract_html_text([None, None]))


if __name__ == "__main__":
    unittest.main()
