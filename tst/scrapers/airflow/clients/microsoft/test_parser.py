import unittest
from unittest.mock import patch

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
                "name": "Software Engineer",
                "postedTs": 1700000000,
                "standardizedLocations": ["Seattle, WA, USA"],
                "positionUrl": "/careers/job/1",
            },
            base_url="https://apply.careers.microsoft.com",
        )
        self.assertEqual(metadata.id, "1")
        self.assertEqual(metadata.company, "microsoft")
        self.assertEqual(metadata.jobCategory, "software_engineer")

        details = parser.parse_job_details(
            payload={
                "name": "Engineer",
                "jobDescription": (
                    "<b>Overview</b><p>desc</p><br><br>"
                    "<b>Responsibilities</b><ul><li>Resp</li></ul><br><br>"
                    "<b>Qualifications</b>"
                    "<p><strong>Required Qualifications:</strong></p><ul><li>Min</li></ul>"
                    "<p><strong>Other Requirements:</strong></p><ul><li>Other</li></ul>"
                    "<p><strong>Preferred Qualifications:</strong></p><ul><li>Pref</li></ul>"
                ),
            }
        )
        self.assertEqual(
            details.jobDescription,
            "\n\n".join(
                [
                    "Engineer",
                    "Overview\ndesc",
                    "Responsibilities\nResp",
                    "Qualifications\nRequired Qualifications:\nMin\n\nOther Requirements:\nOther\n\nPreferred Qualifications:\nPref",
                ]
            ),
        )

    def test_render_job_description_preserves_text_after_heading_br(self) -> None:
        details = parser.parse_job_details(
            payload={
                "name": "Senior Product Manager",
                "jobDescription": (
                    "<b>Overview</b><br>Calendar Copilot is at the forefront.<br><br>"
                    "<b>Responsibilities</b><br><ul><li>Define vision</li></ul>"
                ),
            }
        )
        self.assertEqual(
            details.jobDescription,
            "\n\n".join(
                [
                    "Senior Product Manager",
                    "Overview\nCalendar Copilot is at the forefront.",
                    "Responsibilities\nDefine vision",
                ]
            ),
        )

    def test_render_job_description_preserves_heading_and_block_tail_text(self) -> None:
        rendered = parser.render_job_description(
            "<b>Overview</b>Tail intro<p>Main paragraph</p>Tail outro"
        )
        self.assertEqual(
            rendered,
            "Overview\nTail intro\n\nMain paragraph\n\nTail outro",
        )

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
        self.assertEqual(parser.prepend_title(title="Role", body="Body"), "Role\n\nBody")
        with self.assertRaises(ValueError):
            parser.parse_job_metadata(payload={"id": "1", "name": "n", "postedTs": "bad"}, base_url="https://x")

    def test_detail_rendering_fallback_branches(self) -> None:
        details = parser.parse_job_details(payload={"name": "Role"})
        self.assertEqual(details.jobDescription, "Role")

        self.assertEqual(
            parser.build_job_description(
                payload={
                    "jobDescription": "Intro",
                    "responsibilities": "Resp",
                    "requiredQualifications": "Req",
                    "minimumQualifications": "Min",
                    "otherRequirements": "Other",
                    "preferredQualifications": "Pref",
                    "jobQualifications": "Quals",
                    "qualification": "Qual",
                }
            ),
            "Intro\n\nResponsibilities\nResp\n\nRequired Qualifications\nReq\n\nMinimum Qualifications\nMin\n\nOther Requirements\nOther\n\nPreferred Qualifications\nPref\n\nQuals\n\nQual",
        )
        self.assertEqual(parser.prepend_title(title="Role", body="Role\n\nBody"), "Role\n\nBody")
        self.assertIsNone(parser._format_section("Title", None))
        self.assertIsNone(parser._normalize_text(None))
        self.assertIsNone(parser._normalize_text("   "))
        self.assertIsNone(parser.render_job_description(None))
        self.assertIsNone(parser.render_job_description("   "))
        self.assertEqual(parser.render_job_description("<"), "<")
        with patch.object(parser.lxml_html, "fragments_fromstring", side_effect=ValueError("bad html")):
            self.assertIsNone(parser.render_job_description("<p>x</p>"))
        self.assertFalse(parser._is_section_heading(object()))
        self.assertEqual(parser._join_section("Heading", []), "Heading")
        self.assertEqual(parser._join_section(None, ["Body"]), "Body")
        self.assertIsNone(parser._render_child_block(object()))
        self.assertEqual(parser._render_child_block(parser.lxml_html.fromstring("<div><p>A</p><p>B</p></div>")), "A\n\nB")
        self.assertEqual(parser._render_child_block(parser.lxml_html.fromstring("<ul><li>A</li><li>B</li></ul>")), "A\nB")
        self.assertEqual(parser._render_child_block(parser.lxml_html.fromstring("<p>A<br/><br/>B</p>")), "A\n\nB")
        self.assertEqual(
            parser.build_details_url(position_url=None, public_url=None, job_id="1", base_url="https://x"),
            "https://x/careers/job/1",
        )


if __name__ == "__main__":
    unittest.main()
