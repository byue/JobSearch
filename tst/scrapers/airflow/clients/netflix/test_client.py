import unittest
from unittest.mock import Mock, patch

import requests
from web.backend.schemas import JobDetailsSchema

from common.request_policy import RequestPolicy
from scrapers.airflow.clients.netflix.client import NetflixJobsClient, _dedupe, _require_mapping, _to_int, _to_optional_str


class NetflixClientTest(unittest.TestCase):
    def _client(self) -> NetflixJobsClient:
        return NetflixJobsClient(
            base_url="https://explore.jobs.netflix.net",
            default_request_policy=RequestPolicy(timeout_seconds=1.0, max_retries=1),
            proxy_management_client=Mock(),
        )

    def test_scalar_helpers(self) -> None:
        self.assertEqual(_to_optional_str(" x "), "x")
        self.assertIsNone(_to_optional_str(None))
        self.assertEqual(_to_int("12"), 12)
        self.assertIsNone(_to_int("x"))
        self.assertEqual(_dedupe(["a", "a", "b"]), ["a", "b"])
        self.assertEqual(_require_mapping({"a": 1}, context="x")["a"], 1)
        with self.assertRaises(ValueError):
            _require_mapping([], context="x")

    def test_get_jobs_and_details(self) -> None:
        client = self._client()
        with patch.object(client, "_get_jobs_payload", return_value={"positions": "bad"}):
            with self.assertRaises(ValueError):
                client.get_jobs(page=1)
        with patch.object(client, "_get_jobs_payload", return_value={"positions": [1]}):
            with self.assertRaises(ValueError):
                client.get_jobs(page=1)
        with patch.object(
            client,
            "_get_jobs_payload",
            return_value={"positions": [{"id": "1", "posting_name": "Eng", "locations": ["Seattle, WA, USA"]}], "count": 1},
        ):
            out = client.get_jobs(page=1)
            self.assertEqual(out.status, 200)
            self.assertEqual(len(out.jobs), 1)

        with patch.object(
            client,
            "_get_jobs_payload",
            return_value={"positions": [{"id": "1", "job_description": "<li>x</li>"}]},
        ), patch.object(client, "_extract_description_from_job_page", return_value=None):
            details = client.get_job_details(job_id="1")
            self.assertEqual(details.status, 200)

        with patch.object(client, "_get_jobs_payload", return_value={"positions": []}), patch.object(
            client, "_extract_description_from_job_page", return_value=None
        ):
            details = client.get_job_details(job_id="1")
            self.assertEqual(details.status, 404)
            self.assertEqual(
                details.error,
                "Job '1' not found for company 'netflix' url=https://explore.jobs.netflix.net/api/apply/v2/jobs?domain=netflix.com&pid=1",
            )
            self.assertIsNone(details.job)

        with patch.object(
            client,
            "_get_jobs_payload",
            return_value={"positions": [{"id": "1"}, {"id": "2"}], "count": "bad"},
        ):
            out = client.get_jobs(page=1)
            self.assertFalse(out.has_next_page)

        with self.assertRaises(ValueError):
            client.get_jobs(page=0)
        with self.assertRaises(ValueError):
            client.get_job_details(job_id=" ")
        with patch.object(client, "_get_jobs_payload", return_value={"positions": "bad"}), patch.object(
            client, "_extract_description_from_job_page", return_value=None
        ):
            with self.assertRaises(ValueError):
                client.get_job_details(job_id="1")
        with patch.object(
            client,
            "_get_jobs_payload",
            return_value={"positions": [1, {"id": "1", "job_description": "<ul><li>a</li></ul><ul><li>b</li></ul>"}]},
        ), patch.object(client, "_extract_description_from_job_page", return_value=None):
            details = client.get_job_details(job_id="1")
            self.assertEqual(details.status, 200)
            assert isinstance(details.job, JobDetailsSchema)
            self.assertEqual(details.job.minimumQualifications, ["a"])
            self.assertEqual(details.job.preferredQualifications, ["b"])

    def test_get_job_details_page_first_success_skips_api_fallback(self) -> None:
        client = self._client()
        with patch.object(client, "_extract_description_from_job_page", return_value="desc"), patch.object(
            client, "_get_jobs_payload"
        ) as get_jobs_payload:
            details = client.get_job_details(job_id="1")
            self.assertEqual(details.status, 200)
            self.assertIsNone(details.error)
            assert details.job is not None
            self.assertEqual(details.job.jobDescription, "desc")
            get_jobs_payload.assert_not_called()

    def test_get_job_details_page_request_exception_falls_back_to_api(self) -> None:
        client = self._client()
        with patch.object(
            client,
            "_extract_description_from_job_page",
            side_effect=requests.exceptions.RequestException("timeout"),
        ), patch.object(
            client,
            "_get_jobs_payload",
            return_value={"positions": [{"id": "1", "job_description": "<ul><li>x</li></ul>"}]},
        ):
            details = client.get_job_details(job_id="1")
            self.assertEqual(details.status, 200)
            assert details.job is not None
            self.assertEqual(details.job.minimumQualifications, ["x"])

    def test_get_job_details_api_http_404_returns_not_found(self) -> None:
        client = self._client()
        http_404 = requests.exceptions.HTTPError("not found")
        http_404.response = requests.Response()
        http_404.response.status_code = 404
        with patch.object(client, "_extract_description_from_job_page", return_value=None), patch.object(
            client,
            "_get_jobs_payload",
            side_effect=http_404,
        ):
            details = client.get_job_details(job_id="1")
            self.assertEqual(details.status, 404)

    def test_get_job_details_api_non_404_http_error_reraises(self) -> None:
        client = self._client()
        http_500 = requests.exceptions.HTTPError("server error")
        http_500.response = requests.Response()
        http_500.response.status_code = 500
        with patch.object(client, "_extract_description_from_job_page", return_value=None), patch.object(
            client,
            "_get_jobs_payload",
            side_effect=http_500,
        ):
            with self.assertRaises(requests.exceptions.HTTPError):
                client.get_job_details(job_id="1")

    def test_get_job_details_missing_page_and_api_description_returns_not_found(self) -> None:
        client = self._client()
        with patch.object(client, "_extract_description_from_job_page", return_value=None), patch.object(
            client,
            "_get_jobs_payload",
            return_value={"positions": [{"id": "1", "job_description": None}]},
        ):
            details = client.get_job_details(job_id="1")
            self.assertEqual(details.status, 404)
            self.assertEqual(details.error, "Job '1' not found for company 'netflix' url=https://explore.jobs.netflix.net/careers/job/1")
            self.assertIsNone(details.job)

    def test_get_jobs_payload(self) -> None:
        client = self._client()
        with patch("scrapers.airflow.clients.netflix.client.request_json_with_backoff", return_value={"positions": []}):
            out = client._get_jobs_payload(params=[], request_policy=RequestPolicy(timeout_seconds=1.0, max_retries=1))
            self.assertEqual(out["positions"], [])
        with patch("scrapers.airflow.clients.netflix.client.request_json_with_backoff", return_value=[]):
            with self.assertRaises(ValueError):
                client._get_jobs_payload(params=[], request_policy=RequestPolicy(timeout_seconds=1.0, max_retries=1))

    def test_metadata_and_url_helpers(self) -> None:
        client = self._client()
        md = client._parse_job_metadata({"id": "1", "posting_name": "Eng", "locations": ["Seattle, WA, USA"]})
        self.assertEqual(md.id, "1")
        self.assertIsNotNone(md.detailsUrl)
        with self.assertRaises(ValueError):
            client._parse_job_metadata({"id": " "})

        self.assertEqual(client._to_job_id(1), "1")
        self.assertEqual(client._to_job_id(" 2 "), "2")
        self.assertIsNone(client._to_job_id(None))

        self.assertIn("/job/1", client._build_details_url({"id": "1"}))
        self.assertEqual(client._build_details_url({"canonicalPositionUrl": "https://x"}), "https://x")

        locs = client._extract_locations({"locations": ["Seattle, WA, USA"]})
        self.assertEqual(locs[0].city, "Seattle")
        self.assertEqual(client._extract_locations({"location": "Remote"})[0].country, "Remote")

    def test_description_and_job_page_helpers(self) -> None:
        client = self._client()
        self.assertEqual(client._extract_job_description("x"), "x")
        self.assertEqual(client._extract_job_description(["a", "b"]), "a\n\nb")
        self.assertIsNone(client._extract_job_description(1))

        with patch.object(client, "_get_job_page_html", side_effect=requests.exceptions.RequestException("x")):
            with self.assertRaises(requests.exceptions.RequestException):
                client._extract_description_from_job_page(job_id="1", details_url="https://x")

        html_payload = (
            '<script type="application/ld+json">{"@type":"JobPosting","description":"Hello"}</script>'
            '<meta name="description" content="Meta desc">'
        )
        with patch.object(client, "_get_job_page_html", return_value=html_payload):
            self.assertEqual(client._extract_description_from_job_page(job_id="1", details_url="https://x"), "Hello")
        with patch.object(client, "_get_job_page_html", return_value='<meta name="description" content="Meta desc">'):
            self.assertEqual(client._extract_description_from_job_page(job_id="1", details_url="https://x"), "Meta desc")
        with patch.object(client, "_get_job_page_html", return_value="<html></html>"):
            self.assertIsNone(client._extract_description_from_job_page(job_id="1", details_url="https://x"))

        with patch(
            "scrapers.airflow.clients.netflix.client.request_text_with_backoff",
            return_value="<html/>",
        ):
            self.assertEqual(client._get_job_page_html(job_id="1", details_url="/careers/job/1"), "<html/>")
            self.assertEqual(client._get_job_page_html(job_id="1", details_url="https://absolute"), "<html/>")
            with patch("scrapers.airflow.clients.netflix.client.urllib.parse.urljoin", return_value=" "):
                self.assertEqual(client._get_job_page_html(job_id="1", details_url=""), "<html/>")

    def test_json_ld_and_sections(self) -> None:
        client = self._client()
        html_payload = '<script type="application/ld+json">{"@type":"JobPosting","description":"Hello"}</script>'
        posting = client._extract_job_posting_ld_json(html_payload)
        self.assertIsNotNone(posting)
        self.assertEqual(client._find_job_posting_in_json_ld({"@type": "JobPosting"})["@type"], "JobPosting")
        self.assertIsNone(client._find_job_posting_in_json_ld("x"))

        self.assertEqual(client._extract_meta_description('<meta name="description" content="x">'), "x")
        self.assertIsNone(client._extract_meta_description("no-meta"))

        section = client._extract_section("<h2>Responsibilities</h2><li>A</li>", headings=("responsibilities",))
        self.assertIn("A", section)
        self.assertIsNone(client._extract_section(None, headings=("x",)))

        self.assertEqual(client._extract_list_items("<li>A</li><li>B</li>"), ["A", "B"])
        self.assertEqual(client._extract_list_items(""), [])
        self.assertEqual(client._extract_html_list_blocks("<ul><li>A</li></ul>"), ["<ul><li>A</li></ul>"])
        self.assertEqual(client._extract_html_list_blocks(None), [])
        self.assertEqual(client._clean_html_fragment("<p>A</p>"), "A")
        self.assertIsNone(client._extract_job_posting_ld_json('<script type="application/ld+json"></script>'))
        self.assertIsNone(client._extract_job_posting_ld_json('<script type="application/ld+json">{bad}</script>'))
        self.assertIsNone(client._find_job_posting_in_json_ld({"@graph": [1, {"@type": "Thing"}]}))
        self.assertIsNone(client._find_job_posting_in_json_ld([1, 2, 3]))
        self.assertIsNotNone(client._find_job_posting_in_json_ld({"@graph": [{"@type": "Thing"}, {"@type": "JobPosting"}]}))
        self.assertIsNotNone(client._find_job_posting_in_json_ld([{"@type": "Thing"}, {"@type": "JobPosting"}]))
        self.assertIn("X", client._extract_section("<strong>Responsibilities</strong><li>X</li>", headings=("responsibilities",)) or "")
        self.assertIn("Y", client._extract_section("<b>Responsibilities</b><li>Y</li>", headings=("responsibilities",)) or "")
        self.assertEqual(client._extract_list_items("<p>a<br>b</p>"), ["a", "b"])
        self.assertEqual(client._extract_list_items("   "), [])
        self.assertEqual(client._extract_list_items("<li> </li><li>a</li><li>a</li>"), ["a"])
        self.assertEqual(client._extract_html_list_blocks(" "), [])

    def test_more_metadata_branches(self) -> None:
        client = self._client()
        md = client._parse_job_metadata({"id": 2, "name": "Eng", "location": "Remote", "t_update": 11})
        self.assertEqual(md.id, "2")
        self.assertEqual(md.locations[0].country, "Remote")
        self.assertEqual(client._build_details_url({"id": None}), f"{client.base_url}{client.CAREERS_PATH}")
        self.assertEqual(client._extract_locations({"locations": ["State, Country"]})[0].state, "State")
        self.assertIsNone(client._extract_job_description([" "]))


if __name__ == "__main__":
    unittest.main()
