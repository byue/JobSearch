import unittest
from unittest.mock import Mock, patch

import requests

from scrapers.airflow.clients.amazon.client import AmazonJobsClient
from common.request_policy import RequestPolicy


class AmazonClientTest(unittest.TestCase):
    def _client(self) -> AmazonJobsClient:
        self.features_client = Mock()
        return AmazonJobsClient(
            base_url="https://www.amazon.jobs",
            default_request_policy=RequestPolicy(timeout_seconds=1.0, max_retries=1),
            proxy_management_client=Mock(),
            features_client=self.features_client,
        )

    def test_get_jobs_and_details(self) -> None:
        client = self._client()
        with patch.object(
            client.transport,
            "get_json",
            return_value={
                "jobs": [
                    {
                        "id_icims": "1",
                        "title": "SWE",
                        "job_category": "Software Development",
                        "locations": ['{"normalizedLocation":"Seattle, Washington, USA"}'],
                    }
                ],
                "hits": 1,
            },
        ) as get_json:
            self.features_client.normalize_locations.return_value = {
                "locations": [{"city": "Seattle", "region": "Washington", "country": "United States"}]
            }
            out = client.get_jobs(page=1)
            self.assertEqual(out.status, 200)
            self.assertEqual(len(out.jobs), 1)
            self.assertIsNone(out.jobs[0].jobCategory)
            self.assertEqual(out.jobs[0].locations[0].city, "Seattle")
            self.assertEqual(
                get_json.call_args.kwargs["params"],
                [
                    ("offset", "0"),
                    ("result_limit", "10"),
                    ("sort", "relevant"),
                    ("category[]", "software-development"),
                    ("category[]", "machine-learning-science"),
                ],
            )
            self.features_client.normalize_locations.assert_called_once_with(locations=["Seattle, Washington, USA"])

        with patch.object(
            client.transport,
            "get_text",
            return_value=(
                "<html><body><h1 class='title'>Role</h1>"
                "<div id='job-detail-body'><div class='content'>"
                "<div class='section'><h2>Description</h2><p>Body</p></div>"
                "<div class='section'><h2>Basic Qualifications</h2><ul><li>Python</li></ul></div>"
                "</div></div></body></html>"
            ),
        ):
            details = client.get_job_details(job_id="1")
            self.assertEqual(details.status, 200)
            self.assertIn("Role", details.jobDescription or "")
            self.assertIn("\n\nDescription\n", details.jobDescription or "")
            self.assertIn("\n\nBasic Qualifications\n", details.jobDescription or "")
            self.assertEqual(details.detailsUrl, "https://www.amazon.jobs/en/jobs/1")

    def test_get_jobs_validation(self) -> None:
        client = self._client()
        with self.assertRaises(ValueError):
            client.get_jobs(page=0)
        with patch.object(client.transport, "get_json", return_value={"jobs": "bad"}):
            with self.assertRaises(ValueError):
                client.get_jobs(page=1)
        with patch.object(client.transport, "get_json", return_value={"jobs": [1]}):
            with self.assertRaises(ValueError):
                client.get_jobs(page=1)

    def test_get_job_details_not_found(self) -> None:
        client = self._client()
        with patch.object(client.transport, "get_text", return_value=""):
            details = client.get_job_details(job_id="1")
            self.assertEqual(details.status, 404)
            self.assertEqual(
                details.error,
                "Job '1' not found for company 'amazon' url=https://www.amazon.jobs/en/jobs/1",
            )
            self.assertIsNone(details.jobDescription)
            self.assertEqual(details.detailsUrl, "https://www.amazon.jobs/en/jobs/1")
        http_404 = requests.exceptions.HTTPError("not found")
        http_404.response = requests.Response()
        http_404.response.status_code = 404
        with patch.object(client.transport, "get_text", side_effect=http_404):
            details = client.get_job_details(job_id="1")
            self.assertEqual(details.status, 404)
        http_500 = requests.exceptions.HTTPError("server error")
        http_500.response = requests.Response()
        http_500.response.status_code = 500
        with patch.object(client.transport, "get_text", side_effect=http_500):
            with self.assertRaises(requests.exceptions.HTTPError):
                client.get_job_details(job_id="1")

    def test_get_jobs_has_next_fallback_and_details_edge_cases(self) -> None:
        client = self._client()
        with patch.object(
            client.transport,
            "get_json",
            return_value={
                "jobs": [{"id_icims": str(i), "title": "SWE", "locations": [f"City {i}, State, Country"]} for i in range(client.PAGE_SIZE)],
                "hits": "bad",
            },
        ):
            self.features_client.normalize_locations.return_value = {
                "locations": [{"city": f"City {i}", "region": "State", "country": "Country"} for i in range(client.PAGE_SIZE)]
            }
            out = client.get_jobs(page=1)
            self.assertTrue(out.has_next_page)

    def test_get_jobs_uses_only_locations_payload_and_validates_normalizer(self) -> None:
        client = self._client()
        with patch.object(
            client.transport,
            "get_json",
            return_value={
                "jobs": [
                    {
                        "id_icims": "1",
                        "title": "SWE",
                        "locations": ['{"normalizedLocation":"Seattle, Washington, USA"}'],
                        "location": "SHOULD NOT BE USED",
                        "city": "SHOULD NOT",
                        "state": "SHOULD NOT",
                        "country_code": "SHOULD NOT",
                    }
                ],
                "hits": 1,
            },
        ):
            self.features_client.normalize_locations.return_value = {
                "locations": [{"city": "Seattle", "region": "Washington", "country": "United States"}]
            }
            out = client.get_jobs(page=1)
        self.assertEqual(out.jobs[0].locations[0].country, "United States")

        self.features_client.normalize_locations.reset_mock()
        with patch.object(
            client.transport,
            "get_json",
            return_value={
                "jobs": [{"id_icims": "1", "title": "SWE", "locations": ['{"normalizedLocation":"Seattle, Washington, USA"}']}],
                "hits": 1,
            },
        ):
            self.features_client.normalize_locations.return_value = {"locations": "bad"}
            with self.assertRaises(ValueError):
                client.get_jobs(page=1)

        with self.assertRaises(ValueError):
            client.get_job_details(job_id=" ")

        with patch.object(
            client.transport,
            "get_text",
            return_value=(
                "<html><body><div id='job-detail-body'><div class='content'>"
                "<div class='section'><p>text body</p></div>"
                "<div class='section'><h2>Preferred Qualifications</h2><ul><li>Extra</li></ul></div>"
                "</div></div></body></html>"
            ),
        ):
            out = client.get_job_details(job_id="2")
            self.assertEqual(out.status, 200)
            self.assertEqual(out.jobDescription, "text body\n\nPreferred Qualifications\nExtra")
            self.assertEqual(out.detailsUrl, "https://www.amazon.jobs/en/jobs/2")

    def test_normalize_locations_edge_cases(self) -> None:
        client = self._client()
        self.assertEqual(client._normalize_locations([]), [])
        client.features_client = None
        self.assertEqual(client._normalize_locations([["Seattle, Washington, USA"]]), [[]])
        client.features_client = self.features_client
        self.assertEqual(client._normalize_locations([[]]), [[]])
        self.features_client.normalize_locations.return_value = {"locations": ["bad"]}
        with self.assertRaises(ValueError):
            client._normalize_locations([["Seattle, Washington, USA"]])
        self.features_client.normalize_locations.return_value = {"locations": []}
        with self.assertRaises(ValueError):
            client._normalize_locations([["Seattle, Washington, USA"]])


if __name__ == "__main__":
    unittest.main()
