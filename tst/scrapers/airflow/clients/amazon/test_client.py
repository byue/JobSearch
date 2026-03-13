import unittest
from unittest.mock import Mock, patch

import requests

from scrapers.airflow.clients.amazon.client import AmazonJobsClient
from scrapers.airflow.clients.common.request_policy import RequestPolicy


class AmazonClientTest(unittest.TestCase):
    def _client(self) -> AmazonJobsClient:
        return AmazonJobsClient(
            base_url="https://www.amazon.jobs",
            default_request_policy=RequestPolicy(timeout_seconds=1.0, max_retries=1),
            proxy_management_client=Mock(),
        )

    def test_get_jobs_and_details(self) -> None:
        client = self._client()
        with patch.object(
            client.transport,
            "get_json",
            return_value={
                "jobs": [{"id_icims": "1", "title": "SWE", "location": "Seattle, WA, USA"}],
                "hits": 1,
            },
        ):
            out = client.get_jobs(page=1)
            self.assertEqual(out.status, 200)
            self.assertEqual(len(out.jobs), 1)

        with patch.object(client.transport, "get_json", return_value={"jobs": [{"id_icims": "1", "description": "x"}]}):
            details = client.get_job_details(job_id="1")
            self.assertEqual(details.status, 200)

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
        with patch.object(client.transport, "get_json", return_value={"jobs": []}):
            details = client.get_job_details(job_id="1")
            self.assertEqual(details.status, 404)
            self.assertEqual(
                details.error,
                "Job '1' not found for company 'amazon' url=https://www.amazon.jobs/en/search.json?job_id_icims%5B%5D=1&offset=0&result_limit=1&sort=relevant",
            )
            self.assertIsNone(details.job)
        with patch.object(client.transport, "get_json", return_value={"jobs": "bad"}):
            with self.assertRaises(ValueError):
                client.get_job_details(job_id="1")
        http_404 = requests.exceptions.HTTPError("not found")
        http_404.response = requests.Response()
        http_404.response.status_code = 404
        with patch.object(client.transport, "get_json", side_effect=http_404):
            details = client.get_job_details(job_id="1")
            self.assertEqual(details.status, 404)
        http_500 = requests.exceptions.HTTPError("server error")
        http_500.response = requests.Response()
        http_500.response.status_code = 500
        with patch.object(client.transport, "get_json", side_effect=http_500):
            with self.assertRaises(requests.exceptions.HTTPError):
                client.get_job_details(job_id="1")

    def test_get_jobs_has_next_fallback_and_details_edge_cases(self) -> None:
        client = self._client()
        with patch.object(
            client.transport,
            "get_json",
            return_value={"jobs": [{"id_icims": str(i), "title": "SWE"} for i in range(client.PAGE_SIZE)], "hits": "bad"},
        ):
            out = client.get_jobs(page=1)
            self.assertTrue(out.has_next_page)

        with self.assertRaises(ValueError):
            client.get_job_details(job_id=" ")

        with patch.object(client.transport, "get_json", return_value={"jobs": [1, {"id_icims": "2"}]}):
            out = client.get_job_details(job_id="2")
            self.assertEqual(out.status, 200)


if __name__ == "__main__":
    unittest.main()
