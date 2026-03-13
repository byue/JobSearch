import unittest
from unittest.mock import Mock, patch

import requests

from scrapers.airflow.clients.common.request_policy import RequestPolicy
from scrapers.airflow.clients.microsoft.client import MicrosoftJobsClient


class MicrosoftClientTest(unittest.TestCase):
    def _client(self) -> MicrosoftJobsClient:
        return MicrosoftJobsClient(
            base_url="https://apply.careers.microsoft.com",
            default_request_policy=RequestPolicy(timeout_seconds=1.0, max_retries=1),
            proxy_management_client=Mock(),
        )

    def test_get_jobs_and_details(self) -> None:
        client = self._client()
        search_payload = {
            "status": 200,
            "data": {
                "count": 1,
                "positions": [
                    {
                        "id": "1",
                        "name": "Engineer",
                        "postedTs": 1700000000,
                        "standardizedLocations": ["Seattle, WA, USA"],
                    }
                ],
            },
        }
        with patch.object(client.transport, "get_json", return_value=search_payload):
            out = client.get_jobs(page=1)
            self.assertEqual(out.status, 200)
            self.assertEqual(len(out.jobs), 1)

        with patch.object(client.transport, "get_json", return_value={"status": 200, "data": {"jobDescription": "x"}}):
            details = client.get_job_details(job_id="1")
            self.assertEqual(details.status, 200)

    def test_validation_and_bad_payload(self) -> None:
        client = self._client()
        with self.assertRaises(ValueError):
            client.get_jobs(page=0)
        with self.assertRaises(ValueError):
            client.get_job_details(job_id=" ")
        with patch.object(client.transport, "get_json", return_value={"data": {"positions": "bad"}}):
            with self.assertRaises(ValueError):
                client.get_jobs(page=1)
        with patch.object(client.transport, "get_json", return_value={"data": {"positions": [1]}}):
            with self.assertRaises(ValueError):
                client.get_jobs(page=1)

    def test_get_job_details_not_found_includes_url(self) -> None:
        client = self._client()
        with patch.object(client.transport, "get_json", return_value={"status": 404, "error": "not found"}):
            details = client.get_job_details(job_id="missing-1")
            self.assertEqual(details.status, 404)
            self.assertEqual(
                details.error,
                "not found url=https://apply.careers.microsoft.com/api/pcsx/position_details?position_id=missing-1&domain=microsoft.com&hl=en",
            )
            self.assertIsNone(details.job)

    def test_get_job_details_http_404_and_non_404_error_handling(self) -> None:
        client = self._client()
        http_404 = requests.exceptions.HTTPError("not found")
        http_404.response = requests.Response()
        http_404.response.status_code = 404
        with patch.object(client.transport, "get_json", side_effect=http_404):
            details = client.get_job_details(job_id="missing-1")
            self.assertEqual(details.status, 404)
            self.assertIsNone(details.job)

        http_500 = requests.exceptions.HTTPError("server error")
        http_500.response = requests.Response()
        http_500.response.status_code = 500
        with patch.object(client.transport, "get_json", side_effect=http_500):
            with self.assertRaises(requests.exceptions.HTTPError):
                client.get_job_details(job_id="missing-1")

    def test_get_job_details_empty_200_payload_returns_not_found(self) -> None:
        client = self._client()
        with patch.object(client.transport, "get_json", return_value={"status": 200, "data": {}}):
            details = client.get_job_details(job_id="missing-1")
            self.assertEqual(details.status, 404)
            self.assertEqual(
                details.error,
                "Job 'missing-1' not found for company 'microsoft' url=https://apply.careers.microsoft.com/api/pcsx/position_details?position_id=missing-1&domain=microsoft.com&hl=en",
            )
            self.assertIsNone(details.job)


if __name__ == "__main__":
    unittest.main()
