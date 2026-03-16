import unittest
from unittest.mock import Mock, patch

import requests

from common.request_policy import RequestPolicy
from scrapers.airflow.clients.google.client import GoogleJobsClient


class GoogleClientTest(unittest.TestCase):
    def _client(self) -> GoogleJobsClient:
        self.features_client = Mock()
        return GoogleJobsClient(
            base_url="https://www.google.com",
            default_request_policy=RequestPolicy(timeout_seconds=1.0, max_retries=1),
            proxy_management_client=Mock(),
            features_client=self.features_client,
        )

    def test_get_jobs_and_details(self) -> None:
        client = self._client()
        with patch.object(client.transport, "get_html", return_value="<html/>"), patch(
            "scrapers.airflow.clients.google.client.parser.extract_rows",
            return_value=([["id1", "Engineer", "https://apply"]], 1, 10),
        ):
            self.features_client.normalize_locations.return_value = {"locations": []}
            out_page2 = client.get_jobs(page=2)
            self.assertEqual(out_page2.status, 200)
        with patch.object(client.transport, "get_html", return_value="<html/>"), patch(
            "scrapers.airflow.clients.google.client.parser.extract_rows",
            return_value=([["id1", "Engineer", "https://apply", None, None, None, None, None, None, ["Seattle, WA, USA"], None, None, 1700000000]], 1, 10),
        ):
            self.features_client.normalize_locations.return_value = {
                "locations": [{"city": "Seattle", "region": "Washington", "country": "United States"}]
            }
            out = client.get_jobs(page=1)
            self.assertEqual(out.status, 200)
            self.assertEqual(len(out.jobs), 1)
            self.features_client.normalize_locations.assert_called_with(locations=["Seattle, WA, USA"])

        with patch.object(client.transport, "get_html", return_value="<html/>"), patch(
            "scrapers.airflow.clients.google.client.parser.extract_row_from_ds0",
            return_value=["id1", "Engineer", "https://apply", None, None, None, None, None, None, ["Seattle, WA, USA"], None, None, 1700000000],
        ):
            detail = client.get_job_details(job_id="id1")
            self.assertEqual(detail.status, 200)
            self.assertEqual(
                detail.detailsUrl,
                "https://www.google.com/about/careers/applications/jobs/results/id1-job",
            )

        with patch.object(client.transport, "get_html", return_value="<html/>"), patch(
            "scrapers.airflow.clients.google.client.parser.extract_row_from_ds0",
            return_value=["id2"],
        ):
            with self.assertRaises(ValueError):
                client.get_job_details(job_id="id1")

        with patch.object(client.transport, "get_html", return_value="no ds0"):
            missing = client.get_job_details(job_id="id1")
            self.assertEqual(missing.status, 404)
            self.assertEqual(
                missing.error,
                "Job 'id1' not found for company 'google' on direct job page url=https://www.google.com/about/careers/applications/jobs/results/id1-job",
            )
            self.assertIsNone(missing.jobDescription)
            self.assertEqual(
                missing.detailsUrl,
                "https://www.google.com/about/careers/applications/jobs/results/id1-job",
            )

        http_404 = requests.exceptions.HTTPError("not found")
        http_404.response = requests.Response()
        http_404.response.status_code = 404
        with patch.object(client.transport, "get_html", side_effect=http_404):
            missing = client.get_job_details(job_id="id1")
            self.assertEqual(missing.status, 404)

        http_500 = requests.exceptions.HTTPError("server error")
        http_500.response = requests.Response()
        http_500.response.status_code = 500
        with patch.object(client.transport, "get_html", side_effect=http_500):
            with self.assertRaises(requests.exceptions.HTTPError):
                client.get_job_details(job_id="id1")

    def test_validation(self) -> None:
        client = self._client()
        with self.assertRaises(ValueError):
            client.get_jobs(page=0)
        with self.assertRaises(ValueError):
            client.get_job_details(job_id=" ")

    def test_get_jobs_validates_normalize_locations_payload(self) -> None:
        client = self._client()
        with patch.object(client.transport, "get_html", return_value="<html/>"), patch(
            "scrapers.airflow.clients.google.client.parser.extract_rows",
            return_value=([["id1", "Engineer", "https://apply", None, None, None, None, None, None, ["Seattle, WA, USA"], None, None, 1700000000]], 1, 10),
        ):
            self.features_client.normalize_locations.return_value = {"locations": "bad"}
            with self.assertRaises(ValueError):
                client.get_jobs(page=1)

    def test_normalize_locations_edge_cases(self) -> None:
        client = self._client()
        self.assertEqual(client._normalize_locations([]), [])
        client.features_client = None
        self.assertEqual(client._normalize_locations([["Seattle, WA, USA"]]), [[]])
        client.features_client = self.features_client
        self.assertEqual(client._normalize_locations([[]]), [[]])
        self.features_client.normalize_locations.return_value = {"locations": ["bad"]}
        with self.assertRaises(ValueError):
            client._normalize_locations([["Seattle, WA, USA"]])
        self.features_client.normalize_locations.return_value = {"locations": []}
        with self.assertRaises(ValueError):
            client._normalize_locations([["Seattle, WA, USA"]])


if __name__ == "__main__":
    unittest.main()
