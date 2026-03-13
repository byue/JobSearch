import unittest
from unittest.mock import Mock, patch

import requests

from scrapers.airflow.clients.apple.client import AppleJobsClient
from common.request_policy import RequestPolicy


class AppleClientTest(unittest.TestCase):
    def _client(self) -> AppleJobsClient:
        return AppleJobsClient(
            base_url="https://jobs.apple.com",
            default_request_policy=RequestPolicy(timeout_seconds=1.0, max_retries=1),
            proxy_management_client=Mock(),
        )

    def test_get_jobs_and_get_job_details(self) -> None:
        client = self._client()
        search_html = 'window.__staticRouterHydrationData = JSON.parse("{\\"loaderData\\":{\\"search\\":{\\"searchResults\\":[{\\"positionId\\":\\"1\\",\\"postingTitle\\":\\"Eng\\",\\"transformedPostingTitle\\":\\"eng\\"}],\\"totalRecords\\":1}}}");'
        with patch.object(client.transport, "get_html", return_value=search_html):
            out = client.get_jobs(page=1)
            self.assertEqual(out.status, 200)
            self.assertEqual(len(out.jobs), 1)

        details_html = 'window.__staticRouterHydrationData = JSON.parse("{\\"loaderData\\":{\\"jobDetails\\":{\\"jobsData\\":{\\"description\\":\\"x\\"}}}}");'
        with patch.object(client.transport, "get_html", return_value=details_html):
            details = client.get_job_details(job_id="1")
            self.assertEqual(details.status, 200)

    def test_validations_and_not_found(self) -> None:
        client = self._client()
        with self.assertRaises(ValueError):
            client.get_jobs(page=0)
        with self.assertRaises(ValueError):
            client.get_job_details(job_id=" ")
        bad_html = 'window.__staticRouterHydrationData = JSON.parse("{\\"loaderData\\":{\\"search\\":{\\"searchResults\\":\\"bad\\"}}}");'
        with patch.object(client.transport, "get_html", return_value=bad_html):
            with self.assertRaises(ValueError):
                client.get_jobs(page=1)
        details_html = 'window.__staticRouterHydrationData = JSON.parse("{\\"errors\\":{\\"jobDetails\\":{\\"status\\":404}},\\"loaderData\\":{}}");'
        with patch.object(client.transport, "get_html", return_value=details_html):
            out = client.get_job_details(job_id="1")
            self.assertEqual(out.status, 404)

    def test_more_validation_branches(self) -> None:
        client = self._client()
        bad_item_html = (
            'window.__staticRouterHydrationData = JSON.parse("{\\"loaderData\\":{\\"search\\":{\\"searchResults\\":[1],'
            '\\"totalRecords\\":1}}}");'
        )
        with patch.object(client.transport, "get_html", return_value=bad_item_html):
            with self.assertRaises(ValueError):
                client.get_jobs(page=1)

        details_missing_mapping = (
            'window.__staticRouterHydrationData = JSON.parse("{\\"loaderData\\":{\\"jobDetails\\":\\"bad\\"}}");'
        )
        with patch.object(client.transport, "get_html", return_value=details_missing_mapping):
            with self.assertRaises(ValueError):
                client.get_job_details(job_id="1")

        http_404 = requests.exceptions.HTTPError("not found")
        http_404.response = requests.Response()
        http_404.response.status_code = 404
        with patch.object(client.transport, "get_html", side_effect=http_404):
            out = client.get_job_details(job_id="1")
            self.assertEqual(out.status, 404)

        http_500 = requests.exceptions.HTTPError("server error")
        http_500.response = requests.Response()
        http_500.response.status_code = 500
        with patch.object(client.transport, "get_html", side_effect=http_500):
            with self.assertRaises(requests.exceptions.HTTPError):
                client.get_job_details(job_id="1")


if __name__ == "__main__":
    unittest.main()
