import unittest
from unittest.mock import Mock, patch

from scrapers.airflow.clients.apple.client import AppleJobsClient
from common.request_policy import RequestPolicy


class AppleClientTest(unittest.TestCase):
    def _client(self) -> AppleJobsClient:
        self.features_client = Mock()
        return AppleJobsClient(
            base_url="https://jobs.apple.com",
            default_request_policy=RequestPolicy(timeout_seconds=1.0, max_retries=1),
            proxy_management_client=Mock(),
            features_client=self.features_client,
        )

    def test_get_jobs_and_get_job_details(self) -> None:
        client = self._client()
        search_html = 'window.__staticRouterHydrationData = JSON.parse("{\\"loaderData\\":{\\"search\\":{\\"searchResults\\":[{\\"positionId\\":\\"1\\",\\"postingTitle\\":\\"Software Engineer\\",\\"transformedPostingTitle\\":\\"software-engineer\\"}],\\"totalRecords\\":1}}}");'
        with patch.object(client.transport, "get_html", return_value=search_html):
            self.features_client.normalize_locations.return_value = {"locations": []}
            out = client.get_jobs(page=1)
            self.assertEqual(out.status, 200)
            self.assertEqual(len(out.jobs), 1)
            self.assertEqual(out.jobs[0].jobCategory, "software_engineer")

        with patch.object(
            client.transport,
            "get_json",
            return_value={
                "res": {
                    "postingTitle": "Apple Engineer",
                    "jobSummary": "build products",
                    "description": "design systems",
                    "minimumQualifications": "<li>python</li>",
                    "preferredQualifications": "<li>swift</li>",
                    "eeoContent": "<p>equal opportunity</p>",
                }
            },
        ):
            details = client.get_job_details(job_id="1")
            self.assertEqual(details.status, 200)
            self.assertEqual(details.detailsUrl, "https://jobs.apple.com/en-us/details/1")
            self.assertEqual(
                details.jobDescription,
                "Apple Engineer\n\nSummary\nbuild products\n\nDescription\ndesign systems\n\nMinimum Qualifications\npython\n\nPreferred Qualifications\nswift\n\nequal opportunity",
            )

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

        with patch.object(client.transport, "get_json", return_value={"res": {}}):
            out = client.get_job_details(job_id="1")
            self.assertEqual(out.status, 404)
            self.assertEqual(out.detailsUrl, "https://jobs.apple.com/en-us/details/1")

    def test_more_validation_branches(self) -> None:
        client = self._client()
        bad_item_html = (
            'window.__staticRouterHydrationData = JSON.parse("{\\"loaderData\\":{\\"search\\":{\\"searchResults\\":[1],'
            '\\"totalRecords\\":1}}}");'
        )
        with patch.object(client.transport, "get_html", return_value=bad_item_html):
            with self.assertRaises(ValueError):
                client.get_jobs(page=1)

        with patch.object(client.transport, "get_json", return_value={"res": {"jobSummary": "   "}}):
            out = client.get_job_details(job_id="1")
            self.assertEqual(out.status, 404)
            self.assertEqual(out.detailsUrl, "https://jobs.apple.com/en-us/details/1")

        with patch.object(client.transport, "get_json", return_value={"res": []}):
            with self.assertRaises(ValueError):
                client.get_job_details(job_id="1")

    def test_get_jobs_normalizes_locations_via_features_service(self) -> None:
        client = self._client()
        search_html = (
            'window.__staticRouterHydrationData = JSON.parse("{\\"loaderData\\":{\\"search\\":{\\"searchResults\\":['
            '{\\"positionId\\":\\"1\\",\\"postingTitle\\":\\"Software Engineer\\",'
            '\\"locations\\":[{\\"city\\":\\"Cupertino\\",\\"stateProvince\\":\\"CA\\",\\"countryName\\":\\"USA\\"}]}],'
            '\\"totalRecords\\":1}}}");'
        )
        with patch.object(client.transport, "get_html", return_value=search_html):
            self.features_client.normalize_locations.return_value = {
                "locations": [{"city": "Cupertino", "region": "California", "country": "United States"}]
            }
            out = client.get_jobs(page=1)
        self.features_client.normalize_locations.assert_called_once_with(locations=["Cupertino, CA, USA"])
        self.assertEqual(out.jobs[0].locations[0].state, "California")

    def test_get_jobs_validates_normalize_locations_payload(self) -> None:
        client = self._client()
        search_html = (
            'window.__staticRouterHydrationData = JSON.parse("{\\"loaderData\\":{\\"search\\":{\\"searchResults\\":['
            '{\\"positionId\\":\\"1\\",\\"postingTitle\\":\\"Software Engineer\\",'
            '\\"locations\\":[{\\"city\\":\\"Cupertino\\",\\"stateProvince\\":\\"CA\\",\\"countryName\\":\\"USA\\"}]}],'
            '\\"totalRecords\\":1}}}");'
        )
        with patch.object(client.transport, "get_html", return_value=search_html):
            self.features_client.normalize_locations.return_value = {"locations": "bad"}
            with self.assertRaises(ValueError):
                client.get_jobs(page=1)

    def test_normalize_locations_edge_cases(self) -> None:
        client = self._client()
        self.assertEqual(client._normalize_locations([]), [])
        client.features_client = None
        self.assertEqual(client._normalize_locations([["Cupertino, CA, USA"]]), [[]])
        client.features_client = self.features_client
        self.assertEqual(client._normalize_locations([[]]), [[]])
        self.features_client.normalize_locations.return_value = {"locations": ["bad"]}
        with self.assertRaises(ValueError):
            client._normalize_locations([["Cupertino, CA, USA"]])
        self.features_client.normalize_locations.return_value = {"locations": []}
        with self.assertRaises(ValueError):
            client._normalize_locations([["Cupertino, CA, USA"]])


if __name__ == "__main__":
    unittest.main()
