import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse
from unittest.mock import patch

import requests

from scrapers.airflow.clients.amazon.client import AmazonJobsClient
from scrapers.airflow.clients.apple.client import AppleJobsClient
from scrapers.airflow.clients.google.client import GoogleJobsClient
from scrapers.airflow.clients.microsoft.client import MicrosoftJobsClient
from scrapers.airflow.clients.common.request_policy import RequestPolicy


def _apple_hydration_payload(payload: dict) -> str:
    serialized = json.dumps(payload, separators=(",", ":"))
    encoded = json.dumps(serialized)
    return f"window.__staticRouterHydrationData = JSON.parse({encoded});"


def _google_ds_payload(*, key: str, data: str) -> str:
    return f"AF_initDataCallback({{key: '{key}', hash: 'x', data:{data}, sideChannel: {{}}}});"


class _MockCompanyHandler(BaseHTTPRequestHandler):
    def _json(self, payload: dict, status: int = 200) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _html(self, payload: str, status: int = 200) -> None:
        body = payload.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, _format: str, *_args: object) -> None:
        return

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)

        if parsed.path == "/amazon/en/search.json":
            if "job_id_icims[]" in query:
                self._json(
                    {
                        "jobs": [
                            {
                                "id_icims": "amz-1",
                                "title": "Amazon Engineer",
                                "description": "<li>build systems</li>",
                                "basic_qualifications": "<li>python</li>",
                                "preferred_qualifications": "<li>aws</li>",
                            }
                        ],
                        "hits": 1,
                    }
                )
                return
            self._json(
                {
                    "jobs": [
                        {
                            "id_icims": "amz-1",
                            "title": "Amazon Engineer",
                            "location": "Seattle, WA, USA",
                            "posted_date": "January 01, 2024",
                        }
                    ],
                    "hits": 1,
                }
            )
            return

        if parsed.path == "/amazon/en/jobs/amz-1":
            self._html(
                """
                <html><body>
                <h1 class="title">Amazon Engineer</h1>
                <div id="job-detail-body"><div class="content">
                <div class="section"><p>build systems</p></div>
                <div class="section"><h2>Basic Qualifications</h2><ul><li>python</li></ul></div>
                <div class="section"><h2>Preferred Qualifications</h2><ul><li>aws</li></ul></div>
                </div></div>
                </body></html>
                """
            )
            return

        if parsed.path == "/apple/en-us/search":
            self._html(
                _apple_hydration_payload(
                    {
                        "loaderData": {
                            "search": {
                                "searchResults": [
                                    {
                                        "positionId": "apl-1",
                                        "postingTitle": "Apple Engineer",
                                        "transformedPostingTitle": "apple-engineer",
                                        "locations": [{"city": "Cupertino", "stateProvince": "CA", "countryName": "USA"}],
                                        "postDateInGMT": "2024-01-01T00:00:00Z",
                                    }
                                ],
                                "totalRecords": 1,
                            }
                        }
                    }
                )
            )
            return

        if parsed.path == "/apple/api/v1/jobDetails/apl-1":
            self._json(
                {
                    "res": {
                        "postingTitle": "Apple Engineer",
                        "jobSummary": "build products",
                        "description": "design systems",
                        "minimumQualifications": "<li>python</li>",
                        "preferredQualifications": "<li>swift</li>",
                        "responsibilities": "<li>ship code</li>",
                    }
                }
            )
            return

        if parsed.path == "/google/about/careers/applications/jobs/results/":
            row = [
                "gid-1",
                "Google Engineer",
                "https://apply.example/job",
                None,
                None,
                None,
                None,
                None,
                None,
                ["Seattle, WA, USA"],
                None,
                None,
                1700000000,
            ]
            self._html(_google_ds_payload(key="ds:1", data=json.dumps([[row], None, 1, 10])))
            return

        if parsed.path == "/google/about/careers/applications/jobs/results/gid-1-job":
            row = [
                "gid-1",
                "Google Engineer",
                "https://apply.example/job",
                "<li>build</li>",
                "<h2>Minimum Qualifications</h2><li>python</li><h2>Preferred Qualifications</h2><li>go</li>",
                None,
                None,
                None,
                None,
                None,
                "<p>great job</p>",
                None,
                1700000000,
            ]
            self._html(_google_ds_payload(key="ds:0", data=json.dumps([row])))
            return

        if parsed.path == "/microsoft/api/pcsx/search":
            self._json(
                {
                    "status": 200,
                    "data": {
                        "count": 1,
                        "positions": [
                            {
                                "id": "ms-1",
                                "name": "Microsoft Engineer",
                                "postedTs": 1700000000,
                                "standardizedLocations": ["Redmond, WA, USA"],
                                "positionUrl": "/careers/job/ms-1",
                            }
                        ],
                    },
                }
            )
            return

        if parsed.path == "/microsoft/api/pcsx/position_details":
            self._json(
                {
                    "status": 200,
                    "data": {
                        "jobDescription": "build cloud",
                        "minimumQualifications": "<li>python</li>",
                        "preferredQualifications": "<li>azure</li>",
                        "responsibilities": "<li>deliver</li>",
                    },
                }
            )
            return

        self._json({"error": "not found"}, status=404)


class _DummyProxyManagementClient:
    def acquire_requests_proxy(self, *, scope: str) -> tuple[dict[str, str], str, str]:
        _ = scope
        return (
            {"http": "http://127.0.0.1:9999", "https": "http://127.0.0.1:9999"},
            "http://127.0.0.1:9999",
            "token",
        )

    def complete_requests_proxy(self, *, resource: str, token: str, success: bool, scope: str) -> None:
        _ = (resource, token, success, scope)


def _requests_browser_request(
    *,
    method: str,
    url: str,
    timeout: float,
    headers: dict | None = None,
    proxies: dict | None = None,
    data: dict | None = None,
    impersonate: str | None = None,
    use_random_browser: bool = True,
    require_proxy: bool = False,
):
    _ = (proxies, impersonate, use_random_browser, require_proxy)
    response = requests.request(method=method, url=url, headers=headers, data=data, timeout=timeout)
    if response.status_code >= 400:
        raise requests.exceptions.HTTPError(f"HTTP {response.status_code}", response=response)
    return response


class AirflowClientsMockEndpointsIntegrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._server = ThreadingHTTPServer(("127.0.0.1", 0), _MockCompanyHandler)
        cls._thread = threading.Thread(target=cls._server.serve_forever, daemon=True)
        cls._thread.start()
        cls._base_url = f"http://127.0.0.1:{cls._server.server_port}"

    @classmethod
    def tearDownClass(cls) -> None:
        cls._server.shutdown()
        cls._server.server_close()
        cls._thread.join(timeout=2)

    def setUp(self) -> None:
        self.policy = RequestPolicy(timeout_seconds=2.0, max_retries=1)
        self.proxy_client = _DummyProxyManagementClient()
        self._patcher = patch(
            "scrapers.airflow.clients.common.http_requests.browser_request",
            side_effect=_requests_browser_request,
        )
        self._patcher.start()

    def tearDown(self) -> None:
        self._patcher.stop()

    def test_amazon_client_with_mock_endpoint(self) -> None:
        client = AmazonJobsClient(
            base_url=f"{self._base_url}/amazon",
            default_request_policy=self.policy,
            proxy_management_client=self.proxy_client,
        )
        jobs = client.get_jobs(page=1)
        self.assertEqual(jobs.status, 200)
        self.assertEqual(len(jobs.jobs), 1)
        self.assertEqual(jobs.jobs[0].id, "amz-1")

        details = client.get_job_details(job_id="amz-1")
        self.assertEqual(details.status, 200)
        self.assertEqual(
            details.jobDescription,
            "Amazon Engineer\n\nbuild systems\n\nBasic Qualifications\npython\n\nPreferred Qualifications\naws",
        )

    def test_apple_client_with_mock_endpoint(self) -> None:
        client = AppleJobsClient(
            base_url=f"{self._base_url}/apple",
            default_request_policy=self.policy,
            proxy_management_client=self.proxy_client,
        )
        jobs = client.get_jobs(page=1)
        self.assertEqual(jobs.status, 200)
        self.assertEqual(len(jobs.jobs), 1)
        self.assertEqual(jobs.jobs[0].id, "apl-1")

        details = client.get_job_details(job_id="apl-1")
        self.assertEqual(details.status, 200)
        self.assertEqual(
            details.jobDescription,
            "Apple Engineer\n\nSummary\nbuild products\n\nDescription\ndesign systems\n\nMinimum Qualifications\npython\n\nPreferred Qualifications\nswift\n\nResponsibilities\nship code",
        )

    def test_google_client_with_mock_endpoint(self) -> None:
        client = GoogleJobsClient(
            base_url=f"{self._base_url}/google",
            default_request_policy=self.policy,
            proxy_management_client=self.proxy_client,
        )
        jobs = client.get_jobs(page=1)
        self.assertEqual(jobs.status, 200)
        self.assertEqual(len(jobs.jobs), 1)
        self.assertEqual(jobs.jobs[0].id, "gid-1")

        details = client.get_job_details(job_id="gid-1")
        self.assertEqual(details.status, 200)
        self.assertEqual(
            details.jobDescription,
            "Google Engineer\n\nAbout the job\ngreat job\n\nMinimum Qualifications\npython\n\nPreferred Qualifications\ngo\n\nResponsibilities\nbuild",
        )

    def test_microsoft_client_with_mock_endpoint(self) -> None:
        client = MicrosoftJobsClient(
            base_url=f"{self._base_url}/microsoft",
            default_request_policy=self.policy,
            proxy_management_client=self.proxy_client,
        )
        jobs = client.get_jobs(page=1)
        self.assertEqual(jobs.status, 200)
        self.assertEqual(len(jobs.jobs), 1)
        self.assertEqual(jobs.jobs[0].id, "ms-1")

        details = client.get_job_details(job_id="ms-1")
        self.assertEqual(details.status, 200)
        self.assertEqual(details.jobDescription, "build cloud")
