import unittest

from scrapers.airflow.clients.common.base import JobsClient
from common.request_policy import RequestPolicy
from web.backend.schemas import GetJobDetailsResponse, GetJobsResponse


class _DummyJobsClient(JobsClient):
    def get_jobs(self, *, page: int = 1) -> GetJobsResponse:
        return GetJobsResponse(status=200, jobs=[])

    def get_job_details(self, *, job_id: str) -> GetJobDetailsResponse:
        return GetJobDetailsResponse(status=200, jobDescription=None)


class BaseClientTest(unittest.TestCase):
    def test_get_request_policy_override_and_default(self) -> None:
        default_policy = RequestPolicy(timeout_seconds=1.0, max_retries=5)
        override_policy = RequestPolicy(timeout_seconds=2.0, max_retries=2)
        client = _DummyJobsClient(
            default_request_policy=default_policy,
            endpoint_request_policies={"jobs": override_policy},
        )
        self.assertIs(client.get_request_policy("jobs"), override_policy)
        self.assertIs(client.get_request_policy("details"), default_policy)

    def test_implemented_methods(self) -> None:
        client = _DummyJobsClient(default_request_policy=RequestPolicy(timeout_seconds=1.0, max_retries=1))
        self.assertEqual(client.get_jobs().status, 200)
        self.assertEqual(client.get_job_details(job_id="1").status, 200)


if __name__ == "__main__":
    unittest.main()
