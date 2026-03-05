import unittest
from unittest.mock import Mock, patch

from scrapers.airflow.clients.client_factory import _resolve_endpoint_policies, build_client
from scrapers.airflow.clients.common.request_policy import RequestPolicy


class _DummyClient:
    BASE_URL = "https://example.test"

    def __init__(self, **kwargs):
        self.kwargs = kwargs


class ClientFactoryTest(unittest.TestCase):
    def _default_policy(self) -> RequestPolicy:
        return RequestPolicy(timeout_seconds=1.0, max_retries=5)

    def test_resolve_endpoint_policies_returns_copy(self) -> None:
        default = self._default_policy()
        resolved = _resolve_endpoint_policies(
            company="meta",
            default_request_policy=default,
            endpoint_request_policies=None,
        )
        self.assertEqual(resolved, {})

    def test_resolve_endpoint_policies_keeps_existing_policies(self) -> None:
        default = self._default_policy()
        existing = RequestPolicy(timeout_seconds=2.0, max_retries=9)
        resolved = _resolve_endpoint_policies(
            company="meta",
            default_request_policy=default,
            endpoint_request_policies={"details": existing},
        )
        self.assertIs(resolved["details"], existing)

    def test_build_client_returns_expected_client_type(self) -> None:
        default = self._default_policy()
        proxy_client = Mock()
        with patch("scrapers.airflow.clients.client_factory.AmazonJobsClient", _DummyClient), patch(
            "scrapers.airflow.clients.client_factory.AppleJobsClient", _DummyClient
        ), patch("scrapers.airflow.clients.client_factory.GoogleJobsClient", _DummyClient), patch(
            "scrapers.airflow.clients.client_factory.MetaJobsClient", _DummyClient
        ), patch(
            "scrapers.airflow.clients.client_factory.MicrosoftJobsClient", _DummyClient
        ), patch(
            "scrapers.airflow.clients.client_factory.NetflixJobsClient", _DummyClient
        ):
            for company in ("amazon", "apple", "google", "meta", "microsoft", "netflix"):
                out = build_client(
                    company=company,
                    proxy_management_client=proxy_client,
                    default_request_policy=default,
                    endpoint_request_policies={},
                )
                self.assertIsInstance(out, _DummyClient)
                self.assertEqual(out.kwargs["base_url"], _DummyClient.BASE_URL)
                self.assertIs(out.kwargs["proxy_management_client"], proxy_client)
                self.assertIs(out.kwargs["default_request_policy"], default)

    def test_build_client_unsupported_company(self) -> None:
        with self.assertRaises(ValueError):
            build_client(
                company="nope",
                proxy_management_client=Mock(),
                default_request_policy=self._default_policy(),
            )


if __name__ == "__main__":
    unittest.main()
