import unittest
from unittest.mock import Mock, patch

import requests

from scrapers.airflow.clients.common.errors import RetryableUpstreamError
from scrapers.airflow.clients.common.request_policy import RequestPolicy
from scrapers.airflow.clients.meta.client import MetaJobsClient, _dedupe, _require_mapping, _to_int, _to_optional_str
from web.backend.schemas import JobDetailsSchema, PayDetails, PayRange


class MetaClientTest(unittest.TestCase):
    def _client(self) -> MetaJobsClient:
        return MetaJobsClient(
            base_url="https://www.metacareers.com",
            default_request_policy=RequestPolicy(timeout_seconds=1.0, max_retries=1),
            proxy_management_client=Mock(),
        )

    def test_scalar_helpers(self) -> None:
        self.assertEqual(_to_optional_str(" x "), "x")
        self.assertIsNone(_to_optional_str(1))
        self.assertEqual(_to_int(7), 7)
        self.assertEqual(_to_int("12"), 12)
        self.assertIsNone(_to_int("x"))
        self.assertEqual(_dedupe(["a", "a", "b"]), ["a", "b"])
        self.assertEqual(_require_mapping({"a": 1}, context="x")["a"], 1)
        with self.assertRaises(ValueError):
            _require_mapping([], context="x")

    def test_get_jobs_and_details(self) -> None:
        client = self._client()
        with patch.object(client, "_fetch_search_results", return_value={"all_jobs": "bad"}):
            with self.assertRaises(ValueError):
                client.get_jobs(page=1)
        with patch.object(client, "_fetch_search_results", return_value={"all_jobs": [1]}):
            with self.assertRaises(ValueError):
                client.get_jobs(page=1)
        with patch.object(
            client,
            "_fetch_search_results",
            return_value={"all_jobs": [{"id": "1", "title": "Eng", "locations": ["Seattle, WA, USA"]}]},
        ):
            out = client.get_jobs(page=1)
            self.assertEqual(out.status, 200)
            self.assertEqual(len(out.jobs), 1)
        with self.assertRaises(ValueError):
            client.get_jobs(page=0)

        with patch.object(client, "_run_graphql_query", return_value={"data": {"xcp_requisition_job_description": None}}):
            detail = client.get_job_details(job_id="1")
            self.assertEqual(detail.status, 404)
            self.assertEqual(detail.error, "Job '1' not found for company 'meta' url=https://www.metacareers.com/jobs/1/")
            self.assertIsNone(detail.job)
        http_404 = requests.exceptions.HTTPError("not found")
        http_404.response = requests.Response()
        http_404.response.status_code = 404
        with patch.object(client, "_run_graphql_query", side_effect=http_404):
            detail = client.get_job_details(job_id="1")
            self.assertEqual(detail.status, 404)
        http_500 = requests.exceptions.HTTPError("server error")
        http_500.response = requests.Response()
        http_500.response.status_code = 500
        with patch.object(client, "_run_graphql_query", side_effect=http_500):
            with self.assertRaises(requests.exceptions.HTTPError):
                client.get_job_details(job_id="1")
        with patch.object(client, "_run_graphql_query", return_value={"data": {"xcp_requisition_job_description": "bad"}}):
            with self.assertRaises(ValueError):
                client.get_job_details(job_id="1")
        with self.assertRaises(ValueError):
            client.get_job_details(job_id=" ")

    def test_get_job_details_success(self) -> None:
        client = self._client()
        with patch.object(
            client,
            "_run_graphql_query",
            return_value={"data": {"xcp_requisition_job_description": {"description": "x"}}},
        ), patch.object(client, "_parse_job_details") as parse_details, patch.object(
            client, "_extract_posted_ts_from_job_page", return_value=None
        ):
            parse_details.return_value = JobDetailsSchema(postedTs=1772323200)
            detail = client.get_job_details(job_id="1")
            self.assertEqual(detail.status, 200)
            self.assertIsNotNone(detail.job)
            self.assertIsNone(detail.job.postedTs)

    def test_get_job_details_retryable_upstream_error_returns_not_found(self) -> None:
        client = self._client()
        with patch.object(
            client,
            "_run_graphql_query",
            side_effect=RetryableUpstreamError("bootstrap failed"),
        ):
            detail = client.get_job_details(job_id="1")
            self.assertEqual(detail.status, 404)
            self.assertEqual(detail.error, "Job '1' not found for company 'meta' url=https://www.metacareers.com/jobs/1/")
            self.assertIsNone(detail.job)

    def test_get_job_details_uses_job_page_posted_ts_as_source_of_truth(self) -> None:
        client = self._client()
        with patch.object(
            client,
            "_run_graphql_query",
            return_value={"data": {"xcp_requisition_job_description": {"description": "x"}}},
        ), patch.object(client, "_parse_job_details", return_value=JobDetailsSchema(postedTs=111)), patch.object(
            client, "_extract_posted_ts_from_job_page", return_value=1772323200
        ):
            detail = client.get_job_details(job_id="1")
            self.assertEqual(detail.status, 200)
            self.assertIsNotNone(detail.job)
            self.assertEqual(detail.job.postedTs, 1772323200)

    def test_parse_job_metadata_and_details(self) -> None:
        client = self._client()
        metadata = client._parse_job_metadata(
            {"id": "1", "title": "Eng", "locations": ["Seattle, WA, USA"], "posted_date": "Mar 1, 2026"}
        )
        self.assertEqual(metadata.id, "1")
        self.assertEqual(metadata.company, "meta")
        self.assertIsNone(metadata.postedTs)
        with self.assertRaises(ValueError):
            client._parse_job_metadata({"id": " "})

        details = client._parse_job_details(
            payload={
                "description": '{"__html":"<p>Hello</p>"}',
                "posted_date": "2026-03-01T00:00:00Z",
                "minimum_qualifications": [{"item": "Min"}],
                "preferred_qualifications": ["Pref"],
                "responsibilities": [{"item": "Resp"}],
                "public_compensation": [
                    {"compensation_amount_minimum": "$100,000/year", "compensation_amount_maximum": "$120,000/year"}
                ],
            }
        )
        self.assertIn("Min", details.minimumQualifications)
        self.assertIn("Pref", details.preferredQualifications)
        self.assertIn("Resp", details.responsibilities)
        self.assertIsNotNone(details.payDetails)
        self.assertIsNone(details.postedTs)

    def test_graphql_query_and_bootstrap(self) -> None:
        client = self._client()
        with patch.object(client, "_bootstrap_lsd_token", return_value="abc"), patch(
            "scrapers.airflow.clients.meta.client.request_text_with_managed_proxy_backoff",
            return_value='for (;;);{"data":{"ok":1}}',
        ):
            out = client._run_graphql_query(
                doc_id="1",
                query_name="Q",
                variables={},
                referer_path="/jobsearch",
                endpoint_policy_key=client.SEARCH_POLICY_KEY,
            )
            self.assertEqual(out["data"]["ok"], 1)

        with patch.object(client, "_bootstrap_lsd_token", return_value="abc"), patch(
            "scrapers.airflow.clients.meta.client.request_text_with_managed_proxy_backoff",
            return_value='{"errors":[{"message":"boom"}]}',
        ):
            with self.assertRaises(ValueError):
                client._run_graphql_query(
                    doc_id="1",
                    query_name="Q",
                    variables={},
                    referer_path="/jobsearch",
                    endpoint_policy_key=client.SEARCH_POLICY_KEY,
                )

        with patch(
            "scrapers.airflow.clients.meta.client.request_text_with_backoff",
            return_value='..."LSD",[],{"token":"abc"}...',
        ):
            self.assertEqual(client._bootstrap_lsd_token(), "abc")
        with patch("scrapers.airflow.clients.meta.client.request_text_with_backoff", return_value="no-token"):
            with self.assertRaises(ValueError):
                client._bootstrap_lsd_token()

    def test_search_payload_helpers(self) -> None:
        client = self._client()
        inp = client._build_search_input(q="eng")
        self.assertEqual(inp["q"], "eng")
        extracted = client._extract_search_results_from_payload({"data": {"job_search_with_featured_jobs": {"all_jobs": []}}})
        self.assertEqual(extracted["all_jobs"], [])
        self.assertEqual(client._extract_total_results({"count": "5"}), 5)
        self.assertIsNone(client._extract_total_results({"count": "bad"}))
        self.assertTrue(client._resolve_has_next_page(page=1, jobs_count=25, total_results=30))
        self.assertFalse(client._resolve_has_next_page(page=1, jobs_count=1, total_results=None))
        with patch.object(client, "_run_graphql_query", return_value={"data": {"job_search_with_featured_jobs": {"all_jobs": []}}}):
            out = client._fetch_search_results()
            self.assertEqual(out["all_jobs"], [])

    def test_misc_helpers(self) -> None:
        client = self._client()
        self.assertEqual(client._build_jazoest("ab"), f"2{ord('a') + ord('b')}")
        self.assertEqual(client._strip_for_loop_prefix("for (;;); { }"), " { }")
        self.assertEqual(client._strip_for_loop_prefix("x"), "x")

        self.assertEqual(client._extract_html_fragment('{"__html":"x"}'), "x")
        self.assertEqual(client._extract_html_fragment("plain"), "plain")
        self.assertIsNone(client._extract_html_fragment(None))
        self.assertEqual(client._extract_detail_items([{"item": " A "}, "B"]), ["A", "B"])
        self.assertEqual(
            client._extract_posted_ts_from_html(
                '<script type="application/ld+json">{"@context":"https://schema.org","datePosted":"2026-03-04T14:02:09-08:00"}</script>'
            ),
            1772661729,
        )
        self.assertEqual(
            client._extract_posted_ts_from_html('<script type="application/ld+json">{"x":1}</script>"datePosted":"2026-03-01"'),
            1772323200,
        )

        comp = client._extract_public_compensation(
            [{"compensation_amount_minimum": "$100,000/year", "has_bonus": True, "has_equity": True}]
        )
        self.assertIsNotNone(comp)
        assert comp is not None
        self.assertEqual(comp.ranges[0].currency, "USD")

        merged = client._merge_pay_details(
            PayDetails(ranges=[PayRange(minAmount=1)], notes=["a"]),
            PayDetails(ranges=[PayRange(minAmount=1), PayRange(minAmount=2)], notes=["a", "b"]),
        )
        self.assertIsNotNone(merged)
        assert merged is not None
        self.assertEqual(len(merged.ranges), 2)
        self.assertEqual(merged.notes, ["a", "b"])

        self.assertEqual(client._parse_compensation_amount("$120,000/year")[0], 120000)
        self.assertEqual(client._normalize_currency("£"), "GBP")
        self.assertEqual(client._normalize_currency("€"), "EUR")
        self.assertEqual(client._normalize_interval("yr"), "year")
        self.assertEqual(client._to_locations(["Seattle, WA, USA"])[0].city, "Seattle")

    def test_additional_helper_branches(self) -> None:
        client = self._client()
        self.assertEqual(client._extract_html_fragment('{"__html":"x"}'), "x")
        self.assertEqual(client._extract_html_fragment('{"__html":1}'), '{"__html":1}')
        self.assertEqual(client._extract_html_fragment('{"__html":"x"'), '{"__html":"x"')
        self.assertEqual(client._extract_detail_items("bad"), [])
        self.assertEqual(client._extract_detail_items([{"item": " "}, 1]), [])

        self.assertIsNone(client._extract_public_compensation("bad"))
        self.assertIsNone(client._extract_public_compensation([1, {"x": "y"}]))
        comp = client._extract_public_compensation(
            [{"compensation_amount_minimum": "$100,000/year", "error_apology_note": "Note"}]
        )
        self.assertIsNotNone(comp)
        assert comp is not None
        self.assertIn("Note", comp.notes)

        fallback_only = client._merge_pay_details(None, PayDetails(ranges=[], notes=[]))
        self.assertIsNotNone(fallback_only)
        primary_only = client._merge_pay_details(PayDetails(ranges=[], notes=[]), None)
        self.assertIsNotNone(primary_only)

        self.assertEqual(client._parse_compensation_amount("bad"), (None, None, None))
        self.assertIsNone(client._normalize_currency("x"))
        self.assertEqual(client._normalize_interval("mo"), "month")
        self.assertEqual(client._normalize_interval("wk"), "week")
        self.assertEqual(client._normalize_interval("day"), "day")
        self.assertEqual(client._normalize_interval("hr"), "hour")
        self.assertEqual(client._normalize_interval("custom"), "custom")
        self.assertIsNone(client._normalize_interval(None))

        self.assertEqual(client._to_locations(["USA"])[0].country, "USA")
        two_part_state = client._to_locations(["Seattle, WA"])[0]
        self.assertEqual(two_part_state.state, "WA")
        two_part_ca = client._to_locations(["San Jose, CA"])[0]
        self.assertEqual(two_part_ca.state, "CA")
        two_part_country = client._to_locations(["London, UK"])[0]
        self.assertEqual(two_part_country.country, "UK")
        two_part_generic = client._to_locations(["Paris, France"])[0]
        self.assertEqual(two_part_generic.country, "France")

    def test_posted_ts_helpers_additional_branches(self) -> None:
        client = self._client()
        self.assertEqual(client._parse_timestamp_any(1700000000), 1700000000)
        self.assertEqual(client._parse_timestamp_any(1700000000000), 1700000000)
        self.assertEqual(client._parse_timestamp_any({"timestamp": "2026-03-01T00:00:00Z"}), 1772323200)
        self.assertEqual(client._parse_timestamp_any("2026-03-01T00:00:00"), 1772323200)
        self.assertEqual(client._parse_timestamp_any("Mar 1, 2026"), 1772323200)
        self.assertEqual(client._parse_timestamp_any("March 1, 2026"), 1772323200)
        self.assertEqual(client._parse_timestamp_any("2026-03-01"), 1772323200)
        self.assertIsNone(client._parse_timestamp_any("   "))
        self.assertIsNone(client._parse_timestamp_any("bad-date"))

        # JSON-LD parser branches: empty script, invalid json, and no datePosted fallback.
        self.assertIsNone(
            client._extract_posted_ts_from_html(
                '<script type="application/ld+json">   </script>'
                '<script type="application/ld+json">{bad json</script>'
            )
        )
        self.assertIsNone(client._extract_posted_ts_from_html("<html><body>no date posted</body></html>"))

        found = client._find_key_recursive({"x": [{"y": {"datePosted": "2026-03-01"}}]}, target_key="datePosted")
        self.assertEqual(found, "2026-03-01")
        self.assertIsNone(client._find_key_recursive([1, 2, 3], target_key="datePosted"))

    def test_extract_posted_ts_from_job_page_success_and_failure(self) -> None:
        client = self._client()

        with patch(
            "scrapers.airflow.clients.meta.client.request_text_with_backoff",
            return_value='<script type="application/ld+json">{"datePosted":"2026-03-01T00:00:00Z"}</script>',
        ):
            posted_ts = client._extract_posted_ts_from_job_page(details_url="https://www.metacareers.com/jobs/1/")
            self.assertEqual(posted_ts, 1772323200)

        http_error = requests.exceptions.HTTPError("boom")
        response = requests.Response()
        response.status_code = 502
        http_error.response = response
        with patch("scrapers.airflow.clients.meta.client.request_text_with_backoff", side_effect=http_error):
            posted_ts = client._extract_posted_ts_from_job_page(details_url="https://www.metacareers.com/jobs/1/")
            self.assertIsNone(posted_ts)

    def test_run_graphql_query_error_without_message(self) -> None:
        client = self._client()
        with patch.object(client, "_bootstrap_lsd_token", return_value="abc"), patch(
            "scrapers.airflow.clients.meta.client.request_text_with_managed_proxy_backoff",
            return_value='{"errors":["boom"]}',
        ):
            with self.assertRaises(ValueError):
                client._run_graphql_query(
                    doc_id="1",
                    query_name="Q",
                    variables={},
                    referer_path="/jobsearch",
                    endpoint_policy_key=client.SEARCH_POLICY_KEY,
                )

    def test_run_graphql_query_http_error_status_branch(self) -> None:
        client = self._client()
        http_error = requests.exceptions.HTTPError("boom")
        response = requests.Response()
        response.status_code = 502
        http_error.response = response
        with patch.object(client, "_bootstrap_lsd_token", return_value="abc"), patch(
            "scrapers.airflow.clients.meta.client.request_text_with_managed_proxy_backoff",
            side_effect=http_error,
        ):
            with self.assertRaises(requests.exceptions.HTTPError):
                client._run_graphql_query(
                    doc_id="1",
                    query_name="Q",
                    variables={},
                    referer_path="/jobsearch",
                    endpoint_policy_key=client.SEARCH_POLICY_KEY,
                )


if __name__ == "__main__":
    unittest.main()
