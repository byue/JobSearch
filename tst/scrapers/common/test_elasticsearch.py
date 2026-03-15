from __future__ import annotations

import importlib
import sys
import types
import unittest
from unittest.mock import Mock, patch


class ElasticsearchClientTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._module_names = ["requests", "backoff", "scrapers.common.elasticsearch", "common.request_policy"]
        cls._saved_modules = {name: sys.modules.get(name) for name in cls._module_names}

        requests_mod = types.ModuleType("requests")

        class _RequestException(Exception):
            pass

        class _HTTPError(_RequestException):
            def __init__(self, message: str) -> None:
                super().__init__(message)
                self.response = None

        class _Session:
            def request(self, *args, **kwargs):
                _ = (args, kwargs)
                return None

        requests_mod.Session = _Session
        requests_mod.exceptions = types.SimpleNamespace(
            HTTPError=_HTTPError,
            RequestException=_RequestException,
        )

        backoff_mod = types.ModuleType("backoff")

        def _expo(*args, **kwargs):
            _ = (args, kwargs)
            return None

        def _full_jitter(value):
            return value

        def _on_exception(wait_gen, exception_cls, **decorator_kwargs):
            _ = wait_gen

            def _decorate(fn):
                def _wrapped(*args, **kwargs):
                    max_tries = int(decorator_kwargs.get("max_tries", 1))
                    attempt = 0
                    while True:
                        attempt += 1
                        try:
                            return fn(*args, **kwargs)
                        except exception_cls:
                            if attempt >= max_tries:
                                raise

                return _wrapped

            return _decorate

        backoff_mod.expo = _expo
        backoff_mod.full_jitter = _full_jitter
        backoff_mod.on_exception = _on_exception

        request_policy_mod = types.ModuleType("common.request_policy")

        class _RequestPolicy:
            def __init__(
                self,
                *,
                timeout_seconds: float,
                max_retries: int,
                connect_timeout_seconds: float | None = None,
                backoff_factor: float = 0.5,
                max_backoff_seconds: float = 6.0,
                jitter: bool = False,
            ) -> None:
                self.timeout_seconds = timeout_seconds
                self.max_retries = max_retries
                self.connect_timeout_seconds = connect_timeout_seconds
                self.backoff_factor = backoff_factor
                self.max_backoff_seconds = max_backoff_seconds
                self.jitter = jitter

            def timeout_for_http(self):
                if self.connect_timeout_seconds is None:
                    return float(self.timeout_seconds)
                return (float(self.connect_timeout_seconds), float(self.timeout_seconds))

        request_policy_mod.RequestPolicy = _RequestPolicy

        sys.modules["requests"] = requests_mod
        sys.modules["backoff"] = backoff_mod
        sys.modules["common.request_policy"] = request_policy_mod
        sys.modules.pop("scrapers.common.elasticsearch", None)
        cls.requests = requests_mod
        cls.RequestPolicy = _RequestPolicy
        cls.mod = importlib.import_module("scrapers.common.elasticsearch")

    @classmethod
    def tearDownClass(cls) -> None:
        for name in cls._module_names:
            if cls._saved_modules[name] is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = cls._saved_modules[name]

    def test_init_validation(self) -> None:
        with self.assertRaises(ValueError):
            self.mod.ElasticsearchClient(
                base_url="",
                request_policy=self.RequestPolicy(timeout_seconds=1.0, max_retries=1),
            )
        with self.assertRaises(ValueError):
            self.mod.ElasticsearchClient(
                base_url="http://localhost:9200",
                request_policy=self.RequestPolicy(timeout_seconds=0, max_retries=1),
            )

    def test_request_json_ndjson_and_empty_content(self) -> None:
        class _FakeResponse:
            def __init__(self, *, payload=None, content=b"x") -> None:
                self._payload = payload
                self.content = content

            def raise_for_status(self) -> None:
                return None

            def json(self):
                return self._payload

        client = self.mod.ElasticsearchClient(
            base_url="http://localhost:9200",
            request_policy=self.RequestPolicy(timeout_seconds=2.0, max_retries=1),
        )
        client._session = Mock()
        client._session.request.side_effect = [
            _FakeResponse(payload={"ok": True}),
            _FakeResponse(payload=None, content=b""),
        ]

        out_json = client._request(method="POST", path="/x", payload={"a": 1}, params={"q": "1"})
        out_empty = client._request(method="POST", path="/bulk", ndjson_payload="{}\n")

        self.assertEqual(out_json, {"ok": True})
        self.assertIsNone(out_empty)
        first_call = client._session.request.call_args_list[0].kwargs
        self.assertEqual(first_call["json"], {"a": 1})
        self.assertEqual(first_call["params"], {"q": "1"})
        second_call = client._session.request.call_args_list[1].kwargs
        self.assertEqual(second_call["data"], "{}\n")
        self.assertEqual(second_call["headers"], {"Content-Type": "application/x-ndjson"})

    def test_create_index_bulk_count_alias_and_search(self) -> None:
        class _FakeResponse:
            def __init__(self, payload) -> None:
                self.payload = payload
                self.content = b"x"

            def raise_for_status(self) -> None:
                return None

            def json(self):
                return self.payload

        client = self.mod.ElasticsearchClient(
            base_url="http://localhost:9200",
            request_policy=self.RequestPolicy(timeout_seconds=2.0, max_retries=1),
        )
        client._session = Mock()
        client._session.request.side_effect = [
            _FakeResponse({"acknowledged": True}),
            _FakeResponse({"errors": False}),
            _FakeResponse({"count": 3}),
            _FakeResponse({"old_index": {}}),
            _FakeResponse({"acknowledged": True}),
            _FakeResponse({"hits": {"hits": []}}),
        ]

        self.assertEqual(
            client.create_index(index_name="jobs_catalog__r1", mapping={"properties": {}}),
            {"acknowledged": True},
        )
        bulk_out = client.bulk_index(
            index_name="jobs_catalog__r1",
            docs=[{"_id": "doc-1", "_source": {"title": "Role"}}],
            refresh=True,
        )
        self.assertFalse(bulk_out["errors"])
        self.assertEqual(client.count(index_name="jobs_catalog__r1"), 3)
        self.assertEqual(
            client.swap_alias(alias="jobs_catalog", index_name="jobs_catalog__r1"),
            {"acknowledged": True},
        )
        self.assertEqual(
            client.search(index_name="jobs_catalog", body={"query": {"match_all": {}}}),
            {"hits": {"hits": []}},
        )

        bulk_call = client._session.request.call_args_list[1].kwargs
        self.assertEqual(bulk_call["params"], {"refresh": "true"})
        alias_call = client._session.request.call_args_list[4].kwargs
        self.assertEqual(
            alias_call["json"],
            {
                "actions": [
                    {"remove": {"index": "old_index", "alias": "jobs_catalog"}},
                    {"add": {"index": "jobs_catalog__r1", "alias": "jobs_catalog"}},
                ]
            },
        )

    def test_non_dict_responses_return_defaults(self) -> None:
        client = self.mod.ElasticsearchClient(
            base_url="http://localhost:9200",
            request_policy=self.RequestPolicy(timeout_seconds=2.0, max_retries=1),
        )
        with patch.object(client, "_request", side_effect=[[], [], [], {}, [], []]):
            self.assertEqual(client.create_index(index_name="i", mapping={}), {})
            self.assertEqual(client.bulk_index(index_name="i", docs=[]), {})
            self.assertEqual(client.count(index_name="i"), 0)
            self.assertEqual(client.swap_alias(alias="a", index_name="i"), {})
            self.assertEqual(client.search(index_name="a", body={}), {})

    def test_create_index_handles_existing_index_error_payload(self) -> None:
        client = self.mod.ElasticsearchClient(
            base_url="http://localhost:9200",
            request_policy=self.RequestPolicy(timeout_seconds=2.0, max_retries=1),
        )
        error = self.requests.exceptions.HTTPError("exists")
        error.response = Mock()
        error.response.json.return_value = {
            "error": {"type": "resource_already_exists_exception"},
        }

        with patch.object(client, "_request", side_effect=error):
            self.assertEqual(
                client.create_index(index_name="jobs_catalog", mapping={}),
                {"acknowledged": True, "already_exists": True},
            )

    def test_create_index_reraises_unknown_http_error(self) -> None:
        client = self.mod.ElasticsearchClient(
            base_url="http://localhost:9200",
            request_policy=self.RequestPolicy(timeout_seconds=2.0, max_retries=1),
        )
        error = self.requests.exceptions.HTTPError("boom")
        error.response = Mock()
        error.response.json.side_effect = ValueError("bad json")

        with patch.object(client, "_request", side_effect=error):
            with self.assertRaises(self.requests.exceptions.HTTPError):
                client.create_index(index_name="jobs_catalog", mapping={})

    def test_swap_alias_handles_missing_alias(self) -> None:
        client = self.mod.ElasticsearchClient(
            base_url="http://localhost:9200",
            request_policy=self.RequestPolicy(timeout_seconds=2.0, max_retries=1),
        )
        not_found = self.requests.exceptions.HTTPError("missing")
        not_found.response = Mock(status_code=404)

        with patch.object(client, "_request", side_effect=[not_found, {"acknowledged": True}]) as request_mock:
            self.assertEqual(
                client.swap_alias(alias="jobs_catalog", index_name="jobs_catalog__r2"),
                {"acknowledged": True},
            )
            alias_update_call = request_mock.call_args_list[1]
            self.assertEqual(
                alias_update_call.kwargs["payload"],
                {"actions": [{"add": {"index": "jobs_catalog__r2", "alias": "jobs_catalog"}}]},
            )

    def test_swap_alias_reraises_non_404_http_error(self) -> None:
        client = self.mod.ElasticsearchClient(
            base_url="http://localhost:9200",
            request_policy=self.RequestPolicy(timeout_seconds=2.0, max_retries=1),
        )
        error = self.requests.exceptions.HTTPError("boom")
        error.response = Mock(status_code=500)

        with patch.object(client, "_request", side_effect=error):
            with self.assertRaises(self.requests.exceptions.HTTPError):
                client.swap_alias(alias="jobs_catalog", index_name="jobs_catalog__r2")


if __name__ == "__main__":
    unittest.main()
