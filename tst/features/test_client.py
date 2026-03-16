from __future__ import annotations

import importlib
import sys
import types
import unittest
from unittest.mock import Mock


class FeaturesClientTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._module_names = ["requests", "backoff", "features.client", "common.request_policy"]
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
                    giveup = decorator_kwargs.get("giveup")
                    attempt = 0
                    while True:
                        attempt += 1
                        try:
                            return fn(*args, **kwargs)
                        except exception_cls as exc:
                            if giveup is not None and giveup(exc):
                                raise
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
        sys.modules.pop("features.client", None)
        cls.requests = requests_mod
        cls.RequestPolicy = _RequestPolicy
        cls.mod = importlib.import_module("features.client")

    @classmethod
    def tearDownClass(cls) -> None:
        for name in cls._module_names:
            if cls._saved_modules[name] is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = cls._saved_modules[name]

    def test_init_validation(self) -> None:
        with self.assertRaises(ValueError):
            self.mod.FeaturesClient(
                base_url="",
                request_policy=self.RequestPolicy(timeout_seconds=1.0, max_retries=1),
            )
        with self.assertRaises(ValueError):
            self.mod.FeaturesClient(
                base_url="http://localhost:8010",
                request_policy=self.RequestPolicy(timeout_seconds=0, max_retries=1),
            )

    def test_get_job_skills(self) -> None:
        requests_mod = self.requests

        class _FakeResponse:
            def __init__(self, status_code: int = 200, payload=None) -> None:
                self.status_code = status_code
                self._payload = payload

            def json(self):
                return self._payload

            def raise_for_status(self) -> None:
                if self.status_code >= 400:
                    error = requests_mod.exceptions.HTTPError(f"HTTP {self.status_code}")
                    error.response = self
                    raise error

        client = self.mod.FeaturesClient(
            base_url="http://localhost:8010",
            request_policy=self.RequestPolicy(timeout_seconds=2.0, max_retries=1),
        )
        client._session = Mock()
        client._session.request.return_value = _FakeResponse(
            payload={
                "status": 200,
                "error": None,
                "skills": ["Python"],
                "embedding": [0.1, -0.2],
            }
        )

        out = client.get_job_skills(text="  Need Python  ")

        self.assertEqual(out["skills"], ["Python"])
        self.assertEqual(out["embedding"], [0.1, -0.2])
        client._session.request.assert_called_once_with(
            method="POST",
            url="http://localhost:8010/job_skills",
            json={"text": "Need Python"},
            timeout=2.0,
        )

    def test_get_query_embedding(self) -> None:
        class _FakeResponse:
            def __init__(self, payload=None) -> None:
                self._payload = payload

            def json(self):
                return self._payload

            def raise_for_status(self) -> None:
                return None

        client = self.mod.FeaturesClient(
            base_url="http://localhost:8010",
            request_policy=self.RequestPolicy(timeout_seconds=2.0, max_retries=1),
        )
        client._session = Mock()
        client._session.request.return_value = _FakeResponse(payload={"embedding": [0.1, -0.2]})

        out = client.get_query_embedding(text=" Need Python ")

        self.assertEqual(out["embedding"], [0.1, -0.2])
        client._session.request.assert_called_once_with(
            method="POST",
            url="http://localhost:8010/query_embedding",
            json={"text": "Need Python"},
            timeout=2.0,
        )

    def test_normalize_locations(self) -> None:
        class _FakeResponse:
            def __init__(self, payload=None) -> None:
                self._payload = payload

            def json(self):
                return self._payload

            def raise_for_status(self) -> None:
                return None

        client = self.mod.FeaturesClient(
            base_url="http://localhost:8010",
            request_policy=self.RequestPolicy(timeout_seconds=2.0, max_retries=1),
        )
        client._session = Mock()
        client._session.request.return_value = _FakeResponse(
            payload={
                "status": 200,
                "error": None,
                "locations": [{"city": "Seattle", "region": "Washington", "country": "United States"}],
            }
        )

        out = client.normalize_locations(locations=[" Seattle, WA, USA ", " "])

        self.assertEqual(out["locations"][0]["city"], "Seattle")
        client._session.request.assert_called_once_with(
            method="POST",
            url="http://localhost:8010/normalize_locations",
            json={"locations": ["Seattle, WA, USA"]},
            timeout=2.0,
        )

    def test_normalize_locations_batches_requests(self) -> None:
        class _FakeResponse:
            def __init__(self, payload=None) -> None:
                self._payload = payload

            def json(self):
                return self._payload

            def raise_for_status(self) -> None:
                return None

        client = self.mod.FeaturesClient(
            base_url="http://localhost:8010",
            request_policy=self.RequestPolicy(timeout_seconds=2.0, max_retries=1),
        )
        client._session = Mock()
        client._session.request.side_effect = [
            _FakeResponse(payload={"locations": [{"city": "A"}] * 100}),
            _FakeResponse(payload={"locations": [{"city": "B"}]}),
        ]

        out = client.normalize_locations(locations=[f"Location {index}" for index in range(101)])

        self.assertEqual(len(out["locations"]), 101)
        self.assertEqual(client._session.request.call_count, 2)
        first_call = client._session.request.call_args_list[0]
        second_call = client._session.request.call_args_list[1]
        self.assertEqual(len(first_call.kwargs["json"]["locations"]), 100)
        self.assertEqual(len(second_call.kwargs["json"]["locations"]), 1)

    def test_get_query_embedding_validates_text_and_payload(self) -> None:
        client = self.mod.FeaturesClient(
            base_url="http://localhost:8010",
            request_policy=self.RequestPolicy(timeout_seconds=2.0, max_retries=1),
        )
        with self.assertRaises(ValueError):
            client.get_query_embedding(text="   ")

        class _FakeResponse:
            def json(self):
                return []

            def raise_for_status(self) -> None:
                return None

        client._session = Mock()
        client._session.request.return_value = _FakeResponse()
        with self.assertRaises(ValueError):
            client.get_query_embedding(text="Need Python")

    def test_normalize_locations_validates_input_and_payload(self) -> None:
        client = self.mod.FeaturesClient(
            base_url="http://localhost:8010",
            request_policy=self.RequestPolicy(timeout_seconds=2.0, max_retries=1),
        )
        with self.assertRaises(ValueError):
            client.normalize_locations(locations=[" ", ""])

        class _FakeResponse:
            def json(self):
                return []

            def raise_for_status(self) -> None:
                return None

        client._session = Mock()
        client._session.request.return_value = _FakeResponse()
        with self.assertRaises(ValueError):
            client.normalize_locations(locations=["Seattle, WA, USA"])

        class _FakeResponseWrongShape:
            def json(self):
                return {"locations": "bad"}

            def raise_for_status(self) -> None:
                return None

        client._session.request.return_value = _FakeResponseWrongShape()
        with self.assertRaises(ValueError):
            client.normalize_locations(locations=["Seattle, WA, USA"])

    def test_get_job_skills_uses_connect_timeout_tuple(self) -> None:
        class _FakeResponse:
            def __init__(self, payload=None) -> None:
                self._payload = payload

            def json(self):
                return self._payload

            def raise_for_status(self) -> None:
                return None

        client = self.mod.FeaturesClient(
            base_url="http://localhost:8010",
            request_policy=self.RequestPolicy(timeout_seconds=2.0, connect_timeout_seconds=0.5, max_retries=1),
        )
        client._session = Mock()
        client._session.request.return_value = _FakeResponse(payload={"skills": []})

        client.get_job_skills(text="Need Python")

        client._session.request.assert_called_once_with(
            method="POST",
            url="http://localhost:8010/job_skills",
            json={"text": "Need Python"},
            timeout=(0.5, 2.0),
        )

    def test_get_job_skills_retries_with_backoff(self) -> None:
        requests_mod = self.requests

        class _FakeResponse:
            def __init__(self, payload=None) -> None:
                self._payload = payload

            def json(self):
                return self._payload

            def raise_for_status(self) -> None:
                return None

        client = self.mod.FeaturesClient(
            base_url="http://localhost:8010",
            request_policy=self.RequestPolicy(
                timeout_seconds=2.0,
                max_retries=3,
                backoff_factor=0.5,
                max_backoff_seconds=4.0,
                jitter=False,
            ),
        )
        client._session = Mock()
        client._session.request.side_effect = [
            requests_mod.exceptions.RequestException("boom-1"),
            requests_mod.exceptions.RequestException("boom-2"),
            _FakeResponse(payload={"skills": [], "embedding": []}),
        ]

        out = client.get_job_skills(text="Need Python")

        self.assertEqual(out, {"skills": [], "embedding": []})
        self.assertEqual(client._session.request.call_count, 3)

    def test_get_job_skills_retries_http_error_until_max_tries(self) -> None:
        requests_mod = self.requests

        class _FakeResponse:
            def __init__(self, status_code: int) -> None:
                self.status_code = status_code

            def json(self):
                return {}

            def raise_for_status(self) -> None:
                error = requests_mod.exceptions.HTTPError(f"HTTP {self.status_code}")
                error.response = self
                raise error

        client = self.mod.FeaturesClient(
            base_url="http://localhost:8010",
            request_policy=self.RequestPolicy(timeout_seconds=2.0, max_retries=3),
        )
        client._session = Mock()
        client._session.request.return_value = _FakeResponse(status_code=400)

        with self.assertRaises(requests_mod.exceptions.HTTPError):
            client.get_job_skills(text="Need Python")

        self.assertEqual(client._session.request.call_count, 3)

    def test_get_job_skills_validates_input_and_payload(self) -> None:
        class _FakeResponse:
            def __init__(self, payload=None) -> None:
                self._payload = payload

            def json(self):
                return self._payload

            def raise_for_status(self) -> None:
                return None

        client = self.mod.FeaturesClient(
            base_url="http://localhost:8010",
            request_policy=self.RequestPolicy(timeout_seconds=2.0, max_retries=1),
        )
        with self.assertRaises(ValueError):
            client.get_job_skills(text="   ")

        client._session = Mock()
        client._session.request.return_value = _FakeResponse(payload=["bad"])
        with self.assertRaises(ValueError):
            client.get_job_skills(text="Need Python")


if __name__ == "__main__":
    unittest.main()
