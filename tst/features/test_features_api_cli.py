from __future__ import annotations

import importlib
import io
import sys
import unittest
from unittest.mock import patch

from features.scripts import features_api_cli


class FeaturesApiCliTest(unittest.TestCase):
    def test_import_inserts_root_src_when_missing(self) -> None:
        module_name = "features.scripts.features_api_cli"
        root_src = str(features_api_cli.ROOT_SRC)
        saved_module = sys.modules.pop(module_name, None)
        saved_sys_path = list(sys.path)
        try:
            sys.path = [entry for entry in sys.path if entry != root_src]
            reloaded = importlib.import_module(module_name)
            self.assertIn(root_src, sys.path)
            self.assertEqual(reloaded.ROOT_SRC, features_api_cli.ROOT_SRC)
        finally:
            sys.path = saved_sys_path
            sys.modules.pop(module_name, None)
            if saved_module is not None:
                sys.modules[module_name] = saved_module

    def test_build_parser_accepts_normalize_locations(self) -> None:
        parser = features_api_cli.build_parser()
        args = parser.parse_args(
            [
                "normalize-locations",
                "--location",
                "Seattle, WA, USA",
                "--location",
                "London, UK",
            ]
        )
        self.assertEqual(args.command, "normalize-locations")
        self.assertEqual(args.locations, ["Seattle, WA, USA", "London, UK"])

    def test_main_normalize_locations(self) -> None:
        fake_client = unittest.mock.Mock()
        fake_client.normalize_locations.return_value = {
            "status": 200,
            "error": None,
            "locations": [
                {"city": "Seattle", "region": "Washington", "country": "United States"},
                {"city": "London", "region": None, "country": "United Kingdom"},
            ],
        }
        fake_request_policy = object()

        with patch("sys.argv", ["features_api_cli.py", "normalize-locations", "--location", "Seattle, WA, USA"]), patch(
            "common.request_policy.RequestPolicy", return_value=fake_request_policy
        ), patch(
            "features.client.FeaturesClient", return_value=fake_client
        ), patch("sys.stdout", new_callable=io.StringIO) as stdout:
            code = features_api_cli.main()

        self.assertEqual(code, 0)
        fake_client.normalize_locations.assert_called_once_with(locations=["Seattle, WA, USA"])
        self.assertIn('"city": "Seattle"', stdout.getvalue())

    def test_main_get_job_skills(self) -> None:
        fake_client = unittest.mock.Mock()
        fake_client.get_job_skills.return_value = {
            "status": 200,
            "error": None,
            "skills": ["Python"],
            "embedding": [0.1, -0.2],
        }
        fake_request_policy = object()

        with patch("sys.argv", ["features_api_cli.py", "get-job-skills", "--text", "Need Python"]), patch(
            "common.request_policy.RequestPolicy", return_value=fake_request_policy
        ), patch(
            "features.client.FeaturesClient", return_value=fake_client
        ), patch("sys.stdout", new_callable=io.StringIO) as stdout:
            code = features_api_cli.main()

        self.assertEqual(code, 0)
        fake_client.get_job_skills.assert_called_once_with(text="Need Python")
        self.assertIn('"skills": [', stdout.getvalue())

    def test_main_reports_http_error(self) -> None:
        fake_client = unittest.mock.Mock()
        response = unittest.mock.Mock(status_code=422, text="bad request")
        error = features_api_cli.requests.HTTPError("boom")
        error.response = response
        fake_client.get_job_skills.side_effect = error
        fake_request_policy = object()

        with patch("sys.argv", ["features_api_cli.py", "get-job-skills", "--text", "Need Python"]), patch(
            "common.request_policy.RequestPolicy", return_value=fake_request_policy
        ), patch(
            "features.client.FeaturesClient", return_value=fake_client
        ), patch("sys.stderr", new_callable=io.StringIO) as stderr:
            code = features_api_cli.main()

        self.assertEqual(code, 1)
        self.assertIn("status=422 body=bad request", stderr.getvalue())

    def test_main_reports_generic_error(self) -> None:
        fake_client = unittest.mock.Mock()
        fake_client.normalize_locations.side_effect = RuntimeError("boom")
        fake_request_policy = object()

        with patch("sys.argv", ["features_api_cli.py", "normalize-locations", "--location", "Seattle, WA, USA"]), patch(
            "common.request_policy.RequestPolicy", return_value=fake_request_policy
        ), patch(
            "features.client.FeaturesClient", return_value=fake_client
        ), patch("sys.stderr", new_callable=io.StringIO) as stderr:
            code = features_api_cli.main()

        self.assertEqual(code, 1)
        self.assertIn("RuntimeError: boom", stderr.getvalue())
