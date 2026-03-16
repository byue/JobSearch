import io
import unittest
from contextlib import redirect_stderr, redirect_stdout
from unittest.mock import Mock, patch

import requests

from web.backend.scripts import web_api_cli


class WebApiCliTest(unittest.TestCase):
    def test_build_parser_requires_command(self) -> None:
        parser = web_api_cli.build_parser()
        with self.assertRaises(SystemExit):
            parser.parse_args([])

    def test_main_get_companies_success(self) -> None:
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = {"companies": ["amazon"]}

        with patch("requests.request", return_value=response) as request_mock:
            with patch("sys.argv", ["web_api_cli.py", "get-companies"]):
                stdout = io.StringIO()
                with redirect_stdout(stdout):
                    code = web_api_cli.main()

        self.assertEqual(code, 0)
        request_mock.assert_called_once()
        self.assertIn('"companies"', stdout.getvalue())

    def test_main_get_jobs_location_filters_and_details_success(self) -> None:
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = {"status": 200}

        with patch("requests.request", return_value=response) as request_mock:
            with patch(
                "sys.argv",
                [
                    "web_api_cli.py",
                    "--api-url",
                    "http://x",
                    "get-jobs",
                    "--company",
                    "amazon",
                    "--page",
                    "2",
                    "--query",
                    "python",
                    "--posted-within",
                    "7d",
                    "--job-type",
                    "software_engineer",
                    "--country",
                    "United States",
                    "--region",
                    "Washington",
                    "--city",
                    "Seattle",
                ],
            ):
                with redirect_stdout(io.StringIO()):
                    self.assertEqual(web_api_cli.main(), 0)

            with patch(
                "sys.argv",
                [
                    "web_api_cli.py",
                    "get-location-filters",
                    "--company",
                    "amazon",
                    "--job-type",
                    "software_engineer",
                    "--country",
                    "United States",
                    "--region",
                    "Washington",
                ],
            ):
                with redirect_stdout(io.StringIO()):
                    self.assertEqual(web_api_cli.main(), 0)

            with patch(
                "sys.argv",
                ["web_api_cli.py", "get-job-details", "--company", "amazon", "--job-id", "123"],
            ):
                with redirect_stdout(io.StringIO()):
                    self.assertEqual(web_api_cli.main(), 0)

        self.assertEqual(request_mock.call_count, 3)
        get_jobs_call = request_mock.call_args_list[0]
        self.assertEqual(get_jobs_call.kwargs["json"]["query"], "python")
        self.assertEqual(get_jobs_call.kwargs["json"]["posted_within"], "7d")
        self.assertEqual(get_jobs_call.kwargs["json"]["job_type"], "software_engineer")
        self.assertEqual(get_jobs_call.kwargs["json"]["country"], "United States")
        self.assertEqual(get_jobs_call.kwargs["json"]["region"], "Washington")
        self.assertEqual(get_jobs_call.kwargs["json"]["city"], "Seattle")

        location_filters_call = request_mock.call_args_list[1]
        self.assertEqual(location_filters_call.kwargs["method"], "GET")
        self.assertIn("/get_location_filters?", location_filters_call.kwargs["url"])
        self.assertIn("country=United+States", location_filters_call.kwargs["url"])
        self.assertIn("region=Washington", location_filters_call.kwargs["url"])

    def test_main_http_error(self) -> None:
        response = Mock()
        response.status_code = 503
        response.text = "down"
        http_error = requests.HTTPError(response=response)

        with patch("requests.request", side_effect=http_error):
            with patch("sys.argv", ["web_api_cli.py", "get-companies"]):
                stderr = io.StringIO()
                with redirect_stderr(stderr):
                    code = web_api_cli.main()

        self.assertEqual(code, 1)
        self.assertIn("status=503", stderr.getvalue())

    def test_main_generic_error(self) -> None:
        with patch("requests.request", side_effect=RuntimeError("boom")):
            with patch("sys.argv", ["web_api_cli.py", "get-companies"]):
                stderr = io.StringIO()
                with redirect_stderr(stderr):
                    code = web_api_cli.main()

        self.assertEqual(code, 1)
        self.assertIn("RuntimeError", stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
