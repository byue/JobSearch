from __future__ import annotations

import shutil
import subprocess
import time
import unittest

try:
    import requests

    from common.request_policy import RequestPolicy
    from features.client import FeaturesClient
except ModuleNotFoundError:  # pragma: no cover - depends on local env
    requests = None
    RequestPolicy = None
    FeaturesClient = None


COMPOSE_CMD = ["docker", "compose", "-f", "src/docker-compose.yml"]
FEATURES_BASE_URL = "http://127.0.0.1:8010"


@unittest.skipIf(
    requests is None or RequestPolicy is None or FeaturesClient is None,
    "features integration dependencies are not installed",
)
class FeaturesClientIntegrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        if shutil.which("docker") is None:
            raise unittest.SkipTest("docker is not installed")

        try:
            subprocess.run(
                [*COMPOSE_CMD, "up", "-d", "--build", "features"],
                check=True,
                capture_output=True,
                text=True,
            )
        except subprocess.CalledProcessError as exc:  # pragma: no cover - depends on local env
            raise unittest.SkipTest(
                f"unable to start features container: {exc.stderr or exc.stdout or exc}"
            ) from exc

        deadline = time.time() + 60.0
        last_error: Exception | None = None
        while time.time() < deadline:
            try:
                response = requests.post(
                    f"{FEATURES_BASE_URL}/job_skills",
                    json={"text": "Python"},
                    timeout=2.0,
                )
                if response.status_code == 200:
                    return
            except Exception as exc:
                last_error = exc
            time.sleep(0.5)
        raise RuntimeError(f"features container did not start: {last_error}")

    @classmethod
    def tearDownClass(cls) -> None:
        if shutil.which("docker") is None:
            return
        subprocess.run(
            [*COMPOSE_CMD, "rm", "-f", "-s", "features"],
            check=False,
            capture_output=True,
            text=True,
        )

    def test_get_job_skills_against_features_container(self) -> None:
        client = FeaturesClient(
            base_url=FEATURES_BASE_URL,
            request_policy=RequestPolicy(timeout_seconds=5.0, max_retries=1),
        )

        payload = client.get_job_skills(
            text="Requirements: Python, Docker, and Kubernetes experience."
        )

        self.assertEqual(payload["status"], 200)
        self.assertIsNone(payload["error"])
        self.assertIsInstance(payload["skills"], list)
        canonicals = {str(item).lower() for item in payload["skills"]}
        self.assertIn("python", canonicals)


if __name__ == "__main__":
    unittest.main()
