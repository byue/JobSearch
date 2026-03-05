import os
import unittest
from unittest.mock import patch

from scrapers.common.env import (
    env_bool,
    env_float,
    env_int,
    require_env,
    require_env_float,
    require_env_int,
)


class EnvUtilsTest(unittest.TestCase):
    def test_require_env_success(self) -> None:
        with patch.dict(os.environ, {"X_REQUIRED": "  value  "}, clear=True):
            self.assertEqual(require_env("X_REQUIRED"), "value")

    def test_require_env_missing_and_empty(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(RuntimeError):
                require_env("X_REQUIRED")
        with patch.dict(os.environ, {"X_REQUIRED": "   "}, clear=True):
            with self.assertRaises(RuntimeError):
                require_env("X_REQUIRED")

    def test_require_env_int(self) -> None:
        with patch.dict(os.environ, {"X_INT": "5"}, clear=True):
            self.assertEqual(require_env_int("X_INT", minimum=1), 5)
        with patch.dict(os.environ, {"X_INT": "nope"}, clear=True):
            with self.assertRaises(RuntimeError):
                require_env_int("X_INT")
        with patch.dict(os.environ, {"X_INT": "0"}, clear=True):
            with self.assertRaises(RuntimeError):
                require_env_int("X_INT", minimum=1)

    def test_require_env_float(self) -> None:
        with patch.dict(os.environ, {"X_FLOAT": "1.5"}, clear=True):
            self.assertEqual(require_env_float("X_FLOAT", minimum=0.1), 1.5)
        with patch.dict(os.environ, {"X_FLOAT": "bad"}, clear=True):
            with self.assertRaises(RuntimeError):
                require_env_float("X_FLOAT")
        with patch.dict(os.environ, {"X_FLOAT": "0.01"}, clear=True):
            with self.assertRaises(RuntimeError):
                require_env_float("X_FLOAT", minimum=0.1)

    def test_env_int(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(env_int("X_INT", default=7, minimum=1), 7)
        with patch.dict(os.environ, {"X_INT": "bad"}, clear=True):
            self.assertEqual(env_int("X_INT", default=7, minimum=1), 7)
        with patch.dict(os.environ, {"X_INT": "0"}, clear=True):
            self.assertEqual(env_int("X_INT", default=7, minimum=1), 1)
        with patch.dict(os.environ, {"X_INT": "9"}, clear=True):
            self.assertEqual(env_int("X_INT", default=7, minimum=1), 9)

    def test_env_float(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(env_float("X_FLOAT", default=2.5, minimum=0.1), 2.5)
        with patch.dict(os.environ, {"X_FLOAT": "bad"}, clear=True):
            self.assertEqual(env_float("X_FLOAT", default=2.5, minimum=0.1), 2.5)
        with patch.dict(os.environ, {"X_FLOAT": "0.01"}, clear=True):
            self.assertEqual(env_float("X_FLOAT", default=2.5, minimum=0.1), 0.1)
        with patch.dict(os.environ, {"X_FLOAT": "2.75"}, clear=True):
            self.assertEqual(env_float("X_FLOAT", default=2.5, minimum=0.1), 2.75)

    def test_env_bool(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            self.assertTrue(env_bool("X_BOOL", default=True))
            self.assertFalse(env_bool("X_BOOL", default=False))

        true_values = ["1", "true", "yes", "y", "on", " TrUe "]
        for value in true_values:
            with patch.dict(os.environ, {"X_BOOL": value}, clear=True):
                self.assertTrue(env_bool("X_BOOL", default=False))

        false_values = ["0", "false", "no", "n", "off", " FaLsE "]
        for value in false_values:
            with patch.dict(os.environ, {"X_BOOL": value}, clear=True):
                self.assertFalse(env_bool("X_BOOL", default=True))

        with patch.dict(os.environ, {"X_BOOL": "maybe"}, clear=True):
            self.assertTrue(env_bool("X_BOOL", default=True))
            self.assertFalse(env_bool("X_BOOL", default=False))


if __name__ == "__main__":
    unittest.main()
