import unittest

from scrapers.common import env as common_env
from scrapers.proxy import env as proxy_env


class ProxyEnvModuleTest(unittest.TestCase):
    def test_reexports_match_common_env(self) -> None:
        self.assertIs(proxy_env.require_env, common_env.require_env)
        self.assertIs(proxy_env.require_env_int, common_env.require_env_int)
        self.assertIs(proxy_env.require_env_float, common_env.require_env_float)
        self.assertIs(proxy_env.env_int, common_env.env_int)
        self.assertIs(proxy_env.env_float, common_env.env_float)

    def test_all_exports(self) -> None:
        self.assertEqual(
            proxy_env.__all__,
            ["require_env", "require_env_int", "require_env_float", "env_int", "env_float"],
        )


if __name__ == "__main__":
    unittest.main()
