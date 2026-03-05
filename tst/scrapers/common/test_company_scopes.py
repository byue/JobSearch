import unittest

from scrapers.common.company_scopes import DEFAULT_COMPANIES, resolve_companies, resolve_scopes


class CompanyScopesTest(unittest.TestCase):
    def test_resolve_companies_defaults_when_missing(self) -> None:
        self.assertEqual(resolve_companies(None), list(DEFAULT_COMPANIES))

    def test_resolve_companies_defaults_when_empty_after_parsing(self) -> None:
        self.assertEqual(resolve_companies(" , , "), list(DEFAULT_COMPANIES))

    def test_resolve_companies_filters_unknown_and_normalizes(self) -> None:
        resolved = resolve_companies(" amazon,UNKNOWN,  NETFLIX ,bad ")
        self.assertEqual(resolved, ["amazon", "netflix"])

    def test_resolve_scopes_dedupes_and_skips_unknown(self) -> None:
        resolved = resolve_scopes(["amazon", "amazon", "unknown", "google"])
        self.assertEqual(resolved, ["www.amazon.jobs", "www.google.com"])


if __name__ == "__main__":
    unittest.main()
