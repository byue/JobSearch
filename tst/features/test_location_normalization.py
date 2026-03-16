from __future__ import annotations

import importlib
import sys
import types
import unittest


def _install_fake_location_deps() -> dict[str, object]:
    saved = {}
    for name in ("country_converter", "geonamescache", "pycountry", "thefuzz", "thefuzz.process", "features.location_normalization"):
        saved[name] = sys.modules.get(name)

    country_converter_mod = types.ModuleType("country_converter")

    class _FakeCountryConverter:
        _NAME_MAP = {
            "usa": "United States",
            "us": "United States",
            "uk": "United Kingdom",
            "il": "Israel",
            "jp": "Japan",
            "japan": "Japan",
            "france": "France",
            "germany": "Germany",
            "canada": "Canada",
        }
        _ISO2_MAP = {
            "usa": "US",
            "us": "US",
            "uk": "GB",
            "il": "IL",
            "jp": "JP",
            "japan": "JP",
            "france": "FR",
            "germany": "DE",
            "canada": "CA",
        }

        def convert(self, value, to="name"):
            key = str(value or "").strip().lower()
            if to == "name":
                return self._NAME_MAP.get(key, "not found")
            if to == "ISO2":
                return self._ISO2_MAP.get(key, "not found")
            return "not found"

    country_converter_mod.CountryConverter = _FakeCountryConverter

    geonamescache_mod = types.ModuleType("geonamescache")

    class _FakeGeonamesCache:
        def get_cities(self):
            return {
                "0": {"name": "", "countrycode": "US", "admin1code": "WA"},
                "1": {"name": "Seattle", "countrycode": "US", "admin1code": "WA"},
                "2": {"name": "London", "countrycode": "GB", "admin1code": ""},
                "3": {"name": "Paris", "countrycode": "FR", "admin1code": ""},
                "4": {"name": "Mountain View", "countrycode": "CA", "admin1code": "AB", "population": 1500},
                "5": {"name": "Mountain View", "countrycode": "US", "admin1code": "CA", "population": 82000},
                "6": {"name": "Menlo Park", "countrycode": "CA", "admin1code": "AB", "population": 500},
                "7": {"name": "Menlo Park", "countrycode": "US", "admin1code": "CA", "population": 34000},
                "8": {"name": "Chicago", "countrycode": "US", "admin1code": "IL", "population": 2700000},
            }

    geonamescache_mod.GeonamesCache = _FakeGeonamesCache

    pycountry_mod = types.ModuleType("pycountry")

    class _FakeSubdivision:
        def __init__(self, code: str, name: str, country_code: str, subtype: str) -> None:
            self.code = code
            self.name = name
            self.country_code = country_code
            self.type = subtype

    subdivisions_list = [
        _FakeSubdivision("US-WA", "Washington", "US", "state"),
        _FakeSubdivision("US-CA", "California", "US", "state"),
        _FakeSubdivision("US-IL", "Illinois", "US", "state"),
        _FakeSubdivision("CA-ON", "Ontario", "CA", "province"),
        _FakeSubdivision("CA-AB", "Alberta", "CA", "province"),
        _FakeSubdivision("JP-13", "Tokyo", "JP", "prefecture"),
        _FakeSubdivision("DE-BY", "Bavaria", "DE", "state"),
    ]

    class _FakeSubdivisions(list):
        def get(self, *, code: str):
            for item in self:
                if item.code == code:
                    return item
            return None

    pycountry_mod.subdivisions = _FakeSubdivisions(subdivisions_list)

    thefuzz_mod = types.ModuleType("thefuzz")
    process_mod = types.ModuleType("thefuzz.process")

    def _extract_one(query, choices):
        query = str(query)
        if not choices:
            return None
        if query == "washngton" and "washington" in choices:
            return ("washington", 90)
        if query == "seatl" and "seattle" in choices:
            return ("seattle", 90)
        first = choices[0]
        return (first, 40)

    process_mod.extractOne = _extract_one
    thefuzz_mod.process = process_mod

    sys.modules["country_converter"] = country_converter_mod
    sys.modules["geonamescache"] = geonamescache_mod
    sys.modules["pycountry"] = pycountry_mod
    sys.modules["thefuzz"] = thefuzz_mod
    sys.modules["thefuzz.process"] = process_mod
    sys.modules.pop("features.location_normalization", None)
    return saved


def _restore_modules(saved: dict[str, object]) -> None:
    for name, module in saved.items():
        if module is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = module


class LocationNormalizationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._saved_modules = _install_fake_location_deps()
        cls.mod = importlib.import_module("features.location_normalization")

    @classmethod
    def tearDownClass(cls) -> None:
        _restore_modules(cls._saved_modules)

    def setUp(self) -> None:
        self.mod._country_converter.cache_clear()
        self.mod._geonames_cache.cache_clear()
        self.mod._city_lookup.cache_clear()
        self.mod._subdivision_names.cache_clear()
        self.mod._subdivision_codes.cache_clear()
        self.mod._subdivision_name_keys.cache_clear()
        self.mod._city_name_keys.cache_clear()

    def test_cache_builders(self) -> None:
        self.assertEqual(self.mod._country_converter().convert("USA", to="name"), "United States")
        self.assertIn("seattle", self.mod._city_lookup())
        self.assertNotIn("", self.mod._city_lookup())
        self.assertEqual(len(self.mod._city_lookup()["mountain view"]), 2)
        self.assertIn("washington", self.mod._subdivision_names())
        self.assertIn("wa", self.mod._subdivision_codes())
        self.assertIn("washington", self.mod._subdivision_name_keys())
        self.assertIn("seattle", self.mod._city_name_keys())
        self.assertEqual(self.mod._country_priority_index("ZZ"), len(self.mod._COUNTRY_PRIORITY))
        self.assertIsNone(self.mod._resolve_city_candidate([]))
        self.assertIsNone(
            self.mod._resolve_city_candidate(
                self.mod._city_lookup()["chicago"],
                country="IL",
                require_constraints=True,
            )
        )

    def test_preprocess_and_country_normalization(self) -> None:
        self.assertEqual(self.mod._preprocess_token("Remote - Seattle"), "Seattle")
        self.assertEqual(self.mod._preprocess_token("Tokyo-to"), "Tokyo-to")
        self.assertEqual(self.mod._normalize_country_name("USA"), "United States")
        self.assertEqual(self.mod._normalize_country_iso2("USA"), "US")
        self.assertIsNone(self.mod._normalize_country_name(""))
        self.assertIsNone(self.mod._normalize_country_iso2(""))
        self.assertIsNone(self.mod._normalize_country_name("unknown-country"))
        self.assertIsNone(self.mod._normalize_country_iso2("unknown-country"))
        self.assertEqual(self.mod._country_candidate("USA"), ("United States", "US"))
        self.assertIsNone(self.mod._country_candidate("Atlantis"))
        self.assertEqual(self.mod._state_candidates("Washington")[0].name, "Washington")
        self.assertEqual(self.mod._state_candidates("WA")[0].name, "Washington")
        self.assertEqual(self.mod._state_candidates(""), [])
        self.assertEqual(self.mod._city_candidates("Seattle")[0]["name"], "Seattle")
        self.assertEqual(self.mod._city_candidates(""), [])
        original_name = self.mod._normalize_country_name
        original_iso2 = self.mod._normalize_country_iso2
        try:
            self.mod._normalize_country_name = lambda value: "Exampleland" if value == "example" else original_name(value)
            self.mod._normalize_country_iso2 = lambda value: None if value == "example" else original_iso2(value)
            self.assertIsNone(self.mod._country_candidate("example"))
        finally:
            self.mod._normalize_country_name = original_name
            self.mod._normalize_country_iso2 = original_iso2

    def test_classify_token_branches(self) -> None:
        self.assertEqual(self.mod._classify_token("").token_type, "unknown")
        country = self.mod._classify_token("USA")
        self.assertEqual((country.token_type, country.value, country.meta["iso2"]), ("country", "United States", "US"))

        state_name = self.mod._classify_token("Washington")
        self.assertEqual((state_name.token_type, state_name.value), ("state", "Washington"))

        state_code = self.mod._classify_token("WA")
        self.assertEqual((state_code.token_type, state_code.value), ("state", "Washington"))
        illinois = self.mod._classify_token("IL")
        self.assertEqual((illinois.token_type, illinois.value), ("country", "Israel"))

        city = self.mod._classify_token("Seattle")
        self.assertEqual((city.token_type, city.value), ("city", "Seattle"))

        fuzzy_state = self.mod._classify_token("washngton")
        self.assertEqual((fuzzy_state.token_type, fuzzy_state.value), ("state", "Washington"))

        fuzzy_city = self.mod._classify_token("seatl")
        self.assertEqual((fuzzy_city.token_type, fuzzy_city.value), ("city", "Seattle"))

        unknown = self.mod._classify_token("NotARealPlace123")
        self.assertEqual((unknown.token_type, unknown.value), ("unknown", "NotARealPlace123"))

    def test_assemble_branches(self) -> None:
        city, state, country = self.mod._assemble([self.mod._classify_token("Seattle")])
        self.assertEqual((city, state, country), ("Seattle", None, "United States"))

        city, state, country = self.mod._assemble([self.mod._classify_token("WA")])
        self.assertEqual((city, state, country), (None, "Washington", "United States"))

        city, state, country = self.mod._assemble([self.mod._classify_token("USA")])
        self.assertEqual((city, state, country), (None, None, "United States"))

    def test_normalize_location_paths(self) -> None:
        self.assertEqual(self.mod.normalize_location(""), (None, None, None))
        self.assertEqual(self.mod.normalize_location("Remote"), (None, None, None))
        self.assertEqual(
            self.mod.normalize_location("Seattle, WA, USA"),
            ("Seattle", "Washington", "United States"),
        )
        self.assertEqual(
            self.mod.normalize_location("London, UK"),
            ("London", None, "United Kingdom"),
        )
        self.assertEqual(
            self.mod.normalize_location("UK, London"),
            ("London", None, "United Kingdom"),
        )
        self.assertEqual(
            self.mod.normalize_location("Bavaria, Germany"),
            (None, "Bavaria", "Germany"),
        )
        self.assertEqual(
            self.mod.normalize_location("USA, Washington"),
            (None, "Washington", "United States"),
        )
        self.assertEqual(
            self.mod.normalize_location("Mountain View, Canada"),
            ("Mountain View", None, "Canada"),
        )
        self.assertEqual(
            self.mod.normalize_location("Mountain View, California, USA"),
            ("Mountain View", "California", "United States"),
        )
        self.assertEqual(
            self.mod.normalize_location("Mountain View, CA, USA"),
            ("Mountain View", "California", "United States"),
        )
        self.assertEqual(
            self.mod.normalize_location("Menlo Park, CA"),
            ("Menlo Park", "California", "United States"),
        )
        self.assertEqual(
            self.mod.normalize_location("Chicago, IL"),
            ("Chicago", "Illinois", "United States"),
        )
        self.assertEqual(
            self.mod.normalize_location("Chicago, IL, USA"),
            ("Chicago", "Illinois", "United States"),
        )
        self.assertEqual(
            self.mod.normalize_location("Menlo Park, USA"),
            ("Menlo Park", None, "United States"),
        )
        self.assertEqual(
            self.mod.normalize_location("Japan, Tokyo-to, Tokyo"),
            ("Tokyo", "Tokyo", "Japan"),
        )
        self.assertEqual(
            self.mod.normalize_location("USA, CA, Mountain View"),
            ("Mountain View", "California", "United States"),
        )
        self.assertEqual(
            self.mod.normalize_location(
                '{"normalizedCityName":"Seattle","normalizedStateName":"Washington","normalizedCountryName":"United States"}'
            ),
            ("Seattle", "Washington", "United States"),
        )
        self.assertEqual(
            self.mod.normalize_location('{"bad": "json"'),
            (None, None, None),
        )
        self.assertEqual(
            self.mod.normalize_location("Alpha, Beta"),
            (None, None, None),
        )

    def test_fill_missing_city_from_three_tokens_branches(self) -> None:
        city, state, country = self.mod._fill_missing_city_from_three_tokens(
            ["Seattle", "WA", "USA"],
            [
                self.mod._classify_token("Seattle"),
                self.mod._classify_token("WA"),
                self.mod._classify_token("USA"),
            ],
            None,
            "Washington",
            "United States",
        )
        self.assertEqual((city, state, country), ("Seattle", "Washington", "United States"))

        city, state, country = self.mod._fill_missing_city_from_three_tokens(
            ["Alpha", "Beta", "Gamma"],
            [
                self.mod._ClassifiedToken("state", "Alpha", 1.0, {"country": "US", "code": "AL"}),
                self.mod._ClassifiedToken("state", "Beta", 1.0, {"country": "US", "code": "BE"}),
                self.mod._ClassifiedToken("unknown", "Gamma", 0.0, {}),
            ],
            None,
            "Beta",
            "United States",
        )
        self.assertEqual((city, state, country), ("Gamma", "Beta", "United States"))

        city, state, country = self.mod._fill_missing_city_from_three_tokens(
            ["Alpha", "Beta", "Gamma"],
            [
                self.mod._ClassifiedToken("state", "Alpha", 1.0, {"country": "US", "code": "AL"}),
                self.mod._ClassifiedToken("state", "Beta", 1.0, {"country": "US", "code": "BE"}),
                self.mod._ClassifiedToken("state", "Gamma", 1.0, {"country": "US", "code": "GA"}),
            ],
            None,
            "Beta",
            "United States",
        )
        self.assertEqual((city, state, country), (None, "Beta", "United States"))

    def test_disambiguate_with_context_defensive_branches(self) -> None:
        self.assertEqual(
            self.mod._disambiguate_with_context(
                ["Seattle"],
                [self.mod._classify_token("Seattle")],
            )[0].token_type,
            "city",
        )

        original = self.mod._resolve_city_candidate
        try:
            classified = [self.mod._classify_token("Menlo Park"), self.mod._classify_token("CA")]
            self.mod._resolve_city_candidate = lambda *args, **kwargs: None
            tokens = self.mod._disambiguate_with_context(
                ["Menlo Park", "CA"],
                classified,
            )
            self.assertEqual(tokens[0].token_type, "city")
            self.assertEqual(tokens[1].token_type, "state")
        finally:
            self.mod._resolve_city_candidate = original

    def test_disambiguate_with_context_success_and_no_subdivision_match(self) -> None:
        tokens = self.mod._disambiguate_with_context(
            ["Chicago", "IL"],
            [self.mod._classify_token("Chicago"), self.mod._classify_token("IL")],
        )
        self.assertEqual(tokens[0].token_type, "city")
        self.assertEqual(tokens[1].token_type, "state")
        self.assertEqual(tokens[1].value, "Illinois")

        untouched = self.mod._disambiguate_with_context(
            ["London", "UK"],
            [self.mod._classify_token("London"), self.mod._classify_token("UK")],
        )
        self.assertEqual(untouched[0].token_type, "city")
        self.assertEqual(untouched[1].token_type, "country")

        no_match = self.mod._disambiguate_with_context(
            ["Mountain View", "WA"],
            [self.mod._classify_token("Mountain View"), self.mod._classify_token("WA")],
        )
        self.assertEqual(no_match[0].token_type, "city")
        self.assertEqual(no_match[1].token_type, "state")
        self.assertEqual(no_match[1].value, "Washington")

    def test_pairwise_interpretation_and_strict_constraints_branches(self) -> None:
        self.assertIsNone(
            self.mod._resolve_city_candidate(
                self.mod._city_lookup()["chicago"],
                country="US",
                state_code="ZZ",
                require_constraints=True,
            )
        )
        self.assertEqual(
            self.mod._resolve_two_token_interpretation(["CA", "Menlo Park"]),
            ("Menlo Park", "California", "United States"),
        )
        self.assertEqual(
            self.mod._resolve_three_token_interpretation(["Mountain View", "CA", "USA"]),
            ("Mountain View", "California", "United States"),
        )
        self.assertEqual(
            self.mod._resolve_three_token_interpretation(["CA", "Mountain View", "USA"]),
            ("Mountain View", "California", "United States"),
        )
        self.assertEqual(
            self.mod._resolve_three_token_interpretation(["USA", "CA", "Mountain View"]),
            ("Mountain View", "California", "United States"),
        )
        self.assertEqual(
            self.mod._resolve_three_token_interpretation(["USA", "Mountain View", "CA"]),
            ("Mountain View", "California", "United States"),
        )
        self.assertIsNone(self.mod._resolve_three_token_interpretation(["Canada", "Washington", "Seattle"]))
        self.assertIsNone(self.mod._resolve_three_token_interpretation(["ON", "Menlo Park", "USA"]))
        self.assertIsNone(self.mod._resolve_three_token_interpretation(["USA", "Menlo Park", "ON"]))
        self.assertIsNone(self.mod._resolve_three_token_interpretation(["Alpha", "Beta", "Gamma"]))


if __name__ == "__main__":
    unittest.main()
