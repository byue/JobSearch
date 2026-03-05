import unittest
from unittest.mock import patch

from scrapers.airflow.clients.common import pay


class PayHelpersTest(unittest.TestCase):
    def test_extract_pay_details_from_description(self) -> None:
        text = """
        <p>Base salary range is $120,000 - $150,000 per year.</p>
        <p>Bonus and benefits may apply.</p>
        """
        details = pay.extract_pay_details_from_description(text)
        self.assertIsNotNone(details)
        assert details is not None
        self.assertGreaterEqual(len(details.ranges), 1)
        first = details.ranges[0]
        self.assertEqual(first.minAmount, 120000)
        self.assertEqual(first.maxAmount, 150000)
        self.assertEqual(first.currency, "USD")
        self.assertEqual(first.interval, "year")
        self.assertTrue(any("bonus" in note.lower() for note in details.notes))

    def test_extract_pay_details_no_ranges(self) -> None:
        self.assertIsNone(pay.extract_pay_details_from_description(None))
        self.assertIsNone(pay.extract_pay_details_from_description("No compensation info."))

    def test_internal_helpers(self) -> None:
        self.assertEqual(pay._clean_html_fragment("<p>Hello<br>World</p>"), "Hello\nWorld")
        self.assertEqual(pay._parse_amount("12,345"), 12345)
        self.assertIsNone(pay._parse_amount("abc"))

        self.assertEqual(pay._normalize_currency_code("us"), "USD")
        self.assertEqual(pay._normalize_currency_code("usd"), "USD")
        self.assertIsNone(pay._normalize_currency_code("toolong"))

        self.assertEqual(pay._resolve_interval(context="Pay is hourly", trailing=""), "hour")
        self.assertEqual(pay._resolve_interval(context="base salary range", trailing=""), "year")

        sentence = pay._extract_sentence(text="A. Salary $100,000 - $120,000 per year. End.", start=10, end=41)
        self.assertIn("Salary", sentence)

    def test_pay_helper_edge_branches(self) -> None:
        # no currency + no pay-context should be filtered
        ranges = pay._extract_pay_ranges("Range 120000 - 130000 in text")
        self.assertEqual(ranges, [])

        # parse failures / aliases / symbols
        self.assertIsNone(pay._parse_amount(None))
        self.assertIsNone(pay._normalize_currency_code(""))
        self.assertEqual(pay._normalize_currency_code("ca"), "CAD")
        self.assertEqual(
            pay._resolve_currency(code1=None, code2=None, sym1="$", sym2=None, context="US base pay"),
            "USD",
        )
        self.assertEqual(
            pay._resolve_currency(code1=None, code2=" usd ", sym1=None, sym2=None, context="x"),
            "USD",
        )
        self.assertIsNone(pay._resolve_currency(code1=None, code2=None, sym1=None, sym2=None, context="x"))
        self.assertEqual(pay._resolve_interval(context="something per annum", trailing=""), "year")
        self.assertIsNone(pay._resolve_interval(context="none", trailing=""))
        self.assertIsNone(pay.extract_pay_details_from_description("<p><br/></p>"))

    def test_range_and_note_dedupe_branches(self) -> None:
        text = (
            "Salary range $120,000 - $130,000 per year. "
            "Salary range $120,000 - $130,000 per year. "
            "Compensation includes bonus. "
            "Compensation includes bonus."
        )
        details = pay.extract_pay_details_from_description(text)
        self.assertIsNotNone(details)
        assert details is not None
        self.assertEqual(len(details.ranges), 1)
        self.assertEqual(details.ranges[0].interval, "year")
        self.assertEqual(details.notes, ["Compensation includes bonus."])
        self.assertEqual(pay._extract_pay_notes("Plain sentence.", []), [])

    def test_low_amount_range_filtered(self) -> None:
        with patch("scrapers.airflow.clients.common.pay._parse_amount", side_effect=[100, 200]):
            self.assertEqual(pay._extract_pay_ranges("salary is $1,000 - $2,000 per year"), [])
        with patch("scrapers.airflow.clients.common.pay._parse_amount", side_effect=[None, 1000]):
            self.assertEqual(pay._extract_pay_ranges("salary is $1,000 - $2,000 per year"), [])


if __name__ == "__main__":
    unittest.main()
