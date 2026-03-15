import unittest
from unittest.mock import patch

from scrapers.airflow.clients.common import html_text


class HtmlTextTest(unittest.TestCase):
    def test_extract_text_returns_none_for_non_string_or_blank(self) -> None:
        self.assertEqual(html_text.extract_text(None), None)
        self.assertEqual(html_text.extract_text("   "), None)

    def test_extract_text_wraps_fragment(self) -> None:
        with patch("scrapers.airflow.clients.common.html_text.trafilatura.extract", return_value="Hello\nWorld") as mock_extract:
            self.assertEqual(html_text.extract_text("<p>Hello<br>World</p>"), "Hello\nWorld")
        mock_extract.assert_called_once_with(
            "<html><body><p>Hello<br>World</p></body></html>",
            include_comments=False,
            include_tables=True,
            no_fallback=False,
        )

    def test_extract_text_passes_full_document_through(self) -> None:
        html_doc = "<html><body><h1>Title</h1><p>Body</p></body></html>"
        with patch("scrapers.airflow.clients.common.html_text.trafilatura.extract", return_value="Title\nBody") as mock_extract:
            self.assertEqual(html_text.extract_text(html_doc, full_document=True), "Title\nBody")
        mock_extract.assert_called_once_with(
            html_doc,
            include_comments=False,
            include_tables=True,
            no_fallback=False,
        )

    def test_extract_text_returns_none_on_exception(self) -> None:
        with patch("scrapers.airflow.clients.common.html_text.trafilatura.extract", side_effect=RuntimeError("bad")):
            self.assertEqual(html_text.extract_text("<p>X</p>"), None)


if __name__ == "__main__":
    unittest.main()
