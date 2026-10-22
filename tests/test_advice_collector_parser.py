"""Regression tests for the two supported Advice price-list HTML layouts."""
import importlib.util
from pathlib import Path


SCRIPT = Path(__file__).parents[1] / "scripts" / "collect_advice_th.py"
SPEC = importlib.util.spec_from_file_location("collect_advice_th", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def test_table_layout_is_extracted():
    page = '''<table><tr><td>Example Desktop</td><td><a href="/nescen-n039/product/productdetail/A0123456">Detail</a></td><td>฿ 12,345</td></tr></table>'''
    products, diagnostics = MODULE.extract_products(page, "2026-10-12T00:00:00+00:00")
    assert diagnostics["extraction_mode"] == "table"
    assert products["A0123456"]["price"] == 12345.0
    assert products["A0123456"]["currency"] == "THB"


def test_structured_card_layout_is_extracted():
    page = '''<div class="card"><a href="/nescen-n039/product/productdetail/A0654321" aria-label="Card Desktop">Product</a><span>High performance desktop computer</span><span>฿ 9,999</span></div>'''
    products, diagnostics = MODULE.extract_products(page, "2026-10-12T00:00:00+00:00")
    assert diagnostics["extraction_mode"] == "structured_card"
    assert products["A0654321"]["product_name"] == "Card Desktop"
    assert products["A0654321"]["price"] == 9999.0


def test_unrecognized_page_has_explicit_diagnostics():
    products, diagnostics = MODULE.extract_products("<html><body>Unavailable</body></html>", "2026-10-12T00:00:00+00:00")
    assert products == {}
    assert diagnostics["table_rows"] == 0
    assert diagnostics["card_links"] == 0
