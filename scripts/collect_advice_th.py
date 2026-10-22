import json
import os
import re
import shutil
import urllib.request
import urllib.robotparser
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urljoin

URL = "https://www.advice.co.th/nescen-n039/product/pricelist?cate=desktop-pc-server"
ROBOTS = "https://www.advice.co.th/robots.txt"
UA = "EcommerceResearchProbe/1.0"
DATA_ROOT = Path(os.environ.get("PIPELINE_DATA_ROOT", "/workspace/data"))
OUT = DATA_ROOT / "raw" / "external" / "advice_th"
VOID = {"br", "img", "input", "meta", "link", "hr", "source", "wbr"}
ID_PATTERN = re.compile(r"/productdetail/(A\d+)", re.I)
PRICE_PATTERN = re.compile(r"฿\s*([\d,]+(?:\.\d{1,2})?)")


class Node:
    def __init__(self, tag="", attrs=None, parent=None):
        self.tag = tag
        self.attrs = dict(attrs or [])
        self.parent = parent
        self.children = []


class Parser(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.root = Node("root")
        self.stack = [self.root]
        self.rows = []

    def handle_starttag(self, tag, attrs):
        node = Node(tag, attrs, self.stack[-1])
        self.stack[-1].children.append(node)
        if tag == "tr":
            self.rows.append(node)
        if tag not in VOID:
            self.stack.append(node)

    def handle_startendtag(self, tag, attrs):
        self.handle_starttag(tag, attrs)
        if tag not in VOID:
            self.handle_endtag(tag)

    def handle_endtag(self, tag):
        for i in range(len(self.stack) - 1, 0, -1):
            if self.stack[i].tag == tag:
                del self.stack[i:]
                return

    def handle_data(self, data):
        if data.strip():
            self.stack[-1].children.append(data)


def nodes_of(node):
    stack = [node]
    while stack:
        current = stack.pop()
        if isinstance(current, str):
            continue
        yield current
        stack.extend(reversed(current.children))


def text_of(node):
    if isinstance(node, str):
        return node
    if node.tag in ("script", "style", "svg", "noscript"):
        return ""
    return " ".join(text_of(child) for child in node.children)


def clean(value):
    return re.sub(r"\s+", " ", value).strip()


def product_ids_of(node):
    return {
        match.group(1).upper()
        for item in nodes_of(node)
        if item.tag == "a"
        for match in [ID_PATTERN.search(item.attrs.get("href", ""))]
        if match
    }


def make_record(pid, name, price, href, value, observed_at):
    return {
        "source_system": "advice_th",
        "source_type": "public_price_list",
        "external_product_id": pid,
        "product_name": name,
        "category": "Desktop PC",
        "price": price,
        "currency": "THB",
        "product_url": urljoin(URL, href),
        "source_page": URL,
        "description_raw": value,
        "observed_at_utc": observed_at,
        "price_list_last_update": "2026-07-22 10:30",
        "sold_count": None,
        "sales_rank": None,
    }


def add_product(products, record):
    existing = products.get(record["external_product_id"])
    if existing is not None:
        if existing["price"] != record["price"]:
            raise RuntimeError(
                f"Conflicting prices for {record['external_product_id']}"
            )
        return False
    products[record["external_product_id"]] = record
    return True


def extract_table_products(parser, observed_at):
    """Read the original table-based price-list structure."""
    products = {}
    stats = {
        "table_rows": len(parser.rows),
        "table_no_single_id": 0,
        "table_price_not_single": 0,
        "table_insufficient_cells": 0,
        "table_invalid_name_or_price": 0,
    }
    for row in parser.rows:
        links = []
        for node in nodes_of(row):
            if node.tag != "a":
                continue
            href = node.attrs.get("href", "")
            match = ID_PATTERN.search(href)
            if match:
                links.append((match.group(1).upper(), href))
        ids = {pid for pid, _ in links}
        if len(ids) != 1:
            stats["table_no_single_id"] += 1
            continue
        pid = next(iter(ids))
        value = clean(text_of(row))
        prices = PRICE_PATTERN.findall(value)
        if len(prices) != 1:
            stats["table_price_not_single"] += 1
            continue
        cells = [
            clean(text_of(node))
            for node in row.children
            if isinstance(node, Node) and node.tag in ("td", "th")
        ]
        cells = [cell for cell in cells if cell]
        if len(cells) < 2:
            stats["table_insufficient_cells"] += 1
            continue
        name = cells[0]
        price = float(prices[0].replace(",", ""))
        if not name or price <= 0:
            stats["table_invalid_name_or_price"] += 1
            continue
        add_product(products, make_record(pid, name, price, links[0][1], value, observed_at))
    return products, stats


def extract_structured_card_products(parser, observed_at):
    """Fallback for Advice pages rendered as cards rather than table rows."""
    products = {}
    stats = {"card_links": 0, "card_no_valid_container": 0, "card_no_name": 0}
    candidates = []
    for node in nodes_of(parser.root):
        if node.tag != "a":
            continue
        match = ID_PATTERN.search(node.attrs.get("href", ""))
        if match:
            stats["card_links"] += 1
            candidates.append((match.group(1).upper(), node))
    for pid, anchor in candidates:
        current = anchor
        matched = None
        while current is not None:
            value = clean(text_of(current))
            prices = PRICE_PATTERN.findall(value)
            if (
                20 <= len(value) <= 1000
                and len(prices) == 1
                and product_ids_of(current) == {pid}
            ):
                matched = (current, value, prices[0])
                break
            current = current.parent
        if matched is None:
            stats["card_no_valid_container"] += 1
            continue
        _, value, price_text = matched
        name = clean(
            anchor.attrs.get("aria-label")
            or anchor.attrs.get("title")
            or text_of(anchor)
        )
        if not name:
            stats["card_no_name"] += 1
            continue
        add_product(
            products,
            make_record(
                pid,
                name,
                float(price_text.replace(",", "")),
                anchor.attrs.get("href", ""),
                value,
                observed_at,
            ),
        )
    return products, stats


def extract_products(page, observed_at):
    parser = Parser()
    parser.feed(page)
    products, diagnostics = extract_table_products(parser, observed_at)
    diagnostics["extraction_mode"] = "table"
    # The historical raw snapshot proves the table contract was valid.  The
    # fallback only applies when it yields zero records, preserving that path.
    if not products:
        products, card_stats = extract_structured_card_products(parser, observed_at)
        diagnostics.update(card_stats)
        diagnostics["extraction_mode"] = "structured_card"
    return products, diagnostics


def main():
    print("ADVICE COLLECTOR START", flush=True)

    req = urllib.request.Request(
        ROBOTS, headers={"User-Agent": UA}
    )
    with urllib.request.urlopen(req, timeout=15) as response:
        robots_text = response.read(200000).decode(
            "utf-8", "replace"
        )

    robots = urllib.robotparser.RobotFileParser()
    robots.parse(robots_text.splitlines())

    if not robots.can_fetch(UA, URL):
        raise SystemExit("STOP: robots.txt disallows URL")

    req = urllib.request.Request(
        URL, headers={"User-Agent": UA}
    )
    with urllib.request.urlopen(req, timeout=25) as response:
        if response.status != 200:
            raise RuntimeError(f"HTTP {response.status}")
        page = response.read(2_000_001).decode(
            "utf-8", "replace"
        )

    print("HTML_BYTES:", len(page.encode("utf-8")), flush=True)

    if len(page) > 2_000_000:
        raise RuntimeError("HTML exceeded safety limit")

    observed_at = datetime.now(timezone.utc).isoformat()
    products, diagnostics = extract_products(page, observed_at)

    print("VALID_PRODUCTS:", len(products), flush=True)
    print("EXTRACTION_MODE:", diagnostics["extraction_mode"], flush=True)
    for key, value in sorted(diagnostics.items()):
        if key != "extraction_mode":
            print(f"DIAGNOSTIC_{key.upper()}:", value, flush=True)

    if len(products) < 150:
        raise RuntimeError(
            "Too few valid products; stop without writing."
        )

    if shutil.disk_usage(DATA_ROOT).free < 15 * 1024**3:
        raise RuntimeError(
            "STOP: less than 15 GiB free disk space"
        )

    for record in list(products.values())[:3]:
        print(
            "SAMPLE:",
            record["external_product_id"],
            record["product_name"][:90],
            record["price"],
            "THB",
            flush=True
        )

    OUT.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime(
        "%Y%m%d_%H%M%S_%f"
    )
    destination = OUT / f"advice_th_{stamp}.json"

    payload = {
        "dataset": "Advice Thailand Desktop PC Price List",
        "source_url": URL,
        "observed_at_utc": observed_at,
        "product_count": len(products),
        "products": list(products.values())
    }

    # Exclusive creation: never overwrite an earlier collection.
    with destination.open("x", encoding="utf-8") as stream:
        json.dump(payload, stream, ensure_ascii=False, indent=2)

    print("COLLECT_SUCCESS:", destination, flush=True)
    print("PRODUCTS_SAVED:", len(products), flush=True)


if __name__ == "__main__":
    main()
