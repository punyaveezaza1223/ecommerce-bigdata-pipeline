import json
import re
import urllib.request
import urllib.robotparser
from html.parser import HTMLParser

URL = "https://www.advice.co.th/nescen-n039/product/pricelist?cate=desktop-pc-server"
UA = "EcommerceResearchProbe/1.0"

class Inspector(HTMLParser):
    def __init__(self):
        super().__init__()
        self.skip = 0
        self.jsonld_mode = False
        self.jsonld_buffer = []
        self.jsonld_blocks = []
        self.text = []
        self.links = []

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)

        if tag == "script" and attrs.get("type", "").lower() == "application/ld+json":
            self.jsonld_mode = True
            self.jsonld_buffer = []
        elif tag in ("script", "style", "noscript", "svg"):
            self.skip += 1

        if tag == "a" and attrs.get("href"):
            self.links.append(attrs["href"])

    def handle_data(self, data):
        if self.jsonld_mode:
            self.jsonld_buffer.append(data)
        elif self.skip == 0:
            value = " ".join(data.split())
            if value:
                self.text.append(value)

    def handle_endtag(self, tag):
        if tag == "script" and self.jsonld_mode:
            self.jsonld_blocks.append("".join(self.jsonld_buffer))
            self.jsonld_mode = False
        elif tag in ("script", "style", "noscript", "svg") and self.skip:
            self.skip -= 1

print("ADVICE V2 START", flush=True)

robots_url = "https://www.advice.co.th/robots.txt"

try:
    with urllib.request.urlopen(
        urllib.request.Request(robots_url, headers={"User-Agent": UA}),
        timeout=15
    ) as response:
        robots_text = response.read(200000).decode("utf-8", "replace")

    robots = urllib.robotparser.RobotFileParser()
    robots.parse(robots_text.splitlines())

    if not robots.can_fetch(UA, URL):
        raise SystemExit("STOP: robots.txt disallows page")

    with urllib.request.urlopen(
        urllib.request.Request(URL, headers={"User-Agent": UA}),
        timeout=20
    ) as response:
        raw = response.read(2_000_000)
        encoding = response.headers.get_content_charset() or "utf-8"
        page = raw.decode(encoding, "replace")
        print("PAGE_HTTP:", response.status, flush=True)

    p = Inspector()
    p.feed(page)

    print("VISIBLE_TEXT_SEGMENTS:", len(p.text))
    print("JSONLD_BLOCKS:", len(p.jsonld_blocks))
    print("LINKS:", len(p.links))

    for i, block in enumerate(p.jsonld_blocks[:10], 1):
        try:
            item = json.loads(block)
            print("JSONLD", i, "TYPE:",
                  item.get("@type") if isinstance(item, dict) else type(item).__name__)
        except Exception as exc:
            print("JSONLD", i, "PARSE_ERROR:", str(exc)[:100])

    print("\nTEXT SAMPLE (NO CSS / JS):")
    print(" ".join(p.text[:100])[:1200])

    print("\nPRICE CONTEXT SAMPLES:")
    found = 0
    for i, segment in enumerate(p.text):
        if "฿" in segment or "บาท" in segment or re.search(r"\b\d{1,3}(?:,\d{3})+(?:\.\d{2})?\b", segment):
            context = " ".join(p.text[max(0, i-3):i+4])
            print(f"{found+1}: {context[:260]}")
            found += 1
            if found >= 15:
                break

    print("PRICE_CONTEXT_CANDIDATES_SHOWN:", found)

    print("\nSAMPLE PRODUCT-LIKE LINKS:")
    seen = set()
    count = 0
    for link in p.links:
        if link in seen:
            continue
        seen.add(link)
        if any(term in link.lower() for term in ("product", "item", "detail")):
            print(link[:180])
            count += 1
            if count >= 12:
                break

    print("DONE - READ ONLY", flush=True)

except Exception as exc:
    print("PROBE_ERROR:", type(exc).__name__, str(exc), flush=True)
