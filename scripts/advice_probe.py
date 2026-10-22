import re
import urllib.request
import urllib.robotparser
from html.parser import HTMLParser

URL = "https://www.advice.co.th/nescen-n039/product/pricelist?cate=desktop-pc-server"
ROBOTS = "https://www.advice.co.th/robots.txt"
UA = "EcommerceResearchProbe/1.0"

class Inspector(HTMLParser):
    def __init__(self):
        super().__init__()
        self.tables = 0
        self.rows = 0
        self.text_parts = []

    def handle_starttag(self, tag, attrs):
        if tag == "table":
            self.tables += 1
        elif tag == "tr":
            self.rows += 1

    def handle_data(self, data):
        text = " ".join(data.split())
        if text:
            self.text_parts.append(text)

print("ADVICE PROBE START", flush=True)
print("TARGET:", URL, flush=True)

try:
    request = urllib.request.Request(
        ROBOTS, headers={"User-Agent": UA}
    )

    with urllib.request.urlopen(request, timeout=15) as response:
        robots_text = response.read(200000).decode(
            "utf-8", errors="replace"
        )
        print("ROBOTS_HTTP:", response.status, flush=True)

    robots = urllib.robotparser.RobotFileParser()
    robots.parse(robots_text.splitlines())

    allowed = robots.can_fetch(UA, URL)
    print("ROBOTS_ALLOWED:", allowed, flush=True)

    if not allowed:
        raise SystemExit("STOP: robots.txt disallows target")

    request = urllib.request.Request(
        URL, headers={"User-Agent": UA}
    )

    with urllib.request.urlopen(request, timeout=20) as response:
        raw = response.read(2_000_000)
        encoding = response.headers.get_content_charset() or "utf-8"
        page = raw.decode(encoding, errors="replace")
        print("PAGE_HTTP:", response.status, flush=True)

    inspector = Inspector()
    inspector.feed(page)

    text = " ".join(inspector.text_parts)

    prices = re.findall(
        r"(?:฿\s*)?\d{1,3}(?:,\d{3})+(?:\.\d{1,2})?",
        text
    )

    print("HTML_BYTES:", len(raw), flush=True)
    print("TABLES:", inspector.tables, flush=True)
    print("TABLE_ROWS:", inspector.rows, flush=True)
    print("POSSIBLE_PRICES:", len(prices), flush=True)
    print("PRICE_SAMPLES:", prices[:12], flush=True)
    print("TEXT_SAMPLE:", text[:500], flush=True)
    print("DONE - READ ONLY", flush=True)

except Exception as exc:
    print(
        "PROBE_ERROR:",
        type(exc).__name__,
        str(exc),
        flush=True
    )
