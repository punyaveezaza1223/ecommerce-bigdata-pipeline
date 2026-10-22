import re
import html
import urllib.request
import urllib.robotparser
from html.parser import HTMLParser
from urllib.parse import urljoin

URL = "https://www.advice.co.th/nescen-n039/product/pricelist?cate=desktop-pc-server"
UA = "EcommerceResearchProbe/1.0"

class CleanText(HTMLParser):
    def __init__(self):
        super().__init__()
        self.parts = []
        self.skip = 0

    def handle_starttag(self, tag, attrs):
        if tag in ("style", "script", "svg", "noscript"):
            self.skip += 1

    def handle_endtag(self, tag):
        if tag in ("style", "script", "svg", "noscript") and self.skip:
            self.skip -= 1

    def handle_data(self, value):
        if not self.skip and value.strip():
            self.parts.append(value.strip())

def readable(fragment):
    parser = CleanText()
    parser.feed(fragment)
    return re.sub(r"\s+", " ", html.unescape(" ".join(parser.parts)))

print("ADVICE CARD INSPECTION START", flush=True)

try:
    robots_url = "https://www.advice.co.th/robots.txt"

    with urllib.request.urlopen(
        urllib.request.Request(robots_url, headers={"User-Agent": UA}),
        timeout=15
    ) as response:
        robots_text = response.read(200000).decode("utf-8", "replace")

    rp = urllib.robotparser.RobotFileParser()
    rp.parse(robots_text.splitlines())

    if not rp.can_fetch(UA, URL):
        raise SystemExit("STOP: robots disallow target")

    with urllib.request.urlopen(
        urllib.request.Request(URL, headers={"User-Agent": UA}),
        timeout=20
    ) as response:
        page = response.read(2000000).decode("utf-8", "replace")
        print("HTTP:", response.status, flush=True)

    pattern = re.compile(
        r'''(?:href\s*=\s*["'])([^"']*productdetail/(A\d+)[^"']*)["']''',
        re.I
    )

    unique = {}
    for match in pattern.finditer(page):
        unique.setdefault(match.group(2), (match.start(), match.group(1)))

    print("UNIQUE_PRODUCT_LINK_IDS:", len(unique), flush=True)

    for i, (pid, (position, href)) in enumerate(
        list(unique.items())[:5], 1
    ):
        fragment = page[
            max(0, position - 800):
            min(len(page), position + 1400)
        ]
        text = readable(fragment)
        prices = re.findall(
            r"฿\s*[\d,]+(?:\.\d{1,2})?",
            text
        )

        print("\nPRODUCT_CANDIDATE:", i)
        print("PRODUCT_ID:", pid)
        print("URL:", urljoin(URL, href))
        print("NEARBY_PRICES:", prices[:5])
        print("NEARBY_TEXT:", text[:650])

    print("\nDONE - READ ONLY", flush=True)

except Exception as exc:
    print("PROBE_ERROR:", type(exc).__name__, str(exc), flush=True)
