import urllib.request
import urllib.robotparser
import urllib.error
from html.parser import HTMLParser

URL = "https://www.priceza.com/merchant/priceza-deal"
ROBOTS = "https://www.priceza.com/robots.txt"
UA = "EcommerceResearchProbe/1.0"


class PageInspector(HTMLParser):
    def __init__(self):
        super().__init__()
        self.links = 0
        self.jsonld = 0

    def handle_starttag(self, tag, attrs):
        data = dict(attrs)
        if tag == "a" and data.get("href"):
            self.links += 1
        if tag == "script" and data.get("type") == "application/ld+json":
            self.jsonld += 1


print("PRICEZA PROBE START", flush=True)
print("TARGET:", URL, flush=True)

try:
    req = urllib.request.Request(
        ROBOTS, headers={"User-Agent": UA}
    )

    with urllib.request.urlopen(req, timeout=12) as response:
        robots_text = response.read(200000).decode(
            "utf-8", errors="replace"
        )
        print("ROBOTS_HTTP:", response.status, flush=True)

    rp = urllib.robotparser.RobotFileParser()
    rp.parse(robots_text.splitlines())

    allowed = rp.can_fetch(UA, URL)
    print("ROBOTS_ALLOWED:", allowed, flush=True)

    if not allowed:
        print("STOP: robots.txt disallows this URL", flush=True)
        raise SystemExit(0)

    req = urllib.request.Request(
        URL, headers={"User-Agent": UA}
    )

    with urllib.request.urlopen(req, timeout=15) as response:
        html = response.read(2_000_000).decode(
            "utf-8", errors="replace"
        )
        print("PAGE_HTTP:", response.status, flush=True)

    inspector = PageInspector()
    inspector.feed(html)

    print("HTML_CHARS:", len(html), flush=True)
    print("LINKS:", inspector.links, flush=True)
    print("JSONLD_BLOCKS:", inspector.jsonld, flush=True)
    print("PRICE_SYMBOL_MATCHES:", html.count("฿"), flush=True)
    print("PROBE FINISHED - NO DATABASE WRITES", flush=True)

except Exception as exc:
    print("PROBE ERROR:", type(exc).__name__, str(exc), flush=True)
