import json
import re
import urllib.request
import urllib.robotparser
from html.parser import HTMLParser

URL = "https://www.advice.co.th/nescen-n039/product/productdetail/A0183648"
UA = "EcommerceResearchProbe/1.0"

class Inspector(HTMLParser):
    def __init__(self):
        super().__init__()
        self.skip = 0
        self.text = []
        self.title_mode = False
        self.title = []
        self.ld_mode = False
        self.ld_buffer = []
        self.ld_blocks = []

    def handle_starttag(self, tag, attrs):
        d = dict(attrs)
        if tag == "title":
            self.title_mode = True
        if tag == "script" and d.get("type", "").lower() == "application/ld+json":
            self.ld_mode = True
            self.ld_buffer = []
        elif tag in ("script", "style", "noscript", "svg"):
            self.skip += 1

    def handle_data(self, value):
        if self.ld_mode:
            self.ld_buffer.append(value)
        elif self.title_mode:
            self.title.append(value)
        elif self.skip == 0:
            v = " ".join(value.split())
            if v:
                self.text.append(v)

    def handle_endtag(self, tag):
        if tag == "title":
            self.title_mode = False
        if tag == "script" and self.ld_mode:
            self.ld_blocks.append("".join(self.ld_buffer))
            self.ld_mode = False
        elif tag in ("script", "style", "noscript", "svg") and self.skip:
            self.skip -= 1

print("ADVICE DETAIL PROBE START", flush=True)
print("URL:", URL, flush=True)

try:
    req = urllib.request.Request(
        "https://www.advice.co.th/robots.txt",
        headers={"User-Agent": UA}
    )
    with urllib.request.urlopen(req, timeout=15) as r:
        robots_text = r.read(200000).decode("utf-8", "replace")

    rp = urllib.robotparser.RobotFileParser()
    rp.parse(robots_text.splitlines())

    if not rp.can_fetch(UA, URL):
        raise SystemExit("STOP: robots.txt disallows product detail")

    req = urllib.request.Request(URL, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=20) as r:
        raw = r.read(1500000)
        encoding = r.headers.get_content_charset() or "utf-8"
        page = raw.decode(encoding, "replace")
        print("HTTP:", r.status, flush=True)

    p = Inspector()
    p.feed(page)

    print("TITLE:", " ".join(p.title)[:250])
    print("HTML_BYTES:", len(raw))
    print("JSONLD_BLOCKS:", len(p.ld_blocks))

    for i, block in enumerate(p.ld_blocks[:5], 1):
        try:
            obj = json.loads(block)
            if isinstance(obj, dict):
                print("JSONLD_TYPE:", obj.get("@type"))
                print("JSONLD_KEYS:", sorted(obj.keys())[:25])
        except (ValueError, TypeError):
            print("JSONLD_PARSE_FAILED:", i)

    print("\nPRODUCT TEXT SAMPLE:")
    print(" ".join(p.text)[:1800])

    print("\nPRICE CONTEXT:")
    found = 0
    for i, text in enumerate(p.text):
        if "฿" in text or "บาท" in text:
            print(" ".join(p.text[max(0, i-3):i+4])[:260])
            found += 1
            if found >= 8:
                break

    print("\nDONE - READ ONLY", flush=True)

except Exception as exc:
    print("PROBE_ERROR:", type(exc).__name__, str(exc), flush=True)
