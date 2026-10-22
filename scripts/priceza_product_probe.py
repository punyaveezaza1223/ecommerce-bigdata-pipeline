import html
import re
import urllib.request
import urllib.robotparser
from html.parser import HTMLParser

URL = "https://www.priceza.com/merchant/priceza-deal"
ROBOTS = "https://www.priceza.com/robots.txt"
UA = "EcommerceResearchProbe/1.0"


class StripHTML(HTMLParser):
    def __init__(self):
        super().__init__()
        self.parts = []

    def handle_data(self, data):
        self.parts.append(data)

    def text(self):
        return " ".join(self.parts)


def clean_html(fragment):
    parser = StripHTML()
    parser.feed(fragment)
    text = html.unescape(parser.text())
    return re.sub(r"\s+", " ", text).strip()


print("PRICEZA PRODUCT PROBE START", flush=True)

req = urllib.request.Request(
    ROBOTS,
    headers={"User-Agent": UA}
)

with urllib.request.urlopen(req, timeout=15) as response:
    robots = response.read(200000).decode("utf-8", "replace")

rp = urllib.robotparser.RobotFileParser()
rp.parse(robots.splitlines())

if not rp.can_fetch(UA, URL):
    raise SystemExit("STOP: robots.txt does not allow this URL")

req = urllib.request.Request(
    URL,
    headers={"User-Agent": UA}
)

with urllib.request.urlopen(req, timeout=20) as response:
    page = response.read(2_000_000).decode(
        "utf-8", "replace"
    )

pattern = re.compile(
    r"""(?:https?://(?:www\.)?priceza\.com)?/r/redirect\?id=(\d+)""",
    re.I
)

matches = list(pattern.finditer(page))
unique = {}

for match in matches:
    unique.setdefault(match.group(1), match.start())

print("REDIRECT_OCCURRENCES:", len(matches), flush=True)
print("UNIQUE_REDIRECT_IDS:", len(unique), flush=True)

for index, (product_id, position) in enumerate(
    list(unique.items())[:20], 1
):
    fragment = page[
        max(0, position - 1200):
        min(len(page), position + 1200)
    ]

    text = clean_html(fragment)

    prices = re.findall(
        r"(?:฿\s*[\d,]+(?:\.\d{1,2})?|"
        r"[\d,]+(?:\.\d{1,2})?\s*บาท)",
        text
    )

    print(f"\n--- CANDIDATE {index} ---")
    print("REDIRECT_ID:", product_id)
    print("NEARBY_PRICES:", prices[:8])
    print("NEARBY_TEXT:", text[:650])

print("\nDONE - READ ONLY", flush=True)
