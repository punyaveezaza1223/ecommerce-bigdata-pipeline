import json
import urllib.request
import urllib.robotparser
from html.parser import HTMLParser
from urllib.parse import urljoin, urlparse

URL = "https://www.priceza.com/merchant/priceza-deal"
ROBOTS = "https://www.priceza.com/robots.txt"
UA = "EcommerceResearchProbe/1.0"


class Inspector(HTMLParser):
    def __init__(self):
        super().__init__()
        self.in_jsonld = False
        self.jsonld_text = []
        self.current_text = []
        self.links = []
        self.in_anchor = False
        self.href = None

    def handle_starttag(self, tag, attrs):
        data = dict(attrs)

        if tag == "script" and data.get("type", "").lower() == "application/ld+json":
            self.in_jsonld = True
            self.current_text = []

        if tag == "a":
            self.in_anchor = True
            self.href = data.get("href")

    def handle_data(self, data):
        if self.in_jsonld:
            self.current_text.append(data)

    def handle_endtag(self, tag):
        if tag == "script" and self.in_jsonld:
            self.jsonld_text.append("".join(self.current_text))
            self.in_jsonld = False

        if tag == "a":
            if self.href:
                self.links.append(urljoin(URL, self.href))
            self.in_anchor = False
            self.href = None


print("SCHEMA PROBE START", flush=True)

req = urllib.request.Request(
    ROBOTS, headers={"User-Agent": UA}
)

with urllib.request.urlopen(req, timeout=15) as response:
    robots = response.read(200000).decode("utf-8", "replace")

rp = urllib.robotparser.RobotFileParser()
rp.parse(robots.splitlines())

if not rp.can_fetch(UA, URL):
    raise SystemExit("STOP: robots.txt disallows URL")

req = urllib.request.Request(
    URL, headers={"User-Agent": UA}
)

with urllib.request.urlopen(req, timeout=20) as response:
    html = response.read(2000000).decode("utf-8", "replace")

inspector = Inspector()
inspector.feed(html)

print("HTML_CHARS:", len(html))
print("JSONLD_BLOCKS:", len(inspector.jsonld_text))

for index, block in enumerate(inspector.jsonld_text, 1):
    print("\nJSONLD BLOCK:", index)

    try:
        data = json.loads(block)
        entries = data if isinstance(data, list) else [data]

        for obj in entries:
            if not isinstance(obj, dict):
                print("VALUE_TYPE:", type(obj).__name__)
                continue

            print("TYPE:", obj.get("@type"))
            print("KEYS:", sorted(obj.keys()))

            graph = obj.get("@graph")
            if isinstance(graph, list):
                print("GRAPH_TYPES:", [
                    entry.get("@type")
                    for entry in graph
                    if isinstance(entry, dict)
                ])

            for key in ("name", "description", "url"):
                value = obj.get(key)
                if isinstance(value, str):
                    print(key.upper() + ":", value[:150])

            item_list = obj.get("itemListElement")
            if isinstance(item_list, list):
                print("ITEM_LIST_COUNT:", len(item_list))

    except json.JSONDecodeError as exc:
        print("JSONLD_PARSE_ERROR:", str(exc))

print("\nSAMPLE PRICEZA LINKS:")
seen = set()
count = 0

for link in inspector.links:
    if urlparse(link).netloc not in (
        "www.priceza.com",
        "priceza.com"
    ):
        continue

    if link in seen:
        continue

    seen.add(link)
    print(link[:220])
    count += 1

    if count >= 15:
        break

print("\nDONE - READ ONLY")
