import re
import urllib.request
import urllib.robotparser
from html.parser import HTMLParser
from urllib.parse import urljoin

URL = "https://www.advice.co.th/nescen-n039/product/pricelist?cate=desktop-pc-server"
UA = "EcommerceResearchProbe/1.0"
VOID = {"br", "img", "input", "meta", "link", "hr", "source", "wbr"}

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
        self.links = []

    def handle_starttag(self, tag, attrs):
        node = Node(tag, attrs, self.stack[-1])
        self.stack[-1].children.append(node)
        if tag == "a":
            href = node.attrs.get("href", "")
            match = re.search(r"/productdetail/(A\d+)", href, re.I)
            if match:
                self.links.append((match.group(1), href, node))
        if tag not in VOID:
            self.stack.append(node)

    def handle_startendtag(self, tag, attrs):
        self.handle_starttag(tag, attrs)
        if tag not in VOID:
            self.handle_endtag(tag)

    def handle_endtag(self, tag):
        for i in range(len(self.stack)-1, 0, -1):
            if self.stack[i].tag == tag:
                del self.stack[i:]
                return

    def handle_data(self, data):
        if data.strip():
            self.stack[-1].children.append(data)

def text_of(node):
    if isinstance(node, str):
        return node
    if node.tag in ("script", "style", "svg", "noscript"):
        return ""
    return " ".join(text_of(x) for x in node.children)

def ids_of(node):
    result = set()
    todo = [node]
    while todo:
        item = todo.pop()
        if isinstance(item, str):
            continue
        if item.tag == "a":
            m = re.search(
                r"/productdetail/(A\d+)",
                item.attrs.get("href", ""), re.I
            )
            if m:
                result.add(m.group(1))
        todo.extend(item.children)
    return result

print("ADVICE STRUCTURED CARD PROBE START", flush=True)

try:
    with urllib.request.urlopen(
        urllib.request.Request(
            "https://www.advice.co.th/robots.txt",
            headers={"User-Agent": UA}
        ), timeout=15
    ) as r:
        robots = r.read(200000).decode("utf-8", "replace")

    rp = urllib.robotparser.RobotFileParser()
    rp.parse(robots.splitlines())

    if not rp.can_fetch(UA, URL):
        raise SystemExit("STOP: robots.txt disallows URL")

    with urllib.request.urlopen(
        urllib.request.Request(URL, headers={"User-Agent": UA}),
        timeout=20
    ) as r:
        page = r.read(2000000).decode("utf-8", "replace")
        print("HTTP:", r.status, flush=True)

    parser = Parser()
    parser.feed(page)

    unique = {}
    for pid, href, anchor in parser.links:
        unique.setdefault(pid, (href, anchor))

    print("UNIQUE_PRODUCT_IDS:", len(unique), flush=True)

    matched = []

    for pid, (href, anchor) in unique.items():
        current = anchor
        result = None

        while current is not None:
            value = re.sub(r"\s+", " ", text_of(current)).strip()
            prices = re.findall(
                r"฿\s*([\d,]+(?:\.\d{1,2})?)", value
            )

            if (
                20 <= len(value) <= 1000
                and len(prices) == 1
                and ids_of(current) == {pid}
            ):
                result = {
                    "id": pid,
                    "url": urljoin(URL, href),
                    "price": prices[0],
                    "text": value[:350],
                    "tag": current.tag,
                    "class": current.attrs.get("class", "")
                }
                break

            current = current.parent

        if result:
            matched.append(result)

    print("STRICTLY_MATCHED_CARDS:", len(matched), flush=True)
    print("UNMATCHED_CARDS:", len(unique) - len(matched), flush=True)

    for item in matched[:8]:
        print("\nPRODUCT_ID:", item["id"])
        print("PRICE_THB:", item["price"])
        print("ELEMENT:", item["tag"], item["class"][:100])
        print("TEXT:", item["text"])
        print("URL:", item["url"])

    print("\nDONE - READ ONLY", flush=True)

except Exception as exc:
    print("PROBE_ERROR:", type(exc).__name__, str(exc), flush=True)
