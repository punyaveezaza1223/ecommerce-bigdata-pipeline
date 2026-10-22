#!/usr/bin/env python3
"""One-shot, policy-respecting Advice response diagnostic.

The script checks robots.txt before fetching the public page. It stores a
sanitized HTML artefact (no scripts, no input values, no sensitive attributes)
for offline structural analysis; it never creates product snapshots or writes a
database.
"""
import argparse
import hashlib
import os
import re
import urllib.request
import urllib.robotparser
from datetime import datetime, timezone
from pathlib import Path


URL = "https://www.advice.co.th/nescen-n039/product/pricelist?cate=desktop-pc-server"
ROBOTS = "https://www.advice.co.th/robots.txt"
UA = "EcommerceResearchProbe/1.0"
SENSITIVE_ATTR = re.compile(
    r"\s(?:value|content|data-[\w-]*(?:token|session|auth|csrf)[\w-]*)\s*=\s*(['\"]).*?\1",
    re.I | re.S,
)
SCRIPT_BLOCK = re.compile(r"<script\b[^>]*>.*?</script\s*>", re.I | re.S)
INPUT_TAG = re.compile(r"<input\b[^>]*>", re.I | re.S)


def sanitize_html(page: str) -> str:
    page = SCRIPT_BLOCK.sub("<!-- script removed -->", page)
    page = INPUT_TAG.sub("<!-- input removed -->", page)
    return SENSITIVE_ATTR.sub(' data-redacted="true"', page)


def classify(page: str) -> dict:
    lower = page.lower()
    links = re.findall(r"(?:href\s*=\s*['\"])([^'\"]*productdetail/(A\d+)[^'\"]*)", page, re.I)
    tables = len(re.findall(r"<tr\b", page, re.I))
    # Do not treat an ordinary third-party CDN URL such as
    # cdnjs.cloudflare.com as bot protection.  Only challenge/access markers
    # are evidence of a blocked response.
    markers = [
        item for item in ("access denied", "captcha", "cf-chl", "challenges.cloudflare.com",
                           "just a moment", "forbidden", "bot protection",
                           "verify you are human", "attention required")
        if item in lower
    ]
    js_shell = bool(re.search(r"id\s*=\s*['\"](?:root|app|__next)['\"]|__next_data__|webpack", lower))
    if markers:
        kind = "access_or_bot_protection"
    elif links:
        kind = "server_rendered_product_listing"
    elif js_shell:
        kind = "javascript_shell_or_client_rendered_page"
    else:
        kind = "other_or_unrecognized_html"
    return {"classification": kind, "product_link_count": len(links), "table_row_count": tables,
            "access_markers": markers, "javascript_shell_markers": js_shell}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--root", type=Path,
        default=Path(os.environ.get("PIPELINE_DATA_ROOT", "/workspace/data")),
    )
    args = parser.parse_args()
    with urllib.request.urlopen(urllib.request.Request(ROBOTS, headers={"User-Agent": UA}), timeout=15) as response:
        robots = response.read(200000).decode("utf-8", "replace")
    policy = urllib.robotparser.RobotFileParser()
    policy.parse(robots.splitlines())
    if not policy.can_fetch(UA, URL):
        raise SystemExit("STOP: robots.txt disallows diagnostic URL")

    with urllib.request.urlopen(urllib.request.Request(URL, headers={"User-Agent": UA}), timeout=25) as response:
        if response.status != 200:
            raise RuntimeError(f"HTTP {response.status}")
        raw = response.read(2_000_001)
        charset = response.headers.get_content_charset() or "utf-8"
    if len(raw) > 2_000_000:
        raise RuntimeError("HTML exceeded safety limit")
    page = raw.decode(charset, "replace")
    result = classify(page)
    destination_dir = args.root / "diagnostics" / "advice_th"
    destination_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
    destination = destination_dir / f"advice_response_sanitized_{stamp}.html"
    with destination.open("x", encoding="utf-8") as stream:
        stream.write(sanitize_html(page))
    print("DIAGNOSTIC_HTML_BYTES:", len(raw), flush=True)
    print("DIAGNOSTIC_SHA256:", hashlib.sha256(raw).hexdigest(), flush=True)
    for key, value in result.items():
        print(f"{key.upper()}:", value, flush=True)
    print("SANITIZED_HTML:", destination, flush=True)


if __name__ == "__main__":
    main()
