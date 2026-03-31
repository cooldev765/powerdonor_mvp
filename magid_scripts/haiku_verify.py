#!/usr/bin/env python3
"""
haiku_verify.py — Claude Haiku website verifier
================================================
Verifies whether a candidate URL is the official website for a nonprofit.

Usage (as a module):
    from haiku_verify import verify_website
    verified, reason = verify_website("Colby College", "Waterville", "ME", "https://colby.edu")

Usage (standalone test):
    python haiku_verify.py "Colby College" "Waterville" "ME" "https://colby.edu"

Requires:
    ANTHROPIC_API_KEY in environment (or .env file)
    pip install anthropic requests python-dotenv
"""

import os
import sys
import re
import time
from urllib.parse import urlparse

import requests
from dotenv import load_dotenv

load_dotenv()

import anthropic

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
MODEL         = "claude-haiku-4-5-20251001"
FETCH_TIMEOUT = 8    # seconds for HTTP fetch
MAX_CONTENT   = 800  # chars of page content sent to model (title + description + snippet)

# Domains we never need to verify — always wrong
KNOWN_BAD = {
    "carelistings.com", "nursinghomes.com", "rehab.com", "nursa.com",
    "theorg.com", "501c3lookup.org", "grantedai.com", "seamless.ai",
    "volunteermatch.org", "guidestar.org", "candid.org", "propublica.org",
    "charitynavigator.org", "greatnonprofits.org", "causeiq.com",
    "linkedin.com", "facebook.com", "wikipedia.org", "yelp.com",
    "yellowpages.com", "manta.com", "healthgrades.com", "zoominfo.com",
}

_client = None


def _get_client():
    global _client
    if _client is None:
        if not ANTHROPIC_KEY:
            raise RuntimeError(
                "ANTHROPIC_API_KEY not set. Add it to your .env file."
            )
        _client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
    return _client


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def get_domain(url: str) -> str:
    try:
        p = urlparse(url.lower().strip())
        h = p.netloc or p.path
        return h.replace("www.", "").split("/")[0].split("?")[0].split(":")[0]
    except Exception:
        return ""


def fetch_page_metadata(url: str) -> dict:
    """Fetch title, meta description, and first ~300 chars of visible text."""
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/121.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "en-US,en;q=0.9",
    }
    try:
        resp = requests.get(url, headers=headers, timeout=FETCH_TIMEOUT,
                            allow_redirects=True, stream=True)
        resp.raise_for_status()
        # Read only first 32 KB to keep it fast
        raw = b""
        for chunk in resp.iter_content(chunk_size=4096):
            raw += chunk
            if len(raw) >= 32768:
                break
        html = raw.decode("utf-8", errors="replace")
    except Exception as e:
        return {"error": str(e), "title": "", "description": "", "snippet": ""}

    title = ""
    m = re.search(r"<title[^>]*>(.*?)</title>", html, re.IGNORECASE | re.DOTALL)
    if m:
        title = re.sub(r"\s+", " ", m.group(1)).strip()[:120]

    description = ""
    m = re.search(
        r'<meta[^>]+name=["\']description["\'][^>]+content=["\']([^"\']{0,300})',
        html, re.IGNORECASE
    )
    if not m:
        m = re.search(
            r'<meta[^>]+content=["\']([^"\']{0,300})[^>]+name=["\']description["\']',
            html, re.IGNORECASE
        )
    if m:
        description = re.sub(r"\s+", " ", m.group(1)).strip()[:300]

    # Strip tags and get first visible text
    text = re.sub(r"<script[^>]*>.*?</script>", " ", html, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<style[^>]*>.*?</style>", " ", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    snippet = text[:400]

    return {
        "title": title,
        "description": description,
        "snippet": snippet,
        "final_url": resp.url if "resp" in dir() else url,
    }


def _ask_haiku(org_name: str, city: str, state: str, url: str, meta: dict) -> tuple[bool, str]:
    """Send to Claude Haiku and parse VERIFIED/REJECTED/UNCERTAIN."""
    content_parts = []
    if meta.get("title"):
        content_parts.append(f"Page title: {meta['title']}")
    if meta.get("description"):
        content_parts.append(f"Meta description: {meta['description']}")
    if meta.get("snippet"):
        content_parts.append(f"Page text snippet: {meta['snippet'][:300]}")
    page_info = "\n".join(content_parts) if content_parts else "(no page content available)"

    prompt = f"""You are verifying whether a URL is the official website for a US nonprofit organization.

Organization: {org_name}
Location: {city}, {state}
Candidate URL: {url}

{page_info}

Is this the official website for this specific organization?

Answer with exactly one of these words on the first line:
VERIFIED
REJECTED
UNCERTAIN

Then on the next line, give a single brief reason (max 12 words)."""

    try:
        client = _get_client()
        msg = client.messages.create(
            model=MODEL,
            max_tokens=80,
            messages=[{"role": "user", "content": prompt}],
        )
        response = msg.content[0].text.strip()
    except Exception as e:
        return False, f"api_error: {e}"

    first_line = response.split("\n")[0].strip().upper()
    reason_lines = [l.strip() for l in response.split("\n")[1:] if l.strip()]
    reason = reason_lines[0] if reason_lines else ""

    if "VERIFIED" in first_line:
        return True, reason
    if "REJECTED" in first_line:
        return False, reason
    # UNCERTAIN → treat as unverified
    return False, f"uncertain: {reason}"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
def verify_website(
    org_name: str,
    city: str,
    state: str,
    url: str,
    fetch: bool = True,
) -> tuple[bool, str]:
    """
    Verify whether `url` is the official website for `org_name` in `city, state`.

    Returns:
        (verified: bool, reason: str)

    Args:
        fetch: If True, fetch page metadata before calling Haiku (recommended).
               If False, Haiku judges from domain name only (faster, less accurate).
    """
    if not url:
        return False, "no url"

    domain = get_domain(url)

    # Fast-reject known-bad domains without using any API calls
    for bad in KNOWN_BAD:
        if bad in domain:
            return False, f"known aggregator: {domain}"

    # .gov domains are never the org's own site
    if domain.endswith(".gov"):
        return False, ".gov domain"

    meta = {}
    if fetch:
        meta = fetch_page_metadata(url)
        if meta.get("error"):
            # Still try — Haiku can judge from domain alone
            meta = {}

    return _ask_haiku(org_name, city, state, url, meta)


# ---------------------------------------------------------------------------
# Standalone test
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    if len(sys.argv) < 5:
        print("Usage: python haiku_verify.py <org_name> <city> <state> <url>")
        print('Example: python haiku_verify.py "Colby College" Waterville ME https://colby.edu')
        sys.exit(1)

    org, city, state, url = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
    print(f"\nVerifying: {org} ({city}, {state})")
    print(f"URL: {url}")
    print("Fetching page metadata...", flush=True)
    verified, reason = verify_website(org, city, state, url)
    status = "VERIFIED" if verified else "REJECTED"
    print(f"Result: {status} — {reason}\n")
