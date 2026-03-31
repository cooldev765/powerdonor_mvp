"""
PowerDonor.AI - Pipeline Step 1 Round 2: Crawl Unenriched Charities
====================================================================
Finds all charities that have a URL (from any source) but no LLM enrichment,
crawls their websites, and saves to JSONL for Haiku batch processing.

Sources for URLs:
  - irs_website (from IRS data / URL discovery workers)
  - candid_website (from Candid API)
  - url_discovery_queue.url_found (from DDG search workers)
"""
import json, time, os, sys, signal, asyncio
import psycopg2
import aiohttp
from urllib.parse import urljoin, urlparse
from html.parser import HTMLParser
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

DB = os.environ["DATABASE_URL"]
OUTPUT_DIR = os.environ.get("CRAWL_OUTPUT_DIR", os.path.join(os.path.dirname(os.path.abspath(__file__)), "crawl_output", "round2"))
os.makedirs(OUTPUT_DIR, exist_ok=True)

CRAWL_FILE = os.path.join(OUTPUT_DIR, "crawled_data.jsonl")
PROGRESS_FILE = os.path.join(OUTPUT_DIR, "crawl_progress.json")
ERROR_FILE = os.path.join(OUTPUT_DIR, "crawl_errors.jsonl")

# --- Tuning knobs ---
MAX_CONCURRENT = 80        # simultaneous HTTP connections
PAGES_PER_SITE = 4        # max subpages per charity
REQUEST_TIMEOUT = 10       # seconds per HTTP request
CONNECTOR_LIMIT = 150     # aiohttp connector pool size
MAX_TEXT_PER_PAGE = 8000
MAX_TOTAL_TEXT = 15000
PROGRESS_INTERVAL = 500    # print/save every N charities
BATCH_SIZE = 5000          # charities to load per DB fetch

SUBPAGE_KEYWORDS = [
    "about", "about-us", "about_us", "who-we-are", "our-story",
    "mission", "vision", "history",
    "team", "staff", "board", "leadership", "people", "our-team",
    "programs", "services", "what-we-do", "our-work", "initiatives",
    "impact", "results", "annual-report", "outcomes",
    "volunteer", "get-involved", "take-action", "join",
    "donate", "give", "support", "ways-to-give",
    "contact", "connect",
]

USER_AGENT = "Mozilla/5.0 (compatible; PowerDonor.AI Research Bot; +https://powerdonor.com)"

# Graceful shutdown
shutdown = False
def handle_signal(sig, frame):
    global shutdown
    print("\nShutdown requested, finishing current batch...", flush=True)
    shutdown = True
signal.signal(signal.SIGINT, handle_signal)


class LinkTextExtractor(HTMLParser):
    """Extract visible text and same-domain links from HTML."""
    def __init__(self, base_url):
        super().__init__()
        self.base_url = base_url
        self.base_domain = urlparse(base_url).netloc.lower()
        self.text = []
        self.links = []
        self.skip = False

    def handle_starttag(self, tag, attrs):
        if tag in ('script', 'style', 'noscript'):
            self.skip = True
        if tag == 'a':
            href = dict(attrs).get('href', '')
            if href and not href.startswith(('#', 'mailto:', 'tel:', 'javascript:')):
                full_url = urljoin(self.base_url, href)
                parsed = urlparse(full_url)
                if parsed.netloc.lower() == self.base_domain:
                    clean = full_url.split('?')[0].split('#')[0]
                    self.links.append(clean)

    def handle_endtag(self, tag):
        if tag in ('script', 'style', 'noscript'):
            self.skip = False

    def handle_data(self, data):
        if not self.skip:
            t = data.strip()
            if t:
                self.text.append(t)


def parse_html(html_text, base_url):
    ext = LinkTextExtractor(base_url)
    try:
        ext.feed(html_text)
    except:
        pass
    return " ".join(ext.text), list(set(ext.links))


def score_link(url):
    path = urlparse(url).path.lower().strip('/')
    if len(path.split('/')) > 2:
        return 0
    for kw in SUBPAGE_KEYWORDS:
        if kw in path:
            return 2
    return 0


def pick_subpages(links, max_pages=PAGES_PER_SITE):
    seen = set()
    scored = []
    for link in links:
        path = urlparse(link).path.lower().strip('/')
        if path in seen or path == '':
            continue
        seen.add(path)
        s = score_link(link)
        if s > 0:
            scored.append((s, link))
    scored.sort(key=lambda x: x[0], reverse=True)
    return [url for _, url in scored[:max_pages]]


async def fetch_page(session, url):
    """Fetch a single page, return (text, links, resolved_url)."""
    headers = {"User-Agent": USER_AGENT}
    async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
                           allow_redirects=True, ssl=False) as resp:
        if resp.status != 200:
            return "", [], url
        html = await resp.text(errors='replace')
        text, links = parse_html(html, str(resp.url))
        return text, links, str(resp.url)


async def crawl_one_site(session, ein, name, url):
    """Crawl a charity's homepage + best subpages."""
    all_text = []
    total_len = 0
    try:
        # Ensure URL has scheme
        if not url.startswith("http"):
            url = "https://" + url

        # Fetch homepage
        home_text, links, resolved = await fetch_page(session, url)
        if home_text:
            trimmed = home_text[:MAX_TEXT_PER_PAGE]
            all_text.append(trimmed)
            total_len += len(trimmed)

        # Pick and fetch subpages
        subpages = pick_subpages(links)
        for sub_url in subpages:
            if total_len >= MAX_TOTAL_TEXT:
                break
            try:
                sub_text, _, _ = await fetch_page(session, sub_url)
                if sub_text:
                    trimmed = sub_text[:MAX_TEXT_PER_PAGE]
                    all_text.append(trimmed)
                    total_len += len(trimmed)
            except:
                pass

        combined = "\n\n".join(all_text)[:MAX_TOTAL_TEXT]
        if len(combined) < 50:
            return None  # Not enough text
        return {
            "ein": ein,
            "name": name,
            "url": url,
            "resolved_url": resolved,
            "text": combined,
            "pages_crawled": 1 + len(subpages),
            "crawled_at": datetime.now().isoformat(),
        }
    except Exception as e:
        return {"ein": ein, "name": name, "url": url, "error": str(e)}


def get_unenriched_charities(conn, offset=0, limit=BATCH_SIZE):
    """Get charities with URLs but no LLM enrichment."""
    cur = conn.cursor()
    cur.execute("""
        SELECT c.ein, c.name,
               COALESCE(NULLIF(c.irs_website, ''),
                        NULLIF(c.candid_website, ''),
                        q.url_found) AS url
        FROM charities c
        LEFT JOIN url_discovery_queue q ON q.ein = c.ein AND q.url_found IS NOT NULL
        WHERE c.llm_enriched_at IS NULL
          AND (
              (c.irs_website IS NOT NULL AND c.irs_website != '')
              OR (c.candid_website IS NOT NULL AND c.candid_website != '')
              OR (q.url_found IS NOT NULL AND q.url_found != '')
          )
        ORDER BY c.ein
        OFFSET %s LIMIT %s
    """, (offset, limit))
    return cur.fetchall()


def load_progress():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE) as f:
            return json.load(f)
    return {"crawled": 0, "errors": 0, "offset": 0}


def save_progress(progress):
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f, indent=2)


async def crawl_batch(charities):
    """Crawl a batch of charities concurrently."""
    connector = aiohttp.TCPConnector(limit=CONNECTOR_LIMIT, ssl=False)
    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT * 2)
    results = []
    errors = []

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        tasks = [crawl_one_site(session, ein, name, url) for ein, name, url in charities]
        for coro in asyncio.as_completed(tasks):
            result = await coro
            if result is None:
                errors.append(None)
            elif "error" in result:
                errors.append(result)
            else:
                results.append(result)

    return results, errors


def main():
    global shutdown
    print("=" * 65, flush=True)
    print("POWERDONOR.AI - Step 1 Round 2: Crawl Unenriched Charities", flush=True)
    print("=" * 65, flush=True)

    conn = psycopg2.connect(DB)
    cur = conn.cursor()

    # Count total unenriched
    cur.execute("""
        SELECT count(*) FROM charities c
        LEFT JOIN url_discovery_queue q ON q.ein = c.ein AND q.url_found IS NOT NULL
        WHERE c.llm_enriched_at IS NULL
          AND (
              (c.irs_website IS NOT NULL AND c.irs_website != '')
              OR (c.candid_website IS NOT NULL AND c.candid_website != '')
              OR (q.url_found IS NOT NULL AND q.url_found != '')
          )
    """)
    total = cur.fetchone()[0]
    print(f"Total unenriched charities with URLs: {total:,}", flush=True)

    progress = load_progress()
    offset = progress["offset"]
    crawled = progress["crawled"]
    error_count = progress["errors"]
    start_time = time.time()

    print(f"Resuming from offset {offset:,} (already crawled: {crawled:,})", flush=True)

    # Open output files (append mode for resume)
    crawl_f = open(CRAWL_FILE, "a", encoding="utf-8")
    error_f = open(ERROR_FILE, "a", encoding="utf-8")

    while not shutdown:
        batch = get_unenriched_charities(conn, offset, BATCH_SIZE)
        if not batch:
            print("No more charities to crawl!", flush=True)
            break

        print(f"\nCrawling batch of {len(batch):,} starting at offset {offset:,}...", flush=True)

        # Process in sub-batches for concurrency
        SUB_BATCH = MAX_CONCURRENT
        for i in range(0, len(batch), SUB_BATCH):
            if shutdown:
                break
            sub = batch[i:i + SUB_BATCH]
            results, errors = asyncio.run(crawl_batch(sub))

            for r in results:
                crawl_f.write(json.dumps(r) + "\n")
                crawled += 1

            for e in errors:
                if e:
                    error_f.write(json.dumps(e) + "\n")
                error_count += 1

            if (crawled + error_count) % PROGRESS_INTERVAL < SUB_BATCH:
                elapsed = time.time() - start_time
                rate = crawled / elapsed if elapsed > 0 else 0
                remaining = total - offset - (i + len(sub))
                eta = remaining / rate / 3600 if rate > 0 else 0
                print(f"  Progress: {crawled:,} crawled, {error_count:,} errors | "
                      f"{rate:.1f}/sec | ETA: {eta:.1f}h", flush=True)

        offset += len(batch)
        progress = {"crawled": crawled, "errors": error_count, "offset": offset,
                     "rate_per_sec": crawled / (time.time() - start_time) if crawled > 0 else 0,
                     "last_update": datetime.now().isoformat()}
        save_progress(progress)
        crawl_f.flush()
        error_f.flush()

    crawl_f.close()
    error_f.close()
    conn.close()

    elapsed = time.time() - start_time
    print(f"\n{'='*65}", flush=True)
    print(f"CRAWL COMPLETE", flush=True)
    print(f"  Crawled: {crawled:,}", flush=True)
    print(f"  Errors: {error_count:,}", flush=True)
    print(f"  Time: {elapsed/60:.1f} minutes", flush=True)
    print(f"  Output: {CRAWL_FILE}", flush=True)
    print(f"{'='*65}", flush=True)


if __name__ == "__main__":
    main()
