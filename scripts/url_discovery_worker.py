"""
PowerDonor - Local URL Discovery Worker
========================================
Claims batches from url_discovery_queue, searches DuckDuckGo
for charity websites, updates the queue with results.

Usage:
  python url_discovery_worker.py                # Run until queue is empty
  python url_discovery_worker.py --limit 500    # Process only N charities
  python url_discovery_worker.py --batch 20     # Batch size per claim
  python url_discovery_worker.py --workers 3    # Concurrent workers
"""

import argparse
import logging
import os
import sys
import time
import signal
import threading
from datetime import datetime, timezone
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed

import psycopg2
from psycopg2.extras import execute_batch
from ddgs import DDGS
from dotenv import load_dotenv
load_dotenv()

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DB_URL = os.environ["DATABASE_URL"]
WORKER_ID = "local"
BATCH_SIZE = 20
SEARCH_DELAY = 1.5  # seconds between DuckDuckGo searches (avoid rate limits)
MAX_RESULTS = 5     # DDG results per search

# Logging
LOG_DIR = os.path.dirname(os.path.abspath(__file__))
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, "url_discovery.log"), encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

# Graceful shutdown
shutdown_event = threading.Event()
def signal_handler(sig, frame):
    log.info("Shutdown signal received, finishing current batch...")
    shutdown_event.set()
signal.signal(signal.SIGINT, signal_handler)

# ---------------------------------------------------------------------------
# Skip domains (aggregators, job sites, government, etc.)
# ---------------------------------------------------------------------------
SKIP_DOMAINS = {
    "guidestar.org", "candid.org", "propublica.org", "charitynavigator.org",
    "give.org", "bbb.org", "givefreely.com", "greatnonprofits.org",
    "causeiq.com", "idealist.org", "findhelp.org", "benevity.org",
    "influencewatch.org", "instrumentl.com", "intellispect.co",
    "npidb.org", "fconline.foundationcenter.org", "grantwatch.com",
    "donorbox.org", "networkforgood.org", "volunteermatch.org",
    "justgiving.com", "globalgiving.org", "classy.org",
    "glassdoor.com", "indeed.com", "ziprecruiter.com", "simplyhired.com",
    "careerbuilder.com", "leadiq.com", "govtribe.com", "dnb.com",
    "irs.gov", "govinfo.gov", "sec.gov", "data.cms.gov",
    "google.com", "bing.com", "duckduckgo.com", "yahoo.com", "mapquest.com",
    "yellowpages.com", "yelp.com",
    "facebook.com", "twitter.com", "linkedin.com", "instagram.com",
    "youtube.com", "tiktok.com", "reddit.com", "pinterest.com",
    "wikipedia.org", "wikidata.org", "wikimedia.org",
    "amazon.com", "ebay.com", "etsy.com",
    "bloomberg.com", "forbes.com", "wsj.com", "nytimes.com",
    "ap.org", "reuters.com", "cnn.com", "bbc.com",
    "opencorporates.com", "crunchbase.com", "zoominfo.com",
    "manta.com", "buzzfile.com", "chamberofcommerce.com",
    "nonprofitexplorer.org", "990finder.foundationcenter.org",
    "apps.irs.gov", "thecommunityguide.org",
    "bizapedia.com", "grantadvisor.org", "grantwatch.com",
    "allbiz.com", "nonprofitfacts.com", "open990.org",
    "charitywatch.org", "tax990.com", "nonprofitlight.com",
    "ein-finder.com", "ein-search.com", "990s.foundationcenter.org",
    "projects.propublica.org", "blue-avocado.org",
    "councilofnonprofits.org", "boardsource.org",
    "grantspace.org", "grantforward.com",
    "usa-federal-ein.com", "taxexemptworld.com",
}

# ---------------------------------------------------------------------------
# URL scoring
# ---------------------------------------------------------------------------
def get_root_domain(url):
    """Extract root domain from URL."""
    try:
        netloc = urlparse(url).netloc.lower()
        parts = netloc.split(".")
        if len(parts) >= 2:
            return ".".join(parts[-2:])
        return netloc
    except Exception:
        return ""

def is_skip_domain(url):
    """Check if URL belongs to a skip domain."""
    domain = get_root_domain(url)
    return domain in SKIP_DOMAINS

def score_result(result, name, city, state):
    """Score a search result for how likely it is the charity's official site."""
    url = result.get("href", "")
    title = (result.get("title", "") or "").lower()
    body = (result.get("body", "") or "").lower()
    
    if is_skip_domain(url):
        return -100
    
    score = 0
    name_lower = name.lower() if name else ""
    name_words = [w for w in name_lower.split() if len(w) > 2]
    
    # Name match in title (strong signal)
    title_matches = sum(1 for w in name_words if w in title)
    if title_matches >= len(name_words) * 0.6:
        score += 30
    elif title_matches >= 2:
        score += 15

    # Name match in URL/domain
    url_lower = url.lower()
    url_name_matches = sum(1 for w in name_words if w in url_lower)
    if url_name_matches >= 2:
        score += 20
    
    # City/state match in body or title
    if city and city.lower() in (title + " " + body):
        score += 5
    if state and state.lower() in (title + " " + body):
        score += 3
    
    # Nonprofit signals in body
    nonprofit_signals = ["nonprofit", "non-profit", "501(c)", "charity",
                         "foundation", "donate", "mission", "volunteer"]
    for sig in nonprofit_signals:
        if sig in body or sig in title:
            score += 3
            break
    
    # Penalize very long paths (likely deep pages, not homepages)
    path = urlparse(url).path
    if path.count("/") > 3:
        score -= 10
    
    return score

# ---------------------------------------------------------------------------
# DuckDuckGo search with retry
# ---------------------------------------------------------------------------
def search_ddg(query, max_results=MAX_RESULTS, retries=3):
    """Search DuckDuckGo with retry logic."""
    for attempt in range(retries):
        try:
            results = DDGS().text(query, max_results=max_results)
            return results
        except Exception as e:
            err_str = str(e).lower()
            if "ratelimit" in err_str or "429" in err_str or "202" in err_str:
                wait = (attempt + 1) * 10
                log.warning(f"Rate limited, waiting {wait}s (attempt {attempt+1}/{retries})")
                time.sleep(wait)
            else:
                log.warning(f"DDG search error: {e} (attempt {attempt+1}/{retries})")
                time.sleep(2)
    return None

def find_url_for_charity(name, city, state):
    """Search for a charity's website URL."""
    # Strategy 1: Full name + city + state + "official website"
    query = f"{name} {city or ''} {state or ''} official website"
    results = search_ddg(query.strip())
    
    if results:
        scored = [(score_result(r, name, city, state), r) for r in results]
        scored.sort(key=lambda x: x[0], reverse=True)
        best_score, best_result = scored[0]
        if best_score > 0:
            url = best_result.get("href", "")
            # Clean URL to root domain
            parsed = urlparse(url)
            clean_url = f"{parsed.scheme}://{parsed.netloc}"
            return clean_url, 1

    # Strategy 2: Just the name + "nonprofit"
    time.sleep(SEARCH_DELAY)
    query2 = f"{name} nonprofit"
    results2 = search_ddg(query2)
    
    if results2:
        scored2 = [(score_result(r, name, city, state), r) for r in results2]
        scored2.sort(key=lambda x: x[0], reverse=True)
        best_score2, best_result2 = scored2[0]
        if best_score2 > 0:
            url = best_result2.get("href", "")
            parsed = urlparse(url)
            clean_url = f"{parsed.scheme}://{parsed.netloc}"
            return clean_url, 2
    
    return None, 0


# ---------------------------------------------------------------------------
# Database operations
# ---------------------------------------------------------------------------
def get_connection():
    """Get a fresh database connection."""
    return psycopg2.connect(DB_URL)

def claim_batch(conn, batch_size, worker_id):
    """Claim a batch of pending rows from the queue."""
    cur = conn.cursor()
    cur.execute("""
        UPDATE url_discovery_queue
        SET status = 'claimed', worker_id = %s, claimed_at = NOW()
        WHERE ein IN (
            SELECT ein FROM url_discovery_queue
            WHERE status = 'pending'
            ORDER BY total_expenses DESC NULLS LAST
            LIMIT %s
            FOR UPDATE SKIP LOCKED
        )
        RETURNING ein, name, city, state
    """, (worker_id, batch_size))
    rows = cur.fetchall()
    conn.commit()
    return rows

def mark_done(conn, ein, url_found, strategy):
    """Mark a queue row as done with the found URL."""
    cur = conn.cursor()
    cur.execute("""
        UPDATE url_discovery_queue
        SET status = 'done', url_found = %s, strategy = %s, completed_at = NOW()
        WHERE ein = %s
    """, (url_found, strategy, ein))
    
    # Also update the charities table if we found a URL and it doesn't have one
    if url_found:
        cur.execute("""
            UPDATE charities
            SET irs_website = COALESCE(NULLIF(irs_website, ''), %s),
                updated_at = NOW()
            WHERE ein = %s AND (irs_website IS NULL OR irs_website = '')
        """, (url_found, ein))
    conn.commit()

def mark_no_url(conn, ein):
    """Mark a queue row as done with no URL found."""
    cur = conn.cursor()
    cur.execute("""
        UPDATE url_discovery_queue
        SET status = 'done', strategy = 0, completed_at = NOW()
        WHERE ein = %s
    """, (ein,))
    conn.commit()

def mark_error(conn, ein, error_msg):
    """Mark a queue row with an error."""
    cur = conn.cursor()
    cur.execute("""
        UPDATE url_discovery_queue
        SET status = 'done', error = %s, completed_at = NOW()
        WHERE ein = %s
    """, (str(error_msg)[:500], ein))
    conn.commit()

# ---------------------------------------------------------------------------
# Main worker loop
# ---------------------------------------------------------------------------
def run_worker(worker_id, batch_size, limit=None):
    """Main worker loop: claim batches, search, update."""
    conn = get_connection()
    processed = 0
    found = 0
    not_found = 0
    errors = 0
    start_time = time.time()
    
    log.info(f"Worker '{worker_id}' starting (batch_size={batch_size}, limit={limit})")
    
    while not shutdown_event.is_set():
        # Check limit
        if limit and processed >= limit:
            log.info(f"Reached limit of {limit}, stopping.")
            break
        
        # Claim a batch
        remaining = min(batch_size, (limit - processed) if limit else batch_size)
        batch = claim_batch(conn, remaining, worker_id)
        
        if not batch:
            log.info("No more pending items in queue. Done!")
            break
        
        log.info(f"Claimed {len(batch)} charities (processed so far: {processed})")

        for ein, name, city, state in batch:
            if shutdown_event.is_set():
                break
            
            try:
                url, strategy = find_url_for_charity(name, city, state)
                
                if url:
                    mark_done(conn, ein, url, strategy)
                    found += 1
                    log.info(f"  FOUND: {name} -> {url} (strategy {strategy})")
                else:
                    mark_no_url(conn, ein)
                    not_found += 1
                    log.debug(f"  MISS:  {name}")
                
                processed += 1
                time.sleep(SEARCH_DELAY)
                
            except Exception as e:
                log.error(f"  ERROR: {name} ({ein}): {e}")
                try:
                    mark_error(conn, ein, str(e))
                except Exception:
                    conn = get_connection()  # reconnect on DB error
                errors += 1
                processed += 1
                time.sleep(5)  # longer pause on error
        
        # Progress report
        elapsed = time.time() - start_time
        rate = processed / (elapsed / 3600) if elapsed > 0 else 0
        hit_rate = f"{found/(found+not_found)*100:.1f}%" if (found + not_found) > 0 else "N/A"
        log.info(
            f"Progress: {processed} done | {found} found | {not_found} miss | "
            f"{errors} errors | {rate:.0f}/hr | hit rate: {hit_rate}"
        )
    
    conn.close()
    elapsed = time.time() - start_time
    log.info(f"Worker '{worker_id}' finished: {processed} processed, "
             f"{found} found, {not_found} miss, {errors} errors "
             f"in {elapsed/60:.1f} minutes")

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="URL Discovery Worker")
    parser.add_argument("--limit", type=int, default=None, help="Max charities to process")
    parser.add_argument("--batch", type=int, default=BATCH_SIZE, help="Batch size per claim")
    parser.add_argument("--worker-id", default=WORKER_ID, help="Worker identifier")
    parser.add_argument("--delay", type=float, default=SEARCH_DELAY, help="Seconds between searches")
    args = parser.parse_args()
    
    log.info("=" * 60)
    log.info("PowerDonor URL Discovery Worker")
    log.info(f"Worker ID: {args.worker_id}")
    log.info(f"Batch size: {args.batch}")
    log.info(f"Search delay: {args.delay}s")
    log.info(f"Limit: {args.limit or 'unlimited'}")
    log.info("=" * 60)
    
    # Quick status check
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT status, count(*) FROM url_discovery_queue GROUP BY status ORDER BY status")
    log.info("Queue status before start:")
    for row in cur.fetchall():
        log.info(f"  {row[0]}: {row[1]:,}")
    conn.close()
    
    run_worker(args.worker_id, args.batch, args.limit)

if __name__ == "__main__":
    main()
