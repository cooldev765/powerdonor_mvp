"""
PowerDonor.AI — URL Discovery Worker (Railway-deployable)
Pulls charities from a PostgreSQL queue, searches DuckDuckGo for URLs,
and writes results back. Multiple workers can run in parallel safely.

Environment variables:
    DATABASE_URL  — PostgreSQL connection string
    WORKER_ID     — Unique worker identifier (e.g., "w1", "w2")
    BATCH_SIZE    — How many charities to claim per batch (default: 10)
    DELAY_SECONDS — Delay between DDG queries (default: 2)
"""
import os
import sys
import time
import json
import psycopg2
from psycopg2.extras import execute_values
from urllib.parse import urlparse
from datetime import datetime

try:
    from ddgs import DDGS
except ImportError:
    print("Installing ddgs...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "ddgs"])
    from ddgs import DDGS

# Config from environment
DATABASE_URL = os.environ["DATABASE_URL"]
WORKER_ID = os.environ.get("WORKER_ID", "local")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "10"))
DELAY_SECONDS = float(os.environ.get("DELAY_SECONDS", "2"))

EXCLUDE_DOMAINS = {
    'causeiq.com','guidestar.org','charitynavigator.org','propublica.org',
    'instrumentl.com','candid.org','nonprofitexplorer.org','nonprofitlight.com',
    'foundationcenter.org','greatnonprofits.org','open990.org','nonprofitfacts.com',
    'taxexemptworld.com','networkforgood.org','grantspace.org','boardsource.org',
    'give.org','charity.org','charities.org',
    'justgiving.com','gofundme.com','donorbox.org','classy.org',
    'mightycause.com','qgiv.com','bloomerang.co','neonone.com',
    'givefreely.com','every.org','pledgeling.com','givebutter.com',
    'idealist.org','volunteermatch.org',
    'intellispect.co','economicresearchinstitute.org','erieri.com',
    'projects.propublica.org','nonprofitquarterly.org',
    'nonprofitlocator.org','archive.org','web.archive.org',
    'eintaxid.com','healthgrades.com','vitals.com','webmd.com',
    'zocdoc.com','usnews.com','niche.com','greatschools.org',
    'irs.gov','sec.gov','usaspending.gov','sam.gov','data.gov',
    'congress.gov','federalregister.gov','grants.gov','census.gov',
    'bls.gov','treasury.gov','govinfo.gov','nlrb.gov','emedny.org',
    'cms.gov','medicaid.gov','medicare.gov','hhs.gov','ed.gov',
    'dol.gov','epa.gov','energy.gov','osha.gov',
    'facebook.com','linkedin.com','twitter.com','instagram.com',
    'youtube.com','x.com','tiktok.com','pinterest.com',
    'yelp.com','bbb.org','glassdoor.com','indeed.com','ziprecruiter.com',
    'yellowpages.com','manta.com','chamberofcommerce.com','zoominfo.com',
    'hoovers.com','dnb.com','buzzfile.com','opencorporates.com',
    'crunchbase.com','bloomberg.com','mapquest.com','kompass.com',
    'wikipedia.org','findagrave.com',
    'ehealthinsurance.com','healthmarkets.com',
    'opensecrets.org','followthemoney.org',
    'google.com','coursera.org','amazon.com','ebay.com',
    'nytimes.com','washingtonpost.com','cnn.com','foxnews.com',
    'usatoday.com','reuters.com','apnews.com',
    'beckershospitalreview.com',
}


def is_legitimate_url(url):
    try:
        domain = urlparse(url).netloc.lower()
        if domain.startswith('www.'):
            domain = domain[4:]
        for excl in EXCLUDE_DOMAINS:
            if domain == excl or domain.endswith('.' + excl):
                return False
        return True
    except:
        return False


def search_charity_url(name, city, state, max_retries=3):
    """Search DuckDuckGo for a charity's website. Two strategies + retry."""
    for attempt in range(max_retries):
        try:
            # Strategy 1: full query
            results = DDGS().text(
                f"{name} {city} {state} nonprofit official website",
                max_results=5
            )
            for r in results:
                href = r.get('href', '')
                if is_legitimate_url(href):
                    return href, 1

            # Strategy 2: shorter query
            time.sleep(1)
            results2 = DDGS().text(f"{name} nonprofit", max_results=5)
            for r in results2:
                href = r.get('href', '')
                if is_legitimate_url(href):
                    return href, 2

            return None, 0  # Not found

        except Exception as e:
            if 'Ratelimit' in str(e) and attempt < max_retries - 1:
                wait = 30 * (attempt + 1)
                log(f"Rate limited, waiting {wait}s...")
                time.sleep(wait)
            elif attempt < max_retries - 1:
                time.sleep(5)
            else:
                return None, f"ERR:{str(e)[:60]}"


def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{WORKER_ID} {ts}] {msg}", flush=True)


def setup_queue(conn):
    """Create the queue table and populate it if empty."""
    cur = conn.cursor()

    # Create queue table if not exists
    cur.execute("""
        CREATE TABLE IF NOT EXISTS url_discovery_queue (
            ein TEXT PRIMARY KEY,
            name TEXT,
            city TEXT,
            state TEXT,
            total_expenses BIGINT,
            status TEXT DEFAULT 'pending',
            worker_id TEXT,
            url_found TEXT,
            strategy INT,
            error TEXT,
            claimed_at TIMESTAMP,
            completed_at TIMESTAMP
        )
    """)
    conn.commit()

    # Check if queue is populated
    cur.execute("SELECT COUNT(*) FROM url_discovery_queue")
    count = cur.fetchone()[0]

    if count == 0:
        log("Populating queue with charities needing URLs...")
        cur.execute("""
            INSERT INTO url_discovery_queue (ein, name, city, state, total_expenses)
            SELECT c.ein, c.name, c.city, c.state, i.total_expenses
            FROM charities c
            JOIN irs_990 i ON c.ein = i.ein
            WHERE i.tax_year = 2024
              AND i.total_expenses >= 100000
              AND (c.irs_website IS NULL OR c.irs_website = '')
              AND c.llm_mission IS NULL
            ORDER BY i.total_expenses DESC
            ON CONFLICT (ein) DO NOTHING
        """)
        conn.commit()
        cur.execute("SELECT COUNT(*) FROM url_discovery_queue WHERE status = 'pending'")
        count = cur.fetchone()[0]
        log(f"Queue populated: {count} charities")
    else:
        cur.execute("SELECT COUNT(*) FROM url_discovery_queue WHERE status = 'pending'")
        pending = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM url_discovery_queue WHERE status = 'done'")
        done = cur.fetchone()[0]
        log(f"Queue exists: {count} total, {pending} pending, {done} done")

    return count


def claim_batch(conn, batch_size):
    """Atomically claim a batch of pending charities using SKIP LOCKED."""
    cur = conn.cursor()
    cur.execute("""
        UPDATE url_discovery_queue
        SET status = 'claimed', worker_id = %s, claimed_at = NOW()
        WHERE ein IN (
            SELECT ein FROM url_discovery_queue
            WHERE status = 'pending'
            ORDER BY total_expenses DESC
            LIMIT %s
            FOR UPDATE SKIP LOCKED
        )
        RETURNING ein, name, city, state, total_expenses
    """, (WORKER_ID, batch_size))
    rows = cur.fetchall()
    conn.commit()
    return rows


def mark_done(conn, ein, url_found, strategy, error=None):
    """Mark a charity as processed in the queue + update charities table."""
    cur = conn.cursor()
    cur.execute("""
        UPDATE url_discovery_queue
        SET status = 'done', url_found = %s, strategy = %s,
            error = %s, completed_at = NOW()
        WHERE ein = %s
    """, (url_found, strategy if isinstance(strategy, int) else None, error, ein))

    # If URL found, also update the charities table
    if url_found:
        cur.execute("""
            UPDATE charities SET irs_website = %s WHERE ein = %s
        """, (url_found, ein))

    conn.commit()


def main():
    log(f"Starting URL Discovery Worker")
    log(f"Batch size: {BATCH_SIZE} | Delay: {DELAY_SECONDS}s")
    log(f"Exclude domains: {len(EXCLUDE_DOMAINS)}")

    conn = psycopg2.connect(DATABASE_URL)
    setup_queue(conn)

    total_found = 0
    total_missed = 0
    total_errors = 0
    start_time = time.time()

    while True:
        batch = claim_batch(conn, BATCH_SIZE)
        if not batch:
            log("No more pending charities. Worker done!")
            break

        for ein, name, city, state, expenses in batch:
            name = name.strip()
            url, strategy = search_charity_url(name, city, state)

            if url and isinstance(strategy, int):
                domain = urlparse(url).netloc
                log(f"FOUND(S{strategy}) {name[:40]:<40s} ${expenses:>13,}  {domain}")
                mark_done(conn, ein, url, strategy)
                total_found += 1
            elif isinstance(strategy, str) and strategy.startswith("ERR"):
                log(f"ERROR    {name[:40]:<40s} ${expenses:>13,}  {strategy}")
                mark_done(conn, ein, None, 0, error=strategy)
                total_errors += 1
            else:
                log(f"MISS     {name[:40]:<40s} ${expenses:>13,}")
                mark_done(conn, ein, None, 0, error="not_found")
                total_missed += 1

            time.sleep(DELAY_SECONDS)

        # Progress report after each batch
        processed = total_found + total_missed + total_errors
        elapsed = time.time() - start_time
        rate = processed / elapsed * 3600 if elapsed > 0 else 0
        log(f"Progress: {processed} done | Found: {total_found} | "
            f"Miss: {total_missed} | Err: {total_errors} | {rate:.0f}/hr")

    # Final report
    elapsed = time.time() - start_time
    processed = total_found + total_missed + total_errors
    log(f"=== WORKER {WORKER_ID} COMPLETE ===")
    log(f"Processed: {processed}")
    log(f"Found: {total_found} ({100*total_found/max(processed,1):.1f}%)")
    log(f"Missed: {total_missed} | Errors: {total_errors}")
    log(f"Runtime: {elapsed/3600:.1f} hours")
    conn.close()


if __name__ == "__main__":
    main()
