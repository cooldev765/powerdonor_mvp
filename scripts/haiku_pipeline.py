"""
PowerDonor.AI — Railway-Deployable Haiku Pipeline Worker
Polls Anthropic Batch API, downloads results, loads directly into PostgreSQL.
No local file storage needed — runs entirely in-memory against the DB.

Environment variables:
    DATABASE_URL       — PostgreSQL connection string
    ANTHROPIC_API_KEY  — Anthropic API key for batch operations
    POLL_INTERVAL      — Seconds between status checks (default: 300 = 5 min)
    WORKER_MODE        — "check_and_load" (default), "status", "load_completed"
"""
import os
import sys
import json
import re
import time
import psycopg2
import psycopg2.extras
from psycopg2.extras import execute_values, Json
from datetime import datetime

try:
    import httpx
    from anthropic import Anthropic
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "anthropic", "httpx"])
    import httpx
    from anthropic import Anthropic
# Register JSON adapter so psycopg2 can handle dicts/lists in JSONB columns
psycopg2.extensions.register_adapter(dict, Json)
psycopg2.extensions.register_adapter(list, Json)

# --- Config from environment ---
DATABASE_URL = os.environ["DATABASE_URL"]
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "300"))

client = Anthropic(
    api_key=ANTHROPIC_API_KEY,
    timeout=httpx.Timeout(600.0, connect=30.0),
)

# --- LLM column definitions (must match charities table) ---
LLM_COLUMNS = [
    "llm_mission", "llm_programs", "llm_areas_served", "llm_keywords",
    "llm_cause_categories", "llm_year_founded", "llm_staff_count",
    "llm_volunteer_info", "llm_impact_metrics", "llm_contact_email",
    "llm_contact_phone", "llm_social_media", "llm_leadership",
    "llm_annual_events", "llm_partners", "llm_donation_options",
    "llm_enriched_at", "llm_raw_json",
]

JSONB_COLUMNS = {
    "llm_programs", "llm_keywords", "llm_cause_categories",
    "llm_impact_metrics", "llm_social_media", "llm_leadership",
    "llm_annual_events", "llm_partners", "llm_donation_options",
    "llm_raw_json",
}

FIELD_MAP = {
    "mission_statement": "llm_mission",
    "programs": "llm_programs",
    "areas_served": "llm_areas_served",
    "keywords": "llm_keywords",
    "cause_categories": "llm_cause_categories",
    "year_founded": "llm_year_founded",
    "staff_count": "llm_staff_count",
    "volunteer_info": "llm_volunteer_info",
    "impact_metrics": "llm_impact_metrics",
    "contact_email": "llm_contact_email",
    "contact_phone": "llm_contact_phone",
    "social_media": "llm_social_media",
    "leadership": "llm_leadership",
    "annual_events": "llm_annual_events",
    "partners": "llm_partners",
    "donation_options": "llm_donation_options",
}


def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


def get_db():
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False
    return conn


def to_jsonb(val):
    if val is None:
        return None
    if isinstance(val, (list, dict)):
        return Json(val) if val else None
    return None


def parse_extraction(result_obj):
    """Parse a batch API result into extraction dict."""
    try:
        if result_obj.type == "errored":
            return None
        msg = result_obj.message
        if not msg or not msg.content:
            return None
        text = msg.content[0].text
        if not text:
            return None

        text = text.strip()
        if text.startswith("```"):
            text = re.sub(r'^```\w*\n?', '', text)
            text = re.sub(r'\n?```$', '', text)
            text = text.strip()

        try:
            return json.loads(text)
        except json.JSONDecodeError:
            m = re.search(r'\{[\s\S]*\}', text)
            if m:
                try:
                    return json.loads(m.group())
                except Exception:
                    return None
            return None
    except Exception:
        return None


def record_to_row(ein, data, now):
    """Convert parsed extraction to a tuple for batch insert."""
    return (
        ein,
        data.get("mission_statement"),
        to_jsonb(data.get("programs")),
        data.get("areas_served"),
        to_jsonb(data.get("keywords")),
        to_jsonb(data.get("cause_categories")),
        str(data.get("year_founded")) if data.get("year_founded") else None,
        str(data.get("staff_count")) if data.get("staff_count") else None,
        data.get("volunteer_info"),
        to_jsonb(data.get("impact_metrics")),
        data.get("contact_email"),
        data.get("contact_phone"),
        to_jsonb(data.get("social_media")),
        to_jsonb(data.get("leadership")),
        to_jsonb(data.get("annual_events")),
        to_jsonb(data.get("partners")),
        to_jsonb(data.get("donation_options")),
        now,
        Json(data),
    )


def flush_batch(cur, rows):
    """Insert a batch of rows using temp table + UPDATE join."""
    if not rows:
        return 0

    cols = ["ein"] + LLM_COLUMNS
    set_clauses = ", ".join(f"{c} = tmp.{c}" for c in LLM_COLUMNS)
    col_list = ", ".join(cols)

    cur.execute("""
        CREATE TEMP TABLE IF NOT EXISTS _llm_tmp (
            ein TEXT, llm_mission TEXT, llm_programs JSONB,
            llm_areas_served TEXT, llm_keywords JSONB,
            llm_cause_categories JSONB, llm_year_founded TEXT,
            llm_staff_count TEXT, llm_volunteer_info TEXT,
            llm_impact_metrics JSONB, llm_contact_email TEXT,
            llm_contact_phone TEXT, llm_social_media JSONB,
            llm_leadership JSONB, llm_annual_events JSONB,
            llm_partners JSONB, llm_donation_options JSONB,
            llm_enriched_at TIMESTAMP, llm_raw_json JSONB
        ) ON COMMIT DELETE ROWS
    """)

    insert_sql = f"INSERT INTO _llm_tmp ({col_list}) VALUES %s"
    execute_values(cur, insert_sql, rows, page_size=500)

    cur.execute(f"""
        UPDATE charities c SET {set_clauses}
        FROM _llm_tmp tmp WHERE c.ein = tmp.ein
    """)
    return cur.rowcount


# ---- Main pipeline functions ----

def check_batch_statuses():
    """Poll Anthropic API for batch statuses, update DB."""
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT batch_id, batch_num FROM haiku_batches
        WHERE status != 'ended' ORDER BY batch_num
    """)
    pending = cur.fetchall()

    if not pending:
        log("All batches already completed!")
        cur.close()
        conn.close()
        return True

    log(f"Checking {len(pending)} in-progress batches...")
    all_done = True

    for batch_id, batch_num in pending:
        try:
            batch = client.beta.messages.batches.retrieve(batch_id)
            succeeded = batch.request_counts.succeeded if batch.request_counts else 0
            errored = batch.request_counts.errored if batch.request_counts else 0
            status = batch.processing_status

            cur.execute("""
                UPDATE haiku_batches
                SET status = %s, succeeded = %s, errored = %s,
                    completed_at = CASE WHEN %s = 'ended' THEN NOW() ELSE completed_at END
                WHERE batch_id = %s
            """, (status, succeeded, errored, status, batch_id))

            if status != "ended":
                all_done = False
            log(f"  Batch {batch_num}: {status} (ok={succeeded}, err={errored})")
        except Exception as e:
            log(f"  Batch {batch_num}: ERROR - {e}")
            all_done = False

    conn.commit()
    cur.close()
    conn.close()

    return all_done


def download_and_load_batch(batch_id, batch_num):
    """Download results from one batch and load directly into DB."""
    conn = get_db()
    cur = conn.cursor()
    now = datetime.now()

    log(f"  Downloading+loading batch {batch_num} ({batch_id})...")
    rows = []
    loaded = 0
    skipped = 0
    count = 0

    for result in client.beta.messages.batches.results(batch_id):
        count += 1
        ein = result.custom_id
        data = parse_extraction(result.result)
        if data is None:
            skipped += 1
            continue
        rows.append(record_to_row(ein, data, now))

        if len(rows) >= 500:
            updated = flush_batch(cur, rows)
            conn.commit()
            loaded += updated
            rows = []

    # Flush remaining
    if rows:
        updated = flush_batch(cur, rows)
        conn.commit()
        loaded += updated

    # Mark batch as downloaded+loaded
    cur.execute("""
        UPDATE haiku_batches
        SET downloaded = TRUE, loaded = TRUE
        WHERE batch_id = %s
    """, (batch_id,))
    conn.commit()

    cur.close()
    conn.close()
    log(f"  Batch {batch_num}: {loaded} loaded, {skipped} skipped, {count} total")
    return loaded


def load_completed_batches():
    """Download and load all completed but not-yet-loaded batches."""
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT batch_id, batch_num FROM haiku_batches
        WHERE status = 'ended' AND loaded = FALSE
        ORDER BY batch_num
    """)
    to_load = cur.fetchall()
    cur.close()
    conn.close()

    if not to_load:
        log("No completed batches waiting to be loaded.")
        return 0

    log(f"Loading {len(to_load)} completed batches into DB...")
    total_loaded = 0

    for batch_id, batch_num in to_load:
        try:
            loaded = download_and_load_batch(batch_id, batch_num)
            total_loaded += loaded
        except Exception as e:
            log(f"  ERROR loading batch {batch_num}: {e}")

    log(f"Total loaded this run: {total_loaded:,}")
    return total_loaded


def print_status():
    """Print current pipeline status."""
    conn = get_db()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*), SUM(record_count) FROM haiku_batches")
    total_batches, total_records = cur.fetchone()

    cur.execute("SELECT status, COUNT(*), SUM(succeeded), SUM(errored) FROM haiku_batches GROUP BY status")
    log(f"=== Pipeline Status ({total_batches} batches, {total_records:,} records) ===")
    for status, count, succeeded, errored in cur.fetchall():
        log(f"  {status}: {count} batches (ok={succeeded or 0:,}, err={errored or 0:,})")

    cur.execute("SELECT COUNT(*) FROM haiku_batches WHERE loaded = TRUE")
    loaded_batches = cur.fetchone()[0]
    log(f"  Loaded into DB: {loaded_batches} batches")

    cur.execute("SELECT COUNT(*) FROM charities WHERE llm_mission IS NOT NULL")
    enriched = cur.fetchone()[0]
    log(f"  Total enriched charities: {enriched:,}")

    cur.close()
    conn.close()


def check_and_load_loop():
    """Main loop: check statuses, load completed batches, repeat."""
    log("Starting check-and-load pipeline loop...")
    log(f"Poll interval: {POLL_INTERVAL}s")

    while True:
        # Check statuses
        all_done = check_batch_statuses()

        # Load any newly completed batches
        loaded = load_completed_batches()
        if loaded > 0:
            log(f"Loaded {loaded:,} records this cycle")

        # Print summary
        print_status()

        if all_done:
            log("ALL BATCHES COMPLETE AND LOADED! Pipeline finished.")
            break

        log(f"Next check in {POLL_INTERVAL}s...")
        time.sleep(POLL_INTERVAL)


def main():
    mode = os.environ.get("WORKER_MODE", sys.argv[1] if len(sys.argv) > 1 else "check_and_load")

    if mode == "check_and_load":
        check_and_load_loop()
    elif mode == "status":
        check_batch_statuses()
        print_status()
    elif mode == "load_completed":
        load_completed_batches()
        print_status()
    else:
        log(f"Unknown mode: {mode}")
        log("Modes: check_and_load, status, load_completed")


if __name__ == "__main__":
    main()
