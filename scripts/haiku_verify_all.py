#!/usr/bin/env python3
"""
haiku_verify_all.py — Batch Haiku verification of Brave-found websites
=======================================================================
Reads all orgs in target_charities that have a website (website IS NOT NULL),
sends each to Claude Haiku for verification, and writes results back to DB.

Columns written:
  website_verified   BOOLEAN  — True if Haiku confirmed it's the official site
  website_verify_reason TEXT  — short explanation from Haiku
  website_verified_at TIMESTAMPTZ

Requires:
  ANTHROPIC_API_KEY in .env
  Column additions (run once):
    ALTER TABLE target_charities
      ADD COLUMN IF NOT EXISTS website_verified BOOLEAN,
      ADD COLUMN IF NOT EXISTS website_verify_reason TEXT,
      ADD COLUMN IF NOT EXISTS website_verified_at TIMESTAMPTZ;

Usage:
  python haiku_verify_all.py             # Verify all unverified orgs
  python haiku_verify_all.py --limit 50  # Test on first N orgs
  python haiku_verify_all.py --reverify  # Re-verify even already-verified orgs
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

load_dotenv()
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from haiku_verify import verify_website

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DB_CONN       = os.environ["DATABASE_URL"]
PROGRESS_FILE = Path(__file__).parent / "haiku_verify_progress.json"
RATE_DELAY    = 0.3   # seconds between Haiku calls (Haiku is fast, ~3/sec safe)
SAVE_EVERY    = 50    # commit to DB every N orgs
LOG_EVERY     = 10    # print progress every N orgs

# ---------------------------------------------------------------------------
# Progress helpers
# ---------------------------------------------------------------------------
def load_progress():
    if PROGRESS_FILE.exists():
        with open(PROGRESS_FILE) as f:
            return json.load(f)
    return {"verified_eins": [], "confirmed": 0, "rejected": 0, "errors": 0}


def save_progress(progress):
    progress["last_run"] = datetime.now(timezone.utc).isoformat()
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f, indent=2)


# ---------------------------------------------------------------------------
# Schema migration
# ---------------------------------------------------------------------------
def ensure_columns(conn):
    cur = conn.cursor()
    cur.execute("""
        ALTER TABLE target_charities
          ADD COLUMN IF NOT EXISTS website_verified BOOLEAN,
          ADD COLUMN IF NOT EXISTS website_verify_reason TEXT,
          ADD COLUMN IF NOT EXISTS website_verified_at TIMESTAMPTZ
    """)
    conn.commit()
    cur.close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Haiku batch website verifier")
    parser.add_argument("--limit", type=int, default=0,
                        help="Stop after N orgs (0 = all)")
    parser.add_argument("--reverify", action="store_true",
                        help="Re-verify orgs even if already verified")
    args = parser.parse_args()

    conn = psycopg2.connect(DB_CONN)
    ensure_columns(conn)
    cur = conn.cursor()

    # Build work queue
    if args.reverify:
        where = "website IS NOT NULL AND website != ''"
    else:
        where = "website IS NOT NULL AND website != '' AND website_verified IS NULL"

    cur.execute(f"""
        SELECT ein, name, city, state, total_expenses, website
        FROM target_charities
        WHERE {where}
        ORDER BY total_expenses DESC NULLS LAST
    """)
    all_pending = cur.fetchall()

    progress = load_progress()
    already_done = set(progress.get("verified_eins", []))
    pending = [r for r in all_pending if r[0] not in already_done]

    print(f"Orgs with a website:     {len(all_pending):,}")
    print(f"Already verified:        {len(already_done):,}")
    print(f"Pending verification:    {len(pending):,}")

    if args.limit:
        pending = pending[: args.limit]
        print(f"Limited to:              {args.limit}")

    if not pending:
        print("Nothing to do.")
        conn.close()
        return

    total     = len(pending)
    confirmed = 0
    rejected  = 0
    errors    = 0
    start     = time.time()

    print(f"\nStarting Haiku verification for {total:,} orgs...\n")

    for i, (ein, name, city, state, expenses, website) in enumerate(pending, 1):
        try:
            verified, reason = verify_website(name, city or "", state or "", website)
        except Exception as e:
            verified, reason = False, f"exception: {e}"
            errors += 1

        now = datetime.now(timezone.utc)
        cur.execute("""
            UPDATE target_charities
            SET website_verified      = %s,
                website_verify_reason = %s,
                website_verified_at   = %s
            WHERE ein = %s
        """, (verified, reason[:200], now, ein))

        if verified:
            confirmed += 1
            tag = f"VERIFIED  {website[:50]}"
        else:
            rejected += 1
            tag = f"rejected  {reason[:50]}"

        progress["verified_eins"].append(ein)

        if i % SAVE_EVERY == 0:
            conn.commit()
            save_progress(progress)

        if i % LOG_EVERY == 0 or i <= 5:
            elapsed = time.time() - start
            rate    = i / elapsed if elapsed else 0
            eta_min = (total - i) / rate / 60 if rate else 0
            pct     = i / total * 100
            exp_str = f"${expenses/1e6:.1f}M" if expenses else "n/a"
            print(
                f"  [{i:5,}/{total:,}] {pct:4.1f}% | "
                f"+{confirmed} OK / -{rejected} rej | "
                f"ETA {eta_min:.0f}m"
            )
            print(f"    {name[:48]:48} {state or '??':2} {exp_str:>7}  {tag}")

        time.sleep(RATE_DELAY)

    # Final commit
    conn.commit()
    progress["confirmed"] = progress.get("confirmed", 0) + confirmed
    progress["rejected"]  = progress.get("rejected",  0) + rejected
    progress["errors"]    = progress.get("errors",    0) + errors
    save_progress(progress)

    elapsed = time.time() - start
    print(f"\n{'='*60}")
    print(f"DONE: {total:,} orgs verified in {elapsed/60:.1f} min")
    print(f"  Confirmed (VERIFIED): {confirmed:,}  ({confirmed/total*100:.1f}%)")
    print(f"  Rejected:             {rejected:,}  ({rejected/total*100:.1f}%)")
    print(f"  Errors:               {errors:,}")
    print(f"  Cost estimate:        ~${total * 0.00004:.2f} (Haiku @ $0.04/1K input tokens)")
    print(f"{'='*60}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
