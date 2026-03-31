"""
PowerDonor.AI - Pipeline Step 2 Round 2: Haiku Batch API
=========================================================
Reads round2 crawled data and submits to Anthropic Batch API.
Reuses the exact same logic as pipeline_step2_batch.py but
reads from round2 directory.
"""
import json, os, time, sys
from anthropic import Anthropic
from datetime import datetime
import httpx
from dotenv import load_dotenv
load_dotenv()

client = Anthropic(
    api_key=os.environ["ANTHROPIC_API_KEY"],
    timeout=httpx.Timeout(600.0, connect=30.0),
)
HAIKU = "claude-haiku-4-5-20251001"

OUTPUT_DIR = os.environ.get("CRAWL_OUTPUT_DIR", os.path.join(os.path.dirname(os.path.abspath(__file__)), "crawl_output", "round2"))
CRAWL_FILE = os.path.join(OUTPUT_DIR, "crawled_data.jsonl")
BATCH_STATUS = os.path.join(OUTPUT_DIR, "batch_status.json")
RESULTS_DIR = os.path.join(OUTPUT_DIR, "batch_results")
os.makedirs(RESULTS_DIR, exist_ok=True)

BATCH_SIZE = 1000
MAX_RETRIES = 3
RETRY_DELAY = 15

EXTRACTION_PROMPT = """You are analyzing a nonprofit charity's website. Extract the following information in JSON format. If a field is not found, use null.

{
  "mission_statement": "The organization's mission statement or purpose",
  "programs": ["List of specific programs or services offered"],
  "areas_served": "Geographic areas served (city, state, national, international)",
  "keywords": ["5-10 keywords describing what this organization does"],
  "cause_categories": ["Broad cause categories like Education, Health, Environment, etc."],
  "year_founded": "Year the organization was founded, if mentioned",
  "staff_count": "Number of staff or employees if mentioned",
  "volunteer_info": "Any information about volunteer opportunities",
  "impact_metrics": ["Any specific numbers about impact, people served, etc."],
  "contact_email": "Primary contact email if shown",
  "contact_phone": "Primary phone number if shown",
  "social_media": {"platform": "url"},
  "leadership": [{"name": "Name", "title": "Title"}],
  "annual_events": ["Named annual events or campaigns"],
  "partners": ["Named partner organizations"],
  "donation_options": ["Types of giving: one-time, monthly, planned giving, etc."]
}

Return ONLY valid JSON. No other text."""


def load_crawled_records():
    print("Reading crawled data...", flush=True)
    records = []
    with open(CRAWL_FILE, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            try:
                d = json.loads(line)
                if "text" in d and len(d["text"]) > 50:
                    records.append(d)
            except:
                pass
    print(f"  {len(records):,} records with text", flush=True)
    return records


def load_status():
    if os.path.exists(BATCH_STATUS):
        with open(BATCH_STATUS, "r") as f:
            return json.load(f)
    return {"batches": [], "total_submitted": 0, "total_records": 0}

def save_status(status):
    with open(BATCH_STATUS, "w") as f:
        json.dump(status, f, indent=2)

def get_already_submitted_eins(status):
    eins = set()
    for b in status.get("batches", []):
        for ein in b.get("eins", []):
            eins.add(ein)
    return eins

def make_request(ein, name, text):
    return {
        "custom_id": ein,
        "params": {
            "model": HAIKU,
            "max_tokens": 2000,
            "system": EXTRACTION_PROMPT,
            "messages": [
                {"role": "user",
                 "content": f"Organization: {name}\n\nWebsite content:\n{text[:15000]}"}
            ]
        }
    }


def submit_batches():
    records = load_crawled_records()
    status = load_status()
    already_submitted = get_already_submitted_eins(status)
    remaining = [r for r in records if r["ein"] not in already_submitted]
    print(f"  Already submitted: {len(already_submitted):,}", flush=True)
    print(f"  Remaining to submit: {len(remaining):,}", flush=True)
    if not remaining:
        print("All records already submitted!", flush=True)
        return
    status["total_records"] = len(records)
    for batch_idx in range(0, len(remaining), BATCH_SIZE):
        chunk = remaining[batch_idx:batch_idx + BATCH_SIZE]
        batch_num = len(status["batches"]) + 1
        print(f"\n--- Submitting batch {batch_num} ({len(chunk):,} records) ---", flush=True)
        requests = [make_request(r["ein"], r.get("name", "Unknown"), r["text"]) for r in chunk]
        eins = [r["ein"] for r in chunk]
        submitted = False
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                print(f"  Attempt {attempt}/{MAX_RETRIES}...", flush=True)
                batch = client.beta.messages.batches.create(requests=requests)
                print(f"  Batch ID: {batch.id}", flush=True)
                print(f"  Status: {batch.processing_status}", flush=True)
                batch_info = {
                    "batch_id": batch.id, "batch_num": batch_num,
                    "count": len(chunk), "status": batch.processing_status,
                    "submitted_at": datetime.now().isoformat(), "eins": eins,
                }
                status["batches"].append(batch_info)
                status["total_submitted"] += len(chunk)
                save_status(status)

                print(f"  Progress saved. Total submitted: {status['total_submitted']:,}", flush=True)
                submitted = True
                break
            except Exception as e:
                print(f"  ERROR on attempt {attempt}: {e}", flush=True)
                if attempt < MAX_RETRIES:
                    print(f"  Retrying in {RETRY_DELAY}s...", flush=True)
                    time.sleep(RETRY_DELAY)
                else:
                    print(f"  All retries failed for batch {batch_num}. Saving and stopping.", flush=True)
                    save_status(status)
                    return
    print(f"\nAll {len(status['batches'])} batches submitted!", flush=True)


def check_all_batches():
    status = load_status()
    if not status["batches"]:
        print("No batches submitted yet.", flush=True)
        return False
    all_done = True
    total_succeeded = 0
    total_errored = 0
    for b in status["batches"]:
        batch_id = b["batch_id"]
        try:
            batch = client.beta.messages.batches.retrieve(batch_id)
            b["status"] = batch.processing_status
            if batch.request_counts:
                rc = batch.request_counts
                b["succeeded"] = rc.succeeded
                b["errored"] = rc.errored
                total_succeeded += rc.succeeded
                total_errored += rc.errored

            print(f"  Batch {b['batch_num']} ({batch_id}): {batch.processing_status} "
                  f"(ok={b.get('succeeded',0)}, err={b.get('errored',0)})", flush=True)
            if batch.processing_status != "ended":
                all_done = False
        except Exception as e:
            print(f"  Batch {b['batch_num']} ({batch_id}): ERROR - {e}", flush=True)
            all_done = False
    status["last_checked"] = datetime.now().isoformat()
    status["total_succeeded"] = total_succeeded
    status["total_errored"] = total_errored
    save_status(status)
    print(f"\nTotal: {total_succeeded:,} succeeded, {total_errored:,} errored", flush=True)
    return all_done


def download_all_results():
    status = load_status()
    combined_file = os.path.join(OUTPUT_DIR, "batch_results.jsonl")
    total_downloaded = 0
    with open(combined_file, "w", encoding="utf-8") as out_f:
        for b in status["batches"]:
            batch_id = b["batch_id"]
            batch = client.beta.messages.batches.retrieve(batch_id)
            if batch.processing_status != "ended":
                print(f"  Batch {b['batch_num']} not done yet, skipping", flush=True)
                continue
            print(f"  Downloading batch {b['batch_num']} ({batch_id})...", flush=True)
            count = 0
            for result in client.beta.messages.batches.results(batch_id):
                entry = {"custom_id": result.custom_id, "result": result.result.model_dump()}
                out_f.write(json.dumps(entry) + "\n")
                count += 1
                if count % 5000 == 0:
                    print(f"    {count:,} results...", flush=True)

            print(f"    Batch {b['batch_num']}: {count:,} results", flush=True)
            total_downloaded += count
            b["downloaded"] = count
    save_status(status)
    print(f"\nTotal: {total_downloaded:,} results saved to {combined_file}", flush=True)
    return combined_file


def main():
    print(f"{'='*65}", flush=True)
    print(f"POWERDONOR.AI - Step 2 Round 2: Haiku Batch API", flush=True)
    print(f"{'='*65}", flush=True)
    cmd = sys.argv[1] if len(sys.argv) > 1 else "submit"
    if cmd == "submit":
        submit_batches()
        print("\nBatches submitted! Next:", flush=True)
        print("  python pipeline_step2_batch_round2.py status", flush=True)
        print("  python pipeline_step2_batch_round2.py check_loop", flush=True)
        print("  python pipeline_step2_batch_round2.py download", flush=True)
    elif cmd == "status":
        check_all_batches()
    elif cmd == "download":
        download_all_results()
    elif cmd == "check_loop":
        print("Polling every 5 min until all batches complete...", flush=True)
        while True:
            all_done = check_all_batches()
            if all_done:
                print("\nAll done! Downloading...", flush=True)
                download_all_results()
                break
            print(f"  Next check in 5 min ({datetime.now().strftime('%H:%M:%S')})...", flush=True)
            time.sleep(300)
    else:
        print(f"Unknown command: {cmd}. Use: submit|status|check_loop|download", flush=True)

if __name__ == "__main__":
    main()
