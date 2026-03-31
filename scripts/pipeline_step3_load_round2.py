"""
PowerDonor.AI - Pipeline Step 3 Round 2: Load LLM Results into Database
Reads round2 batch API results, parses JSON, updates charities table.
Identical logic to pipeline_step3_load.py but reads from round2 dir.
"""
import json, os, re
import psycopg2
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

DB = os.environ["DATABASE_URL"]
OUTPUT_DIR = os.environ.get("CRAWL_OUTPUT_DIR", os.path.join(os.path.dirname(os.path.abspath(__file__)), "crawl_output", "round2"))
RESULTS_FILE = os.path.join(OUTPUT_DIR, "batch_results.jsonl")

def parse_extraction(result_obj):
    try:
        res = result_obj.get("result", {}) if isinstance(result_obj, dict) else result_obj
        msg = res.get("message", {})
        content = msg.get("content", [])
        if not content:
            return None
        text = content[0].get("text", "")
        if not text:
            return None
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            m = re.search(r'\{[\s\S]*\}', text)
            if m:
                try:
                    return json.loads(m.group())
                except:
                    return None
            return None
    except:
        return None

def to_json_or_null(val):
    if val is None:
        return None
    if isinstance(val, (list, dict)):
        return json.dumps(val) if len(val) > 0 else None
    return None

def main():
    print(f"{'='*65}", flush=True)
    print("POWERDONOR.AI - Step 3 Round 2: Load LLM Results", flush=True)
    print(f"{'='*65}", flush=True)
    if not os.path.exists(RESULTS_FILE):
        print(f"Results file not found: {RESULTS_FILE}", flush=True)
        return
    total = sum(1 for _ in open(RESULTS_FILE))
    print(f"Results file: {total:,} records", flush=True)
    conn = psycopg2.connect(DB)
    cur = conn.cursor()
    UPDATE_SQL = """
        UPDATE charities SET
            llm_mission=%s, llm_programs=%s, llm_areas_served=%s,
            llm_keywords=%s, llm_cause_categories=%s, llm_year_founded=%s,
            llm_staff_count=%s, llm_volunteer_info=%s, llm_impact_metrics=%s,
            llm_contact_email=%s, llm_contact_phone=%s, llm_social_media=%s,
            llm_leadership=%s, llm_annual_events=%s, llm_partners=%s,
            llm_donation_options=%s, llm_enriched_at=%s, llm_raw_json=%s
        WHERE ein = %s
    """
    loaded = 0
    parse_errors = 0
    now = datetime.now()

    with open(RESULTS_FILE) as f:
        for line in f:
            try:
                record = json.loads(line)
                ein = record["custom_id"]
                data = parse_extraction(record)
                if data is None:
                    parse_errors += 1
                    continue
                cur.execute(UPDATE_SQL, (
                    data.get("mission_statement"),
                    to_json_or_null(data.get("programs")),
                    data.get("areas_served"),
                    to_json_or_null(data.get("keywords")),
                    to_json_or_null(data.get("cause_categories")),
                    data.get("year_founded"),
                    data.get("staff_count"),
                    data.get("volunteer_info"),
                    to_json_or_null(data.get("impact_metrics")),
                    data.get("contact_email"),
                    data.get("contact_phone"),
                    to_json_or_null(data.get("social_media")),
                    to_json_or_null(data.get("leadership")),
                    to_json_or_null(data.get("annual_events")),
                    to_json_or_null(data.get("partners")),
                    to_json_or_null(data.get("donation_options")),
                    now, json.dumps(data), ein,
                ))
                loaded += 1
            except Exception:
                parse_errors += 1
            if (loaded + parse_errors) % 5000 == 0:
                conn.commit()
                print(f"  {loaded+parse_errors:,}/{total:,} - Loaded: {loaded:,} | Errors: {parse_errors:,}", flush=True)
    conn.commit()
    cur.close()
    conn.close()
    print(f"\n{'='*65}", flush=True)
    print(f"LOAD COMPLETE: {loaded:,} loaded, {parse_errors:,} errors", flush=True)
    print(f"{'='*65}", flush=True)

if __name__ == "__main__":
    main()
