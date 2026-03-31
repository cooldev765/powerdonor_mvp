"""
PowerDonor.AI — Category & Population Classifier
=================================================
Reads from mvp_charities, submits to Anthropic Batch API.

Populates:
  - cat_* boolean columns (11 donor-facing categories)
  - populations_served JSONB array (44 options)
  - geographic_scope TEXT (rule-based, no LLM needed)

Subcategories (llm_subcategories) are skipped until taxonomy.json is finalized.

Usage:
  python pipeline_categorize.py submit
  python pipeline_categorize.py status
  python pipeline_categorize.py check_loop
  python pipeline_categorize.py download
  python pipeline_categorize.py load
"""
import json, os, sys, time, re
import psycopg2
from psycopg2.extras import execute_batch
from anthropic import Anthropic
from datetime import datetime
import httpx
from dotenv import load_dotenv
load_dotenv()

client = Anthropic(
    api_key=os.environ["ANTHROPIC_API_KEY"],
    timeout=httpx.Timeout(600.0, connect=30.0),
)
DB  = os.environ["DATABASE_URL"]
HAIKU = "claude-haiku-4-5-20251001"

OUTPUT_DIR   = os.path.join(os.path.dirname(os.path.abspath(__file__)), "categorize_output")
os.makedirs(OUTPUT_DIR, exist_ok=True)
BATCH_STATUS = os.path.join(OUTPUT_DIR, "batch_status.json")
RESULTS_FILE = os.path.join(OUTPUT_DIR, "batch_results.jsonl")

BATCH_SIZE  = 1000
MAX_RETRIES = 3
RETRY_DELAY = 15

# ── 11 donor-facing categories ────────────────────────────────────────────────
CATEGORIES = [
    "Arts, Culture & Media",
    "Basic Needs, Human Services & Families",
    "Economic Empowerment & Community Development & Housing",
    "Education & Skill Development",
    "Environment, Climate & Animals",
    "Equity, Justice & Civic Life",
    "Faith & Interfaith Initiatives",
    "International Aid & Global Development",
    "Medical Research, Health & Wellbeing",
    "Public Policy, Civic Engagement & Democracy",
    "Science, Technology & Innovation for Good",
]

# ── Category name → DB column ─────────────────────────────────────────────────
CATEGORY_TO_COL = {
    "Arts, Culture & Media":                                  "cat_arts",
    "Basic Needs, Human Services & Families":                 "cat_basic_needs",
    "Economic Empowerment & Community Development & Housing": "cat_economic",
    "Education & Skill Development":                          "cat_education",
    "Environment, Climate & Animals":                         "cat_environment",
    "Equity, Justice & Civic Life":                           "cat_equity",
    "Faith & Interfaith Initiatives":                         "cat_faith",
    "International Aid & Global Development":                 "cat_international",
    "Medical Research, Health & Wellbeing":                   "cat_health",
    "Public Policy, Civic Engagement & Democracy":            "cat_policy",
    "Science, Technology & Innovation for Good":              "cat_science",
}

# ── 44 populations ────────────────────────────────────────────────────────────
POPULATIONS = [
    # Age
    "Adults (25-64)", "Children (4-12)", "Infants & Toddlers (0-3)",
    "Seniors (65+)", "Adolescents & Teens (13-17)", "Young Adults (18-24)",
    # Family
    "Single Mothers", "Single Fathers", "Foster families",
    "Families supporting elder care",
    # Occupations
    "Caregivers", "Educators", "First Responders", "Students",
    "Medical professionals", "Military Families", "Veterans",
    # Socioeconomic
    "Homeless / Housing Insecure", "Immigrants / Newcomers",
    "Low-Income / Working Poor", "People Below Poverty Line",
    "Refugees / Asylum Seekers", "Undocumented / DACA",
    # Vulnerable
    "At-Risk Youth", "Domestic Violence Survivors",
    "Currently Incarcerated", "Formerly Incarcerated",
    "Sexual Assault/Domestic Abuse Survivors", "Human Trafficking Survivors",
    # Health
    "Chronic Illness", "Intellectual/Developmental Disabilities (IDD)",
    "Mental Health Conditions", "Neurodivergent Individuals",
    "Physical Disabilities", "Substance Use Disorders/Addiction",
    # Identity
    "Men & Boys", "Women & Girls", "LGBTQ+ Individuals",
    "Transgender and Non-Binary Individuals",
    # Race / Ethnicity
    "Asian American / Pacific Islander", "Black / African American",
    "Hispanic / Latino", "Middle Eastern / North African (MENA)",
    "Multiracial Communities", "Native American / Indigenous",
    # Community
    "Rural", "Tribal", "Urban",
]

POPULATIONS_SET = set(POPULATIONS)

# ── System prompt ─────────────────────────────────────────────────────────────
SYSTEM_PROMPT = f"""You are classifying a nonprofit charity for a philanthropic donor matching platform.

Given charity information, return a JSON object with exactly two fields:
1. "categories" — ALL applicable categories from the list below (can be multiple)
2. "populations" — ALL populations this charity serves from the list below (can be multiple, use [] if unclear)

CATEGORIES — use exact names only:
{chr(10).join(f'{i+1}. {c}' for i, c in enumerate(CATEGORIES))}

POPULATIONS — use exact names only:
AGE: Adults (25-64), Children (4-12), Infants & Toddlers (0-3), Seniors (65+), Adolescents & Teens (13-17), Young Adults (18-24)
FAMILY: Single Mothers, Single Fathers, Foster families, Families supporting elder care
OCCUPATIONS: Caregivers, Educators, First Responders, Students, Medical professionals, Military Families, Veterans
SOCIOECONOMIC: Homeless / Housing Insecure, Immigrants / Newcomers, Low-Income / Working Poor, People Below Poverty Line, Refugees / Asylum Seekers, Undocumented / DACA
VULNERABLE: At-Risk Youth, Domestic Violence Survivors, Currently Incarcerated, Formerly Incarcerated, Sexual Assault/Domestic Abuse Survivors, Human Trafficking Survivors
HEALTH: Chronic Illness, Intellectual/Developmental Disabilities (IDD), Mental Health Conditions, Neurodivergent Individuals, Physical Disabilities, Substance Use Disorders/Addiction
IDENTITY: Men & Boys, Women & Girls, LGBTQ+ Individuals, Transgender and Non-Binary Individuals
RACE/ETHNICITY: Asian American / Pacific Islander, Black / African American, Hispanic / Latino, Middle Eastern / North African (MENA), Multiracial Communities, Native American / Indigenous
COMMUNITY: Rural, Tribal, Urban

Rules:
- Use EXACT names from the lists — no paraphrasing
- Select ALL that genuinely apply — do not under-select
- Use NTEE code and keywords as strong signals if mission is unclear
- Return empty arrays [] only if truly cannot determine

Return ONLY valid JSON:
{{"categories": ["Education & Skill Development"], "populations": ["Children (4-12)", "Low-Income / Working Poor"]}}"""


# ── Build user message per charity ────────────────────────────────────────────
def build_message(row):
    ein, name, ntee, city, state, mission, irs_mission, programs, keywords, areas, cause_cats, volunteer, impact = row
    mission_text = mission or irs_mission or "Not available"
    parts = [
        f"Organization: {name}",
        f"NTEE Code: {ntee or 'N/A'}",
        f"Location: {(city or '').strip()}, {(state or '').strip()}",
        f"Mission: {mission_text[:500]}",
    ]
    if programs:
        parts.append(f"Programs: {json.dumps(programs)[:400]}")
    if keywords:
        parts.append(f"Keywords: {json.dumps(keywords)[:300]}")
    if areas:
        parts.append(f"Areas Served: {areas[:200]}")
    if cause_cats:
        parts.append(f"Existing Categories (unstructured): {json.dumps(cause_cats)[:200]}")
    if volunteer:
        parts.append(f"Volunteer Info: {volunteer[:200]}")
    if impact:
        parts.append(f"Impact Metrics: {json.dumps(impact)[:200]}")
    return "\n".join(parts)


# ── Rule-based geographic scope ───────────────────────────────────────────────
def derive_geographic_scope(areas_served, city, state):
    if not areas_served:
        return "National"
    t = areas_served.lower()
    if any(w in t for w in ["global", "worldwide", "international", "world", "countries"]):
        return "Global"
    if any(w in t for w in ["national", "nationwide", "across the us", "across the united states", "all 50 states"]):
        return "National"
    if any(w in t for w in ["multi-state", "multistate", "several states", "region",
                             "midwest", "southeast", "northeast", "southwest", "northwest", "mid-atlantic"]):
        return "Regional"
    if "county" in t or "counties" in t:
        return "County"
    if state and state.strip().lower() in t and "national" not in t:
        return "State"
    if city and city.strip().lower() in t:
        return "City"
    if any(w in t for w in ["neighborhood", "zip code", "borough", "local community"]):
        return "Neighborhood"
    return "National"


# ── Status helpers ────────────────────────────────────────────────────────────
def load_status():
    if os.path.exists(BATCH_STATUS):
        with open(BATCH_STATUS) as f:
            return json.load(f)
    return {"batches": [], "total_submitted": 0}

def save_status(s):
    with open(BATCH_STATUS, "w") as f:
        json.dump(s, f, indent=2)

def get_submitted_eins(s):
    eins = set()
    for b in s.get("batches", []):
        eins.update(b.get("eins", []))
    return eins


# ── SUBMIT helpers ────────────────────────────────────────────────────────────
def _fetch_remaining(submitted: set) -> list:
    """Stream unenriched rows from DB using server-side cursor."""
    conn = psycopg2.connect(DB)
    cur  = conn.cursor("fetch_cursor")
    cur.execute("""
        SELECT ein, name, ntee_code, city, state,
               llm_mission, irs_mission, llm_programs, llm_keywords,
               llm_areas_served, llm_cause_categories,
               llm_volunteer_info, llm_impact_metrics
        FROM mvp_charities
        WHERE populations_served IS NULL
          AND geographic_scope IS NULL
    """)
    rows = []
    while True:
        chunk = cur.fetchmany(5000)
        if not chunk:
            break
        rows.extend(r for r in chunk if r[0] not in submitted)
    cur.close(); conn.close()
    return rows


def _submit_chunk(chunk: list, batch_num: int, status: dict) -> bool:
    """Submit one batch to Anthropic. Returns True on success."""
    requests = [{
        "custom_id": row[0],
        "params": {
            "model": HAIKU,
            "max_tokens": 500,
            "system": SYSTEM_PROMPT,
            "messages": [{"role": "user", "content": build_message(row)}],
        }
    } for row in chunk]

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            batch = client.beta.messages.batches.create(requests=requests)
            status["batches"].append({
                "batch_id": batch.id, "batch_num": batch_num,
                "count": len(chunk), "status": batch.processing_status,
                "submitted_at": datetime.now().isoformat(),
                "eins": [r[0] for r in chunk],
            })
            status["total_submitted"] += len(chunk)
            save_status(status)
            print(f"  Submitted: {batch.id}", flush=True)
            return True
        except Exception as e:
            print(f"  Attempt {attempt} failed: {e}", flush=True)
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
    save_status(status)
    return False


# ── SUBMIT ────────────────────────────────────────────────────────────────────
def submit_batches():
    status    = load_status()
    submitted = get_submitted_eins(status)
    remaining = _fetch_remaining(submitted)

    print(f"Records to categorize: {len(remaining):,}", flush=True)
    print(f"Already submitted: {len(submitted):,}", flush=True)

    if not remaining:
        print("All records already submitted!", flush=True)
        return

    for i in range(0, len(remaining), BATCH_SIZE):
        chunk     = remaining[i:i + BATCH_SIZE]
        batch_num = len(status["batches"]) + 1
        print(f"\n--- Batch {batch_num} ({len(chunk):,} records) ---", flush=True)
        if not _submit_chunk(chunk, batch_num, status):
            print("Stopping after failed batch.", flush=True)
            return

    print(f"\nAll batches submitted. Total: {status['total_submitted']:,}", flush=True)


# ── CHECK ─────────────────────────────────────────────────────────────────────
def check_batches():
    status = load_status()
    if not status["batches"]:
        print("No batches submitted yet.", flush=True)
        return False
    all_done = True
    for b in status["batches"]:
        batch = client.beta.messages.batches.retrieve(b["batch_id"])
        b["status"] = batch.processing_status
        if batch.request_counts:
            b["succeeded"] = batch.request_counts.succeeded
            b["errored"]   = batch.request_counts.errored
        print(f"  Batch {b['batch_num']} ({b['batch_id']}): {batch.processing_status} "
              f"(ok={b.get('succeeded',0)}, err={b.get('errored',0)})", flush=True)
        if batch.processing_status != "ended":
            all_done = False
    save_status(status)
    return all_done


# ── DOWNLOAD ──────────────────────────────────────────────────────────────────
def download_results():
    status = load_status()
    # Track already-downloaded batch nums to allow resume
    already_downloaded = {b["batch_num"] for b in status["batches"] if b.get("downloaded")}
    total = sum(b.get("downloaded", 0) for b in status["batches"])

    with open(RESULTS_FILE, "a") as out_f:
        for b in status["batches"]:
            if b["batch_num"] in already_downloaded:
                print(f"  Batch {b['batch_num']} already downloaded, skipping", flush=True)
                continue
            batch = client.beta.messages.batches.retrieve(b["batch_id"])
            if batch.processing_status != "ended":
                print(f"  Batch {b['batch_num']} not done yet, skipping", flush=True)
                continue
            print(f"  Downloading batch {b['batch_num']}...", flush=True)
            count = 0
            for result in client.beta.messages.batches.results(b["batch_id"]):
                out_f.write(json.dumps({
                    "custom_id": result.custom_id,
                    "result": result.result.model_dump()
                }) + "\n")
                count += 1
            total += count
            b["downloaded"] = count
            save_status(status)
            print(f"    {count:,} results", flush=True)
    print(f"\nTotal downloaded: {total:,} → {RESULTS_FILE}", flush=True)


# ── PARSE ─────────────────────────────────────────────────────────────────────
def parse_result(record):
    try:
        text = record["result"]["message"]["content"][0]["text"]
        m = re.search(r'\{[\s\S]*?\}(?=\s*(?:```|$|\*\*))', text)
        if not m:
            m = re.search(r'\{[\s\S]*\}', text)
        return json.loads(m.group()) if m else None
    except Exception:
        return None


# ── LOAD ──────────────────────────────────────────────────────────────────────
LOAD_BATCH = 2000  # rows per execute_batch call

def load_results():
    if not os.path.exists(RESULTS_FILE):
        print("No results file found. Run download first.", flush=True)
        return

    conn = psycopg2.connect(DB)
    cur  = conn.cursor()

    cur.execute("SELECT ein, llm_areas_served, city, state FROM mvp_charities")
    geo_map = {r[0]: (r[1], r[2], r[3]) for r in cur.fetchall()}

    total  = sum(1 for _ in open(RESULTS_FILE))
    loaded = errors = 0
    batch  = []
    print(f"Loading {total:,} results...", flush=True)

    UPDATE_SQL = """
        UPDATE mvp_charities SET
            cat_arts=%s, cat_basic_needs=%s, cat_economic=%s,
            cat_education=%s, cat_environment=%s, cat_equity=%s,
            cat_faith=%s, cat_international=%s, cat_health=%s,
            cat_policy=%s, cat_science=%s,
            populations_served=%s,
            geographic_scope=%s
        WHERE ein=%s
    """

    def flush(b):
        execute_batch(cur, UPDATE_SQL, b, page_size=LOAD_BATCH)
        conn.commit()

    with open(RESULTS_FILE) as f:
        for line in f:
            try:
                record = json.loads(line)
                ein    = record["custom_id"]
                data   = parse_result(record)
                if data is None:
                    errors += 1; continue

                selected = set(data.get("categories", []))
                cats = {col: (name in selected) for name, col in CATEGORY_TO_COL.items()}
                pops = [p for p in data.get("populations", []) if p in POPULATIONS_SET]
                areas, city, state = geo_map.get(ein, (None, None, None))
                geo = derive_geographic_scope(areas, city, state)

                batch.append((
                    cats["cat_arts"], cats["cat_basic_needs"], cats["cat_economic"],
                    cats["cat_education"], cats["cat_environment"], cats["cat_equity"],
                    cats["cat_faith"], cats["cat_international"], cats["cat_health"],
                    cats["cat_policy"], cats["cat_science"],
                    json.dumps(pops) if pops else None,
                    geo, ein,
                ))
                loaded += 1

            except Exception:
                errors += 1

            if len(batch) >= LOAD_BATCH:
                flush(batch)
                batch = []
                print(f"  {loaded+errors:,}/{total:,} — Loaded: {loaded:,} | Errors: {errors:,}", flush=True)

    if batch:
        flush(batch)

    cur.close(); conn.close()
    print(f"\nLOAD COMPLETE: {loaded:,} loaded, {errors:,} errors", flush=True)


# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    print("=" * 65, flush=True)
    print("POWERDONOR.AI — Category & Population Classifier", flush=True)
    print("=" * 65, flush=True)

    cmd = sys.argv[1] if len(sys.argv) > 1 else "help"
    if cmd == "submit":
        submit_batches()
        print("\nNext steps:", flush=True)
        print("  python pipeline_categorize.py check_loop", flush=True)
        print("  python pipeline_categorize.py download", flush=True)
        print("  python pipeline_categorize.py load", flush=True)
    elif cmd == "status":
        check_batches()
    elif cmd == "check_loop":
        print("Polling every 5 min until all batches complete...", flush=True)
        while True:
            if check_batches():
                print("\nAll batches done!", flush=True)
                print("  Run: python pipeline_categorize.py download", flush=True)
                break
            print(f"  Next check in 5 min ({datetime.now().strftime('%H:%M:%S')})...", flush=True)
            time.sleep(300)
    elif cmd == "download":
        download_results()
    elif cmd == "load":
        load_results()
    else:
        print("Usage: python pipeline_categorize.py [submit|status|check_loop|download|load]", flush=True)

if __name__ == "__main__":
    main()
