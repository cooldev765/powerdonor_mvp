"""
PowerDonor.AI — Subcategory Classifier
=======================================
Hybrid NTEE + keyword approach. Zero LLM calls.

For each org:
  1. Map NTEE code prefix → subcategories (high confidence)
  2. Match llm_keywords against trigger word lists → subcategories (medium confidence)
  3. Store union of both signals in llm_subcategories JSONB array

Usage:
  python pipeline_subcategory.py run      # classify all unclassified orgs
  python pipeline_subcategory.py rerun    # overwrite all (re-run from scratch)
  python pipeline_subcategory.py quality  # coverage + top subcategory report
"""
import json
import os
import sys
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv

load_dotenv()

DB         = os.environ["DATABASE_URL"]
BATCH_SIZE = 5000

# ── Load taxonomy ──────────────────────────────────────────────────────────────
TAXONOMY_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "backend", "taxonomy.json"
)
with open(TAXONOMY_PATH) as f:
    TAXONOMY = json.load(f)

ALL_SUBCATEGORIES = {s for subs in TAXONOMY.values() for s in subs}

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
COL_TO_CATEGORY = {v: k for k, v in CATEGORY_TO_COL.items()}
CAT_COLS        = list(CATEGORY_TO_COL.values())


# ── NTEE prefix → subcategories ───────────────────────────────────────────────
# Key = first two characters of ntee_code (letter + digit)
NTEE_MAP = {
    # Arts (A)
    "A1": ["Arts & Social Justice"],
    "A2": ["Visual & Community Arts"],
    "A3": ["Cultural Preservation & Heritage"],
    "A5": ["Performing Arts (Theater & Music)"],
    "A6": ["Museums & Exhibitions"],
    "A8": ["Performing Arts (Theater & Music)"],

    # Education (B)
    "B0": ["K-12 & General Education"],
    "B1": ["K-12 & General Education"],
    "B2": ["K-12 & General Education"],
    "B3": ["Workforce & Professional Development"],
    "B4": ["Scholarships & Education Funding"],
    "B5": ["Scholarships & Education Funding"],
    "B6": ["Community Learning & Youth Development"],
    "B7": ["Community Learning & Youth Development"],
    "B8": ["Community Learning & Youth Development"],
    "B9": ["Workforce & Professional Development"],

    # Environment (C)
    "C1": ["Environmental Justice & Advocacy"],
    "C2": ["Environmental Justice & Advocacy"],
    "C3": ["Environmental Justice & Advocacy"],
    "C4": ["Land, Water & Ocean Conservation"],
    "C5": ["Land, Water & Ocean Conservation"],
    "C6": ["Climate & Clean Energy"],

    # Animals (D)
    "D2": ["Animal Welfare & Rescue"],
    "D3": ["Wildlife & Habitat Conservation"],
    "D4": ["Animal Welfare & Rescue"],
    "D5": ["Wildlife & Habitat Conservation"],

    # Health (E)
    "E1": ["Community & Public Health"],
    "E2": ["Hospitals & Primary Care"],
    "E3": ["Medical & Biomedical Research"],
    "E4": ["Hospitals & Primary Care"],
    "E5": ["Community & Public Health"],
    "E6": ["Mental Health & Counseling"],
    "E9": ["Community & Public Health"],

    # Mental Health (F)
    "F2": ["Mental Health & Counseling"],
    "F3": ["Mental Health & Counseling"],
    "F4": ["Substance Abuse & Recovery"],

    # Disease / Medical Research (G)
    "G1": ["Medical & Biomedical Research"],
    "G2": ["Medical & Biomedical Research"],
    "G3": ["Medical & Biomedical Research"],
    "G4": ["Medical & Biomedical Research"],
    "G5": ["Medical & Biomedical Research"],
    "G9": ["Medical & Biomedical Research"],

    # Medical Research (H)
    "H2": ["Medical & Biomedical Research"],
    "H3": ["Medical & Biomedical Research"],

    # Crime / Legal (I)
    "I2": ["Criminal Justice & Legal Aid"],
    "I3": ["Criminal Justice & Legal Aid"],

    # Employment (J)
    "J2": ["Workforce Development & Job Training"],
    "J3": ["Workforce Development & Job Training"],

    # Food / Agriculture (K)
    "K2": ["Sustainable Agriculture & Food Systems"],
    "K3": ["Food Security & Hunger Relief"],
    "K4": ["Sustainable Agriculture & Food Systems"],

    # Housing (L)
    "L2": ["Affordable Housing & Neighborhood Revitalization"],
    "L3": ["Affordable Housing & Neighborhood Revitalization"],
    "L4": ["Housing & Homelessness Services"],

    # Public Safety (M)
    "M2": ["Public Safety & Emergency Services"],
    "M3": ["Emergency Response & Disaster Relief"],

    # Recreation / Sports (N)
    "N5": ["Community Learning & Youth Development"],
    "N6": ["Community Learning & Youth Development"],

    # Youth Development (O)
    "O2": ["Community Learning & Youth Development"],
    "O5": ["Community Learning & Youth Development"],

    # Human Services (P)
    "P2": ["Family, Child & Foster Care Services"],
    "P3": ["Housing & Homelessness Services"],
    "P4": ["Family, Child & Foster Care Services"],
    "P5": ["Senior & Disability Services"],
    "P6": ["Disability & Rehabilitation Services"],
    "P7": ["Emergency Response & Disaster Relief"],
    "P8": ["Senior & Disability Services"],
    "P9": ["Mental Health & Behavioral Health"],

    # International (Q)
    "Q1": ["Peacebuilding, Human Rights & Refugees"],
    "Q2": ["Humanitarian Relief & Disaster Response"],
    "Q3": ["International Education & Poverty Alleviation"],
    "Q4": ["Clean Water, Food Security & Sustainable Development"],
    "Q5": ["Global Health & Medical Missions"],

    # Civil Rights (R)
    "R2": ["Racial Justice & Civil Rights"],
    "R3": ["Gender Justice & Domestic Violence"],
    "R4": ["LGBTQ+ Rights & Advocacy"],
    "R5": ["Criminal Justice & Legal Aid"],
    "R6": ["Civic Engagement & Community Organizing"],

    # Community (S)
    "S2": ["Community & Economic Development"],
    "S3": ["Community & Economic Development"],
    "S4": ["Business & Trade Advocacy"],
    "S5": ["Financial Literacy & Economic Mobility"],

    # Science / Tech (U)
    "U0": ["STEM Education & Youth Programs", "Technology Access & Digital Equity"],
    "U2": ["AI, Data & Emerging Technology"],
    "U3": ["Environmental & Conservation Science"],
    "U4": ["Environmental & Conservation Science"],
    "U5": ["Technology Access & Digital Equity"],

    # Public Benefit (W)
    "W2": ["Government Accountability & Transparency"],
    "W3": ["Policy Advocacy & Legislation"],
    "W4": ["Civic Engagement & Voting Rights"],

    # Religion (X)
    # X20s = Christian, X30 = Jewish, X40 = Islamic, X50 = Buddhist,
    # X60 = Hindu, X70 = Other Eastern/Sikh, X80 = Religious Media, X90 = Interfaith
    "X1": ["Faith-Driven Social Services"],
    "X2": ["Christian Outreach & Evangelism"],
    "X3": ["Jewish Community & Organizations"],
    "X4": ["Islamic & Interfaith Initiatives"],
    "X5": ["Islamic & Interfaith Initiatives"],
    "X6": ["Faith-Driven Social Services"],   # Hindu, Buddhist
    "X7": ["Faith-Driven Social Services"],   # Sikh, other eastern
    "X8": ["Faith-Driven Social Services"],   # Religious media
    "X9": ["Faith-Driven Social Services"],   # Interfaith / other
}


# ── Keyword trigger map ────────────────────────────────────────────────────────
# Values are substrings matched case-insensitively against each llm_keyword item.
# Scoped to relevant categories during classification — no cross-category pollution.
KEYWORD_MAP = {
    # ── Arts ──────────────────────────────────────────────────────────────────
    "Performing Arts (Theater & Music)": [
        "performing arts", "theater", "theatre", "orchestra", "symphony",
        "opera", "ballet", "dance performance", "music performance", "concert",
        "choir", "broadway", "stage production", "jazz", "classical music",
        "documentary film", "film preservation", "independent film",
        "film seminar", "cinematic", "non-fiction cinema",
    ],
    "Arts Education": [
        "arts education", "art education", "music education", "dance education",
        "arts program", "creative learning", "art classes", "arts curriculum",
    ],
    "Visual & Community Arts": [
        "visual arts", "fine arts", "painting", "sculpture", "photography",
        "community art", "public art", "mural", "printmaking", "illustration",
    ],
    "Cultural Preservation & Heritage": [
        "cultural preservation", "heritage", "cultural heritage",
        "local history", "historical society", "indigenous culture",
        "cultural identity", "folklore", "oral history",
    ],
    "Museums & Exhibitions": [
        "museum", "exhibition", "exhibit", "collections", "curator",
        "children's museum", "natural history", "interactive exhibits",
        "history museum",
    ],
    "Arts & Social Justice": [
        "arts advocacy", "art activism", "social justice", "marginalized voices",
        "community storytelling", "arts access", "inclusive arts",
    ],

    # ── Basic Needs ───────────────────────────────────────────────────────────
    "Food Security & Hunger Relief": [
        "food pantry", "food bank", "hunger relief", "hunger",
        "food assistance", "feeding", "soup kitchen", "food insecurity",
        "food distribution", "nutrition assistance", "meals program",
    ],
    "Housing & Homelessness Services": [
        "homeless shelter", "homelessness", "housing assistance",
        "transitional housing", "shelter", "housing insecurity",
        "eviction prevention", "rapid rehousing", "street outreach",
    ],
    "Family, Child & Foster Care Services": [
        "foster care", "child welfare", "child advocacy", "family services",
        "child abuse", "child protection", "foster youth", "adoption",
        "parenting support", "family preservation", "casa volunteers",
    ],
    "Mental Health & Behavioral Health": [
        "mental health", "behavioral health", "emotional support",
        "mental wellness", "crisis support", "psychological services",
    ],
    "Emergency Response & Disaster Relief": [
        "disaster relief", "emergency response", "disaster recovery",
        "emergency assistance", "natural disaster", "flood relief",
        "hurricane relief",
    ],
    "Senior & Disability Services": [
        "senior services", "elderly", "aging", "senior care",
        "disability services", "elder care", "older adults", "senior center",
        "assisted living",
    ],

    # ── Economic ──────────────────────────────────────────────────────────────
    "Affordable Housing & Neighborhood Revitalization": [
        "affordable housing", "neighborhood revitalization",
        "low-income housing", "housing development", "urban renewal",
        "community land trust",
    ],
    "Workforce Development & Job Training": [
        "workforce development", "job training", "employment training",
        "job skills", "career development", "vocational training",
        "apprenticeship", "skilled trades", "job placement",
    ],
    "Small Business & Entrepreneurship": [
        "small business", "entrepreneurship", "entrepreneur", "startup",
        "business development", "microenterprise", "business incubator",
        "self-employment",
    ],
    "Community & Economic Development": [
        "economic development", "community development",
        "community empowerment", "local economy", "community revitalization",
        "neighborhood improvement",
    ],
    "Financial Literacy & Economic Mobility": [
        "financial literacy", "financial education", "economic mobility",
        "financial empowerment", "asset building", "savings program",
        "credit building",
    ],
    "Labor Rights & Worker Organizations": [
        "labor union", "union", "collective bargaining", "worker rights",
        "labor rights", "labor movement", "trade union", "workers' rights",
        "labor enforcement", "worker misclassification", "wage theft",
    ],

    # ── Education ─────────────────────────────────────────────────────────────
    "K-12 & General Education": [
        "k-12", "elementary school", "middle school", "high school",
        "primary school", "secondary school", "classroom", "teacher",
        "student achievement", "public school", "charter school",
        "k12",
    ],
    "Early Childhood Education": [
        "early childhood", "preschool", "pre-k", "kindergarten",
        "head start", "early learning", "toddler", "child care", "vpk",
    ],
    "Scholarships & Education Funding": [
        "scholarship", "scholarships", "education funding", "financial aid",
        "tuition assistance", "fellowship", "bursary", "grants for students",
        "higher education funding",
    ],
    "Workforce & Professional Development": [
        "professional development", "career training", "continuing education",
        "adult education", "skills training", "professional skills",
        "job readiness", "upskilling",
    ],
    "Community Learning & Youth Development": [
        "youth development", "after school", "out of school", "tutoring",
        "mentoring", "youth program", "summer program", "enrichment",
        "youth enrichment", "summer camp",
    ],

    # ── Environment ───────────────────────────────────────────────────────────
    "Animal Welfare & Rescue": [
        "animal shelter", "animal rescue", "animal welfare", "pet adoption",
        "spay", "neuter", "humane society", "animal protection",
        "stray animals", "foster animals",
    ],
    "Wildlife & Habitat Conservation": [
        "wildlife", "habitat conservation", "wildlife conservation",
        "endangered species", "wildlife rescue", "habitat restoration",
        "biodiversity", "wildlife sanctuary",
    ],
    "Climate & Clean Energy": [
        "climate change", "clean energy", "renewable energy", "solar",
        "wind energy", "carbon", "greenhouse gas", "climate action",
        "climate justice", "clean power",
    ],
    "Environmental Justice & Advocacy": [
        "environmental justice", "environmental advocacy", "pollution",
        "environmental health", "clean air", "toxic", "environmental policy",
        "environmental racism",
    ],
    "Land, Water & Ocean Conservation": [
        "land conservation", "water quality", "ocean conservation", "wetlands",
        "watershed", "marine", "rivers", "forest conservation", "parks",
        "water conservation",
    ],
    "Sustainable Agriculture & Food Systems": [
        "sustainable agriculture", "organic farming", "food systems",
        "urban farming", "local food", "community garden", "farm to table",
        "regenerative agriculture", "urban agriculture",
    ],

    # ── Equity ────────────────────────────────────────────────────────────────
    "Racial Justice & Civil Rights": [
        "racial justice", "civil rights", "racial equity", "anti-racism",
        "racial equality", "systemic racism", "racial discrimination",
        "black community",
    ],
    "Gender Justice & Domestic Violence": [
        "domestic violence", "gender justice", "sexual assault", "gender equity",
        "women's rights", "gender-based violence", "intimate partner violence",
        "dv services",
    ],
    "Human Trafficking & Exploitation": [
        "human trafficking", "trafficking", "sex trafficking", "labor trafficking",
        "anti-trafficking", "survivor support", "exploitation",
    ],
    "LGBTQ+ Rights & Advocacy": [
        "lgbtq", "lgbt", "gay", "lesbian", "transgender", "queer",
        "gender identity", "sexual orientation", "lgbtq+ rights",
    ],
    "Criminal Justice & Legal Aid": [
        "criminal justice", "legal aid", "reentry", "incarceration",
        "prison reform", "public defender", "legal services", "justice reform",
        "formerly incarcerated",
    ],
    "Civic Engagement & Community Organizing": [
        "civic engagement", "community organizing", "voter registration",
        "grassroots", "civic participation", "community advocacy",
    ],

    # ── Faith ─────────────────────────────────────────────────────────────────
    "Christian Outreach & Evangelism": [
        "christian evangelism", "evangelism", "gospel", "church planting",
        "missionary support", "discipleship", "jesus christ",
        "christian outreach", "faith-based outreach",
    ],
    "Faith-Based Education & Schools": [
        "christian school", "faith-based school", "christian education",
        "religious education", "parochial", "faith-based education",
        "christian principles",
    ],
    "Youth Ministry & Spiritual Formation": [
        "youth ministry", "spiritual formation", "youth discipleship",
        "sunday school", "vacation bible", "youth group", "campus ministry",
        "men's ministry", "women's ministry",
    ],
    "Jewish Community & Organizations": [
        "jewish", "judaism", "synagogue", "jewish community",
        "jewish education", "jewish culture", "holocaust",
    ],
    "Islamic & Interfaith Initiatives": [
        "islamic", "muslim", "mosque", "interfaith", "interfaith dialogue",
        "interfaith cooperation", "multi-faith",
    ],
    "Faith-Driven Social Services": [
        "faith-based", "faith based", "faith community",
        "faith-driven", "congregation", "christian social services",
    ],

    # ── International ─────────────────────────────────────────────────────────
    "Humanitarian Relief & Disaster Response": [
        "humanitarian aid", "humanitarian relief", "disaster response",
        "emergency relief", "crisis response", "refugee relief",
    ],
    "Global Health & Medical Missions": [
        "global health", "medical missions", "international health",
        "health missions", "global medicine", "overseas medical",
    ],
    "Children, Orphan Care & Family Support": [
        "orphan", "orphanage", "orphan care", "child sponsorship",
        "street children", "vulnerable children", "children international",
    ],
    "Clean Water, Food Security & Sustainable Development": [
        "clean water", "water access", "water sanitation", "water wells",
        "sanitation", "sustainable development",
    ],
    "International Education & Poverty Alleviation": [
        "international education", "poverty alleviation", "global education",
        "microfinance", "poverty reduction", "economic development international",
    ],
    "Peacebuilding, Human Rights & Refugees": [
        "peacebuilding", "human rights", "refugees", "asylum",
        "conflict resolution", "displaced persons", "refugee services",
    ],

    # ── Health ────────────────────────────────────────────────────────────────
    "Mental Health & Counseling": [
        "mental health counseling", "therapy services", "counseling services",
        "psychological", "behavioral therapy", "trauma informed", "ptsd",
    ],
    "Substance Abuse & Recovery": [
        "substance abuse", "addiction recovery", "drug treatment",
        "alcohol recovery", "sobriety", "rehab", "recovery support",
        "addiction treatment",
    ],
    "Hospitals & Primary Care": [
        "hospital", "primary care", "clinic", "medical center",
        "patient care", "urgent care", "free clinic", "healthcare services",
    ],
    "Senior Care & Long-Term Care": [
        "long-term care", "nursing home", "memory care", "hospice",
        "aging in place", "senior living",
    ],
    "Disability & Rehabilitation Services": [
        "disability", "rehabilitation", "adaptive", "special needs",
        "accessibility", "developmental disability", "developmental disabilities",
        "physical therapy", "occupational therapy", "disability support",
    ],
    "Community & Public Health": [
        "public health", "community health", "health education",
        "health promotion", "preventive care", "health equity",
        "community wellness", "population health",
    ],

    # ── Policy ────────────────────────────────────────────────────────────────
    "Public Safety & Emergency Services": [
        "public safety", "fire department", "police", "first responders",
        "law enforcement", "emergency services", "ambulance", "ems",
        "emergency medical", "paramedic", "fire rescue",
    ],
    "Civic Engagement & Voting Rights": [
        "voting rights", "voter education", "get out the vote",
        "election", "democracy", "voter access", "civic education",
    ],
    "Policy Advocacy & Legislation": [
        "policy advocacy", "legislation", "lobbying", "policy reform",
        "public policy", "legislative advocacy",
    ],
    "Social Justice & Community Organizing": [
        "social justice", "community organizing", "grassroots organizing",
        "social change", "equity advocacy",
    ],
    "Government Accountability & Transparency": [
        "government accountability", "transparency", "watchdog",
        "government reform", "open government", "public accountability",
    ],
    "Business & Trade Advocacy": [
        "chamber of commerce", "business advocacy", "trade association",
        "farm bureau", "agriculture advocacy", "industry advocacy",
        "business community",
    ],

    # ── Science ───────────────────────────────────────────────────────────────
    "STEM Education & Youth Programs": [
        "stem", "science education", "math education", "robotics program",
        "coding", "computer science education", "stem program", "science fair",
        "kids enrichment", "youth stem", "maker", "hackathon",
        "science technology", "technology education", "math program",
    ],
    "Medical & Biomedical Research": [
        "medical research", "biomedical", "clinical research", "cancer research",
        "disease research", "scientific research", "laboratory research",
        "pediatric cancer", "brain tumor", "glioma", "clinical trial",
        "drug research", "genomics", "rare disease",
    ],
    "Technology Access & Digital Equity": [
        "digital equity", "technology access", "digital divide",
        "computer access", "internet access", "digital literacy",
    ],
    "Robotics, Engineering & Innovation": [
        "robotics", "engineering", "innovation", "maker", "3d printing",
        "mechanical engineering", "design thinking",
    ],
    "Environmental & Conservation Science": [
        "conservation science", "environmental science", "ecology",
        "marine science", "climate science", "environmental research",
    ],
    "AI, Data & Emerging Technology": [
        "artificial intelligence", "machine learning", "data science",
        "emerging technology", "data analytics", "tech innovation",
    ],
}


# ── Classification logic ───────────────────────────────────────────────────────

def _ntee_subcategories(ntee_code: str | None) -> set[str]:
    if not ntee_code:
        return set()
    prefix = ntee_code[:2].upper()
    return set(NTEE_MAP.get(prefix, []))


def _keyword_subcategories(keywords_raw, active_categories: list[str]) -> set[str]:
    if not keywords_raw:
        return set()

    # keywords_raw is already a list from psycopg2 JSONB parsing
    if isinstance(keywords_raw, str):
        try:
            keywords = json.loads(keywords_raw)
        except Exception:
            return set()
    else:
        keywords = keywords_raw

    keywords_lower = [str(k).lower() for k in keywords]

    matched = set()
    # Only check subcategories that belong to the org's active categories
    for cat in active_categories:
        for subcat in TAXONOMY.get(cat, []):
            triggers = KEYWORD_MAP.get(subcat, [])
            for trigger in triggers:
                if any(trigger in kw for kw in keywords_lower):
                    matched.add(subcat)
                    break  # one trigger match is enough for this subcat

    return matched


def classify_row(row) -> list[str]:
    (ein, ntee_code, keywords_raw, *cat_flags) = row

    active_categories = [
        COL_TO_CATEGORY[col]
        for col, flag in zip(CAT_COLS, cat_flags)
        if flag
    ]

    ntee_subs    = _ntee_subcategories(ntee_code)
    keyword_subs = _keyword_subcategories(keywords_raw, active_categories)

    # Validate — only keep subcategories that belong to the org's active categories
    valid_subcats_for_org = {
        s for cat in active_categories
        for s in TAXONOMY.get(cat, [])
    }

    combined = (ntee_subs | keyword_subs) & valid_subcats_for_org
    return sorted(combined)


# ── DB operations ──────────────────────────────────────────────────────────────

def _fetch_rows(cur, only_unclassified: bool) -> list:
    condition = "llm_subcategories IS NULL AND" if only_unclassified else ""
    cur.execute(f"""
        SELECT ein, ntee_code, llm_keywords,
               {', '.join(CAT_COLS)}
        FROM mvp_charities
        WHERE {condition} (
            cat_arts OR cat_basic_needs OR cat_economic OR cat_education OR
            cat_environment OR cat_equity OR cat_faith OR cat_international OR
            cat_health OR cat_policy OR cat_science
        )
        ORDER BY ein
    """)
    return cur.fetchall()


def run(only_unclassified: bool = True):
    label = "unclassified" if only_unclassified else "all"
    print(f"Fetching {label} orgs...", flush=True)

    conn = psycopg2.connect(DB)
    cur  = conn.cursor()

    rows = _fetch_rows(cur, only_unclassified)
    print(f"Orgs to process: {len(rows):,}", flush=True)

    updates = []
    empty   = 0

    for row in rows:
        ein      = row[0]
        subcats  = classify_row(row)
        updates.append((json.dumps(subcats), ein))
        if not subcats:
            empty += 1

    print(f"Writing {len(updates):,} updates ({empty:,} with no subcategories)...", flush=True)

    execute_batch(
        cur,
        "UPDATE mvp_charities SET llm_subcategories = %s::jsonb WHERE ein = %s",
        updates,
        page_size=BATCH_SIZE,
    )
    conn.commit()
    cur.close()
    conn.close()
    print("Done.", flush=True)


def quality():
    conn = psycopg2.connect(DB)
    cur  = conn.cursor()

    cur.execute("""
        SELECT
            COUNT(*)                                                      AS total,
            COUNT(llm_subcategories)                                      AS has_value,
            COUNT(CASE WHEN llm_subcategories != '[]'::jsonb THEN 1 END) AS non_empty,
            ROUND(AVG(jsonb_array_length(
                CASE WHEN llm_subcategories != '[]'::jsonb
                     THEN llm_subcategories END)), 2)                     AS avg_subcats
        FROM mvp_charities
        WHERE cat_arts OR cat_basic_needs OR cat_economic OR cat_education OR
              cat_environment OR cat_equity OR cat_faith OR cat_international OR
              cat_health OR cat_policy OR cat_science
    """)
    total, has_value, non_empty, avg = cur.fetchone()
    pct = round(100 * non_empty / total) if total else 0

    print(f"\nQuality Report")
    print(f"  Total categorized orgs : {total:>10,}")
    print(f"  Has llm_subcategories  : {has_value:>10,}")
    print(f"  Non-empty              : {non_empty:>10,}  ({pct}%)")
    print(f"  Avg subcats per org    : {avg}")

    cur.execute("""
        SELECT s.value AS subcat, COUNT(*) AS cnt
        FROM mvp_charities,
             jsonb_array_elements_text(llm_subcategories) s(value)
        WHERE llm_subcategories IS NOT NULL
          AND llm_subcategories != '[]'::jsonb
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT 20
    """)
    print(f"\n  Top 20 subcategories:")
    for subcat, cnt in cur.fetchall():
        print(f"    {cnt:>7,}  {subcat}")

    cur.close()
    conn.close()


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    cmd = sys.argv[1] if len(sys.argv) > 1 else "help"

    if cmd == "run":
        run(only_unclassified=True)
        quality()
    elif cmd == "rerun":
        run(only_unclassified=False)
        quality()
    elif cmd == "quality":
        quality()
    else:
        print("Usage: python pipeline_subcategory.py [run|rerun|quality]")


if __name__ == "__main__":
    main()
