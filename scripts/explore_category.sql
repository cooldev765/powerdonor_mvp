-- ─────────────────────────────────────────────────────────────────────────────
-- explore_category.sql
-- Explore any cat_* column to inform subcategory decisions.
--
-- Replace the category variable below with any of:
--   cat_arts | cat_basic_needs | cat_economic | cat_education
--   cat_environment | cat_equity | cat_faith | cat_health
--   cat_international | cat_policy | cat_science
--
-- Usage (psql): \set cat cat_education
--               \i explore_category.sql
-- ─────────────────────────────────────────────────────────────────────────────

-- Set your target category here:
\set cat cat_education

-- 1. Random sample of 30 orgs — read missions and keywords to spot patterns
SELECT name, ntee_code, llm_mission, llm_keywords
FROM mvp_charities
WHERE :cat = true
  AND llm_mission IS NOT NULL
ORDER BY RANDOM()
LIMIT 30;

-- 2. Top 40 keywords — fastest signal for subcategory clusters
SELECT kw, COUNT(*) cnt
FROM mvp_charities,
     jsonb_array_elements_text(llm_keywords) kw
WHERE :cat = true
GROUP BY 1
ORDER BY 2 DESC
LIMIT 40;

-- 3. NTEE code prefix distribution — cross-reference with NTEE taxonomy
SELECT LEFT(ntee_code, 2) ntee_prefix, COUNT(*)
FROM mvp_charities
WHERE :cat = true
  AND ntee_code IS NOT NULL
GROUP BY 1
ORDER BY 2 DESC
LIMIT 20;
