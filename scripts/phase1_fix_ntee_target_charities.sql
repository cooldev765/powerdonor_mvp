-- ================================================================
-- PowerDonor.ai — Phase 1 Fix: NTEE Codes in target_charities
-- Run as: postgres (superuser)
-- Note: Same logic as phase1_fix_ntee_codes.sql applied to
--       target_charities table (source list for url_discovery_queue)
-- ================================================================

-- ----------------------------------------------------------------
-- STEP 1: Preview — confirm counts before changing anything
-- ----------------------------------------------------------------

SELECT
  COUNT(*) FILTER (WHERE ntee_code ~ '^[A-Z][0-9]{2}Z$')          AS trailing_z,
  COUNT(*) FILTER (WHERE ntee_code ~ '^[A-Z][0-9]{3}$')           AS extra_digit,
  COUNT(*) FILTER (WHERE ntee_code ~ '^[A-Z][0-9]{2}[A-Z]$')      AS trailing_letter,
  COUNT(*) FILTER (WHERE ntee_code ~ '^[A-Z][0-9][A-Z].$')        AS mid_letter,
  COUNT(*) FILTER (WHERE LENGTH(ntee_code) = 2)                    AS two_char,
  COUNT(*) FILTER (WHERE LEFT(ntee_code, 1) ~ '[^A-Z]')           AS numeric_prefix
FROM target_charities
WHERE ntee_code IS NOT NULL AND LENGTH(ntee_code) != 3;

-- ----------------------------------------------------------------
-- STEP 2: Fix trailing Z (e.g. X20Z → X20)
-- ----------------------------------------------------------------

UPDATE target_charities
SET ntee_code = LEFT(ntee_code, 3)
WHERE ntee_code ~ '^[A-Z][0-9]{2}Z$'
  AND LEFT(ntee_code, 1) IN (SELECT code FROM ntee_codes);

-- ----------------------------------------------------------------
-- STEP 3: Fix extra digit (e.g. B112 → B11)
-- ----------------------------------------------------------------

UPDATE target_charities
SET ntee_code = LEFT(ntee_code, 3)
WHERE ntee_code ~ '^[A-Z][0-9]{3}$'
  AND LEFT(ntee_code, 1) IN (SELECT code FROM ntee_codes);

-- ----------------------------------------------------------------
-- STEP 4: Fix trailing letter (e.g. T20J → T20)
-- ----------------------------------------------------------------

UPDATE target_charities
SET ntee_code = LEFT(ntee_code, 3)
WHERE ntee_code ~ '^[A-Z][0-9]{2}[A-Z]$'
  AND LEFT(ntee_code, 1) IN (SELECT code FROM ntee_codes);

-- ----------------------------------------------------------------
-- STEP 5: NULL out unrecoverable codes
-- ----------------------------------------------------------------

UPDATE target_charities
SET ntee_code = NULL
WHERE ntee_code IS NOT NULL
  AND (
    ntee_code ~ '^[A-Z][0-9][A-Z].$'        -- mid letter (A6BZ etc)
    OR LENGTH(ntee_code) = 2                  -- two char (X2 etc)
    OR LEFT(ntee_code, 1) ~ '[^A-Z]'         -- numeric prefix (052 etc)
    OR LENGTH(ntee_code) != 3                 -- anything else remaining
  );

-- ----------------------------------------------------------------
-- STEP 6: Verify — should return 0 rows
-- ----------------------------------------------------------------

SELECT ntee_code, LENGTH(ntee_code), COUNT(*)
FROM target_charities
WHERE ntee_code IS NOT NULL
  AND LENGTH(ntee_code) != 3
GROUP BY ntee_code, LENGTH(ntee_code)
ORDER BY COUNT(*) DESC;

-- ----------------------------------------------------------------
-- STEP 7: Final distribution check
-- ----------------------------------------------------------------

SELECT
  LEFT(ntee_code, 1) AS category,
  COUNT(*) AS count
FROM target_charities
WHERE ntee_code IS NOT NULL
GROUP BY category
ORDER BY count DESC;
