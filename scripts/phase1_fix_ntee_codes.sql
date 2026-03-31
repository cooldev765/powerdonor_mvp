-- ================================================================
-- PowerDonor.ai — Phase 1 Fix: Malformed NTEE Codes
-- Run as: postgres (superuser)
-- Backlog: reparse from IRS BMF annual update (post-MVP)
-- ================================================================

-- ----------------------------------------------------------------
-- STEP 1: Preview — confirm counts before changing anything
-- ----------------------------------------------------------------

SELECT
  COUNT(*) FILTER (WHERE ntee_code ~ '^[A-Z][0-9]{2}Z$')          AS trailing_z,
  COUNT(*) FILTER (WHERE ntee_code ~ '^[A-Z][0-9]{3}$')           AS extra_digit,
  COUNT(*) FILTER (WHERE LENGTH(ntee_code) = 2)                    AS two_char_null_out,
  COUNT(*) FILTER (WHERE LENGTH(ntee_code) NOT IN (2, 3, 4))       AS other_unexpected
FROM charities
WHERE ntee_code IS NOT NULL AND LENGTH(ntee_code) != 3;


-- ----------------------------------------------------------------
-- STEP 2: Fix trailing Z (e.g. X20Z → X20)
-- Only keep if result is valid format AND letter exists in ntee_codes
-- ----------------------------------------------------------------

UPDATE charities
SET ntee_code = LEFT(ntee_code, 3)
WHERE ntee_code ~ '^[A-Z][0-9]{2}Z$'
  AND LEFT(ntee_code, 1) IN (SELECT code FROM ntee_codes);

-- ----------------------------------------------------------------
-- STEP 3: Fix extra digit (e.g. B112 → B11)
-- Only keep if result is valid format AND letter exists in ntee_codes
-- ----------------------------------------------------------------

UPDATE charities
SET ntee_code = LEFT(ntee_code, 3)
WHERE ntee_code ~ '^[A-Z][0-9]{3}$'
  AND LEFT(ntee_code, 1) IN (SELECT code FROM ntee_codes);

-- ----------------------------------------------------------------
-- STEP 4: NULL out 2-char codes (e.g. X2, T2) — unrecoverable
-- ----------------------------------------------------------------

UPDATE charities
SET ntee_code = NULL
WHERE ntee_code IS NOT NULL
  AND LENGTH(ntee_code) = 2;

-- ----------------------------------------------------------------
-- STEP 5: Verify — should return 0 rows if all fixed
-- ----------------------------------------------------------------

SELECT ntee_code, LENGTH(ntee_code), COUNT(*)
FROM charities
WHERE ntee_code IS NOT NULL
  AND LENGTH(ntee_code) != 3
GROUP BY ntee_code, LENGTH(ntee_code)
ORDER BY COUNT(*) DESC
LIMIT 20;

-- Fix: trailing letter (T20J → T20, E22I → E22)
UPDATE charities
SET ntee_code = LEFT(ntee_code, 3)
WHERE ntee_code ~ '^[A-Z][0-9]{2}[A-Z]$'
  AND LEFT(ntee_code, 1) IN (SELECT code FROM ntee_codes);

-- NULL out: letter+digit+letter+anything (A6BZ, A6E0) — unrecoverable
UPDATE charities
SET ntee_code = NULL
WHERE ntee_code ~ '^[A-Z][0-9][A-Z].$';

-- Verify again
SELECT ntee_code, LENGTH(ntee_code), COUNT(*)
FROM charities
WHERE ntee_code IS NOT NULL
  AND LENGTH(ntee_code) != 3
GROUP BY ntee_code, LENGTH(ntee_code)
ORDER BY COUNT(*) DESC
LIMIT 20;

-- Fix lowercase (c030 → C03, c300 → C30)
UPDATE charities
SET ntee_code = LEFT(UPPER(ntee_code), 3)
WHERE ntee_code ~ '^[a-z][0-9]{3}$'
  AND UPPER(LEFT(ntee_code, 1)) IN (SELECT code FROM ntee_codes);

-- NULL out the genuinely garbage ones
UPDATE charities
SET ntee_code = NULL
WHERE ntee_code IS NOT NULL
  AND LENGTH(ntee_code) != 3;

-- Final verify — should return 0 rows
SELECT ntee_code, LENGTH(ntee_code), COUNT(*)
FROM charities
WHERE ntee_code IS NOT NULL
  AND LENGTH(ntee_code) != 3
GROUP BY ntee_code, LENGTH(ntee_code)
ORDER BY COUNT(*) DESC;

-- Preview
SELECT ntee_code, COUNT(*) 
FROM charities 
WHERE LEFT(ntee_code, 1) ~ '[^A-Z]'
  AND ntee_code IS NOT NULL
GROUP BY ntee_code
ORDER BY COUNT(*) DESC;

UPDATE charities
SET ntee_code = NULL
WHERE LEFT(ntee_code, 1) ~ '[^A-Z]'
  AND ntee_code IS NOT NULL;


-- FINAL verify on ntee_code
SELECT 
  LEFT(ntee_code, 1) AS category,
  COUNT(*) AS count
FROM charities
WHERE ntee_code IS NOT NULL
GROUP BY category
ORDER BY count DESC;

