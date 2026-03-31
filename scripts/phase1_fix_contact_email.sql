-- ================================================================
-- PowerDonor.ai — Phase 1 Fix 7: Clean llm_contact_email
-- Run as: pd_admin
--
-- Problems addressed:
--   1. Placeholder emails     [email protected], mymail@mailservice.com
--   2. Non-email text         "Available through website contact form", "Email Us"
--   3. Aggregator emails      topgovernmentgrants.com, grantstation.com
--   4. Multiple emails        "email1@org.com, email2@org.com" → keep first only
--   5. JSON stored as text    {"general": "email@org.com"} → extract first value
-- ================================================================

-- ----------------------------------------------------------------
-- STEP 1: Preview
-- ----------------------------------------------------------------

SELECT
  COUNT(*) FILTER (WHERE llm_contact_email IS NOT NULL)            AS total_non_null,
  COUNT(*) FILTER (WHERE llm_contact_email ~ '^[^@\s]+@[^@\s]+\.[^@\s]+$')
                                                                   AS single_valid_email,
  COUNT(*) FILTER (WHERE llm_contact_email ~ ',')                  AS multiple_emails,
  COUNT(*) FILTER (WHERE llm_contact_email ~ '^\{')               AS json_format,
  COUNT(*) FILTER (WHERE llm_contact_email !~ '@'
    AND llm_contact_email IS NOT NULL)                             AS no_at_sign_garbage
FROM charities;

-- ----------------------------------------------------------------
-- STEP 2a: Extract first email from JSON-stored-as-text
-- {"general": "email@org.com", "press": "press@org.com"} → email@org.com
-- ----------------------------------------------------------------

UPDATE charities
SET llm_contact_email = (
  SELECT MIN(val)
  FROM jsonb_each_text(llm_contact_email::jsonb) AS kv(key, val)
  WHERE val ~ '^[^@\s]+@[^@\s]+\.[^@\s]+'
)
WHERE llm_contact_email IS NOT NULL
  AND llm_contact_email ~ '^\{';

-- ----------------------------------------------------------------
-- STEP 2b: Extract first email from comma/space-separated lists
-- "email1@org.com, email2@org.com" → email1@org.com
-- "email1@org.com or email2@org.com" → email1@org.com
-- ----------------------------------------------------------------

UPDATE charities
SET llm_contact_email = TRIM(REGEXP_REPLACE(
  llm_contact_email,
  '\s*(,|\bor\b)\s*.*$',
  '',
  'i'
))
WHERE llm_contact_email IS NOT NULL
  AND (llm_contact_email ~ ',' OR llm_contact_email ~* '\bor\b')
  AND TRIM(REGEXP_REPLACE(llm_contact_email, '\s*(,|\bor\b)\s*.*$', '', 'i'))
      ~ '^[^@\s]+@[^@\s]+\.[^@\s]+$';

-- ----------------------------------------------------------------
-- STEP 2c: NULL out placeholder, fake, and aggregator emails
-- ----------------------------------------------------------------

UPDATE charities
SET llm_contact_email = NULL
WHERE llm_contact_email IS NOT NULL
  AND (
    LOWER(llm_contact_email) IN (
      '[email protected]',
      'mymail@mailservice.com',
      'info@mysite.com',
      'email@example.com',
      'user@example.com',
      'test@test.com',
      'info@website.com',
      'contact@website.com'
    )
    OR llm_contact_email ~* '@(example\.com|mailservice\.com|mysite\.com|test\.com)'
    OR llm_contact_email ~* '@(topgovernmentgrants\.com|grantstation\.com|grantforward\.com)'
  );

-- ----------------------------------------------------------------
-- STEP 2d: NULL out anything remaining that is not a valid email
-- Covers all non-email text: "Available through website contact
-- form", "Email Us", "Click Here", "Not explicitly provided", etc.
-- ----------------------------------------------------------------

UPDATE charities
SET llm_contact_email = NULL
WHERE llm_contact_email IS NOT NULL
  AND llm_contact_email !~ '^[^@\s]+@[^@\s]+\.[^@\s]+$';

-- ----------------------------------------------------------------
-- STEP 3: Verify
-- ----------------------------------------------------------------

SELECT
  COUNT(*) FILTER (WHERE llm_contact_email IS NOT NULL)  AS valid_emails_remaining,
  COUNT(*) FILTER (WHERE llm_contact_email IS NULL
    AND llm_enriched_at IS NOT NULL)                     AS nulled_out,
  COUNT(*) FILTER (WHERE llm_contact_email IS NOT NULL
    AND llm_contact_email !~ '^[^@\s]+@[^@\s]+\.[^@\s]+$')
                                                         AS still_invalid
FROM charities;

-- Spot check top remaining
SELECT llm_contact_email, COUNT(*)
FROM charities
WHERE llm_contact_email IS NOT NULL
GROUP BY llm_contact_email
ORDER BY COUNT(*) DESC
LIMIT 20;
