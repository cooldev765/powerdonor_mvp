-- ================================================================
-- PowerDonor.ai — Phase 1 Fix 2: Clean & Flag Charity URLs
-- Run as: postgres (superuser)
-- Covers:
--   1. Fix malformed URLs (missing http prefix, uppercase)
--   2. NULL out garbage values (N A, SEE SCHEDULE O, etc.)
--   3. Flag aggregator domains with is_aggregator_url boolean
-- ================================================================

-- ----------------------------------------------------------------
-- STEP 1: Add is_aggregator_url column if not exists
-- ----------------------------------------------------------------

ALTER TABLE charities
  ADD COLUMN IF NOT EXISTS is_aggregator_url BOOLEAN DEFAULT FALSE;

-- ----------------------------------------------------------------
-- STEP 2: Preview malformed URLs (missing http prefix)
-- ----------------------------------------------------------------

SELECT COUNT(*)
FROM charities
WHERE irs_website IS NOT NULL
  AND irs_website !~ 'https?://'
  AND irs_website !~ '^(N A|n a|NA|SEE SCHEDULE|NONE|N/A)';

-- ----------------------------------------------------------------
-- STEP 3: Fix malformed URLs — add https:// prefix and lowercase
-- Handles 3 patterns:
--   a) WWW.ELKS.ORG       → https://www.elks.org  (uppercase WWW)
--   b) www.legatus.org    → https://www.legatus.org (lowercase www)
--   c) VOASE.ORG          → https://voase.org      (bare domain, no www)
-- ----------------------------------------------------------------

-- 3a: Uppercase WWW.
UPDATE charities
SET irs_website = 'https://' || LOWER(irs_website)
WHERE irs_website IS NOT NULL
  AND irs_website !~ 'https?://'
  AND irs_website ~* '^WWW\.'
  AND irs_website !~ '^\s*(N ?/?A|NONE|NULL|NO WEBSITE|SEE SCHEDULE|-)';

-- 3b: Lowercase www. (already has correct case, just add prefix)
UPDATE charities
SET irs_website = 'https://' || irs_website
WHERE irs_website IS NOT NULL
  AND irs_website !~ 'https?://'
  AND irs_website ~ '^www\.'
  AND irs_website !~ '^\s*(N ?/?A|NONE|NULL|NO WEBSITE|SEE SCHEDULE|-)';

-- 3c: Bare domain (no www, looks like a real domain e.g. VOASE.ORG)
UPDATE charities
SET irs_website = 'https://' || LOWER(irs_website)
WHERE irs_website IS NOT NULL
  AND irs_website !~ 'https?://'
  AND irs_website ~ '\.(org|com|net|edu|gov|us|info|co)$'
  AND irs_website !~* '^\s*(N ?/?A|NONE|NULL|NO WEBSITE|SEE SCHEDULE|-)'
  AND irs_website !~ '\s';  -- no spaces (rules out "None at this time." etc)

-- ----------------------------------------------------------------
-- STEP 4: NULL out garbage values
-- ----------------------------------------------------------------

UPDATE charities
SET irs_website = NULL
WHERE irs_website IS NOT NULL
  AND (
    TRIM(UPPER(irs_website)) IN ('N A', 'NA', 'N/A', 'NONE', 'NULL', 'NO WEBSITE', '-', '.', 'SEE SCHEDULE O', 'SEE SCHEDULE')
    OR irs_website !~ 'https?://'  -- anything still missing a protocol after step 3
  );

-- ----------------------------------------------------------------
-- STEP 5: Flag known aggregator domains
-- ----------------------------------------------------------------

UPDATE charities
SET is_aggregator_url = TRUE
WHERE irs_website IS NOT NULL
  AND SUBSTRING(irs_website FROM 'https?://(?:www\.)?([^/]+)') IN (
    -- Business/org directories
    'bizapedia.com', 'bizprofile.net', 'allbiz.com', 'bisprofiles.com',
    'opengovus.com', 'opengovny.com', 'opengovwa.com', 'opengovco.com',
    'orgcouncil.com', 'findglocal.com', 'countyoffice.org',
    'georgiacompanyregistry.com', 'manta.com', 'chamberofcommerce.com',
    'zoominfo.com', 'dnb.com', 'buzzfile.com', 'opencorporates.com',
    -- Nonprofit-specific directories
    'grantable.co', 'grantmakers.io', 'grantbay.org', 'grantedai.com',
    'charityscoop.info', 'charitopedia.com', 'nonprofitinfomart.org',
    'npino.com', 'npiprofile.com',
    'guidestar.org', 'candid.org', 'charitynavigator.org',
    'propublica.org', 'causeiq.com', 'greatnonprofits.org',
    'open990.org', 'nonprofitfacts.com', 'taxexemptworld.com',
    'nonprofitlocator.org', 'nonprofitlight.com',
    -- Other aggregators
    'publichousing.com', 'childcarecenter.us', 'usfiredept.com',
    'hinchilla.com', 'eintaxid.com', 'healthgrades.com',
    'yelp.com', 'yellowpages.com',
    -- Social media (added after initial run)
    'f990.org', 'influencewatch.org',
    'milliegiving.com', 'app.milliegiving.com'
  );

-- ----------------------------------------------------------------
-- STEP 5b: Move Facebook URLs to llm_social_media, NULL irs_website
-- ----------------------------------------------------------------

-- Where no social media exists yet
UPDATE charities
SET
  llm_social_media = jsonb_build_object('facebook', irs_website),
  irs_website = NULL,
  is_aggregator_url = FALSE
WHERE irs_website IS NOT NULL
  AND SUBSTRING(irs_website FROM 'https?://(?:www\.)?([^/]+)') = 'facebook.com'
  AND llm_social_media IS NULL;

-- Where social media already exists
UPDATE charities
SET
  irs_website = NULL,
  is_aggregator_url = FALSE
WHERE irs_website IS NOT NULL
  AND SUBSTRING(irs_website FROM 'https?://(?:www\.)?([^/]+)') = 'facebook.com'
  AND llm_social_media IS NOT NULL;

-- ----------------------------------------------------------------
-- STEP 6: Verify
-- ----------------------------------------------------------------

-- How many flagged as aggregator?
SELECT COUNT(*) AS aggregator_flagged
FROM charities
WHERE is_aggregator_url = TRUE;

-- How many valid URLs remain (not aggregator, not null)?
SELECT COUNT(*) AS clean_urls
FROM charities
WHERE irs_website IS NOT NULL
  AND (is_aggregator_url = FALSE OR is_aggregator_url IS NULL);

-- Top domains in clean URLs (spot check)
SELECT
  SUBSTRING(irs_website FROM 'https?://(?:www\.)?([^/]+)') AS domain,
  COUNT(*)
FROM charities
WHERE irs_website IS NOT NULL
  AND (is_aggregator_url = FALSE OR is_aggregator_url IS NULL)
GROUP BY domain
ORDER BY COUNT(*) DESC
LIMIT 20;
