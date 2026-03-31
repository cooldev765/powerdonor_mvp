-- ================================================================
-- PowerDonor.ai — Phase 1 Fix 3: Clean Garbage irs_mission Rows
-- Run as: postgres (superuser)
-- Note:
--   - NULL out SEE SCHEDULE / SEE ATTACHED (IRS filing instructions)
--   - ALL CAPS rows left as-is — apply INITCAP() at query time
--   - Short rows left as-is — short missions are still valid data
-- ================================================================

-- ----------------------------------------------------------------
-- STEP 1: Preview
-- ----------------------------------------------------------------

SELECT COUNT(*)
FROM charities
WHERE irs_mission ILIKE '%SEE SCHEDULE%'
   OR irs_mission ILIKE '%SEE ATTACHED%';

-- ----------------------------------------------------------------
-- STEP 2: NULL out garbage rows
-- ----------------------------------------------------------------

UPDATE charities
SET irs_mission = NULL
WHERE irs_mission ILIKE '%SEE SCHEDULE%'
   OR irs_mission ILIKE '%SEE ATTACHED%';

-- ----------------------------------------------------------------
-- STEP 3: Verify — should return 0
-- ----------------------------------------------------------------

SELECT COUNT(*)
FROM charities
WHERE irs_mission ILIKE '%SEE SCHEDULE%'
   OR irs_mission ILIKE '%SEE ATTACHED%';
