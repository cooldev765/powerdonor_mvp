-- ================================================================
-- PowerDonor.ai — Phase 1 Fix 6: Clean llm_social_media JSONB
-- Run as: postgres (superuser)
--
-- Problems addressed:
--   1. All-null objects   {"twitter": null, "facebook": null, ...}
--   2. Homepage-only URLs {"facebook": "https://facebook.com"} (no profile path)
--   3. Garbage keys       "Follow Us", "platform", "general"
--   4. Missing protocol   facebook.com/orgname → https://facebook.com/orgname
--   5. Non-URL text       "X/Twitter", "Vilade Baseball Academy on YouTube"
--   6. Mixed-case keys    Twitter/Facebook/X → twitter/facebook/twitter
-- ================================================================

-- ----------------------------------------------------------------
-- STEP 1: Preview — count each garbage pattern
-- ----------------------------------------------------------------

SELECT
  COUNT(*) FILTER (WHERE NOT EXISTS (
    SELECT 1 FROM jsonb_each_text(llm_social_media) kv WHERE kv.value IS NOT NULL
  ))                                                              AS all_null_objects,

  COUNT(*) FILTER (WHERE EXISTS (
    SELECT 1 FROM jsonb_each_text(llm_social_media) kv
    WHERE LOWER(kv.key) IN ('follow us', 'platform', 'general', 'other', 'social media')
       OR LOWER(kv.key) ~* '^follow'
  ))                                                              AS garbage_key_rows,

  COUNT(*) FILTER (WHERE EXISTS (
    SELECT 1 FROM jsonb_each_text(llm_social_media) kv
    WHERE kv.value ~* '^https?://(www\.)?(facebook|twitter|x|instagram|youtube|linkedin|tiktok)\.com/?$'
       OR kv.value ~* '^(www\.)?(facebook|twitter|x|instagram|youtube|linkedin|tiktok)\.com/?$'
  ))                                                              AS homepage_only_rows

FROM charities
WHERE llm_social_media IS NOT NULL;

-- ----------------------------------------------------------------
-- STEP 2a: NULL out all-null objects
-- {"twitter": null, "facebook": null, ...} → NULL
-- ----------------------------------------------------------------

UPDATE charities
SET llm_social_media = NULL
WHERE llm_social_media IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM jsonb_each_text(llm_social_media) kv
    WHERE kv.value IS NOT NULL
  );

-- ----------------------------------------------------------------
-- STEP 2b: Normalize keys to lowercase + rename X → twitter
-- Rebuilds JSON per row using correlated subquery
-- ----------------------------------------------------------------

UPDATE charities
SET llm_social_media = (
  SELECT jsonb_object_agg(
    CASE LOWER(kv.key) WHEN 'x' THEN 'twitter' ELSE LOWER(kv.key) END,
    kv.value
  )
  FROM jsonb_each(llm_social_media) AS kv
)
WHERE llm_social_media IS NOT NULL
  AND llm_social_media::text ~ '[A-Z]';

-- ----------------------------------------------------------------
-- STEP 2c: Drop garbage keys + null values + homepage-only URLs
--          + non-URL text. Set field to NULL if nothing remains.
-- ----------------------------------------------------------------

UPDATE charities
SET llm_social_media = (
  SELECT jsonb_object_agg(
    kv.key,
    CASE
      -- Add https:// to bare social paths (facebook.com/orgname)
      WHEN kv.value ~* '^(facebook|twitter|instagram|youtube|linkedin|tiktok)\.com/.+'
        THEN to_jsonb('https://' || kv.value)
      ELSE to_jsonb(kv.value)
    END
  )
  FROM jsonb_each_text(llm_social_media) AS kv
  WHERE
    -- Drop garbage keys
    LOWER(kv.key) NOT IN ('follow us', 'platform', 'general', 'other', 'social media')
    AND LOWER(kv.key) !~* '^follow'
    -- Drop null values
    AND kv.value IS NOT NULL
    -- Drop homepage-only URLs (no profile path after domain)
    AND kv.value !~* '^https?://(www\.)?(facebook|twitter|x|instagram|youtube|linkedin|tiktok|youtu\.be)\.com/?$'
    AND kv.value !~* '^(www\.)?(facebook|twitter|x|instagram|youtube|linkedin|tiktok)\.com/?$'
    -- Drop non-URL text: must be a full URL or bare domain/path
    AND (
      kv.value ~* '^https?://'
      OR kv.value ~* '^[a-z0-9.-]+\.[a-z]{2,}/.+'
    )
)
WHERE llm_social_media IS NOT NULL;
-- Note: jsonb_object_agg returns NULL when no rows match → sets field to NULL

-- ----------------------------------------------------------------
-- STEP 2d: Extended cleanup
--   1. Add https:// to bare threads.net paths (threads.net/@handle)
--   2. NULL out homepage-only for extended platform list
--   3. NULL out URLs with spaces in path (org name used as handle)
--   4. NULL out fake/dead platforms (x-twitter.com, plus.google.com)
--   5. Drop google_plus key entirely
--   6. Set field to NULL if nothing valid remains
-- ----------------------------------------------------------------

UPDATE charities
SET llm_social_media = (
  SELECT jsonb_object_agg(kv.key, to_jsonb(kv.value))
  FROM jsonb_each_text(llm_social_media) AS kv
  WHERE
    -- Drop google_plus key (dead platform)
    kv.key != 'google_plus'
    AND kv.value IS NOT NULL
    -- Add https:// to bare threads.net paths
    -- (handled in value transform below — filter keeps them in, CASE adds prefix)
    -- Drop URLs with spaces in path (org name used as handle, not a real URL)
    AND kv.value !~ ' '
    -- Drop fake/dead domains
    AND kv.value !~* 'x-twitter\.com'
    AND kv.value !~* 'plus\.google\.com'
    -- Drop homepage-only for extended platform list (root domain, no profile path)
    AND kv.value !~* '^https?://(www\.)?pinterest\.com/?$'
    AND kv.value !~* '^https?://(www\.)?flickr\.com/?$'
    AND kv.value !~* '^https?://(www\.)?vimeo\.com/?$'
    AND kv.value !~* '^https?://(bluesky\.social|bsky\.app|bsky\.social)/?$'
    AND kv.value !~* '^https?://(www\.)?threads\.net/?$'
    AND kv.value !~* '^https?://(www\.)?spotify\.com/?$'
    AND kv.value !~* '^https?://(www\.)?substack\.com/?$'
    AND kv.value !~* '^https?://(www\.)?discord\.com/?$'
    AND kv.value !~* '^https?://(www\.)?whatsapp\.com/?$'
    AND kv.value !~* '^https?://podcasts\.apple\.com/?$'
    AND kv.value !~* '^https?://(www\.)?tumblr\.com/?$'
    AND kv.value !~* '^https?://(www\.)?youtube\.com/channel/?$'
)
WHERE llm_social_media IS NOT NULL
  AND (
    llm_social_media ? 'google_plus'
    OR EXISTS (
      SELECT 1 FROM jsonb_each_text(llm_social_media) kv
      WHERE kv.value ~ ' '
         OR kv.value ~* 'x-twitter\.com'
         OR kv.value ~* 'plus\.google\.com'
         OR kv.value ~* '^https?://(www\.)?pinterest\.com/?$'
         OR kv.value ~* '^https?://(www\.)?flickr\.com/?$'
         OR kv.value ~* '^https?://(www\.)?vimeo\.com/?$'
         OR kv.value ~* '^https?://(bluesky\.social|bsky\.app|bsky\.social)/?$'
         OR kv.value ~* '^https?://(www\.)?threads\.net/?$'
         OR kv.value ~* '^https?://(www\.)?spotify\.com/?$'
         OR kv.value ~* '^https?://(www\.)?substack\.com/?$'
         OR kv.value ~* '^https?://(www\.)?discord\.com/?$'
         OR kv.value ~* '^https?://(www\.)?whatsapp\.com/?$'
         OR kv.value ~* '^https?://podcasts\.apple\.com/?$'
         OR kv.value ~* '^https?://(www\.)?tumblr\.com/?$'
         OR kv.value ~* '^https?://(www\.)?youtube\.com/channel/?$'
    )
  );

-- Add https:// to bare social paths missing protocol (fb.me/, threads.net/, etc.)
UPDATE charities
SET llm_social_media = (
  SELECT jsonb_object_agg(kv.key, to_jsonb(
    CASE
      WHEN kv.value ~* '^(threads\.net|fb\.me|t\.me)/.+'
        THEN 'https://' || kv.value
      ELSE kv.value
    END
  ))
  FROM jsonb_each_text(llm_social_media) AS kv
  WHERE kv.value IS NOT NULL
)
WHERE llm_social_media IS NOT NULL
  AND EXISTS (
    SELECT 1 FROM jsonb_each_text(llm_social_media) kv
    WHERE kv.value ~* '^(threads\.net|fb\.me|t\.me)/.+'
  );

-- ----------------------------------------------------------------
-- STEP 2e: Homepage-only cleanup — extended platform list
-- Any URL with no meaningful path (just root domain) is useless
-- as a social media profile. NULL those values out.
-- ----------------------------------------------------------------

UPDATE charities
SET llm_social_media = (
  SELECT jsonb_object_agg(kv.key, to_jsonb(kv.value))
  FROM jsonb_each_text(llm_social_media) AS kv
  WHERE kv.value IS NOT NULL
    -- General rule: homepage-only (no path beyond root) for known platforms
    -- Pattern: https://(www.)?DOMAIN.TLD/? with no path after
    AND NOT (
      kv.value ~ '^https?://[^/]+/?$'   -- no meaningful path
      AND kv.value ~* '(yelp\.com|google\.com|maps\.google\.com|podcasts\.google\.com|nextdoor\.com|threads\.com|tripadvisor\.com|bluesky\.com|reddit\.com|mastodon\.social|snapchat\.com|soundcloud\.com|medium\.com|linktree\.com|petfinder\.com|rss\.com|rumble\.com|patreon\.com|eventbrite\.com|dribbble\.com|smugmug\.com|smile\.amazon\.com|music\.amazon\.com|strava\.com|meetup\.com|open\.spotify\.com|wordpress\.com|truthsocial\.com|paypal\.com|foursquare\.com|issuu\.com|letterboxd\.com|github\.com|podcasts\.apple\.com|music\.apple\.com)'
    )
    -- Also drop google key (google.com is never a social profile)
    AND kv.key != 'google'
)
WHERE llm_social_media IS NOT NULL
  AND (
    llm_social_media ? 'google'
    OR EXISTS (
      SELECT 1 FROM jsonb_each_text(llm_social_media) kv
      WHERE kv.value ~ '^https?://[^/]+/?$'
        AND kv.value ~* '(yelp|google\.com|nextdoor|threads\.com|tripadvisor|bluesky\.com|reddit\.com|mastodon|snapchat|soundcloud|medium\.com|linktree|petfinder|rss\.com|rumble|patreon|eventbrite|dribbble|smugmug|amazon\.com|strava|meetup|spotify\.com|wordpress\.com|truthsocial|paypal|foursquare|issuu|letterboxd|github\.com|podcasts\.apple|music\.apple)'
    )
  );

-- ----------------------------------------------------------------
-- STEP 2f: Universal cleanup — final pass
--
-- Rule: a valid social media value must have a real path after
-- the domain (e.g. /orgname, /channel, /r/subreddit).
-- Homepage-only = useless. Pseudo-URLs = garbage.
--
-- Actions:
--   1. KEEP values that have https:// + a real path
--   2. ADD https:// to bare domain/path values (www.facebook.com/org,
--      pinterest.com/org, x.com/handle, linktr.ee/org, etc.)
--   3. NULL everything else (homepage-only, pseudo-URLs,
--      typo domains, aggregator sites)
-- ----------------------------------------------------------------

UPDATE charities
SET llm_social_media = (
  SELECT jsonb_object_agg(
    kv.key,
    to_jsonb(
      CASE
        -- Add https:// to bare domain/path values
        WHEN kv.value !~* '^https?://'
          THEN 'https://' || kv.value
        ELSE kv.value
      END
    )
  )
  FROM jsonb_each_text(llm_social_media) AS kv
  WHERE kv.value IS NOT NULL
    -- Must have a real path after the domain (not homepage-only)
    AND (
      -- Has https:// and a non-empty path (not just trailing slash)
      (kv.value ~* '^https?://' AND kv.value ~ '^https?://[^/]+/[^/]')
      -- OR bare domain/path that we will add https:// to
      OR (kv.value !~* '^https?://' AND kv.value ~* '^(www\.)?[a-z0-9][a-z0-9.-]+\.[a-z]{2,}/.+')
    )
    -- Drop typo and fake domains
    AND kv.value !~* 'instagram\.org'
    AND kv.value !~* 'twitter\.com\.com'
    AND kv.value !~* '(x-twitter|x-twitter-square)\.com'
    AND kv.value !~* '(google-plus|googleplus)\.com'
    AND kv.value !~* 'social\.example\.com'
    -- Drop aggregator/charity ratings sites stored as social media
    AND kv.value !~* '(charitynavigator\.org|candid\.org|candiddotorg\.com|guidestar\.org|greatnonprofits\.org|propublica\.org)'
)
WHERE llm_social_media IS NOT NULL;

-- ----------------------------------------------------------------
-- STEP 3: Verify
-- ----------------------------------------------------------------

-- How many rows still have llm_social_media?
SELECT COUNT(*) AS rows_with_social_media
FROM charities
WHERE llm_social_media IS NOT NULL;

-- How many were NULLed out (all garbage)?
SELECT COUNT(*) AS nulled_out
FROM charities
WHERE llm_social_media IS NULL
  AND llm_enriched_at IS NOT NULL;  -- was enriched but social cleaned to NULL

-- Spot check — top patterns remaining
SELECT llm_social_media::text, COUNT(*)
FROM charities
WHERE llm_social_media IS NOT NULL
GROUP BY llm_social_media::text
ORDER BY COUNT(*) DESC
LIMIT 20;
