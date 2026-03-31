-- ================================================================
-- PowerDonor.ai — Role & Permission Management Script
-- Idempotent: safe to re-run anytime
-- Run as: postgres (superuser)
-- ================================================================

-- ----------------------------------------------------------------
-- STEP 1: Create roles if they don't already exist
-- ----------------------------------------------------------------

DO $$ BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'pd_admin') THEN
    CREATE ROLE pd_admin WITH LOGIN PASSWORD 'CHANGE_ME_ADMIN_STRONG';
    RAISE NOTICE 'Created role: pd_admin';
  ELSE
    RAISE NOTICE 'Role already exists: pd_admin';
  END IF;
END $$;

DO $$ BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'pd_dev') THEN
    CREATE ROLE pd_dev WITH LOGIN PASSWORD 'CHANGE_ME_DEV_STRONG';
    RAISE NOTICE 'Created role: pd_dev';
  ELSE
    RAISE NOTICE 'Role already exists: pd_dev';
  END IF;
END $$;

DO $$ BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'test_user') THEN
    CREATE ROLE test_user WITH LOGIN PASSWORD 'CHANGE_ME_TEST_STRONG';
    RAISE NOTICE 'Created role: test_user';
  ELSE
    RAISE NOTICE 'Role already exists: test_user';
  END IF;
END $$;

-- ----------------------------------------------------------------
-- STEP 2: Revoke everything first (clean slate on each run)
-- ----------------------------------------------------------------

REVOKE ALL ON ALL TABLES    IN SCHEMA public FROM pd_admin, pd_dev, test_user;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA public FROM pd_admin, pd_dev, test_user;
REVOKE ALL ON SCHEMA public                  FROM pd_admin, pd_dev, test_user;
REVOKE ALL ON DATABASE railway               FROM pd_admin, pd_dev, test_user;

-- ----------------------------------------------------------------
-- STEP 3: pd_admin — full table control, NOT a superuser (can't drop DB)
-- ----------------------------------------------------------------

GRANT CONNECT                                          ON DATABASE railway TO pd_admin;
GRANT USAGE, CREATE                                    ON SCHEMA public    TO pd_admin;
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE
      ON ALL TABLES    IN SCHEMA public                                    TO pd_admin;
GRANT USAGE, SELECT   ON ALL SEQUENCES IN SCHEMA public                   TO pd_admin;

-- ----------------------------------------------------------------
-- STEP 4: pd_dev — row-level DML only, no TRUNCATE, no DDL
-- ----------------------------------------------------------------

GRANT CONNECT                                          ON DATABASE railway TO pd_dev;
GRANT USAGE                                            ON SCHEMA public    TO pd_dev;
GRANT SELECT, INSERT, UPDATE, DELETE
      ON ALL TABLES    IN SCHEMA public                                    TO pd_dev;
GRANT USAGE, SELECT   ON ALL SEQUENCES IN SCHEMA public                   TO pd_dev;

-- ----------------------------------------------------------------
-- STEP 5: test_user — read only
-- ----------------------------------------------------------------

GRANT CONNECT                                          ON DATABASE railway TO test_user;
GRANT USAGE                                            ON SCHEMA public    TO test_user;
GRANT SELECT ON ALL TABLES    IN SCHEMA public                             TO test_user;

-- ----------------------------------------------------------------
-- STEP 6: Default privileges — apply to future tables automatically
-- (only affects tables created by the current session user: postgres)
-- ----------------------------------------------------------------

ALTER DEFAULT PRIVILEGES IN SCHEMA public
  GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON TABLES    TO pd_admin;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
  GRANT USAGE, SELECT                            ON SEQUENCES TO pd_admin;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
  GRANT SELECT, INSERT, UPDATE, DELETE           ON TABLES    TO pd_dev;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
  GRANT USAGE, SELECT                            ON SEQUENCES TO pd_dev;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
  GRANT SELECT                                   ON TABLES    TO test_user;

-- ----------------------------------------------------------------
-- STEP 7: Verify — confirm what each role can do
-- ----------------------------------------------------------------

SELECT grantee, table_name, privilege_type
FROM information_schema.role_table_grants
WHERE grantee IN ('pd_admin', 'pd_dev', 'test_user')
  AND table_name = 'charities'  -- spot check against your main table
ORDER BY grantee, privilege_type;
