-- Deploy admin
-- admin

BEGIN;

CREATE EXTENSION "pg_uuidv7";
CREATE EXTENSION ltree;
CREATE EXTENSION pgcrypto;
CREATE EXTENSION pgjwt;

COMMIT;
