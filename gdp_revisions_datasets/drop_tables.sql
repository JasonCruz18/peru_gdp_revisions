-- Script: Drop Tables Starting Exactly with "r_", "e_", or "z_"
-- Purpose: This script deletes all tables in the 'public' schema whose names start specifically with "r_", "e_", or "z_".
-- Note: Use with caution. Ensure you have a backup if you need to retain any data.

DO $$ 
DECLARE
    tbl RECORD;  -- Variable to hold each table record from the loop
BEGIN
    -- Loop through all tables in the 'public' schema that start with "r_", "e_", or "z_"
    FOR tbl IN
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'public' 
          AND (tablename LIKE 'r\_%' OR tablename LIKE 'e\_%' OR tablename LIKE 'z\_%')
          AND SUBSTRING(tablename, 2, 1) = '_'
    LOOP
        -- Dynamically execute a DROP TABLE command for each matching table
        EXECUTE FORMAT('DROP TABLE IF EXISTS %I CASCADE', tbl.tablename);
    END LOOP;
END $$;


