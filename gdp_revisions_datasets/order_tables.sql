--******************************
-- ORDER TABLE
--******************************

--CREATE TABLE construction_monthly_growth_rates_sorted AS
--SELECT * FROM construction_monthly_growth_rates
--ORDER BY year, id_ns, date;

--DROP TABLE IF EXISTS "construction_monthly_growth_rates";

--ALTER TABLE construction_monthly_growth_rates_sorted RENAME TO construction_monthly_growth_rates;

--SELECT * FROM construction_monthly_growth_rates;

--++++++++++++++++++++++++++++++
-- MONTHLY
--++++++++++++++++++++++++++++++

DO $$
DECLARE
    tbl RECORD;
BEGIN
    FOR tbl IN 
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_name LIKE '%_monthly_growth_rates'
    LOOP
        EXECUTE format('
            CREATE TABLE %I_sorted AS
            SELECT * FROM %I
            ORDER BY year, id_ns, date;

            DROP TABLE IF EXISTS %I;

            ALTER TABLE %I_sorted RENAME TO %I;
        ', tbl.table_name, tbl.table_name, tbl.table_name, tbl.table_name, tbl.table_name);
    END LOOP;
END $$;


--++++++++++++++++++++++++++++++
-- QUARTERLY
--++++++++++++++++++++++++++++++

DO $$
DECLARE
    tbl RECORD;
BEGIN
    FOR tbl IN 
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_name LIKE '%_quarterly_growth_rates'
    LOOP
        EXECUTE format('
            CREATE TABLE %I_sorted AS
            SELECT * FROM %I
            ORDER BY year, id_ns, date;

            DROP TABLE IF EXISTS %I;

            ALTER TABLE %I_sorted RENAME TO %I;
        ', tbl.table_name, tbl.table_name, tbl.table_name, tbl.table_name, tbl.table_name);
    END LOOP;
END $$;


--++++++++++++++++++++++++++++++
-- ANNUAL
--++++++++++++++++++++++++++++++

DO $$
DECLARE
    tbl RECORD;
BEGIN
    FOR tbl IN 
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_name LIKE '%_annual_growth_rates'
    LOOP
        EXECUTE format('
            CREATE TABLE %I_sorted AS
            SELECT * FROM %I
            ORDER BY year, id_ns, date;

            DROP TABLE IF EXISTS %I;

            ALTER TABLE %I_sorted RENAME TO %I;
        ', tbl.table_name, tbl.table_name, tbl.table_name, tbl.table_name, tbl.table_name);
    END LOOP;
END $$;

--
-- SELECT
--

SELECT * FROM "gdp_monthly_growth_rates";