--------------------------
-- ORDER TABLE
--------------------------

CREATE TABLE gdp_monthly_growth_rates_sorted AS
SELECT * FROM gdp_monthly_growth_rates
ORDER BY year, id_ns, date;

DROP TABLE IF EXISTS "gdp_monthly_growth_rates";

ALTER TABLE gdp_monthly_growth_rates_sorted RENAME TO gdp_monthly_growth_rates;

SELECT * FROM gdp_monthly_growth_rates;

