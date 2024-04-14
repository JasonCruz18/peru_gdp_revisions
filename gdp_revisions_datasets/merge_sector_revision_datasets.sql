--***********************************************
-- Merging all economic sector revision datasets
--***********************************************

-- Checking full dataset
SELECT * FROM "services_monthly_revisions";


--*******
-- Add an id column to every revisions dataset
--*******

DO $$
DECLARE
    tabla TEXT;
BEGIN
    FOR tabla IN
        SELECT table_name
        FROM information_schema.tables
        WHERE table_name LIKE '%_monthly_revisions'
    LOOP
        -- Comprobamos si la columna id ya existe en la tabla
        IF NOT EXISTS (
            SELECT 1 
            FROM information_schema.columns 
            WHERE table_name = tabla 
            AND column_name = 'id'
        ) THEN
            EXECUTE format('
                ALTER TABLE %I
                ADD COLUMN id SERIAL PRIMARY KEY;
            ', tabla);
        END IF;
    END LOOP;
END$$;


--*******
-- Merge
--*******

-- Crear la tabla 'sectors_monthly_revisions'
CREATE TABLE sectors_monthly_revisions (
    id SERIAL PRIMARY KEY,  -- Suponiendo que 'id' es una clave primaria autoincremental
    revision_date DATE,
    gdp_revision DECIMAL,
    agriculture_revision DECIMAL,
    commerce_revision DECIMAL,
    construction_revision DECIMAL,
    electricity_revision DECIMAL,
    fishing_revision DECIMAL,
    manufacturing_revision DECIMAL,
    mining_revision DECIMAL,
    services_revision DECIMAL
);

SELECT * FROM sectors_monthly_revisions;

-- Insertar datos de 'gdp_monthly_revision' en 'sectors_monthly_revisions'
INSERT INTO sectors_monthly_revisions (revision_date, gdp_revision)
SELECT revision_date, gdp_revision FROM gdp_monthly_revisions;

-- Actualizar datos de 'agriculture_monthly_revision' en 'sector_monthly_revision'
UPDATE sectors_monthly_revisions AS smr
SET agriculture_revision = amr.agriculture_revision
FROM agriculture_monthly_revisions AS amr
WHERE smr.revision_date = amr.revision_date;

-- Actualizar datos de 'commerce_monthly_revision' en 'sector_monthly_revision'
UPDATE sectors_monthly_revisions AS smr
SET commerce_revision = cmr.commerce_revision
FROM commerce_monthly_revisions AS cmr
WHERE smr.revision_date = cmr.revision_date;

-- Actualizar datos de 'construction_monthly_revision' en 'sector_monthly_revision'
UPDATE sectors_monthly_revisions AS smr
SET construction_revision = cmr.construction_revision
FROM construction_monthly_revisions AS cmr
WHERE smr.revision_date = cmr.revision_date;

-- Actualizar datos de 'electricity_monthly_revision' en 'sector_monthly_revision'
UPDATE sectors_monthly_revisions AS smr
SET electricity_revision = emr.electricity_revision
FROM electricity_monthly_revisions AS emr
WHERE smr.revision_date = emr.revision_date;

-- Actualizar datos de 'fishing_monthly_revision' en 'sector_monthly_revision'
UPDATE sectors_monthly_revisions AS smr
SET fishing_revision = fmr.fishing_revision
FROM fishing_monthly_revisions AS fmr
WHERE smr.revision_date = fmr.revision_date;

-- Actualizar datos de 'manufacturing_monthly_revision' en 'sector_monthly_revision'
UPDATE sectors_monthly_revisions AS smr
SET manufacturing_revision = mmr.manufacturing_revision
FROM manufacturing_monthly_revisions AS mmr
WHERE smr.revision_date = mmr.revision_date;

-- Actualizar datos de 'mining_monthly_revision' en 'sector_monthly_revision'
UPDATE sectors_monthly_revisions AS smr
SET mining_revision = mmr.mining_revision
FROM mining_monthly_revisions AS mmr
WHERE smr.revision_date = mmr.revision_date;

-- Actualizar datos de 'services_monthly_revision' en 'sector_monthly_revision'
UPDATE sectors_monthly_revisions AS smr
SET services_revision = svr.services_revision
FROM services_monthly_revisions AS svr
WHERE smr.revision_date = svr.revision_date;

SELECT * FROM "sectors_monthly_revisions";