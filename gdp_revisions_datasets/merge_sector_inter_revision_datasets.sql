--*************************************************************
-- Merging all economic sector intermediate revision datasets
--*************************************************************

-- Checking full dataset
SELECT * FROM "gdp_monthly_inter_revisions";


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
        WHERE table_name LIKE '%_monthly_inter_revisions'
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
CREATE TABLE sectors_monthly_inter_revisions (
    id SERIAL PRIMARY KEY,  -- Suponiendo que 'id' es una clave primaria autoincremental
    inter_revision_date DATE,
    gdp_revision_1 DECIMAL,
	gdp_revision_2 DECIMAL,
	gdp_revision_3 DECIMAL,
	gdp_revision_4 DECIMAL,
	gdp_revision_5 DECIMAL,
	gdp_revision_6 DECIMAL,
	gdp_revision_7 DECIMAL,
	gdp_revision_8 DECIMAL, -- PENDING TO DOUBLE CHECK
    agriculture_revision DECIMAL,
    commerce_revision DECIMAL,
    construction_revision DECIMAL,
    electricity_revision DECIMAL,
    fishing_revision DECIMAL,
    manufacturing_revision DECIMAL,
    mining_revision DECIMAL,
    services_revision DECIMAL
);

SELECT * FROM sectors_monthly_inter_revisions;

-- Insertar datos de 'gdp_monthly_inter_revisions' en 'sectors_monthly_inter_revisions'
INSERT INTO sectors_monthly_inter_revisions (inter_revision_date, gdp_revision_1)
SELECT inter_revision_date, gdp_revision_1 FROM gdp_monthly_inter_revisions;

-- Actualizar datos de 'agriculture_monthly_revisions' en 'sectors_monthly_inter_revisions'
UPDATE sectors_monthly_inter_revisions AS smr
SET agriculture_inter_revisions = amr.agriculture_revision
FROM agriculture_inter_revisions AS amr
WHERE smr.inter_revision_date = amr.inter_revision_date;

-- PENDING

--SELECT * FROM "sectors_monthly_revisions";