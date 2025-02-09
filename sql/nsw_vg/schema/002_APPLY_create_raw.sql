-- When ingesting this in events and source:
--
--   - `source_date` is `source_file.date_published`
--   - `CURRENT_DATE()` is `source_file.date_recorded`
--   - `basis_date_N` is `event.effective_date`
--
CREATE TABLE IF NOT EXISTS nsw_vg_raw.land_value_row (
    district_code INT NOT NULL,
    district_name TEXT,
    property_id INT NOT NULL,
    property_type TEXT,
    property_name TEXT,
    unit_number TEXT,
    house_number TEXT,
    street_name TEXT,
    suburb_name TEXT NOT NULL,
    postcode varchar(4),
    property_description TEXT,
    zone_code varchar(3) NOT NULL,
    zone_standard nsw_vg.zoning_standard,
    area FLOAT,
    area_type TEXT,
    base_date_1 DATE,
    land_value_1 BIGINT,
    authority_1 TEXT,
    basis_1 TEXT,
    base_date_2 DATE,
    land_value_2 BIGINT,
    authority_2 TEXT,
    basis_2 TEXT,
    base_date_3 DATE,
    land_value_3 BIGINT,
    authority_3 TEXT,
    basis_3 TEXT,
    base_date_4 DATE,
    land_value_4 BIGINT,
    authority_4 TEXT,
    basis_4 TEXT,
    base_date_5 DATE,
    land_value_5 BIGINT,
    authority_5 TEXT,
    basis_5 TEXT,
    source_file_name TEXT NOT NULL,
    source_line_number INT NOT NULL,
    source_date DATE NOT NULL,

    UNIQUE (property_id, source_date)
) PARTITION BY HASH (property_id);

CREATE INDEX ON nsw_vg_raw.land_value_row(property_id, source_date);
CREATE INDEX ON nsw_vg_raw.land_value_row(source_file_name);
