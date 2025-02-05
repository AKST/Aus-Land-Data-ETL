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

CREATE INDEX idx_land_value_row_pk
    ON nsw_vg_raw.land_value_row(property_id, source_date);
CREATE INDEX idx_land_value_row_source_file_name
    ON nsw_vg_raw.land_value_row(source_file_name);

CREATE TABLE nsw_vg_raw.land_value_row_p0 PARTITION OF nsw_vg_raw.land_value_row
    FOR VALUES WITH (MODULUS 8, REMAINDER 0);
CREATE TABLE nsw_vg_raw.land_value_row_p1 PARTITION OF nsw_vg_raw.land_value_row
    FOR VALUES WITH (MODULUS 8, REMAINDER 1);
CREATE TABLE nsw_vg_raw.land_value_row_p2 PARTITION OF nsw_vg_raw.land_value_row
    FOR VALUES WITH (MODULUS 8, REMAINDER 2);
CREATE TABLE nsw_vg_raw.land_value_row_p3 PARTITION OF nsw_vg_raw.land_value_row
    FOR VALUES WITH (MODULUS 8, REMAINDER 3);
CREATE TABLE nsw_vg_raw.land_value_row_p4 PARTITION OF nsw_vg_raw.land_value_row
    FOR VALUES WITH (MODULUS 8, REMAINDER 4);
CREATE TABLE nsw_vg_raw.land_value_row_p5 PARTITION OF nsw_vg_raw.land_value_row
    FOR VALUES WITH (MODULUS 8, REMAINDER 5);
CREATE TABLE nsw_vg_raw.land_value_row_p6 PARTITION OF nsw_vg_raw.land_value_row
    FOR VALUES WITH (MODULUS 8, REMAINDER 6);
CREATE TABLE nsw_vg_raw.land_value_row_p7 PARTITION OF nsw_vg_raw.land_value_row
    FOR VALUES WITH (MODULUS 8, REMAINDER 7);

