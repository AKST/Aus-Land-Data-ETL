-- When ingesting this in events and source:
--
-- - legacy data
--   - `source_date` is `source_file.date_published`
--   - `CURRENT_DATE()` is `source_file.date_recorded`
--   - `basis_date_N` is `event.effective_date`
--
CREATE TABLE IF NOT EXISTS nsw_vg_raw.ps_row_a_legacy (
    a_legacy_source_id UUID NOT NULL,
    position bigint NOT NULL,
    file_path TEXT NOT NULL UNIQUE,
    file_source_id UUID NOT NULL,
    year_of_sale INT NOT NULL,
    submitting_user_id TEXT,
    date_provided DATE NOT NULL
);

CREATE INDEX idx_ps_row_a_legacy_a_legacy_source_id
    ON nsw_vg_raw.ps_row_a_legacy(a_legacy_source_id);

CREATE TABLE IF NOT EXISTS nsw_vg_raw.ps_row_b_legacy (
    b_legacy_source_id UUID NOT NULL,
    position bigint NOT NULL,
    file_source_id UUID NOT NULL,
    district_code INT NOT NULL,
    source TEXT,
    valuation_number varchar(16),
    property_id INT,
    unit_number TEXT,
    house_number TEXT,
    street_name TEXT,
    locality_name TEXT,
    postcode varchar(4),
    contract_date DATE,
    purchase_price FLOAT,
    land_description TEXT,
    area FLOAT,
    area_type varchar(1),
    dimensions TEXT,
    comp_code varchar(3),
    zone_code varchar(4),
    zone_standard nsw_vg.zoning_standard,
);

CREATE INDEX idx_ps_row_b_legacy_b_legacy_source_i
    ON nsw_vg_raw.ps_row_b_legacy(b_legacy_source_id);

--
-- # Non Legacy
--

CREATE TABLE IF NOT EXISTS nsw_vg_raw.ps_row_a (
    a_source_id UUID UNIQUE NOT NULL,
    position bigint NOT NULL,
    year_of_sale INT NOT NULL,
    file_path TEXT NOT NULL UNIQUE,
    file_source_id UUID NOT NULL,
    file_type TEXT,
    district_code INT NOT NULL,
    date_provided DATE NOT NULL,
    submitting_user_id TEXT NOT NULL
);

CREATE INDEX idx_ps_row_a_a_source_id
    ON nsw_vg_raw.ps_row_a(a_source_id);

CREATE TABLE IF NOT EXISTS nsw_vg_raw.ps_row_b (
    b_source_id UUID UNIQUE NOT NULL,
    position bigint NOT NULL,
    file_source_id UUID NOT NULL,
    district_code INT NOT NULL,
    property_id INT,
    sale_counter INT NOT NULL,
    date_provided DATE NOT NULL,
    property_name TEXT,
    unit_number TEXT,
    house_number TEXT,
    street_name TEXT,
    locality_name TEXT,
    postcode varchar(4),
    area FLOAT,
    area_type varchar(1),
    contract_date DATE,
    settlement_date DATE,
    purchase_price FLOAT,

    -- EP&A act zoning will not be more than 3 characters
    -- long however, some of the legacy zones (which can
    -- appear in this data due to the fact in 2011 there
    -- is abit of overlap in data between the old and new
    -- format) can appear here in this column.
    zone_code varchar(4),
    zone_standard nsw_vg.zoning_standard,

    nature_of_property varchar(1) NOT NULL,
    primary_purpose varchar(20),
    strata_lot_number INT,
    comp_code varchar(3),
    sale_code varchar(3),
    interest_of_sale INT,
    dealing_number varchar(10) NOT NULL,
);

CREATE INDEX idx_ps_row_b_b_source_id
    ON nsw_vg_raw.ps_row_b(b_source_id);

CREATE INDEX idx_ps_row_b_file_source_id_sale_counter
    ON nsw_vg_raw.ps_row_b(file_source_id, sale_counter);

CREATE TABLE IF NOT EXISTS nsw_vg_raw.ps_row_c (
    c_source_id UUID UNIQUE NOT NULL,
    position bigint NOT NULL,
    file_source_id UUID NOT NULL,
    district_code INT NOT NULL,
    property_id INT,
    sale_counter INT NOT NULL,
    date_provided DATE NOT NULL,
    property_description TEXT,
);

CREATE INDEX idx_ps_row_c_c_source_id
    ON nsw_vg_raw.ps_row_c(c_source_id);

CREATE INDEX idx_ps_row_c_file_source_id_sale_counter
    ON nsw_vg_raw.ps_row_c(file_source_id, sale_counter);

CREATE TABLE IF NOT EXISTS nsw_vg_raw.ps_row_d (
    d_source_id UUID UNIQUE NOT NULL,
    position bigint NOT NULL,
    file_source_id UUID NOT NULL,
    district_code INT NOT NULL,
    property_id INT,
    sale_counter INT NOT NULL,
    date_provided DATE NOT NULL,
    participant nsw_lrs.sale_participant NOT NULL,
);

CREATE INDEX idx_ps_row_d_d_source_id
    ON nsw_vg_raw.ps_row_d(d_source_id);

CREATE INDEX idx_ps_row_d_file_source_id_sale_counter
    ON nsw_vg_raw.ps_row_d(file_source_id, sale_counter);
