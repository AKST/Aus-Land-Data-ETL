CREATE TABLE IF NOT EXISTS nsw_vg_raw.ps_row_b_legacy_complementary(
    b_legacy_source_id UUID NOT NULL,
    effective_date DATE NOT NULL,
    seen_in_modern_psi BOOL NOT NULL,
    canonical BOOL NOT NULL
);

CREATE INDEX ON nsw_vg_raw.ps_row_b_legacy_complementary(b_legacy_source_id);


CREATE TABLE IF NOT EXISTS nsw_vg_raw.ps_row_b_complementary(
    b_source_id UUID NOT NULL,
    property_id INT NOT NULL,
    effective_date DATE NOT NULL,
    seen_in_land_values BOOL NOT NULL,
    canonical BOOL NOT NULL
) PARTITION BY HASH (property_id);

CREATE INDEX ON nsw_vg_raw.ps_row_b_complementary(canonical, b_source_id);
CREATE INDEX ON nsw_vg_raw.ps_row_b_complementary(b_source_id);
CREATE INDEX ON nsw_vg_raw.ps_row_b_complementary(canonical);
CREATE INDEX ON nsw_vg_raw.ps_row_b_complementary(effective_date DESC);

