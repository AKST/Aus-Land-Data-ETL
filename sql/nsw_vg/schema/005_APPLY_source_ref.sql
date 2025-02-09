CREATE TABLE IF NOT EXISTS nsw_vg_raw.ps_row_b_complementary(
    b_source_id UUID UNIQUE NOT NULL,
    effective_date DATE NOT NULL,
    seen_in_land_values BOOL NOT NULL,
    canonical BOOL NOT NULL
);

CREATE INDEX idx_ps_row_b_complementary_canonical_b_source_id
    ON nsw_vg_raw.ps_row_b_complementary(canonical, b_source_id);

CREATE INDEX idx_ps_row_b_complementary_b_source_id
    ON nsw_vg_raw.ps_row_b_complementary(b_source_id);

CREATE INDEX idx_ps_row_b_complementary_canonical
    ON nsw_vg_raw.ps_row_b_complementary(canonical);

CREATE INDEX idx_ps_row_b_complementary_effective_date
    ON nsw_vg_raw.ps_row_b_complementary(effective_date);

CREATE TABLE IF NOT EXISTS nsw_vg_raw.ps_row_b_legacy_complementary(
    b_legacy_source_id UUID NOT NULL,
    effective_date DATE NOT NULL,
    seen_in_modern_psi BOOL NOT NULL,
    canonical BOOL NOT NULL
);

CREATE INDEX idx_ps_row_b_legacy_complementary_b_legacy_source_id
    ON nsw_vg_raw.ps_row_b_legacy_complementary(b_legacy_source_id);
