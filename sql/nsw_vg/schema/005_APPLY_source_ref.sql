CREATE TABLE IF NOT EXISTS nsw_vg_raw.ps_row_a_legacy_source(
    ps_row_a_legacy_id UUID NOT NULL,
    source_id UUID UNIQUE NOT NULL,
    FOREIGN KEY (source_id) REFERENCES meta.source(source_id)
);

CREATE TABLE IF NOT EXISTS nsw_vg_raw.ps_row_b_legacy_source(
    ps_row_b_legacy_id UUID NOT NULL,
    source_id UUID UNIQUE NOT NULL,
    FOREIGN KEY (source_id) REFERENCES meta.source(source_id)
);

CREATE INDEX idx_ps_row_a_legacy_source_a ON nsw_vg_raw.ps_row_a_legacy_source(ps_row_a_legacy_id);
CREATE INDEX idx_ps_row_a_legacy_source_b ON nsw_vg_raw.ps_row_a_legacy_source(source_id);
CREATE INDEX idx_ps_row_b_legacy_source_a ON nsw_vg_raw.ps_row_b_legacy_source(ps_row_b_legacy_id);
CREATE INDEX idx_ps_row_b_legacy_source_b ON nsw_vg_raw.ps_row_b_legacy_source(source_id);

CREATE TABLE IF NOT EXISTS nsw_vg_raw.ps_row_a_source(
    ps_row_a_id UUID NOT NULL,
    source_id UUID UNIQUE NOT NULL,
    FOREIGN KEY (source_id) REFERENCES meta.source(source_id)
);

CREATE TABLE IF NOT EXISTS nsw_vg_raw.ps_row_b_source(
    ps_row_b_id UUID NOT NULL,
    source_id UUID UNIQUE NOT NULL,
    FOREIGN KEY (source_id) REFERENCES meta.source(source_id)
);

CREATE TABLE IF NOT EXISTS nsw_vg_raw.ps_row_c_source(
    ps_row_c_id UUID NOT NULL,
    source_id UUID UNIQUE NOT NULL,
    FOREIGN KEY (source_id) REFERENCES meta.source(source_id)
);

CREATE TABLE IF NOT EXISTS nsw_vg_raw.ps_row_d_source(
    ps_row_d_id UUID NOT NULL,
    source_id UUID UNIQUE NOT NULL,
    FOREIGN KEY (source_id) REFERENCES meta.source(source_id)
);

CREATE INDEX idx_ps_row_a_source_a ON nsw_vg_raw.ps_row_a_source(ps_row_a_id);
CREATE INDEX idx_ps_row_a_source_b ON nsw_vg_raw.ps_row_a_source(source_id);
CREATE INDEX idx_ps_row_b_source_a ON nsw_vg_raw.ps_row_b_source(ps_row_b_id);
CREATE INDEX idx_ps_row_b_source_b ON nsw_vg_raw.ps_row_b_source(source_id);
CREATE INDEX idx_ps_row_c_source_a ON nsw_vg_raw.ps_row_c_source(ps_row_c_id);
CREATE INDEX idx_ps_row_c_source_b ON nsw_vg_raw.ps_row_c_source(source_id);
CREATE INDEX idx_ps_row_d_source_a ON nsw_vg_raw.ps_row_d_source(ps_row_d_id);
CREATE INDEX idx_ps_row_d_source_b ON nsw_vg_raw.ps_row_d_source(source_id);

