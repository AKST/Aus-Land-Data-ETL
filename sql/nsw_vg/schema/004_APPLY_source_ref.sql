CREATE TABLE IF NOT EXISTS nsw_vg_raw.land_value_row_complement(
    property_id INT NOT NULL,
    source_date DATE NOT NULL,
    effective_date DATE NOT NULL,
    source_id UUID NOT NULL,
    FOREIGN KEY (source_id) REFERENCES meta.source(source_id)
) PARTITION BY HASH (property_id);

CREATE INDEX idx_land_value_row_complement_a
    ON nsw_vg_raw.land_value_row_complement(property_id, source_id);
CREATE INDEX idx_land_value_row_complement_b
    ON nsw_vg_raw.land_value_row_complement(source_id);
CREATE INDEX idx_land_value_row_complement_c
    ON nsw_vg_raw.land_value_row_complement(effective_date DESC);
