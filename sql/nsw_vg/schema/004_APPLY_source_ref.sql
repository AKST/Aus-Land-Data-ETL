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

CREATE TABLE nsw_vg_raw.land_value_row_complement_p0 PARTITION OF nsw_vg_raw.land_value_row_complement
    FOR VALUES WITH (MODULUS 8, REMAINDER 0);
CREATE TABLE nsw_vg_raw.land_value_row_complement_p1 PARTITION OF nsw_vg_raw.land_value_row_complement
    FOR VALUES WITH (MODULUS 8, REMAINDER 1);
CREATE TABLE nsw_vg_raw.land_value_row_complement_p2 PARTITION OF nsw_vg_raw.land_value_row_complement
    FOR VALUES WITH (MODULUS 8, REMAINDER 2);
CREATE TABLE nsw_vg_raw.land_value_row_complement_p3 PARTITION OF nsw_vg_raw.land_value_row_complement
    FOR VALUES WITH (MODULUS 8, REMAINDER 3);
CREATE TABLE nsw_vg_raw.land_value_row_complement_p4 PARTITION OF nsw_vg_raw.land_value_row_complement
    FOR VALUES WITH (MODULUS 8, REMAINDER 4);
CREATE TABLE nsw_vg_raw.land_value_row_complement_p5 PARTITION OF nsw_vg_raw.land_value_row_complement
    FOR VALUES WITH (MODULUS 8, REMAINDER 5);
CREATE TABLE nsw_vg_raw.land_value_row_complement_p6 PARTITION OF nsw_vg_raw.land_value_row_complement
    FOR VALUES WITH (MODULUS 8, REMAINDER 6);
CREATE TABLE nsw_vg_raw.land_value_row_complement_p7 PARTITION OF nsw_vg_raw.land_value_row_complement
    FOR VALUES WITH (MODULUS 8, REMAINDER 7);

