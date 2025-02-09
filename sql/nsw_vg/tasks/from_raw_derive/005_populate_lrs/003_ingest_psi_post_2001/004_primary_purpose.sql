--
-- ## Ingest Primary Purpose
--

SET session_replication_role = 'replica';

INSERT INTO nsw_lrs.property_primary_purpose(
    source_id,
    effective_date,
    primary_purpose_id,
    property_id,
    strata_lot_number)
SELECT DISTINCT ON (effective_date, property_id, strata_lot_number)
    b_source_id,
    effective_date,
    primary_purpose_id,
    property_id,
    strata_lot_number
  FROM nsw_vg_raw.ps_row_b_complementary
  LEFT JOIN nsw_vg_raw.ps_row_b b USING (b_source_id)
  LEFT JOIN nsw_lrs.primary_purpose USING (primary_purpose)
  WHERE b.primary_purpose IS NOT NULL
    AND property_id IS NOT NULL
    AND canonical;

SET session_replication_role = 'origin';
SELECT meta.check_constraints('nsw_lrs', 'property_primary_purpose');


