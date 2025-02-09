--
-- ## Ingest Nature of property
--

SET session_replication_role = 'replica';

INSERT INTO nsw_lrs.nature_of_property(
    source_id,
    effective_date,
    property_id,
    nature_of_property,
    strata_lot_number)
SELECT DISTINCT ON (effective_date, property_id, strata_lot_number)
    b_source_id,
    effective_date,
    property_id,
    (CASE
      WHEN nature_of_property = 'V' THEN 'Vaccant'
      WHEN nature_of_property = 'R' THEN 'Residence'

      -- do not ask me why they use 3 for this.
      WHEN nature_of_property = '3' THEN 'Other'
    END)::nsw_lrs.property_nature,
    strata_lot_number
  FROM nsw_vg_raw.ps_row_b_complementary
  LEFT JOIN nsw_vg_raw.ps_row_b USING (b_source_id)
  WHERE nature_of_property IS NOT NULL
    AND property_id IS NOT NULL
    AND canonical;

SET session_replication_role = 'origin';
SELECT meta.check_constraints('nsw_lrs', 'nature_of_property');

