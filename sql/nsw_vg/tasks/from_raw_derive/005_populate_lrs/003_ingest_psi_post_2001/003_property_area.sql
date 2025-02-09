--
-- ## Ingest Property Area
--

SET session_replication_role = 'replica';

INSERT INTO nsw_lrs.property_area(
    source_id,
    effective_date,
    property_id,
    strata_lot_number,
    sqm_area)
SELECT DISTINCT ON (effective_date, property_id, strata_lot_number)
    b_source_id,
    effective_date,
    property_id,
    strata_lot_number,
    pg_temp.sqm_area(area, area_type)
  FROM nsw_vg_raw.ps_row_b_complementary
  LEFT JOIN nsw_vg_raw.ps_row_b USING (property_id, b_source_id)
  WHERE pg_temp.sqm_area(area, area_type) IS NOT NULL
    AND NOT seen_in_land_values AND canonical
  ORDER BY effective_date,
           property_id,
           strata_lot_number,
           date_provided DESC;

SET session_replication_role = 'origin';
SELECT meta.check_constraints('nsw_lrs', 'property_area');

