--
-- ## Zones
--

SET session_replication_role = 'replica';

INSERT INTO nsw_lrs.zone_observation(
    source_id,
    effective_date,
    property_id,
    zone_code)
SELECT DISTINCT ON (effective_date, property_id)
       b_source_id,
       effective_date,
       property_id,
       zone_code
  FROM nsw_vg_raw.ps_row_b_complementary
  LEFT JOIN nsw_vg_raw.ps_row_b b USING (b_source_id)
  WHERE zone_standard = 'ep&a_2006'
    AND canonical
    AND NOT seen_in_land_values
    AND strata_lot_number IS NULL
  ORDER BY effective_date, property_id, date_provided DESC;

SET session_replication_role = 'origin';
SELECT meta.check_constraints('nsw_lrs', 'zone_observation');

