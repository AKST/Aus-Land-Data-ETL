--
-- ## Ingest Property Description
--

SET session_replication_role = 'replica';

WITH
  aggregated AS (
    SELECT (ARRAY_AGG(c_source_id ORDER BY position))[1] AS c_source_id,
           file_source_id,
           sale_counter,
           STRING_AGG(property_description, '' ORDER BY position) AS full_desc
      FROM nsw_vg_raw.ps_row_c
      WHERE property_description IS NOT NULL AND sale_counter IS NOT NULL
      GROUP BY file_source_id, sale_counter),

  consolidated_property_description_c AS (
    SELECT c_source_id, b_source_id, full_desc, b.effective_date
      FROM nsw_vg_raw.ps_row_b_complementary b
      LEFT JOIN nsw_vg_raw.ps_row_b USING (b_source_id)
      JOIN aggregated c USING (file_source_id, sale_counter)
      WHERE c.full_desc IS NOT NULL
        AND b.canonical AND NOT seen_in_land_values)

INSERT INTO nsw_lrs.legal_description(
  source_id,
  effective_date,
  property_id,
  strata_lot_number,
  legal_description,
  legal_description_id,
  legal_description_kind
)
SELECT DISTINCT ON (effective_date, property_id, strata_lot_number)
       c_source_id,
       effective_date,
       property_id,
       strata_lot_number,
       full_desc,
       uuid_generate_v4(),
       (case
         when date_provided > '2004-08-17' then '> 2004-08-17'
         else 'initial'
       end)::nsw_lrs.legal_description_kind
  FROM consolidated_property_description_c c
  LEFT JOIN nsw_vg_raw.ps_row_b USING (b_source_id)
  ORDER BY effective_date,
           property_id,
           strata_lot_number,
           date_provided DESC;

SET session_replication_role = 'origin';
SELECT meta.check_constraints('nsw_lrs', 'legal_description');

