CREATE OR REPLACE FUNCTION pg_temp.sqm_area(
    area FLOAT,
    area_unit VARCHAR(1)
) RETURNS FLOAT AS $$
    SELECT CASE area_unit
        WHEN 'H' THEN area * 10000
        WHEN 'M' THEN area
        ELSE NULL
    END;
$$ LANGUAGE sql PARALLEL SAFE;

--
-- # Init Temp tables
--

WITH
  with_baseline_information AS (
    SELECT *,
           COALESCE(contract_date, settlement_date, date_provided) as effective_date
      FROM nsw_vg_raw.ps_row_b
      WHERE property_id IS NOT NULL
        AND sale_counter IS NOT NULL
        -- TODO document what's going on here.
        AND length(dealing_number) > 1),

  --
  -- Rank the contents of `pg_temp.sourced_raw_property_sales_b`
  -- to prioritise the most complete and up to date rows.
  --
  with_rank AS (
    SELECT b_source_id,
           ROW_NUMBER() OVER (
             PARTITION BY dealing_number, property_id, strata_lot_number
             ORDER BY (
               (CASE WHEN contract_date IS NOT NULL THEN 1 ELSE 0 END) +
               (CASE WHEN settlement_date IS NOT NULL THEN 1 ELSE 0 END) +
               (CASE WHEN purchase_price IS NOT NULL THEN 1 ELSE 0 END) +
               (CASE WHEN area_type IS NOT NULL THEN 1 ELSE 0 END) +
               (CASE WHEN area IS NOT NULL THEN 1 ELSE 0 END)) DESC,
             date_provided DESC
           ) AS score
      FROM with_baseline_information)

INSERT INTO nsw_vg_raw.ps_row_b_complementary(
  b_source_id,
  effective_date,
  seen_in_land_values,
  canonical)
SELECT b_source_id,
       effective_date,
       (e.source_id IS NOT NULL),
       (r.score = 1)
  FROM with_rank r
  LEFT JOIN with_baseline_information AS b USING (b_source_id)
  LEFT JOIN nsw_vg_raw.land_value_row_complement AS e USING (property_id, effective_date);

--
-- # Init Temp tables
--

WITH
  canonical AS (
    SELECT DISTINCT ON (property_id, effective_date)
           b_legacy_source_id,
           property_id,
           contract_date as effective_date,
           ROW_NUMBER() OVER (
              PARTITION BY property_id, contract_date
              ORDER BY a.date_provided DESC
           ) AS rank
      FROM nsw_vg_raw.ps_row_b_legacy
      LEFT JOIN nsw_vg_raw.ps_row_a_legacy a USING (file_source_id)),

  relevant_modern_psi AS (
    SELECT DISTINCT ON (property_id, effective_date)
           property_id, effective_date, b_source_id
      FROM nsw_vg_raw.ps_row_b
      LEFT JOIN nsw_vg_raw.ps_row_b_complementary USING (b_source_id)
      WHERE strata_lot_number IS NULL)

INSERT INTO nsw_vg_raw.ps_row_b_legacy_complementary(b_legacy_source_id, effective_date, seen_in_modern_psi, canonical)
SELECT b_legacy_source_id, r.effective_date, (m.b_source_id IS NOT NULL), (r.rank = 1)
  FROM canonical r
  LEFT JOIN relevant_modern_psi m USING (property_id, effective_date);
