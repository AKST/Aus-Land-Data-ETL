--
-- # Ingest Legacy PSI from sourced tables
--
-- ## Ingest Property Sales
--

INSERT INTO nsw_lrs.notice_of_sale_archived(
       source_id, effective_date, property_id, purchase_price,
       contract_date, valuation_number, comp_code)

SELECT b_legacy_source_id, effective_date, property_id, purchase_price,
       contract_date, valuation_number, comp_code
  FROM nsw_vg_raw.ps_row_b_legacy_complementary
  LEFT JOIN nsw_vg_raw.ps_row_b_legacy USING (b_legacy_source_id)
  WHERE property_id IS NOT NULL
    AND canonical;

--
-- ## Ingest legal description
--

INSERT INTO nsw_lrs.archived_legal_description(source_id, effective_date, property_id, legal_description)
SELECT b_legacy_source_id, effective_date, property_id, land_description
  FROM nsw_vg_raw.ps_row_b_legacy_complementary
  LEFT JOIN nsw_vg_raw.ps_row_b_legacy USING (b_legacy_source_id)
  WHERE land_description IS NOT NULL
    AND property_id IS NOT NULL
    AND canonical;

--
-- ## Ingest property_area
--

INSERT INTO nsw_lrs.property_area(source_id, effective_date, property_id, sqm_area)
SELECT b_legacy_source_id, effective_date, property_id, pg_temp.sqm_area(area, area_type)
  FROM nsw_vg_raw.ps_row_b_legacy_complementary
  LEFT JOIN nsw_vg_raw.ps_row_b_legacy USING (b_legacy_source_id)
  WHERE pg_temp.sqm_area(area, area_type) IS NOT NULL
    AND property_id IS NOT NULL
    AND NOT seen_in_modern_psi
    AND canonical;

--
-- ## Ingest dimensions
--

INSERT INTO nsw_lrs.described_dimensions(source_id, effective_date, property_id, dimension_description)
SELECT b_legacy_source_id, effective_date, property_id, dimensions
  FROM nsw_vg_raw.ps_row_b_legacy_complementary
  LEFT JOIN nsw_vg_raw.ps_row_b_legacy USING (b_legacy_source_id)
  WHERE property_id IS NOT NULL
    AND dimensions IS NOT NULL
    AND canonical;

