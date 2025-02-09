INSERT INTO nsw_gnb.locality(locality_name)
SELECT * FROM
   (SELECT DISTINCT upper(locality_name) as locality_name
      FROM nsw_vg_raw.ps_row_b) as t
  WHERE t.locality_name IS NOT NULL
     ON CONFLICT (locality_name) DO NOTHING;

INSERT INTO nsw_gnb.street (street_name, locality_id)
SELECT DISTINCT t.street_name, l.locality_id FROM
   (SELECT DISTINCT
           upper(street_name) as street_name,
           upper(locality_name) as locality_name
      FROM nsw_vg_raw.ps_row_b) as t
  LEFT JOIN nsw_gnb.locality l USING (locality_name)
  WHERE t.street_name IS NOT NULL
     ON CONFLICT (street_name, locality_id) DO NOTHING;

WITH sourced_addresses AS (
  SELECT DISTINCT ON (property_id, strata_lot_number, effective_date)
         b_source_id as source_id,
         property_id, property_name,
         strata_lot_number, unit_number,
         house_number as street_number,
         upper(street_name) as street_name,
         upper(locality_name) as locality_name,
         COALESCE(contract_date, settlement_date) as effective_date,
         postcode
    FROM nsw_vg_raw.ps_row_b as r
    WHERE property_id IS NOT NULL
    ORDER BY property_id, strata_lot_number, effective_date, date_provided DESC)

INSERT INTO nsw_gnb.address (
  source_id,
  effective_date,
  property_id,
  strata_lot_number,
  property_name,
  unit_number,
  street_number,
  street_id,
  locality_id,
  postcode)
SELECT source_id,
       effective_date,
       property_id,
       strata_lot_number,
       property_name,
       unit_number,
       street_number,
       street_id,
       locality_id,
       postcode
  FROM sourced_addresses
  LEFT JOIN nsw_gnb.locality l USING (locality_name)
  LEFT JOIN nsw_gnb.street s USING (street_name, locality_id);

