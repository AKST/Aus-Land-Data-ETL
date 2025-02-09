--
-- # Ingest PSI
--
-- ## Property Sales
--

SET session_replication_role = 'replica';

WITH
  --
  -- Group sale participants by their sale counter (unique to a file)
  -- and file_source_id, and place them in an array containing the different
  -- participant kinds.
  --
  sale_participant_groupings AS (
    SELECT file_source_id, sale_counter, property_id, ARRAY_AGG(d.participant) as participants
      FROM nsw_vg_raw.ps_row_d d
      WHERE property_id IS NOT NULL
        AND sale_counter IS NOT NULL
      GROUP BY file_source_id, sale_counter, property_id),

  --
  -- Whilst reducing the ranked data to rows with a rank of 1, it also
  -- ensures sale participants are linked as well, reducing the need
  -- for an additional join later on.
  --
  with_sale_partipants AS (
    SELECT b_source_id,
           property_id,
           COALESCE(participants, '{}'::nsw_lrs.sale_participant[]) as participants
      FROM nsw_vg_raw.ps_row_b b
      LEFT JOIN sale_participant_groupings USING (file_source_id, sale_counter, property_id))

INSERT INTO nsw_lrs.notice_of_sale(
  source_id, effective_date, property_id, strata_lot_number,
  dealing_number, purchase_price, contract_date, settlement_date,
  interest_of_sale, sale_participants, comp_code, sale_code)
SELECT b_source_id,
       COALESCE(contract_date, settlement_date),
       property_id, strata_lot_number, dealing_number,
       purchase_price, contract_date, settlement_date,
       interest_of_sale, participants, comp_code, sale_code
  FROM nsw_vg_raw.ps_row_b_complementary b
  LEFT JOIN with_sale_partipants p USING (property_id, b_source_id)
  LEFT JOIN nsw_vg_raw.ps_row_b USING (property_id, b_source_id)
  WHERE b.canonical;

SET session_replication_role = 'origin';
SELECT meta.check_constraints('nsw_lrs', 'notice_of_sale');

