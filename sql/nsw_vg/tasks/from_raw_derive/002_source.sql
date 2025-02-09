--
-- # Establish source for Land Values
-- ## Requires
--
-- 1. Populating the contents of meta.file_source
-- 2. Generating ids for the source table
--


SET session_replication_role = 'replica';

INSERT INTO nsw_vg_raw.land_value_row_complement(property_id, source_date, effective_date, source_id)
  SELECT property_id,
         source_date,
         COALESCE(base_date_1, source_date),
         uuid_generate_v4()
  FROM nsw_vg_raw.land_value_row;

INSERT INTO meta.source(source_id) SELECT source_id FROM nsw_vg_raw.land_value_row_complement;

CREATE TEMP TABLE pg_temp.lv_uningested_files AS
  WITH unique_files AS (
    SELECT DISTINCT ON (source_file_name) source_file_name, source_date
    FROM nsw_vg_raw.land_value_row_complement
    LEFT JOIN nsw_vg_raw.land_value_row USING (property_id, source_date))
  SELECT *, uuid_generate_v4() AS file_source_id
  FROM unique_files;

INSERT INTO meta.file_source(file_source_id, file_path, date_recorded, date_published)
  SELECT file_source_id, source_file_name, CURRENT_DATE, source_date
  FROM pg_temp.lv_uningested_files;

INSERT INTO meta.source_file_line(source_id, file_source_id, source_file_line)
  SELECT source_id, file_source_id, source_line_number
  FROM nsw_vg_raw.land_value_row_complement
  LEFT JOIN nsw_vg_raw.land_value_row USING (property_id, source_date)
  JOIN pg_temp.lv_uningested_files USING (source_file_name);

DROP TABLE pg_temp.lv_uningested_files;

--
-- # Ingest PSI
--
-- ## Create Sources
--

INSERT INTO meta.source(source_id) SELECT a_legacy_source_id FROM nsw_vg_raw.ps_row_a_legacy;
INSERT INTO meta.source(source_id) SELECT b_legacy_source_id FROM nsw_vg_raw.ps_row_b_legacy;
INSERT INTO meta.source(source_id) SELECT a_source_id FROM nsw_vg_raw.ps_row_a;
INSERT INTO meta.source(source_id) SELECT b_source_id FROM nsw_vg_raw.ps_row_b;
INSERT INTO meta.source(source_id) SELECT c_source_id FROM nsw_vg_raw.ps_row_c;
INSERT INTO meta.source(source_id) SELECT d_source_id FROM nsw_vg_raw.ps_row_d;

--
-- ## Create File Source
--

INSERT INTO meta.file_source(file_source_id, file_path, date_recorded, date_published)
  SELECT file_source_id, file_path, CURRENT_DATE, date_provided
  FROM nsw_vg_raw.ps_row_a_legacy;

INSERT INTO meta.file_source(file_source_id, file_path, date_recorded, date_published)
  SELECT file_source_id, file_path, CURRENT_DATE, date_provided
  FROM nsw_vg_raw.ps_row_a;

--
-- ## Create Position Entries
--

INSERT INTO meta.source_byte_position(source_id, file_source_id, source_byte_position)
  SELECT a_legacy_source_id, file_source_id, position
  FROM nsw_vg_raw.ps_row_a_legacy;

INSERT INTO meta.source_byte_position(source_id, file_source_id, source_byte_position)
  SELECT b_legacy_source_id, file_source_id, position
  FROM nsw_vg_raw.ps_row_b_legacy;

INSERT INTO meta.source_byte_position(source_id, file_source_id, source_byte_position)
  SELECT a_source_id, file_source_id, position
  FROM nsw_vg_raw.ps_row_a;

INSERT INTO meta.source_byte_position(source_id, file_source_id, source_byte_position)
  SELECT b_source_id, file_source_id, position
  FROM nsw_vg_raw.ps_row_b;

INSERT INTO meta.source_byte_position(source_id, file_source_id, source_byte_position)
  SELECT c_source_id, file_source_id, position
  FROM nsw_vg_raw.ps_row_c;

INSERT INTO meta.source_byte_position(source_id, file_source_id, source_byte_position)
  SELECT d_source_id, file_source_id, position
  FROM nsw_vg_raw.ps_row_d;

--
-- # End
--

SET session_replication_role = 'origin';

SELECT meta.check_constraints('nsw_vg_raw', 'land_value_row_complement');
