--
-- # Observations and Events
--
-- ## legal_description
--
-- We don't have access to the on going history of the property
-- description of these properties, observed property descriptions
-- at different points in time, so it makes sense to treat this
-- as event.
--
-- Representing this as an event allows capture of this information over time.

CREATE TYPE nsw_lrs.legal_description_kind AS ENUM ('initial', '> 2004-08-17');

CREATE TABLE IF NOT EXISTS nsw_lrs.legal_description (
  LIKE meta.event INCLUDING ALL,
  legal_description_id UUID NOT NULL,
  legal_description TEXT NOT NULL,
  legal_description_kind nsw_lrs.legal_description_kind NOT NULL,
  property_id INT NOT NULL,
  strata_lot_number INT,

  normalised_property_id nsw_lrs.normalised_property_id
    GENERATED ALWAYS AS
    (ROW(property_id, COALESCE(strata_lot_number, -1))::nsw_lrs.normalised_property_id)
    STORED,

  FOREIGN KEY (property_id) REFERENCES nsw_lrs.property(property_id),
  UNIQUE (effective_date, property_id, strata_lot_number)

) PARTITION BY HASH (property_id);

CREATE INDEX legal_description_id_idx
    ON nsw_lrs.legal_description (legal_description_id);
CREATE INDEX idx_property_id_legal_description
    ON nsw_lrs.legal_description(property_id);
CREATE INDEX idx_normalised_property_id_legal_description
    ON nsw_lrs.legal_description(normalised_property_id);
CREATE INDEX idx_effective_date_legal_description
    ON nsw_lrs.legal_description(effective_date DESC);

CREATE TABLE IF NOT EXISTS nsw_lrs.archived_legal_description (
  LIKE meta.event INCLUDING ALL,
  legal_description_id BIGSERIAL PRIMARY KEY,
  legal_description TEXT NOT NULL,
  property_id INT NOT NULL,

  FOREIGN KEY (property_id) REFERENCES nsw_lrs.property(property_id),
  UNIQUE (property_id, effective_date)
);

--
-- ## Property Area
--
-- I think it's possible this could change over time due
-- to how cadastra stuff works.
--

CREATE TABLE IF NOT EXISTS nsw_lrs.property_area (
  LIKE meta.event INCLUDING ALL,
  property_id INT NOT NULL,
  strata_lot_number INT,
  sqm_area FLOAT NOT NULL,

  normalised_property_id nsw_lrs.normalised_property_id
    GENERATED ALWAYS AS
    (ROW(property_id, COALESCE(strata_lot_number, -1))::nsw_lrs.normalised_property_id)
    STORED,

  UNIQUE (property_id, strata_lot_number, effective_date),
  FOREIGN KEY (property_id) REFERENCES nsw_lrs.property(property_id)
);

CREATE INDEX idx_property_id_property_area
    ON nsw_lrs.property_area(property_id);

CREATE INDEX idx_normalised_property_id_property_area
    ON nsw_lrs.property_area(normalised_property_id);

CREATE INDEX idx_effective_date_property_area
    ON nsw_lrs.property_area(effective_date DESC);

--
-- ## Has Strata Plan
--

CREATE TABLE IF NOT EXISTS nsw_lrs.property_under_strata_plan(
  LIKE meta.event INCLUDING ALL,
  property_id INT NOT NULL,
  under_strata_plan BOOL NOT NULL,

  UNIQUE (property_id, effective_date),
  FOREIGN KEY (property_id) REFERENCES nsw_lrs.property(property_id)
);

--
-- ## Nature of Property
--

CREATE TABLE IF NOT EXISTS nsw_lrs.nature_of_property(
  LIKE meta.event INCLUDING ALL,
  property_id INT NOT NULL,
  nature_of_property nsw_lrs.property_nature NOT NULL,
  strata_lot_number INT,

  normalised_property_id nsw_lrs.normalised_property_id
    GENERATED ALWAYS AS
    (ROW(property_id, COALESCE(strata_lot_number, -1))::nsw_lrs.normalised_property_id)
    STORED,

  UNIQUE (property_id, effective_date, strata_lot_number),
  FOREIGN KEY (property_id) REFERENCES nsw_lrs.property(property_id)
);

CREATE INDEX idx_property_id_nature_of_property
    ON nsw_lrs.property_area(property_id);

CREATE INDEX idx_normalised_property_id_nature_of_property
    ON nsw_lrs.property_area(normalised_property_id);

--
-- ## Primary Purpose
--

CREATE TABLE IF NOT EXISTS nsw_lrs.property_primary_purpose(
  LIKE meta.event INCLUDING ALL,
  primary_purpose_id INT NOT NULL,
  property_id INT NOT NULL,
  strata_lot_number INT,

  normalised_property_id nsw_lrs.normalised_property_id
    GENERATED ALWAYS AS
    (ROW(property_id, COALESCE(strata_lot_number, -1))::nsw_lrs.normalised_property_id)
    STORED,

  UNIQUE (property_id, effective_date, strata_lot_number),
  FOREIGN KEY (primary_purpose_id) REFERENCES nsw_lrs.primary_purpose(primary_purpose_id),
  FOREIGN KEY (property_id) REFERENCES nsw_lrs.property(property_id)
);

CREATE INDEX idx_property_id_property_primary_purpose
    ON nsw_lrs.property_area(property_id);

CREATE INDEX idx_normalised_property_id_property_primary_purpose
    ON nsw_lrs.property_area(normalised_property_id);

--
-- ## Record of Zone
--

CREATE TABLE IF NOT EXISTS nsw_lrs.zone_observation(
  LIKE meta.event INCLUDING ALL,
  property_id INT NOT NULL,
  zone_code varchar(4) NOT NULL,
  UNIQUE (property_id, effective_date),
  FOREIGN KEY (property_id) REFERENCES nsw_lrs.property(property_id)
);

CREATE INDEX idx_effecitve_date_zone_observation
    ON nsw_lrs.zone_observation(effective_date DESC);

--
-- ## Described Dimensions
--
-- These are descriptions of dimensions, mostly found
-- in older property sales data (as of writing this).
--

CREATE TABLE IF NOT EXISTS nsw_lrs.described_dimensions (
  LIKE meta.event INCLUDING ALL,
  property_id INT NOT NULL,
  dimension_description TEXT NOT NULL,
  UNIQUE (property_id, dimension_description, effective_date),

  FOREIGN KEY (property_id) REFERENCES nsw_lrs.property(property_id)
);

--
-- ## Notice of Sale
--

CREATE TABLE IF NOT EXISTS nsw_lrs.notice_of_sale (
  LIKE meta.event INCLUDING ALL,
  notice_of_sale_id BIGSERIAL PRIMARY KEY,
  property_id int NOT NULL,
  purchase_price FLOAT,
  contract_date DATE,
  strata_lot_number INT,
  dealing_number varchar(10) NOT NULL,
  settlement_date DATE,
  interest_of_sale INT,
  sale_participants nsw_lrs.sale_participant[] NOT NULL,

  -- what are these? I don't know!
  sale_code varchar(3),
  comp_code varchar(3),

  normalised_property_id nsw_lrs.normalised_property_id
    GENERATED ALWAYS AS
    (ROW(property_id, COALESCE(strata_lot_number, -1))::nsw_lrs.normalised_property_id)
    STORED,

  UNIQUE (dealing_number, property_id, strata_lot_number),
  FOREIGN KEY (property_id) REFERENCES nsw_lrs.property(property_id)
);

CREATE INDEX idx_property_id_notice_of_sale
    ON nsw_lrs.property_area(property_id);

CREATE INDEX idx_normalised_property_id_notice_of_sale
    ON nsw_lrs.property_area(normalised_property_id);

CREATE TABLE IF NOT EXISTS nsw_lrs.notice_of_sale_archived (
  LIKE meta.event INCLUDING ALL,
  notice_of_sale_archived_id BIGSERIAL PRIMARY KEY,
  property_id int NOT NULL,
  purchase_price FLOAT,
  contract_date DATE,

  -- what is this? I don't know!
  valuation_number varchar(16),
  comp_code varchar(3),

  UNIQUE (property_id, contract_date, purchase_price),
  FOREIGN KEY (property_id) REFERENCES nsw_lrs.property(property_id)
);

