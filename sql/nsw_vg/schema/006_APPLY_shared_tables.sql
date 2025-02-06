--
-- These districts are a valuer general specific
-- boundary, some of the them are associated with
-- local government areas but not all are.
--
CREATE TABLE IF NOT EXISTS nsw_vg.valuation_district (
  valuation_district_code INT PRIMARY KEY,
  valuation_district_name TEXT
);

CREATE UNIQUE INDEX nsw_vg_valuation_district_unique_name
    ON nsw_vg.valuation_district(valuation_district_name)
    WHERE valuation_district_name IS NOT NULL;

CREATE TABLE IF NOT EXISTS nsw_vg.land_valuation (
  LIKE meta.event INCLUDING ALL,
  valuation_district_code INT NOT NULL,
  property_id INT NOT NULL,

  --
  -- This is the date at which this basis was setup to
  -- valuate the property, not to be comfused with the
  -- date the land valuation for the property occured.
  --
  valuation_base_date DATE NOT NULL,

  --
  -- The valuation method as outlined in the valuation
  -- act, read more here:
  --
  --   https://legislation.nsw.gov.au/view/whole/html/inforce/current/act-1916-002
  --
  -- This is almost never null, however I have found
  -- one occasion where this has occurred.
  --
  valuation_basis varchar(10),

  --
  -- The basis in which the valuation was done. This column
  -- points to the portion of the land valuation act which
  -- specifies under which instrument the land valuation was
  -- initiated.
  --
  --   https://legislation.nsw.gov.au/view/whole/html/inforce/current/act-1916-002
  --
  valuation_authority varchar(9),

  land_value float NOT NULL,

  PRIMARY KEY (property_id, effective_date),
  FOREIGN KEY (property_id) REFERENCES nsw_lrs.property(property_id),
) PARTITION BY HASH (property_id);

CREATE INDEX idx_property_id_land_valuation
    ON nsw_vg.land_valuation(property_id);

CREATE INDEX idx_effective_date_land_valuation
    ON nsw_vg.land_valuation(effective_date DESC);

