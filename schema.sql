--Creating schema

CREATE SCHEMA IF NOT EXISTS carbon;


-- creating Regions table

CREATE TABLE IF NOT EXISTS carbon.dim_region (
    regionid    INTEGER         PRIMARY KEY,
    shortname   VARCHAR(150)    NOT NULL,
    dno         VARCHAR(150)
);

-- creating dates table

CREATE TABLE IF NOT EXISTS carbon.dim_date (
    date_recorded   DATE        PRIMARY KEY,
    year            INT         NOT NULL,
    quarter         INT         NOT NULL,
    month           INT         NOT NULL,
    month_name      VARCHAR(20)     NOT NULL,
    week            INT         NOT NULL,
    day_of_week     VARCHAR(20)     NOT NULL,
    is_weekend       BOOLEAN     NOT NULL
    
);

-- creating Carbon Intensity Index table for category label reference
CREATE TABLE IF NOT EXISTS carbon.dim_index (
    index_id        INTEGER         PRIMARY KEY,
    index_label     VARCHAR(20)     NOT NULL
);


-- creating the fact table(core daily measurements)
CREATE TABLE IF NOT EXISTS carbon.fact_historical_averages (
    id              SERIAL      PRIMARY KEY,
    regionid        INT         NOT NULL REFERENCES carbon.dim_region(regionid),
    date_recorded   DATE        NOT NULL REFERENCES carbon.dim_date(date_recorded),
    index_id        INT         REFERENCES carbon.dim_index(index_id),
    intensity_avg   DECIMAL(8,2),
    fuel_biomass    DECIMAL(6,2),
    fuel_coal       DECIMAL(6,2),
    fuel_gas        DECIMAL(6,2),
    fuel_hydro      DECIMAL(6,2),
    fuel_imports    DECIMAL(6,2),
    fuel_nuclear    DECIMAL(6,2),
    fuel_other      DECIMAL(6,2),
    fuel_solar      DECIMAL(6,2),
    fuel_wind       DECIMAL(6,2),
    UNIQUE (regionid, date_recorded) -- to prevent duplicate entries for the same region and date
);

-- creating Indexes for faster queries
CREATE INDEX IF NOT EXISTS idx_fact_date
    ON carbon.fact_historical_averages (date_recorded);

CREATE INDEX IF NOT EXISTS idx_fact_region
    ON carbon.fact_historical_averages (regionid);

CREATE INDEX IF NOT EXISTS idx_fact_index
    ON carbon.fact_historical_averages (index_id);

CREATE INDEX IF NOT EXISTS idx_date_year_month
    ON carbon.dim_date (year, month);