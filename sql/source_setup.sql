-- ============================================================
-- WORKSHOP 2: OPERATIONAL SOURCE SETUP FOR GRAMMY AWARDS
-- Database: grammy_source
-- Purpose: Pre-populates relational database for the Grammy Awards.
--          Airflow's extract_grammys task extracts from this table,
--          NEVER directly from the raw CSV file.
-- ============================================================

DROP TABLE IF EXISTS the_grammy_awards;

CREATE TABLE the_grammy_awards (
    id SERIAL PRIMARY KEY,
    year INT NOT NULL,
    title VARCHAR(255),
    published_at VARCHAR(50),
    updated_at VARCHAR(50),
    category VARCHAR(255),
    nominee VARCHAR(500),
    artist VARCHAR(500),
    workers TEXT,
    img TEXT,
    winner BOOLEAN NOT NULL
);

-- Copy data from mounted CSV into the relational source table
\copy the_grammy_awards(year, title, published_at, updated_at, category, nominee, artist, workers, img, winner) FROM '/data/the_grammy_awards.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',');

-- Reconciliation check: confirm that all records were imported
SELECT COUNT(*) AS total_imported_records FROM the_grammy_awards;
