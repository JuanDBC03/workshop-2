-- ============================================================
-- WORKSHOP 2: DATA WAREHOUSE DIMENSIONAL SCHEMA (STAR SCHEMA)
-- Target Database: music_dw
-- Purpose: Defines dimensions and facts supporting REQ-01, REQ-02, and REQ-03.
-- Architecture: Star Schema with surrogate keys and referential integrity.
-- ============================================================

-- Drop tables in reverse order of foreign key dependencies
DROP SCHEMA IF EXISTS public CASCADE;
CREATE SCHEMA public;

-- ------------------------------------------------------------
-- 1. DIMENSION: dim_ceremony_time
-- Supports REQ-01: Temporal aggregation by award year and decade
-- ------------------------------------------------------------
CREATE TABLE dim_ceremony_time (
    time_id SERIAL PRIMARY KEY,
    ceremony_year INT UNIQUE NOT NULL,
    decade VARCHAR(20) NOT NULL,
    era_name VARCHAR(50) NOT NULL
);

-- ------------------------------------------------------------
-- 2. DIMENSION: dim_genre
-- Supports REQ-02 and REQ-03: Genre filtering and diversity analysis
-- ------------------------------------------------------------
CREATE TABLE dim_genre (
    genre_id SERIAL PRIMARY KEY,
    genre_name VARCHAR(100) UNIQUE NOT NULL,
    macro_category VARCHAR(100) NOT NULL
);

-- ------------------------------------------------------------
-- 3. DIMENSION: dim_artist
-- Supports REQ-02 and REQ-03: Winner status and cumulative awards count
-- ------------------------------------------------------------
CREATE TABLE dim_artist (
    artist_id SERIAL PRIMARY KEY,
    artist_name TEXT UNIQUE NOT NULL,
    is_grammy_winner BOOLEAN NOT NULL DEFAULT FALSE,
    grammy_awards_count INT NOT NULL DEFAULT 0,
    grammy_categories_count INT NOT NULL DEFAULT 0
);

-- ------------------------------------------------------------
-- 4. DIMENSION: dim_track
-- Grain: One record per unique Spotify audio track
-- ------------------------------------------------------------
CREATE TABLE dim_track (
    track_id VARCHAR(100) PRIMARY KEY,
    track_name TEXT NOT NULL,
    album_name TEXT,
    explicit BOOLEAN NOT NULL DEFAULT FALSE,
    duration_ms INT NOT NULL DEFAULT 0,
    is_grammy_nominee BOOLEAN NOT NULL DEFAULT FALSE,
    grammy_category TEXT
);

-- ------------------------------------------------------------
-- 5. FACT TABLE: fact_track_performance
-- Grain: One track performance observation per genre
-- Measures: Streaming popularity and algorithmic acoustic features
-- ------------------------------------------------------------
CREATE TABLE fact_track_performance (
    performance_id SERIAL PRIMARY KEY,
    track_id VARCHAR(100) NOT NULL REFERENCES dim_track(track_id) ON DELETE CASCADE,
    artist_id INT NOT NULL REFERENCES dim_artist(artist_id) ON DELETE CASCADE,
    genre_id INT NOT NULL REFERENCES dim_genre(genre_id) ON DELETE CASCADE,
    time_id INT REFERENCES dim_ceremony_time(time_id) ON DELETE SET NULL,
    
    -- Numerical measures
    popularity INT NOT NULL CHECK (popularity >= 0 AND popularity <= 100),
    danceability NUMERIC(6,4) CHECK (danceability >= 0 AND danceability <= 1),
    energy NUMERIC(6,4) CHECK (energy >= 0 AND energy <= 1),
    speechiness NUMERIC(6,4) CHECK (speechiness >= 0 AND speechiness <= 1),
    acousticness NUMERIC(6,4) CHECK (acousticness >= 0 AND acousticness <= 1),
    instrumentalness NUMERIC(6,4) CHECK (instrumentalness >= 0 AND instrumentalness <= 1),
    liveness NUMERIC(6,4) CHECK (liveness >= 0 AND liveness <= 1),
    valence NUMERIC(6,4) CHECK (valence >= 0 AND valence <= 1),
    loudness NUMERIC(7,3),
    tempo NUMERIC(7,3),
    
    -- Unique constraint for idempotent loads (safe rerun)
    CONSTRAINT uq_track_genre UNIQUE (track_id, genre_id)
);

-- Create indexes to optimize analytical BI queries
CREATE INDEX idx_fact_artist ON fact_track_performance(artist_id);
CREATE INDEX idx_fact_genre ON fact_track_performance(genre_id);
CREATE INDEX idx_fact_time ON fact_track_performance(time_id);
CREATE INDEX idx_fact_popularity ON fact_track_performance(popularity);
