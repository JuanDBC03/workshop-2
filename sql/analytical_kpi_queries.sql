-- ============================================================
-- WORKSHOP 2: ANALYTICAL QUERIES & KPI FORMULAS FOR POWER BI
-- Database: music_dw
-- Queries directly support REQ-01, REQ-02, and REQ-03
-- ============================================================

-- ------------------------------------------------------------
-- KPI / REQ-01: Temporal Evolution of Historical Winners' Commercial Success
-- Business Question: How is current Spotify popularity distributed across ceremony decades?
-- ------------------------------------------------------------
SELECT 
    t.decade,
    t.era_name,
    COUNT(DISTINCT f.track_id) AS awarded_tracks_catalog,
    ROUND(AVG(f.popularity), 2) AS avg_spotify_popularity,
    MAX(f.popularity) AS peak_popularity,
    MIN(f.popularity) AS min_popularity
FROM fact_track_performance f
JOIN dim_ceremony_time t ON f.time_id = t.time_id
GROUP BY t.decade, t.era_name
ORDER BY t.decade;

-- ------------------------------------------------------------
-- KPI / REQ-02: Acoustic Behavior in Pop and Urban Genres
-- Business Question: What acoustic gap (energy, speechiness) exists between Grammy winners and non-winners?
-- ------------------------------------------------------------
SELECT 
    g.macro_category AS genre_cluster,
    CASE WHEN a.is_grammy_winner THEN 'Grammy Winner' ELSE 'Non-Winner' END AS award_status,
    COUNT(DISTINCT f.track_id) AS total_evaluated_tracks,
    ROUND(AVG(f.energy), 4) AS avg_energy,
    ROUND(AVG(f.speechiness), 4) AS avg_speechiness,
    ROUND(AVG(f.danceability), 4) AS avg_danceability,
    ROUND(AVG(f.valence), 4) AS avg_valence
FROM fact_track_performance f
JOIN dim_genre g ON f.genre_id = g.genre_id
JOIN dim_artist a ON f.artist_id = a.artist_id
WHERE g.macro_category IN ('Pop', 'Urban')
GROUP BY g.macro_category, a.is_grammy_winner
ORDER BY g.macro_category, a.is_grammy_winner DESC;

-- ------------------------------------------------------------
-- KPI / REQ-03: Genre Diversity in Multi-Awarded Artists
-- Business Question: Do artists with multiple Grammy wins explore more genres in Spotify?
-- ------------------------------------------------------------
WITH artist_genre_counts AS (
    SELECT 
        f.artist_id,
        COUNT(DISTINCT f.genre_id) AS distinct_genres_count
    FROM fact_track_performance f
    GROUP BY f.artist_id
)
SELECT 
    CASE 
        WHEN a.grammy_awards_count >= 3 THEN 'Highly Awarded (>=3 Wins)'
        WHEN a.grammy_awards_count BETWEEN 1 AND 2 THEN 'Single/Dual Award (1-2 Wins)'
        ELSE 'Non-Winners (0 Wins)'
    END AS award_tier,
    COUNT(DISTINCT a.artist_id) AS artist_population,
    ROUND(AVG(sub.distinct_genres_count), 2) AS avg_genres_explored,
    MAX(sub.distinct_genres_count) AS max_genres_explored
FROM dim_artist a
JOIN artist_genre_counts sub ON a.artist_id = sub.artist_id
GROUP BY 
    CASE 
        WHEN a.grammy_awards_count >= 3 THEN 'Highly Awarded (>=3 Wins)'
        WHEN a.grammy_awards_count BETWEEN 1 AND 2 THEN 'Single/Dual Award (1-2 Wins)'
        ELSE 'Non-Winners (0 Wins)'
    END
ORDER BY avg_genres_explored DESC;
