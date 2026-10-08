"""
Loading Module: src/load.py
Loads validated prepared data into the PostgreSQL Dimensional Data Warehouse (Star Schema).
Implements a high-performance staging + set-based upsert strategy ensuring safe,
repeatable reruns (idempotency) without duplicate records.
"""

from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text


def load_data_warehouse(prepared_path: str, dw_connection_uri: str) -> dict:
    """
    Loads dimensional tables and fact table into music_dw idempotently.

    Parameters:
        prepared_path: Path to the validated prepared dataset CSV.
        dw_connection_uri: PostgreSQL connection string for music_dw.

    Returns:
        summary: Dictionary recording final row counts for each DW table.
    """
    df = pd.read_csv(prepared_path)
    engine = create_engine(dw_connection_uri)

    with engine.begin() as conn:
        print("[LOAD DW] Beginning set-based idempotent transaction...")

        # ----------------------------------------------------
        # 1. Load dim_ceremony_time (Batch set-based)
        # ----------------------------------------------------
        time_df = (
            df.dropna(subset=["ceremony_year"])[["ceremony_year", "decade", "era_name"]]
            .drop_duplicates(subset=["ceremony_year"])
            .copy()
        )
        time_df["ceremony_year"] = time_df["ceremony_year"].astype(int)

        if not time_df.empty:
            conn.execute(text("CREATE TEMP TABLE stage_time (ceremony_year INT, decade VARCHAR(20), era_name VARCHAR(50)) ON COMMIT DROP;"))
            time_df.to_sql("stage_time", conn, if_exists="append", index=False)
            conn.execute(text("""
                INSERT INTO dim_ceremony_time (ceremony_year, decade, era_name)
                SELECT ceremony_year, decade, era_name FROM stage_time
                ON CONFLICT (ceremony_year) DO NOTHING;
            """))

        # ----------------------------------------------------
        # 2. Load dim_genre (Batch set-based)
        # ----------------------------------------------------
        genre_df = (
            df[["track_genre", "macro_category"]]
            .drop_duplicates(subset=["track_genre"])
            .copy()
            .rename(columns={"track_genre": "genre_name"})
        )
        conn.execute(text("CREATE TEMP TABLE stage_genre (genre_name VARCHAR(100), macro_category VARCHAR(100)) ON COMMIT DROP;"))
        genre_df.to_sql("stage_genre", conn, if_exists="append", index=False)
        conn.execute(text("""
            INSERT INTO dim_genre (genre_name, macro_category)
            SELECT genre_name, macro_category FROM stage_genre
            ON CONFLICT (genre_name) DO NOTHING;
        """))

        # ----------------------------------------------------
        # 3. Load dim_artist (Batch set-based)
        # ----------------------------------------------------
        artist_df = (
            df[["artist_name", "is_grammy_winner", "grammy_awards_count", "grammy_categories_count"]]
            .drop_duplicates(subset=["artist_name"])
            .copy()
        )
        artist_df["is_grammy_winner"] = artist_df["is_grammy_winner"].astype(bool)
        artist_df["grammy_awards_count"] = artist_df["grammy_awards_count"].astype(int)
        artist_df["grammy_categories_count"] = artist_df["grammy_categories_count"].astype(int)

        conn.execute(text("""
            CREATE TEMP TABLE stage_artist (
                artist_name TEXT,
                is_grammy_winner BOOLEAN,
                grammy_awards_count INT,
                grammy_categories_count INT
            ) ON COMMIT DROP;
        """))
        artist_df.to_sql("stage_artist", conn, if_exists="append", index=False)
        conn.execute(text("""
            INSERT INTO dim_artist (artist_name, is_grammy_winner, grammy_awards_count, grammy_categories_count)
            SELECT artist_name, is_grammy_winner, grammy_awards_count, grammy_categories_count FROM stage_artist
            ON CONFLICT (artist_name) DO UPDATE SET
                is_grammy_winner = EXCLUDED.is_grammy_winner,
                grammy_awards_count = EXCLUDED.grammy_awards_count,
                grammy_categories_count = EXCLUDED.grammy_categories_count;
        """))

        # ----------------------------------------------------
        # 4. Load dim_track (Batch set-based)
        # ----------------------------------------------------
        track_df = (
            df[["track_id", "track_name", "album_name", "explicit", "duration_ms", "is_grammy_nominee", "grammy_category"]]
            .drop_duplicates(subset=["track_id"])
            .copy()
        )
        track_df["explicit"] = track_df["explicit"].astype(bool)
        track_df["is_grammy_nominee"] = track_df["is_grammy_nominee"].astype(bool)
        track_df["duration_ms"] = track_df["duration_ms"].astype(int)
        track_df["album_name"] = track_df["album_name"].fillna("Unknown Album").astype(str)
        track_df["grammy_category"] = track_df["grammy_category"].fillna("None").astype(str)

        conn.execute(text("""
            CREATE TEMP TABLE stage_track (
                track_id VARCHAR(100),
                track_name TEXT,
                album_name TEXT,
                explicit BOOLEAN,
                duration_ms INT,
                is_grammy_nominee BOOLEAN,
                grammy_category TEXT
            ) ON COMMIT DROP;
        """))
        track_df.to_sql("stage_track", conn, if_exists="append", index=False)
        conn.execute(text("""
            INSERT INTO dim_track (track_id, track_name, album_name, explicit, duration_ms, is_grammy_nominee, grammy_category)
            SELECT track_id, track_name, album_name, explicit, duration_ms, is_grammy_nominee, grammy_category FROM stage_track
            ON CONFLICT (track_id) DO UPDATE SET
                track_name = EXCLUDED.track_name,
                album_name = EXCLUDED.album_name,
                is_grammy_nominee = EXCLUDED.is_grammy_nominee;
        """))

        # ----------------------------------------------------
        # 5. Load fact_track_performance (Batch set-based via Staging)
        # ----------------------------------------------------
        artist_map = dict(conn.execute(text("SELECT artist_name, artist_id FROM dim_artist")).fetchall())
        genre_map = dict(conn.execute(text("SELECT genre_name, genre_id FROM dim_genre")).fetchall())
        time_map = dict(conn.execute(text("SELECT ceremony_year, time_id FROM dim_ceremony_time")).fetchall())

        facts = df[[
            "track_id", "artist_name", "track_genre", "ceremony_year",
            "popularity", "danceability", "energy", "speechiness",
            "acousticness", "instrumentalness", "liveness", "valence",
            "loudness", "tempo"
        ]].copy()

        facts["artist_id"] = facts["artist_name"].map(artist_map)
        facts["genre_id"] = facts["track_genre"].map(genre_map)
        facts["time_id"] = facts["ceremony_year"].map(time_map)

        facts_clean = facts[[
            "track_id", "artist_id", "genre_id", "time_id",
            "popularity", "danceability", "energy", "speechiness",
            "acousticness", "instrumentalness", "liveness", "valence",
            "loudness", "tempo"
        ]].copy()

        conn.execute(text("CREATE TEMP TABLE stage_facts (LIKE fact_track_performance INCLUDING DEFAULTS) ON COMMIT DROP;"))
        facts_clean.to_sql("stage_facts", conn, if_exists="append", index=False)

        conn.execute(text("""
            INSERT INTO fact_track_performance (
                track_id, artist_id, genre_id, time_id,
                popularity, danceability, energy, speechiness,
                acousticness, instrumentalness, liveness, valence,
                loudness, tempo
            )
            SELECT 
                track_id, artist_id, genre_id, time_id,
                popularity, danceability, energy, speechiness,
                acousticness, instrumentalness, liveness, valence,
                loudness, tempo
            FROM stage_facts
            ON CONFLICT (track_id, genre_id) DO UPDATE SET
                popularity = EXCLUDED.popularity,
                danceability = EXCLUDED.danceability,
                energy = EXCLUDED.energy,
                speechiness = EXCLUDED.speechiness,
                acousticness = EXCLUDED.acousticness,
                instrumentalness = EXCLUDED.instrumentalness,
                liveness = EXCLUDED.liveness,
                valence = EXCLUDED.valence,
                loudness = EXCLUDED.loudness,
                tempo = EXCLUDED.tempo;
        """))

        # Reconcile row counts
        count_tracks = conn.execute(text("SELECT COUNT(*) FROM dim_track")).scalar()
        count_artists = conn.execute(text("SELECT COUNT(*) FROM dim_artist")).scalar()
        count_genres = conn.execute(text("SELECT COUNT(*) FROM dim_genre")).scalar()
        count_times = conn.execute(text("SELECT COUNT(*) FROM dim_ceremony_time")).scalar()
        count_facts = conn.execute(text("SELECT COUNT(*) FROM fact_track_performance")).scalar()

    summary = {
        "dim_track": count_tracks,
        "dim_artist": count_artists,
        "dim_genre": count_genres,
        "dim_ceremony_time": count_times,
        "fact_track_performance": count_facts,
    }

    print(f"[LOAD DW SUCCESS] Reconciled DW row counts: {summary}")
    return summary
