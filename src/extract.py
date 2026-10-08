"""
Extraction Module: src/extract.py
Handles extraction from Spotify CSV and Grammy Awards relational PostgreSQL database.
"""

from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine

SPOTIFY_REQUIRED_COLUMNS = [
    "track_id",
    "artists",
    "album_name",
    "track_name",
    "popularity",
    "duration_ms",
    "explicit",
    "danceability",
    "energy",
    "key",
    "loudness",
    "mode",
    "speechiness",
    "acousticness",
    "instrumentalness",
    "liveness",
    "valence",
    "tempo",
    "time_signature",
    "track_genre",
]

GRAMMYS_QUERY = """
SELECT 
    id,
    year,
    title,
    published_at,
    updated_at,
    category,
    nominee,
    artist,
    workers,
    img,
    winner
FROM the_grammy_awards;
"""


def extract_spotify_data(source_path: str, raw_output_path: str) -> str:
    """
    Extracts raw Spotify data from CSV file, verifies structural schema,
    and stores an uncorrupted raw working copy for raw validation.
    """
    src = Path(source_path)
    out = Path(raw_output_path)
    out.parent.mkdir(parents=True, exist_ok=True)

    if not src.exists():
        raise FileNotFoundError(f"Spotify source file not found at: {src}")

    df = pd.read_csv(src)

    missing = [c for c in SPOTIFY_REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Spotify dataset missing required columns: {missing}")

    raw_df = df[SPOTIFY_REQUIRED_COLUMNS].copy()
    raw_df.to_csv(out, index=False)

    print(f"[EXTRACT SPOTIFY] Successfully extracted {len(raw_df)} rows to {out}")
    return str(out)


def extract_grammys_data(db_connection_uri: str, raw_output_path: str) -> str:
    """
    Extracts Grammy Awards data from the relational PostgreSQL source database using SQL.
    Fulfills the mandatory requirement: operational extraction must query SQL,
    never reading directly from the raw Grammy CSV.
    """
    out = Path(raw_output_path)
    out.parent.mkdir(parents=True, exist_ok=True)

    engine = create_engine(db_connection_uri)
    with engine.connect() as conn:
        df = pd.read_sql(GRAMMYS_QUERY, conn)

    if df.empty:
        raise ValueError("Grammy source database returned 0 records.")

    df.to_csv(out, index=False)
    print(f"[EXTRACT GRAMMYS] Extracted {len(df)} records from relational DB to {out}")
    return str(out)
