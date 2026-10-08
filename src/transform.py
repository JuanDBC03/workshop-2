"""
Transformation and Integration Module: src/transform.py
Cleans, harmonizes, and integrates Spotify and Grammy Awards data
according to the documented integration contract and analytical requirements.
"""

from pathlib import Path
import pandas as pd


def get_macro_genre(genre: str) -> str:
    """Categorizes granular Spotify genre into analytical macro-categories for REQ-02."""
    g = str(genre).lower().strip()
    pop_genres = {"pop", "dance-pop", "indie-pop", "synth-pop", "power-pop", "cantopop", "mandopop", "j-pop", "k-pop"}
    urban_genres = {"hip-hop", "r-n-b", "reggaeton", "urban", "latino", "rap", "trap"}

    if g in pop_genres:
        return "Pop"
    if g in urban_genres:
        return "Urban"
    return "Other"


def get_era_name(year: int) -> str:
    """Classifies ceremony year into historical eras for REQ-01."""
    if pd.isna(year):
        return "Unknown"
    y = int(year)
    if y < 1980:
        return "Classic Era (1958-1979)"
    elif y < 2000:
        return "Golden Era (1980-1999)"
    elif y < 2010:
        return "Digital Transition (2000-2009)"
    else:
        return "Streaming Era (2010-present)"


def transform_and_integrate(spotify_raw_path: str, grammys_raw_path: str, prepared_output_path: str) -> str:
    """
    Executes the integration contract between Spotify and Grammy Awards.

    Steps:
    1. Removes incomplete Spotify records (missing artists/track_name).
    2. Extracts clean primary artist name from semicolon-delimited artists list.
    3. Aggregates historical Grammy wins, distinct categories, and awarded song titles.
    4. Enriches Spotify tracks with artist winner status, cumulative awards, and ceremony year.
    5. Derives macro genre and historical decade/era dimensions.
    6. Guarantees composite grain uniqueness at (track_id, track_genre).
    7. Persists prepared dataset to disk.
    """
    out = Path(prepared_output_path)
    out.parent.mkdir(parents=True, exist_ok=True)

    df_spotify = pd.read_csv(spotify_raw_path)
    df_grammys = pd.read_csv(grammys_raw_path)

    # 1. Cleaning null records in Spotify
    df_spotify = df_spotify.dropna(subset=["track_id", "artists", "track_name"]).copy()

    # 2. Harmonize primary artist name
    df_spotify["artist_name"] = (
        df_spotify["artists"]
        .astype(str)
        .apply(lambda x: x.split(";")[0].strip())
    )
    df_spotify["artist_lookup"] = df_spotify["artist_name"].str.lower().str.strip()
    df_spotify["track_lookup"] = df_spotify["track_name"].astype(str).str.lower().str.strip()

    # 3. Grammy Aggregations by Artist
    grammy_winners = df_grammys[df_grammys["winner"] == True].copy()
    grammy_winners["artist_clean"] = grammy_winners["artist"].dropna().astype(str).str.lower().str.strip()

    artist_awards = (
        grammy_winners.groupby("artist_clean")
        .agg(
            grammy_awards_count=("category", "count"),
            grammy_categories_count=("category", "nunique"),
        )
        .reset_index()
    )

    # Grammy Nominee/Song level lookup
    grammy_songs = (
        grammy_winners.dropna(subset=["nominee"])
        .assign(song_clean=lambda d: d["nominee"].astype(str).str.lower().str.strip())
        .groupby("song_clean")
        .agg(
            ceremony_year=("year", "min"),
            grammy_category=("category", "first"),
        )
        .reset_index()
    )

    # 4. Cross-Source Enrichment
    merged = df_spotify.merge(
        artist_awards,
        left_on="artist_lookup",
        right_on="artist_clean",
        how="left",
    )

    merged["grammy_awards_count"] = merged["grammy_awards_count"].fillna(0).astype(int)
    merged["grammy_categories_count"] = merged["grammy_categories_count"].fillna(0).astype(int)
    merged["is_grammy_winner"] = (merged["grammy_awards_count"] > 0).astype(int)

    # Merge song-level award information
    final_df = merged.merge(
        grammy_songs,
        left_on="track_lookup",
        right_on="song_clean",
        how="left",
    )

    final_df["is_grammy_nominee"] = final_df["ceremony_year"].notna()

    # 5. Dimension derivations
    final_df["macro_category"] = final_df["track_genre"].apply(get_macro_genre)
    final_df["decade"] = final_df["ceremony_year"].apply(
        lambda y: f"{int((y // 10) * 10)}s" if pd.notna(y) else "Non-Awarded"
    )
    final_df["era_name"] = final_df["ceremony_year"].apply(get_era_name)

    # 6. Enforce grain uniqueness: (track_id, track_genre)
    initial_rows = len(final_df)
    final_df = final_df.drop_duplicates(subset=["track_id", "track_genre"]).copy()
    dedup_rows = len(final_df)
    print(f"[TRANSFORM] Deduplication at grain (track_id, track_genre): {initial_rows} -> {dedup_rows}")

    # 7. Persist prepared working file
    final_df.to_csv(out, index=False)
    print(f"[TRANSFORM] Persisted {len(final_df)} prepared rows to {out}")

    return str(out)
