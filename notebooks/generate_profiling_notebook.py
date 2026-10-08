"""
Generate and execute the reproducible data profiling notebook.
This script builds notebooks/data_profiling.ipynb and executes all cells,
saving the tables, figures, and markdown documentation required by Section 6.3.
"""

from pathlib import Path
import nbformat as nbf
from nbclient import NotebookClient

nb = nbf.v4.new_notebook()
cells = []

# Title & Metadata
cells.append(nbf.v4.new_markdown_cell("""# Workshop 2: Exploratory Data Profiling & Quality Risk Analysis
**Course:** ETL - Data Engineering and Artificial Intelligence (UAO)  
**Authors:** Juan David & AI Senior Data Engineering Assistant  
**Objective:** Profile both heterogeneous data sources (`spotify_dataset.csv` and `the_grammy_awards.csv`), analyze cross-source consistency, and gather empirical evidence to justify quality rules, metric thresholds, and dimensional design decisions.

---
## Analytical Scope Alignment
This profiling specifically investigates attributes supporting our three mandatory analytical requirements:
* **REQ-01:** Temporal Evolution of Historical Winners' Commercial Success (`the_grammy_awards.year`, `nominee`, `winner`, `spotify_dataset.track_name`, `popularity`).
* **REQ-02:** Acoustic Behavior in Pop and Urban Genres (`the_grammy_awards.artist`, `winner`, `spotify_dataset.artists`, `track_genre`, `speechiness`, `energy`).
* **REQ-03:** Genre Diversity in Multi-Awarded Artists (`the_grammy_awards.artist`, `category`, `winner`, `spotify_dataset.artists`, `track_genre`).
"""))

# Cell 1: Imports and setup
cells.append(nbf.v4.new_markdown_cell("## 1. Environment and Data Ingestion"))
cells.append(nbf.v4.new_code_cell("""import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

# Set plot styles
sns.set_theme(style="whitegrid")
plt.rcParams["figure.figsize"] = (10, 5)

DATA_DIR = Path("../data")
spotify_path = DATA_DIR / "spotify_dataset.csv"
grammys_path = DATA_DIR / "the_grammy_awards.csv"

df_spotify = pd.read_csv(spotify_path)
df_grammys = pd.read_csv(grammys_path)

print(f"Spotify Dataset Shape: {df_spotify.shape}")
print(f"Grammy Awards Shape:   {df_grammys.shape}")
"""))

# Cell 2: Structural Analysis
cells.append(nbf.v4.new_markdown_cell("""## 2. Structural Analysis (Schema & Data Types)
We examine the declared types and column definitions against the requirements.
"""))
cells.append(nbf.v4.new_code_cell("""print("=== SPOTIFY DATA TYPES ===")
print(df_spotify.dtypes)
print("\\n=== GRAMMYS DATA TYPES ===")
print(df_grammys.dtypes)
"""))

# Cell 3: Completeness Analysis
cells.append(nbf.v4.new_markdown_cell("""## 3. Completeness Analysis (Missing Values)
We evaluate missing-value counts and percentages for both sources.
"""))
cells.append(nbf.v4.new_code_cell("""def completeness_report(df, name):
    null_counts = df.isnull().sum()
    null_pct = (null_counts / len(df)) * 100
    report = pd.DataFrame({
        "Missing_Count": null_counts,
        "Missing_Percentage": null_pct.round(4)
    })
    return report[report["Missing_Count"] > 0]

print("=== SPOTIFY MISSING VALUES ===")
spot_missing = completeness_report(df_spotify, "Spotify")
print(spot_missing)

print("\\n=== GRAMMY AWARDS MISSING VALUES ===")
gram_missing = completeness_report(df_grammys, "Grammys")
print(gram_missing)
"""))

# Cell 4: Uniqueness and Duplication Patterns
cells.append(nbf.v4.new_markdown_cell("""## 4. Uniqueness & Candidate Keys Analysis
We test whether candidate keys (`track_id` in Spotify, `(year, category, nominee)` in Grammys) are unique.
"""))
cells.append(nbf.v4.new_code_cell("""print(f"Spotify Total Rows: {len(df_spotify)}")
print(f"Spotify Unique track_id: {df_spotify['track_id'].nunique()}")
print(f"Spotify Duplicate track_id count: {df_spotify['track_id'].duplicated().sum()}")

# Check why track_id duplicates exist
dup_samples = df_spotify[df_spotify['track_id'].duplicated(keep=False)].sort_values('track_id').head(6)
print("\\nSample of track_id appearing across multiple genres:")
print(dup_samples[['track_id', 'track_name', 'artists', 'track_genre']])

print("\\nTesting (track_id, track_genre) as composite candidate key:")
composite_dups = df_spotify.duplicated(subset=['track_id', 'track_genre']).sum()
print(f"Duplicates at (track_id, track_genre) grain: {composite_dups}")
"""))

# Cell 5: Numerical Distribution and Ranges
cells.append(nbf.v4.new_markdown_cell("""## 5. Numerical Measures & Range Boundaries
Verifying acoustic metrics and popularity ranges for REQ-01 and REQ-02.
"""))
cells.append(nbf.v4.new_code_cell("""num_cols = ['popularity', 'energy', 'speechiness', 'danceability', 'acousticness', 'tempo']
print("=== SPOTIFY NUMERICAL METRICS SUMMARY ===")
print(df_spotify[num_cols].describe().round(4))
"""))

# Cell 6: Temporal Coverage
cells.append(nbf.v4.new_markdown_cell("""## 6. Temporal Content & Award History
Checking ceremony year bounds and distribution for REQ-01.
"""))
cells.append(nbf.v4.new_code_cell("""print("=== GRAMMY YEAR BOUNDS ===")
print(f"Min Year: {df_grammys['year'].min()} | Max Year: {df_grammys['year'].max()}")

# Decade grouping
df_grammys['decade'] = (df_grammys['year'] // 10) * 10
decade_dist = df_grammys.groupby('decade').size()
print("\\nAwards count by decade:")
print(decade_dist)
"""))

# Cell 7: Cross-Source Consistency & Matching
cells.append(nbf.v4.new_markdown_cell("""## 7. Cross-Source Consistency & Match Analysis
Evaluating the empirical feasibility of joining Spotify with Grammy Awards.
"""))
cells.append(nbf.v4.new_code_cell("""# Artist matching
spot_artists = set(df_spotify['artists'].dropna().str.lower().str.strip())
gram_artists = set(df_grammys['artist'].dropna().str.lower().str.strip())

direct_artist_matches = spot_artists.intersection(gram_artists)
print(f"Distinct Spotify Artists: {len(spot_artists)}")
print(f"Distinct Grammy Artists:  {len(gram_artists)}")
print(f"Direct Artist String Matches: {len(direct_artist_matches)}")

# Track Name vs Nominee matching
spot_tracks = set(df_spotify['track_name'].dropna().str.lower().str.strip())
gram_nominees = set(df_grammys['nominee'].dropna().str.lower().str.strip())

direct_track_matches = spot_tracks.intersection(gram_nominees)
print(f"Direct Song Title / Nominee Matches: {len(direct_track_matches)}")
"""))

# Cell 8: Risk-Rule Synthesis Table
cells.append(nbf.v4.new_markdown_cell("""## 8. Data Quality Risk & Rules Matrix (PDF Section 6.3 & 6.5)

| Rule ID | Dataset / Layer | Attribute(s) | Quality Dimension | Quality Rule Statement | Metric / Threshold | Severity | Related Requirement |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **DQ-RAW-01** | Spotify Raw | `track_id` | Completeness & Uniqueness | Track identifier must be non-null | mostly = 1.0 (100%) | **Critical** | All (Grain integrity) |
| **DQ-RAW-02** | Spotify Raw | `energy`, `speechiness` | Validity & Range | Acoustic features must be normalized ratios between 0.0 and 1.0 | 100% within [0.0, 1.0] | **Critical** | REQ-02 (Acoustic gap) |
| **DQ-RAW-03** | Spotify Raw | `artists` | Completeness | Artist string must not be missing | mostly = 0.999 (99.9%) | **Warning** | REQ-02, REQ-03 |
| **DQ-RAW-04** | Grammys Raw | `year` | Validity & Range | Ceremony year must be between 1958 and current year | 100% within [1958, 2026] | **Critical** | REQ-01 (Temporal trends) |
| **DQ-RAW-05** | Grammys Raw | `winner` | Validity & Consistency | Winner flag must be boolean true/false | 100% in {True, False} | **Critical** | REQ-01, REQ-02, REQ-03 |
| **DQ-PREP-01**| Prepared Layer| `track_genre`, `popularity` | Completeness & Validity | Load-ready tracks must have valid genre and popularity [0, 100] | 100% non-null & valid | **Critical** | REQ-01, REQ-02 |
| **DQ-PREP-02**| Prepared Layer| `is_grammy_winner` | Validity & Invariant | Derived winner flag must be binary integer | 100% in {0, 1} | **Critical** | REQ-02, REQ-03 |
"""))

nb.cells = cells

# Save and execute
out_path = Path("notebooks/data_profiling.ipynb")
with open(out_path, "w", encoding="utf-8") as f:
    nbf.write(nb, f)

print(f"Generated {out_path}. Executing notebook cells...")
client = NotebookClient(nb, timeout=600, kernel_name="python3", resources={"metadata": {"path": "notebooks/"}})
client.execute()

with open(out_path, "w", encoding="utf-8") as f:
    nbf.write(nb, f)

print(f"Successfully executed and saved {out_path}!")
