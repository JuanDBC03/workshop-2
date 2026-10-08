# Workshop 2: System Operations & Technical Defense Guide (`readme2.md`)

This operational manual is prepared for the engineering evaluation, oral defense, and Power BI dashboard configuration of **Workshop-2: Building a Reliable Batch Data Pipeline**.

---

## 1. Quick Reference: Live Evaluation Demonstrations

During the oral defense, the evaluation committee expects three verifiable demonstrations:

### Demonstration 1: Successful End-to-End Execution (Test A)
1. Open your browser and navigate to the Airflow 3 Web UI: [http://localhost:8080](http://localhost:8080).
2. Locate the DAG `reliable_music_pipeline`.
3. Show the **Grid View** or **Graph View**:
   * Both parallel branches (`extract_spotify` and `extract_grammys`) running concurrently.
   * Both raw validation gates (`validate_spotify_raw` and `validate_grammys_raw`) showing green `success` badges.
   * `transform_and_integrate_sources` executing only after both raw gates succeed.
   * `validate_prepared` passing before `load_dw`.
4. Run the verification query in PostgreSQL:
   ```sql
   SELECT 'dim_track' AS tbl, count(*) FROM dim_track
   UNION ALL SELECT 'dim_artist', count(*) FROM dim_artist
   UNION ALL SELECT 'dim_genre', count(*) FROM dim_genre
   UNION ALL SELECT 'dim_ceremony_time', count(*) FROM dim_ceremony_time
   UNION ALL SELECT 'fact_track_performance', count(*) FROM fact_track_performance;
   ```
   **Expected Verification:** Exactly 113,549 fact records, 89,740 tracks, 17,648 artists, 114 genres, and 62 ceremony eras.

---

### Demonstration 2: Controlled Critical Failure & Blocking (Test B)
1. Trigger the pipeline with an invalid payload directly via CLI:
   ```powershell
   docker exec workshop2-airflow airflow dags trigger -c '{"spotify_file": "spotify_bad.csv"}' reliable_music_pipeline
   ```
2. In the Airflow Web UI:
   * Point out that `extract_spotify` succeeds (raw extraction is uncorrupted).
   * Show `validate_spotify_raw` transitioning to **`failed` (red)**.
   * Show that `transform_and_integrate_sources`, `validate_prepared`, and `load_dw` transition to **`upstream_failed` (orange)** and never execute.
3. Open the task log for `validate_spotify_raw`:
   * Point out the explicit Great Expectations log:
     `[GX VALIDATION] Stage: spotify_raw | Success: False | Max Failure Severity: FailureSeverity.CRITICAL`
   * Explain: *"The pipeline failed deterministically because Rule DQ-RAW-02a detected energy = 15.0 (outside allowed range [0.0, 1.0]). The downstream loading tasks were completely blocked, guaranteeing zero data corruption in the warehouse."*

---

### Demonstration 3: Safe Rerun & Idempotency (Test C)
1. Execute a subsequent rerun with identical input data:
   ```powershell
   docker exec workshop2-airflow airflow dags trigger reliable_music_pipeline
   ```
2. Demonstrate that after the rerun completes with `success`, the Data Warehouse metrics remain completely identical:
   ```sql
   SELECT COUNT(*) AS total_facts, SUM(popularity) AS sum_popularity, ROUND(AVG(energy), 4) AS avg_energy 
   FROM fact_track_performance;
   ```
   **Pre-Rerun:** Facts = `113,549` | Popularity Sum = `3,783,956` | Avg Energy = `0.6421`  
   **Post-Rerun:** Facts = `113,549` | Popularity Sum = `3,783,956` | Avg Energy = `0.6421`  
   **Difference:** $\Delta = 0$ (Zero duplication).

---

## 2. Power BI Dashboard Configuration Guide

### Connecting Power BI to the Data Warehouse
1. Open **Power BI Desktop**.
2. Click **Get Data** $\rightarrow$ **More...** $\rightarrow$ **PostgreSQL database**.
3. Enter connection parameters:
   * **Server:** `localhost:5432`
   * **Database:** `music_dw`
   * **Data Connectivity mode:** DirectQuery (or Import)
4. Under authentication credentials:
   * **User name:** `postgres`
   * **Password:** `postgres`
5. Select the 5 tables:
   * `dim_artist`
   * `dim_track`
   * `dim_genre`
   * `dim_ceremony_time`
   * `fact_track_performance`
6. Click **Load**. In the Model view, verify that relationships between `fact_track_performance` and dimensions match the Star Schema.

---

### Creating the 3 Analytical Visualizations & KPIs

#### Visual 1 (Supporting REQ-01: Temporal Streaming Popularity)
* **Visual Type:** Line Chart / Clustered Column Chart.
* **X-Axis:** `dim_ceremony_time.decade` (filtered to ceremony decades: 1950s through 2010s).
* **Y-Axis:** Average of `fact_track_performance.popularity`.
* **KPI Card 1:** Card displaying `Average Popularity of Awarded Catalog` (`40.31` for 2010s vs `41.18` for 1960s).
* **Analytical Insight:** Tracks from the 1960s retain the highest catalog longevity, while modern 2010s tracks retain high streaming velocity.

#### Visual 2 (Supporting REQ-02: Acoustic Gap in Pop and Urban)
* **Visual Type:** Clustered Bar Chart / Scatter Plot.
* **Axis:** `dim_genre.macro_category` (filtered to Pop and Urban).
* **Legend:** `dim_artist.is_grammy_winner` (`True` vs `False`).
* **Values:** Average of `fact_track_performance.energy` and Average of `fact_track_performance.speechiness`.
* **KPI Card 2:** Card displaying `Acoustic Energy Gap` ($-0.0662$ in Pop; $-0.0601$ in Urban).
* **Analytical Insight:** Grammy-winning songs in Pop and Urban genres exhibit consistently lower acoustic energy than non-winners, reflecting an institutional preference for organic dynamic headroom over hyper-compressed tracks.

#### Visual 3 (Supporting REQ-03: Genre Exploration by Award Tier)
* **Visual Type:** Column Chart.
* **Axis:** Calculated Award Tier (`Highly Awarded (>=3)`, `Awarded (1-2)`, `Non-Winners (0)`).
* **Values:** Average of `Distinct Genres Count` per artist.
* **KPI Card 3:** Card displaying `Versatility Ratio` ($2.05$ vs $1.32$ genres explored).
* **Analytical Insight:** Highly awarded Grammy artists explore 55% more distinct genres across their catalog than non-winners, confirming that genre fluidity correlates with elite industry recognition.

---

## 3. Defense Preparation: Typical Technical Questions & Senior Answers

### Q1: Why did you use TaskFlow API (`from airflow.sdk import dag, task`) instead of standard `PythonOperator`?
> *"The TaskFlow API is the modern standard introduced by Apache Airflow. It eliminates boilerplate code, automatically handles XCom serialization/deserialization for metadata, and clearly represents data dependencies. It also enforces explicit task boundaries where only lightweight references (paths) are exchanged rather than heavy in-memory DataFrames."*

### Q2: How does the pipeline guarantee idempotency? What happens if `load_dw` fails midway?
> *"Idempotency is guaranteed through a two-level defense: First, all database loading operations run inside an atomic transaction block (`with engine.begin()`). If an exception occurs, PostgreSQL immediately rolls back the entire transaction, leaving the Data Warehouse uncorrupted. Second, the loading logic uses temporary staging tables and set-based upserts (`ON CONFLICT (track_id, genre_id) DO UPDATE`), ensuring that re-running the job over the same batch updates existing keys rather than duplicating rows."*

### Q3: Why is selective retrying used instead of a blanket `retries=3` on every task?
> *"In production data engineering, a blanket retry policy is considered poor practice. Data quality violations (such as Great Expectations catching an invalid schema or impossible values) are deterministic: running them three times will produce the exact same failure three times, wasting CPU and worker slots. Therefore, validation tasks are configured with `retries=0`. In contrast, database I/O tasks (`extract_grammys`, `load_dw`) are configured with bounded retries (`retries=2, retry_delay=10s`) to withstand transient connection drops or temporary database write locks."*

### Q4: Why use Great Expectations 1.x instead of basic Python assertions or Pandas checks?
> *"Great Expectations provides enterprise-grade data contracts. It provides structured validation results, tracks execution statistics, assigns explicit severity levels (Critical vs Warning vs Info), and generates machine-readable documentation. Basic assertions halt execution without context, whereas Great Expectations produces auditable evidence that can be integrated directly into operational alerting and monitoring systems."*

### Q5: How is REQ-01 answered without duplicating track records for songs awarded multiple times?
> *"In our Star Schema, `dim_track` maintains one unique record per Spotify song. Historical ceremony context is normalized into `dim_ceremony_time`. The fact table `fact_track_performance` models each track performance by genre and links to the award ceremony time key, allowing analytical queries to aggregate popularity by decade without duplicating acoustic measurements."*
