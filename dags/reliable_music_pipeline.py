"""
Reliable Music Pipeline: dags/reliable_music_pipeline.py
============================================================
Workshop-2: Building a Reliable Batch Data Pipeline
Course: ETL - Data Engineering and Artificial Intelligence (UAO)
Airflow: 3.1.8 (TaskFlow API via airflow.sdk)
Great Expectations: Core 1.x (1.24.x)
============================================================
Architecture:
- Spotify Branch: extract_spotify -> validate_spotify_raw
- Grammy Branch:  extract_grammys -> validate_grammys_raw
- Convergence:    transform_and_integrate (gated by both raw validations)
- Gate 2:         validate_prepared
- Target Load:    load_dw (gated by prepared validation)
- Inter-task:     File paths exchanged via XCom, never DataFrames.
- Selective retry: 0 retries on deterministic DQ checks, bounded retries on DB I/O.
"""

from datetime import timedelta
from pathlib import Path
import os
import pandas as pd
import pendulum

from airflow.sdk import dag, task
from great_expectations.expectations.metadata_types import FailureSeverity

from src.extract import extract_spotify_data, extract_grammys_data
from src.validation import run_gx_validation
from src.transform import transform_and_integrate
from src.load import load_data_warehouse

# ============================================================
# SHARED VOLUME PATHS & CONNECTION SETTINGS
# ============================================================

DATA_DIR = Path("/opt/airflow/data")
WORK_DIR = DATA_DIR / "work"
OUTPUT_DIR = DATA_DIR / "output"

# Source file path (toggleable via environment variable for Test B)
DEFAULT_SPOTIFY_FILENAME = "spotify_dataset.csv"
SPOTIFY_FILENAME = os.getenv("SPOTIFY_SOURCE_FILE", DEFAULT_SPOTIFY_FILENAME)
SPOTIFY_SOURCE_PATH = DATA_DIR / SPOTIFY_FILENAME

SPOTIFY_RAW_PATH = WORK_DIR / "spotify_raw.csv"
GRAMMYS_RAW_PATH = WORK_DIR / "grammys_raw.csv"
PREPARED_PATH = WORK_DIR / "music_prepared.csv"

# PostgreSQL connection strings inside Docker network
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
SOURCE_DB = os.getenv("POSTGRES_SOURCE_DB", "grammy_source")
DW_DB = os.getenv("POSTGRES_DW_DB", "music_dw")

GRAMMY_SOURCE_URI = (
    f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{SOURCE_DB}"
)
MUSIC_DW_URI = (
    f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{DW_DB}"
)


# ============================================================
# DAG ORCHESTRATION DEFINITION
# ============================================================

@dag(
    dag_id="reliable_music_pipeline",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["etl", "workshop-2", "airflow-3", "gx-core", "reliable-pipeline"],
)
def reliable_music_pipeline():

    # --------------------------------------------------------
    # BRANCH 1: SPOTIFY EXTRACTION (Deterministic Local File Read)
    # Retries = 0: If missing or unreadable, repeated attempts will not fix it.
    # --------------------------------------------------------
    @task(retries=0)
    def extract_spotify(**context) -> str:
        """Extracts raw Spotify CSV data and verifies initial file schema."""
        WORK_DIR.mkdir(parents=True, exist_ok=True)
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

        dag_run = context.get("dag_run")
        conf = (dag_run.conf or {}) if dag_run else {}
        filename = conf.get("spotify_file", SPOTIFY_FILENAME)
        src_path = DATA_DIR / filename

        print(f"[EXTRACT SPOTIFY] Ingesting source file: {src_path}")
        return extract_spotify_data(str(src_path), str(SPOTIFY_RAW_PATH))

    # --------------------------------------------------------
    # BRANCH 2: GRAMMY EXTRACTION (Transient DB Query)
    # Retries = 2: DB connections may suffer transient network timeouts.
    # --------------------------------------------------------
    @task(retries=2, retry_delay=timedelta(seconds=10))
    def extract_grammys() -> str:
        """Extracts Grammy Awards data from operational PostgreSQL database using SQL."""
        WORK_DIR.mkdir(parents=True, exist_ok=True)
        return extract_grammys_data(GRAMMY_SOURCE_URI, str(GRAMMYS_RAW_PATH))

    # --------------------------------------------------------
    # GATE 1A: SPOTIFY RAW VALIDATION
    # Retries = 0: Data contract violations are deterministic.
    # --------------------------------------------------------
    @task(retries=0)
    def validate_spotify_raw(raw_path: str):
        """Validates incoming Spotify raw dataset with Great Expectations 1.x."""
        df = pd.read_csv(raw_path)
        result, max_failure = run_gx_validation(df, stage="spotify_raw")

        if max_failure == FailureSeverity.CRITICAL:
            raise ValueError(
                f"[CRITICAL QUALITY FAILURE] Spotify raw validation failed on critical expectation. "
                f"Downstream transformation is blocked. Success: {result.success}"
            )

        if max_failure == FailureSeverity.WARNING:
            print("[WARNING] Spotify raw validation produced warnings. Continuing per documented policy.")

        print("[GATE PASSED] Spotify raw data satisfies critical quality policy.")

    # --------------------------------------------------------
    # GATE 1B: GRAMMYS RAW VALIDATION
    # Retries = 0: Data contract violations are deterministic.
    # --------------------------------------------------------
    @task(retries=0)
    def validate_grammys_raw(raw_path: str):
        """Validates incoming Grammy Awards raw dataset with Great Expectations 1.x."""
        df = pd.read_csv(raw_path)
        result, max_failure = run_gx_validation(df, stage="grammys_raw")

        if max_failure == FailureSeverity.CRITICAL:
            raise ValueError(
                f"[CRITICAL QUALITY FAILURE] Grammy raw validation failed on critical expectation. "
                f"Downstream transformation is blocked. Success: {result.success}"
            )

        print("[GATE PASSED] Grammy raw data satisfies critical quality policy.")

    # --------------------------------------------------------
    # CONVERGENCE & TRANSFORMATION
    # Retries = 0: Deterministic business logic.
    # --------------------------------------------------------
    @task(retries=0)
    def transform_and_integrate_sources(spot_path: str, gram_path: str) -> str:
        """Applies cleaning, harmonization, and cross-source integration contract."""
        return transform_and_integrate(spot_path, gram_path, str(PREPARED_PATH))

    # --------------------------------------------------------
    # GATE 2: PREPARED DATA VALIDATION
    # Retries = 0: Deterministic data quality gate before DW load.
    # --------------------------------------------------------
    @task(retries=0)
    def validate_prepared(prep_path: str):
        """Validates integrated dataset prior to loading into the Data Warehouse."""
        df = pd.read_csv(prep_path)
        result, max_failure = run_gx_validation(df, stage="prepared")

        if max_failure == FailureSeverity.CRITICAL:
            raise ValueError(
                f"[CRITICAL QUALITY FAILURE] Prepared dataset failed validation. "
                f"DW Load is blocked. Success: {result.success}"
            )

        print("[GATE PASSED] Prepared dataset is verified load-ready.")

    # --------------------------------------------------------
    # TARGET LOAD: DATA WAREHOUSE (IDEMPOTENT UPSERT)
    # Retries = 2: Handles transient database write locks.
    # --------------------------------------------------------
    @task(retries=2, retry_delay=timedelta(seconds=10))
    def load_dw(prep_path: str):
        """Loads validated dataset into the PostgreSQL Star Schema Data Warehouse."""
        summary = load_data_warehouse(prep_path, MUSIC_DW_URI)
        print(f"[TARGET LOAD COMPLETE] Summary of loaded dimensions and facts: {summary}")

    # ========================================================
    # DAG WORKFLOW ORCHESTRATION & DEPENDENCIES
    # ========================================================

    # Branch 1 Execution
    spot_raw = extract_spotify()
    spot_val = validate_spotify_raw(spot_raw)

    # Branch 2 Execution
    gram_raw = extract_grammys()
    gram_val = validate_grammys_raw(gram_raw)

    # Transformation Step
    prep_data = transform_and_integrate_sources(spot_raw, gram_raw)

    # ENFORCE GATE 1: Both raw validations must succeed before transformation begins
    [spot_val, gram_val] >> prep_data

    # Prepared Validation Gate
    prep_val = validate_prepared(prep_data)

    # Target Load Step
    dw_load_step = load_dw(prep_data)

    # ENFORCE GATE 2: Prepared validation must succeed before Data Warehouse load
    prep_val >> dw_load_step


# Make DAG discoverable
reliable_music_pipeline()
