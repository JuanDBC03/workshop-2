"""
Validation Module: src/validation.py
Implements automated data quality validation gates with Great Expectations Core 1.x
following the exact architectural patterns taught by the course.
"""

from typing import Tuple, Any
import great_expectations as gx
from great_expectations.expectations.metadata_types import FailureSeverity
import pandas as pd


def run_gx_validation(dataframe: pd.DataFrame, stage: str) -> Tuple[Any, FailureSeverity]:
    """
    Builds and executes an automated Great Expectations 1.x validation definition.

    Parameters:
        dataframe: The pandas DataFrame to evaluate.
        stage: The pipeline stage ("spotify_raw", "grammys_raw", or "prepared").

    Returns:
        (result, max_failure): Tuple containing the GX ValidationResult and the
        maximum detected FailureSeverity (CRITICAL, WARNING, or None).
    """
    context = gx.get_context(mode="ephemeral")

    data_source = context.data_sources.add_pandas(name=f"{stage}_source")
    data_asset = data_source.add_dataframe_asset(name=f"{stage}_asset")
    batch_definition = data_asset.add_batch_definition_whole_dataframe(f"{stage}_batch")

    suite = gx.ExpectationSuite(name=f"{stage}_suite")
    suite = context.suites.add(suite)

    if stage == "spotify_raw":
        # DQ-RAW-01: Essential natural identifier completeness (Critical)
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column="track_id",
                severity="critical",
            )
        )

        # DQ-RAW-02a: Audio measure range validity for REQ-02 (Critical)
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="energy",
                min_value=0.0,
                max_value=1.0,
                severity="critical",
            )
        )

        # DQ-RAW-02b: Audio measure range validity for REQ-02 (Critical)
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="speechiness",
                min_value=0.0,
                max_value=1.0,
                severity="critical",
            )
        )

        # DQ-RAW-03: Artist completeness tolerance (Warning: profiling found 1 null in 114k)
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column="artists",
                mostly=0.999,
                severity="warning",
            )
        )

    elif stage == "grammys_raw":
        # DQ-RAW-04: Award ceremony year bound validity for REQ-01 (Critical)
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="year",
                min_value=1958,
                max_value=2026,
                severity="critical",
            )
        )

        # DQ-RAW-05: Winner truth value validity (Critical)
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeInSet(
                column="winner",
                value_set=[True, False],
                severity="critical",
            )
        )

    elif stage == "prepared":
        # DQ-PREP-01a: Integrated dataset genre assignment readiness for REQ-02/REQ-03 (Critical)
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column="track_genre",
                severity="critical",
            )
        )

        # DQ-PREP-01b: Popularity score validity [0 - 100] for REQ-01 (Critical)
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="popularity",
                min_value=0,
                max_value=100,
                severity="critical",
            )
        )

        # DQ-PREP-02: Derived winner indicator integrity (Critical)
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeInSet(
                column="is_grammy_winner",
                value_set=[0, 1],
                severity="critical",
            )
        )

        # DQ-PREP-03: Clean primary artist name completeness (Critical)
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column="artist_name",
                severity="critical",
            )
        )

    else:
        raise ValueError(f"Unknown validation stage specified: {stage}")

    validation_definition = gx.ValidationDefinition(
        name=f"{stage}_validation",
        data=batch_definition,
        suite=suite,
    )
    validation_definition = context.validation_definitions.add(validation_definition)

    result = validation_definition.run(
        batch_parameters={"dataframe": dataframe},
        result_format={"result_format": "SUMMARY"},
    )

    max_failure = result.get_max_severity_failure()
    print(f"[GX VALIDATION] Stage: {stage} | Success: {result.success} | Max Failure Severity: {max_failure}")

    return result, max_failure
