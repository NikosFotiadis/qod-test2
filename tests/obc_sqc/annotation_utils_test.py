import pandas as pd

import pytest

from obc_sqc.model.annotation_utils import AnnotationUtils
from tests.obc_sqc.fixtures.annotation_utils_fixtures_test import *  # noqa: F403


class TestErrorCodesHourly:
    """Tests the error_codes_hourly() function in multiple scenarios."""

    @pytest.mark.parametrize(
        "fnl_raw_process_input_df, minute_averaging_input_df, annotations_output_series",
        [
            (variable, variable, variable)
            for variable in ["temperature", "wind_speed", "wind_direction", "precipitation_accumulated"]
        ],
        indirect=["fnl_raw_process_input_df", "minute_averaging_input_df", "annotations_output_series"],
    )
    def test_normal_return_success(
        self,
        fnl_raw_process_input_df: pd.DataFrame,
        minute_averaging_input_df: pd.DataFrame,
        annotations_output_series: pd.Series,
    ) -> None:
        """Tests error_codes_hourly() in normal scenarios.

        Args:
        ----
            fnl_raw_process_input_df (pd.DataFrame): the dataframe containing input raw data data
            minute_averaging_input_df (pd.DataFrame): the dataframe containing input minute averaged data
            annotations_output_series (pd.Series): the series containing the desired output

        Returns:
        -------
            None
        """
        annotation_utils_result: pd.Series = AnnotationUtils.error_codes_hourly(
            fnl_raw_process_input_df,
            minute_averaging_input_df,
        )

        annotation_utils_result = annotation_utils_result.astype(str)

        result: bool = annotation_utils_result.equals(annotations_output_series)
        assert result

    @pytest.mark.parametrize(
        "fnl_raw_process_input_nan_df, minute_averaging_input_nan_df, annotations_output_nan_series",
        [
            (variable, variable, variable)
            for variable in ["temperature", "wind_speed", "wind_direction", "precipitation_accumulated"]
        ],
        indirect=["fnl_raw_process_input_nan_df", "minute_averaging_input_nan_df", "annotations_output_nan_series"],
    )
    def test_nans_return_success(
        self,
        fnl_raw_process_input_nan_df: pd.DataFrame,
        minute_averaging_input_nan_df: pd.DataFrame,
        annotations_output_nan_series: pd.Series,
    ) -> None:
        """Tests error_codes_hourly() with a nan-filled dataframe.

        Args:
        ----
            fnl_raw_process_input_nan_df (pd.DataFrame): the dataframe containing input raw data data
            minute_averaging_input_nan_df (pd.DataFrame): the dataframe containing input minute averaged data
            annotations_output_nan_series (pd.Series): the series containing the desired output

        Returns:
        -------
            None
        """
        annotation_utils_result: pd.Series = AnnotationUtils.error_codes_hourly(
            fnl_raw_process_input_nan_df,
            minute_averaging_input_nan_df,
        )

        annotation_utils_result = annotation_utils_result.astype(str)

        result: bool = annotation_utils_result.equals(annotations_output_nan_series)
        assert result

    @pytest.mark.parametrize(
        "fnl_raw_process_input_empty_df, minute_averaging_input_empty_df, annotations_output_empty_series",
        [
            (variable, variable, variable)
            for variable in ["temperature", "wind_speed", "wind_direction", "precipitation_accumulated"]
        ],
        indirect=[
            "fnl_raw_process_input_empty_df",
            "minute_averaging_input_empty_df",
            "annotations_output_empty_series",
        ],
    )
    def test_empty_return_success(
        self,
        fnl_raw_process_input_empty_df: pd.DataFrame,
        minute_averaging_input_empty_df: pd.DataFrame,
        annotations_output_empty_series: pd.Series,
    ) -> None:
        """Tests error_codes_hourly() with an empty dataframe.

        Args:
        ----
            fnl_raw_process_input_empty_df (pd.DataFrame): the empty dataframe containing input raw data data
            minute_averaging_input_empty_df (pd.DataFrame): the empty dataframe containing input minute averaged data
            annotations_output_empty_series (pd.Series): the series containing the desired output

        Returns:
        -------
            None
        """
        annotation_utils_result: pd.Series = AnnotationUtils.error_codes_hourly(
            fnl_raw_process_input_empty_df,
            minute_averaging_input_empty_df,
        )

        annotation_utils_result = annotation_utils_result.astype(str)

        result: bool = annotation_utils_result.equals(annotations_output_empty_series)
        assert result
