import pandas as pd
import numpy as np
import pytest

from obc_sqc.model.raw_data_check import RawDataCheck
from tests.obc_sqc.fixtures.raw_data_check_fixtures_test import *  # noqa: F403

control_threshold_dict: dict[str, float] = {
    "temperature": 2,
    "wind_speed": 20,
    "wind_direction": np.nan,
    "precipitation_accumulated": np.nan,
}

availability_threshold_median_dict: dict[str, float] = {
    "temperature": 0.67,
    "wind_speed": 0.75,
    "wind_direction": 0.75,
    "precipitation_accumulated": np.nan,
}


class TestRawDataSuspiciousCheck:
    """Tests the raw_data_suspicious_check() function in multiple scenarios."""

    @pytest.mark.parametrize(
        "raw_data_check_input_df, raw_data_check_output_df, parameter, control_threshold,"
        " availability_threshold_median",
        [
            (
                variable,
                variable,
                variable,
                control_threshold_dict.get(variable),
                availability_threshold_median_dict.get(variable),
            )
            for variable in ["temperature", "wind_speed", "wind_direction", "precipitation_accumulated"]
        ],
        indirect=["raw_data_check_input_df", "raw_data_check_output_df"],
    )
    def test_normal_return_float_success(
        self,
        raw_data_check_input_df: pd.DataFrame,
        raw_data_check_output_df: pd.DataFrame,
        parameter: str,
        control_threshold: float,
        availability_threshold_median: float,
    ) -> None:
        """Tests hour_averaging() with a sample dataframe and different thresholds.

        Args:
        ----
            raw_data_check_input_df (pd.DataFrame): the dataframe containing input time-normalized
                                                    raw data
            raw_data_check_output_df (pd.DataFrame): the dataframe containing output time-normalized
                                                    raw data, checked for constant values
            parameter (str): the name of the examined parameter
            control_threshold (float): the threshold to check for jumps in a parameter
            availability_threshold_median (float): the availability threshold, e.g., we are able to calculate
                                                    median only if <67%/75% of timeslots within a certain time
                                                    period are available

        Returns:
        -------
            None
        """
        minimum_timestep: int = 16
        time_window: int = 10
        ann_no_median: int = 2
        ann_no_datum: int = 3
        ann_invalid_datum: int = 4

        raw_data_check_result: pd.DataFrame = RawDataCheck.raw_data_suspicious_check(
            raw_data_check_input_df,
            parameter,
            control_threshold,
            minimum_timestep,
            time_window,
            availability_threshold_median,
            ann_no_median,
            ann_no_datum,
            ann_invalid_datum,
        )

        # re-order columns
        raw_data_check_result = raw_data_check_result[raw_data_check_output_df.columns]

        for column1, column2 in zip(raw_data_check_result.columns, raw_data_check_output_df.columns, strict=True):
            result: bool = raw_data_check_result[column1].equals(raw_data_check_output_df[column2])
            assert result

    @pytest.mark.parametrize(
        "raw_data_check_input_nan_df, parameter, control_threshold, availability_threshold_median",
        [
            (
                variable,
                variable,
                control_threshold_dict.get(variable),
                availability_threshold_median_dict.get(variable),
            )
            for variable in ["temperature", "wind_speed"]
        ],
        indirect=["raw_data_check_input_nan_df"],
    )
    def test_nans_temperature_wind_speed_crash(
        self,
        raw_data_check_input_nan_df: pd.DataFrame,
        parameter: str,
        control_threshold: float,
        availability_threshold_median: float,
    ) -> None:
        """Tests raw_data_check() with a nan-filled dataframe.

        Args:
        ----
            raw_data_check_input_nan_df (pd.DataFrame): the dataframe containing input nan data
            parameter (str): the name of the examined parameter
            control_threshold (float): the threshold to check for jumps in a parameter
            availability_threshold_median (float): the availability threshold, e.g., we are able to calculate
                                                    median only if <67%/75% of timeslots within a certain time
                                                    period are available

        Returns:
        -------
            None
        """
        minimum_timestep: int = 16
        time_window: int = 10
        ann_no_median: int = 2
        ann_no_datum: int = 3
        ann_invalid_datum: int = 4

        with pytest.raises(ValueError):
            RawDataCheck.raw_data_suspicious_check(
                raw_data_check_input_nan_df,
                parameter,
                control_threshold,
                minimum_timestep,
                time_window,
                availability_threshold_median,
                ann_no_median,
                ann_no_datum,
                ann_invalid_datum,
            )

    @pytest.mark.parametrize(
        "raw_data_check_input_nan_df, raw_data_check_output_nan_df, parameter, control_threshold,"
        "availability_threshold_median",
        [
            (
                variable,
                variable,
                variable,
                control_threshold_dict.get(variable),
                availability_threshold_median_dict.get(variable),
            )
            for variable in ["wind_direction", "precipitation_accumulated"]
        ],
        indirect=["raw_data_check_input_nan_df", "raw_data_check_output_nan_df"],
    )
    def test_nans_wind_direction_precipitation_accumulated_success(
        self,
        raw_data_check_input_nan_df: pd.DataFrame,
        raw_data_check_output_nan_df: pd.DataFrame,
        parameter: str,
        control_threshold: float,
        availability_threshold_median: float,
    ) -> None:
        """Tests raw_data_check() with a nan-filled dataframe.

        Args:
        ----
            raw_data_check_input_nan_df (pd.DataFrame): the dataframe containing input nan data
            raw_data_check_output_nan_df (pd.DataFrame): the dataframe containing output time-normalized
                                                        raw data, checked for constant values
            parameter (str): the name of the examined parameter
            control_threshold (float): the threshold to check for jumps in a parameter
            availability_threshold_median (float): the availability threshold, e.g., we are able to calculate
                                                    median only if <67%/75% of timeslots within a certain time
                                                    period are available

        Returns:
        -------
            None
        """
        minimum_timestep: int = 16
        time_window: int = 10
        ann_no_median: int = 2
        ann_no_datum: int = 3
        ann_invalid_datum: int = 4

        raw_data_check_result: pd.DataFrame = RawDataCheck.raw_data_suspicious_check(
            raw_data_check_input_nan_df,
            parameter,
            control_threshold,
            minimum_timestep,
            time_window,
            availability_threshold_median,
            ann_no_median,
            ann_no_datum,
            ann_invalid_datum,
        )

        # re-order columns
        raw_data_check_result = raw_data_check_result[raw_data_check_output_nan_df.columns]
        raw_data_check_result = raw_data_check_result.fillna(pd.NA)

        for column1, column2 in zip(raw_data_check_result.columns, raw_data_check_output_nan_df.columns, strict=True):
            result: bool = raw_data_check_result[column1].equals(raw_data_check_output_nan_df[column2])
            assert result

    @pytest.mark.parametrize(
        "raw_data_check_input_empty_df, raw_data_check_output_empty_df, parameter, control_threshold,"
        " availability_threshold_median",
        [
            (
                variable,
                variable,
                variable,
                control_threshold_dict.get(variable),
                availability_threshold_median_dict.get(variable),
            )
            for variable in ["temperature", "wind_speed", "wind_direction", "precipitation_accumulated"]
        ],
        indirect=["raw_data_check_input_empty_df", "raw_data_check_output_empty_df"],
    )
    def test_empty_return_success(
        self,
        raw_data_check_input_empty_df: pd.DataFrame,
        raw_data_check_output_empty_df: pd.DataFrame,
        parameter: str,
        control_threshold: float,
        availability_threshold_median: float,
    ) -> None:
        """Tests hour_averaging() with a sample dataframe and different thresholds.

        Args:
        ----
            raw_data_check_input_empty_df (pd.DataFrame): the dataframe containing input time-normalized
                                                    raw data
            raw_data_check_output_empty_df (pd.DataFrame): the dataframe containing output time-normalized
                                                    raw data, checked for constant values
            parameter (str): the name of the examined parameter
            control_threshold (float): the threshold to check for jumps in a parameter
            availability_threshold_median (float): the availability threshold, e.g., we are able to calculate
                                                    median only if <67%/75% of timeslots within a certain time
                                                    period are available

        Returns:
        -------
            None
        """
        minimum_timestep: int = 16
        time_window: int = 10
        ann_no_median: int = 2
        ann_no_datum: int = 3
        ann_invalid_datum: int = 4

        raw_data_check_result: pd.DataFrame = RawDataCheck.raw_data_suspicious_check(
            raw_data_check_input_empty_df,
            parameter,
            control_threshold,
            minimum_timestep,
            time_window,
            availability_threshold_median,
            ann_no_median,
            ann_no_datum,
            ann_invalid_datum,
        )

        # re-order columns
        raw_data_check_result = raw_data_check_result[raw_data_check_output_empty_df.columns]

        for column1, column2 in zip(raw_data_check_result.columns, raw_data_check_output_empty_df.columns, strict=True):
            result: bool = raw_data_check_result[column1].equals(raw_data_check_output_empty_df[column2])
            assert result
