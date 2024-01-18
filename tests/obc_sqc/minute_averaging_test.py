import pandas as pd
import numpy as np
import pytest

from obc_sqc.model.minute_averaging import MinuteAveraging
from tests.obc_sqc.fixtures.minute_averaging_fixtures_test import *  # noqa: F403

averaging_period_dict: dict[str, int] = {
    "temperature": 1,
    "wind_speed": 2,
    "wind_direction": 2,
    "precipitation_accumulated": 1,
}

availability_threshold_dict: dict[str, float] = {
    "temperature": 0.25,
    "wind_speed": 0.25,
    "wind_direction": 0.25,
    "precipitation_accumulated": 0.25,
}

availability_threshold_median_dict: dict[str, float] = {
    "temperature": 0.67,
    "wind_speed": 0.75,
    "wind_direction": 0.75,
    "precipitation_accumulated": np.nan,
}

control_threshold_dict: dict[str, float] = {
    "temperature": 3,
    "wind_speed": 10,
    "wind_direction": np.nan,
    "precipitation_accumulated": np.nan,
}


class TestMinuteAveraging:
    """Tests the minute_averaging() function in multiple scenarios."""

    @pytest.mark.parametrize(
        "time_normalized_df, parameter, averaging_period, availability_threshold, availability_threshold_median,"
        " control_threshold, minute_averaging_df",
        [
            (
                variable,
                variable,
                averaging_period_dict.get(variable),
                availability_threshold_dict.get(variable),
                availability_threshold_median_dict.get(variable),
                control_threshold_dict.get(variable),
                variable,
            )
            for variable in ["temperature", "wind_speed", "wind_direction", "precipitation_accumulated"]
        ],
        indirect=["time_normalized_df", "minute_averaging_df"],
    )
    def test_normal_return_float_success(
        self,
        time_normalized_df: pd.DataFrame,
        parameter: str,
        averaging_period: int,
        availability_threshold: float,
        availability_threshold_median: float,
        control_threshold: float,
        minute_averaging_df: pd.DataFrame,
    ) -> None:
        """Tests minute_averaging() with a sample dataframe and different thresholds.

        Args:
        ----
            time_normalized_df (pd.DataFrame): the dataframe containing minute averaged data
            parameter (str): the name of the examined parameter
            averaging_period (int): the period (in minutes) over which the data are averaged
            availability_threshold (float): the threshold (out of 1) under which data are insufficient
            availability_threshold_median (float): the availability threshold, e.g. we are able to calculate
                                                    median only if <67%/75% of timeslots within a certain
                                                    period are available
            control_threshold (float): the threshold to check for jumps in a parameter
            minute_averaging_df (pd.DataFrame): the dataframe containing target minute averaged data

        Returns:
        -------
            None
        """
        time_window_median: int = 10
        ann_invalid_datum: int = 4
        ann_unident_spk: int = 2
        pr_int: float = 0.254
        preprocess_time_window: int = 360

        average_result: pd.DataFrame = MinuteAveraging.minute_averaging(
            time_normalized_df,
            parameter,
            averaging_period,
            availability_threshold,
            availability_threshold_median,
            time_window_median,
            control_threshold,
            ann_invalid_datum,
            ann_unident_spk,
            pr_int,
            preprocess_time_window,
        )[1]

        minute_averaging_df = minute_averaging_df.fillna(pd.NA)

        # re-order columns
        average_result = average_result[minute_averaging_df.columns]
        average_result = average_result.fillna(pd.NA)

        for column1, column2 in zip(average_result.columns, minute_averaging_df.columns, strict=True):
            result: bool = average_result[column1].equals(minute_averaging_df[column2])
            assert result

    @pytest.mark.parametrize(
        "time_normalized_nan_df, parameter, averaging_period, availability_threshold, availability_threshold_median,"
        " control_threshold",
        [
            (
                variable,
                variable,
                averaging_period_dict.get(variable),
                availability_threshold_dict.get(variable),
                availability_threshold_median_dict.get(variable),
                control_threshold_dict.get(variable),
            )
            for variable in ["temperature", "wind_speed", "wind_direction", "precipitation_accumulated"]
        ],
        indirect=["time_normalized_nan_df"],
    )
    def test_nans_crash(
        self,
        time_normalized_nan_df: pd.DataFrame,
        parameter: str,
        averaging_period: int,
        availability_threshold: float,
        availability_threshold_median: float,
        control_threshold: float,
    ) -> None:
        """Tests minute_averaging() with a dataframe filled with nans.

        Args:
        ----
            time_normalized_nan_df (pd.DataFrame): the dataframe containing minute averaged data
            parameter (str): the name of the examined parameter
            averaging_period (int): the period (in minutes) over which the data are averaged
            availability_threshold (float): the threshold (out of 1) under which data are insufficient
            availability_threshold_median (float): the availability threshold, e.g. we are able to calculate
                                                    median only if <67%/75% of timeslots within a certain
                                                    period are available
            control_threshold (float): the threshold to check for jumps in a parameter

        Returns:
        -------
            None
        """
        time_window_median: int = 10
        ann_invalid_datum: int = 4
        ann_unident_spk: int = 2
        pr_int: float = 0.254
        preprocess_time_window: int = 360

        with pytest.raises(TypeError):
            MinuteAveraging.minute_averaging(
                time_normalized_nan_df,
                parameter,
                averaging_period,
                availability_threshold,
                availability_threshold_median,
                time_window_median,
                control_threshold,
                ann_invalid_datum,
                ann_unident_spk,
                pr_int,
                preprocess_time_window,
            )

    @pytest.mark.parametrize(
        "time_normalized_empty_df, parameter, averaging_period, availability_threshold, availability_threshold_median,"
        "control_threshold",
        [
            (
                variable,
                variable,
                averaging_period_dict.get(variable),
                availability_threshold_dict.get(variable),
                availability_threshold_median_dict.get(variable),
                control_threshold_dict.get(variable),
            )
            for variable in ["temperature", "wind_speed", "wind_direction"]
        ],
        indirect=["time_normalized_empty_df"],
    )
    def test_empty_crash(
        self,
        time_normalized_empty_df: pd.DataFrame,
        parameter: str,
        averaging_period: int,
        availability_threshold: float,
        availability_threshold_median: float,
        control_threshold: float,
    ) -> None:
        """Tests minute_averaging() with an empty dataframe.

        Args:
        ----
            time_normalized_empty_df (pd.DataFrame): the dataframe containing minute averaged data
            parameter (str): the name of the examined parameter
            averaging_period (int): the period (in minutes) over which the data are averaged
            availability_threshold (float): the threshold (out of 1) under which data are insufficient
            availability_threshold_median (float): the availability threshold, e.g. we are able to calculate
                                                    median only if <67%/75% of timeslots within a certain
                                                    period are available
            control_threshold (float): the threshold to check for jumps in a parameter

        Returns:
        -------
            None
        """
        time_window_median: int = 10
        ann_invalid_datum: int = 4
        ann_unident_spk: int = 2
        pr_int: float = 0.254
        preprocess_time_window: int = 360

        with pytest.raises(ValueError):
            MinuteAveraging.minute_averaging(
                time_normalized_empty_df,
                parameter,
                averaging_period,
                availability_threshold,
                availability_threshold_median,
                time_window_median,
                control_threshold,
                ann_invalid_datum,
                ann_unident_spk,
                pr_int,
                preprocess_time_window,
            )

    @pytest.mark.parametrize(
        "time_normalized_empty_df, parameter, averaging_period, availability_threshold, availability_threshold_median,"
        "control_threshold",
        [
            (
                variable,
                variable,
                averaging_period_dict.get(variable),
                availability_threshold_dict.get(variable),
                availability_threshold_median_dict.get(variable),
                control_threshold_dict.get(variable),
            )
            for variable in ["precipitation_accumulated"]
        ],
        indirect=["time_normalized_empty_df"],
    )
    def test_precipitation_accumulated_empty_crash(
        self,
        time_normalized_empty_df: pd.DataFrame,
        parameter: str,
        averaging_period: int,
        availability_threshold: float,
        availability_threshold_median: float,
        control_threshold: float,
    ) -> None:
        """Tests minute_averaging() with an empty dataframe.

        Args:
        ----
            time_normalized_empty_df (pd.DataFrame): the dataframe containing minute averaged data
            parameter (str): the name of the examined parameter
            averaging_period (int): the period (in minutes) over which the data are averaged
            availability_threshold (float): the threshold (out of 1) under which data are insufficient
            availability_threshold_median (float): the availability threshold, e.g. we are able to calculate
                                                    median only if <67%/75% of timeslots within a certain
                                                    period are available
            control_threshold (float): the threshold to check for jumps in a parameter

        Returns:
        -------
            None
        """
        time_window_median: int = 10
        ann_invalid_datum: int = 4
        ann_unident_spk: int = 2
        pr_int: float = 0.254
        preprocess_time_window: int = 360

        with pytest.raises(IndexError):
            MinuteAveraging.minute_averaging(
                time_normalized_empty_df,
                parameter,
                averaging_period,
                availability_threshold,
                availability_threshold_median,
                time_window_median,
                control_threshold,
                ann_invalid_datum,
                ann_unident_spk,
                pr_int,
                preprocess_time_window,
            )
