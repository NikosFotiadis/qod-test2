import pandas as pd
import pytest

from obc_sqc.model.hour_averaging import HourAveraging
from tests.obc_sqc.fixtures.hour_averaging_fixtures_test import *  # noqa: F403

control_thresholds: dict[str, float] = {
    "temperature": 0.67,
    "wind_speed": 0.75,
    "wind_direction": 0.75,
    "precipitation_accumulated": 0.85,
}


class TestHourAveraging:
    """Tests the hour_averaging() function in multiple scenarios."""

    @pytest.mark.parametrize(
        "minute_averaging_df, hour_averaging_df, availability_threshold, parameter",
        [
            (variable, variable, threshold, variable)
            for variable in ["temperature", "wind_speed", "wind_direction", "precipitation_accumulated"]
            for threshold in [0.01, control_thresholds.get(variable), 0.99]
        ],
        indirect=["minute_averaging_df", "hour_averaging_df"],
    )
    def test_normal_return_float_success(
        self,
        minute_averaging_df: pd.DataFrame,
        hour_averaging_df: pd.DataFrame,
        availability_threshold: float,
        parameter: str,
    ) -> None:
        """Tests hour_averaging() with a sample dataframe and different thresholds.

        Args:
        ----
            minute_averaging_df (pd.DataFrame): the dataframe containing minute averaged data
            hour_averaging_df (pd.DataFrame): the dataframe containing hour averaged data
            availability_threshold (float): the threshold used for hour_averaging()
            parameter (str): the name of the examined parameter

        Returns:
        -------
            None
        """
        fnl_timeslot: int = 60

        average_result: pd.DataFrame = HourAveraging.hour_averaging(
            minute_averaging_df, fnl_timeslot, availability_threshold, parameter
        )

        # re-order columns
        average_result = average_result[hour_averaging_df.columns]

        for column1, column2 in zip(average_result.columns, hour_averaging_df.columns, strict=True):
            result: bool = average_result[column1].equals(hour_averaging_df[column2])
            assert result

    @pytest.mark.parametrize(
        "minute_averaging_nan_df, availability_threshold, parameter",
        [
            (variable, threshold, variable)
            for variable in ["temperature", "wind_speed", "wind_direction", "precipitation_accumulated"]
            for threshold in [0.01, control_thresholds.get(variable), 0.99]
        ],
        indirect=["minute_averaging_nan_df"],
    )
    def test_nans_crash(
        self,
        minute_averaging_nan_df: pd.DataFrame,
        availability_threshold: float,
        parameter: str,
    ) -> None:
        """Tests hour_averaging() with a dataframe filled with nans and different thresholds.

        Args:
        ----
            minute_averaging_nan_df (pd.DataFrame): the dataframe containing nans for minute averaged data
            availability_threshold (float): the threshold used for hour_averaging()
            parameter (str): the name of the examined parameter

        Returns:
        -------
            None
        """
        fnl_timeslot: int = 60

        with pytest.raises(AttributeError):
            HourAveraging.hour_averaging(minute_averaging_nan_df, fnl_timeslot, availability_threshold, parameter)

    @pytest.mark.parametrize(
        "minute_averaging_empty_df, availability_threshold, parameter",
        [
            (variable, threshold, variable)
            for variable in ["temperature", "wind_speed", "wind_direction"]
            for threshold in [0.01, control_thresholds.get(variable), 0.99]
        ],
        indirect=["minute_averaging_empty_df"],
    )
    def test_empty_crash(
        self,
        minute_averaging_empty_df: pd.DataFrame,
        availability_threshold: float,
        parameter: str,
    ) -> None:
        """Tests hour_averaging() with an empty dataframe and different thresholds.

        Args:
        ----
            minute_averaging_empty_df (pd.DataFrame): the empty dataframe for minute averaged data
            availability_threshold (float): the threshold used for hour_averaging()
            parameter (str): the name of the examined parameter

        Returns:
        -------
            None
        """
        fnl_timeslot: int = 60

        with pytest.raises(ValueError):
            HourAveraging.hour_averaging(minute_averaging_empty_df, fnl_timeslot, availability_threshold, parameter)

    @pytest.mark.parametrize(
        "minute_averaging_empty_df, hour_averaging_precipitation_accumulated_empty_df, availability_threshold,"
        " parameter",
        [
            (variable, variable, threshold, variable)
            for variable in ["precipitation_accumulated"]
            for threshold in [0.01, control_thresholds.get(variable), 0.99]
        ],
        indirect=["minute_averaging_empty_df", "hour_averaging_precipitation_accumulated_empty_df"],
    )
    def test_precipitation_accumulated_empty_success(
        self,
        minute_averaging_empty_df: pd.DataFrame,
        hour_averaging_precipitation_accumulated_empty_df: pd.DataFrame,
        availability_threshold: float,
        parameter: str,
    ) -> None:
        """Tests hour_averaging() for precipitation with an empty dataframe and different thresholds.

        Args:
        ----
            minute_averaging_empty_df (pd.DataFrame): the empty dataframe for minute averaged data
            hour_averaging_precipitation_accumulated_empty_df (pd.DataFrame): the dataframe containing
                                                                                hour averaged data
            availability_threshold (float): the threshold used for hour_averaging()
            parameter (str): the name of the examined parameter

        Returns:
        -------
            None
        """
        fnl_timeslot: int = 60

        average_result: pd.DataFrame = HourAveraging.hour_averaging(
            minute_averaging_empty_df, fnl_timeslot, availability_threshold, parameter
        )

        # re-order columns
        average_result = average_result[hour_averaging_precipitation_accumulated_empty_df.columns]

        result: bool = average_result.equals(hour_averaging_precipitation_accumulated_empty_df)
        assert result
