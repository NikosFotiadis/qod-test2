import pandas as pd
import pytest

from obc_sqc.model.constant_data_check import ConstantDataCheck
from tests.obc_sqc.fixtures.constant_data_check_fixtures_test import *  # noqa: F403


time_window_constant_dict: dict[str, int] = {
    "temperature": 240,
    "wind_speed": 360,
    "wind_direction": 360,
}

time_window_constant_max_dict: dict[str, int] = {
    "temperature": 1440,
    "wind_speed": 1440,
    "wind_direction": 1440,
}


class TestCheckForConstantData:
    """Tests the constant_data_check() function in multiple scenarios."""

    @pytest.mark.parametrize(
        "constant_data_check_input_df, constant_data_check_output_df, parameter, time_window_constant,"
        " time_window_constant_max",
        [
            (
                (variable, i),
                (variable, i),
                variable,
                time_window_constant_dict.get(variable),
                time_window_constant_max_dict.get(variable),
            )
            for variable in ["temperature", "wind_speed", "wind_direction"]
            for i in [1, 2]  # check for two different stations
        ],
        indirect=["constant_data_check_input_df", "constant_data_check_output_df"],
    )
    def test_normal_return_float_success(
        self,
        constant_data_check_input_df: pd.DataFrame,
        constant_data_check_output_df: pd.DataFrame,
        parameter: str,
        time_window_constant: int,
        time_window_constant_max: int,
    ) -> None:
        """Tests hour_averaging() with a sample dataframe and different thresholds.

        Args:
        ----
            constant_data_check_input_df (pd.DataFrame): the dataframe containing input raw data data
            constant_data_check_output_df (pd.DataFrame): the dataframe containing output raw data, checked
                                                            for constant values
            parameter (str): the name of the examined parameter
            time_window_constant (int): the rolling time window for checking constant values [in minutes]
            time_window_constant_max (int): the time window for checking values that are constant during a
                                            whole day [in minutes]

        Returns:
        -------
            None
        """
        ann_constant: int = 5
        ann_constant_frozen: int = 6
        rh_threshold: float = 95
        ann_constant_max: int = 7

        constant_data_check_result: pd.DataFrame = ConstantDataCheck.constant_data_check(
            constant_data_check_input_df,
            parameter,
            time_window_constant,
            ann_constant,
            ann_constant_frozen,
            rh_threshold,
            time_window_constant_max,
            ann_constant_max,
        )

        # re-order columns
        constant_data_check_result = constant_data_check_result[constant_data_check_output_df.columns]

        for column1, column2 in zip(
            constant_data_check_result.columns, constant_data_check_output_df.columns, strict=True
        ):
            result: bool = constant_data_check_result[column1].equals(constant_data_check_output_df[column2])
            assert result

    @pytest.mark.parametrize(
        "constant_data_check_input_nan_df, parameter, time_window_constant, time_window_constant_max",
        [
            (variable, variable, time_window_constant_dict.get(variable), time_window_constant_max_dict.get(variable))
            for variable in ["temperature", "wind_speed", "wind_direction"]
        ],
        indirect=["constant_data_check_input_nan_df"],
    )
    def test_nans_crash(
        self,
        constant_data_check_input_nan_df: pd.DataFrame,
        parameter: str,
        time_window_constant: int,
        time_window_constant_max: int,
    ) -> None:
        """Tests constant_data_check() with a nan-filled dataframe.

        Args:
        ----
            constant_data_check_input_nan_df (pd.DataFrame): the dataframe containing input raw data data
            parameter (str): the name of the examined parameter
            time_window_constant (int): the rolling time window for checking constant values [in minutes]
            time_window_constant_max (int): the time window for checking values that are constant during a
                                            whole day [in minutes]

        Returns:
        -------
            None
        """
        ann_constant: int = 5
        ann_constant_frozen: int = 6
        rh_threshold: float = 95
        ann_constant_max: int = 7

        with pytest.raises(ValueError):
            ConstantDataCheck.constant_data_check(
                constant_data_check_input_nan_df,
                parameter,
                time_window_constant,
                ann_constant,
                ann_constant_frozen,
                rh_threshold,
                time_window_constant_max,
                ann_constant_max,
            )

    @pytest.mark.parametrize(
        "constant_data_check_input_empty_df, parameter, time_window_constant, time_window_constant_max",
        [
            (variable, variable, time_window_constant_dict.get(variable), time_window_constant_max_dict.get(variable))
            for variable in ["temperature", "wind_speed", "wind_direction"]
        ],
        indirect=["constant_data_check_input_empty_df"],
    )
    def test_empty_crash(
        self,
        constant_data_check_input_empty_df: pd.DataFrame,
        parameter: str,
        time_window_constant: int,
        time_window_constant_max: int,
    ) -> None:
        """Tests constant_data_check() with an empty dataframe.

        Args:
        ----
            constant_data_check_input_empty_df (pd.DataFrame): the dataframe containing input raw data data
            parameter (str): the name of the examined parameter
            time_window_constant (int): the rolling time window for checking constant values [in minutes]
            time_window_constant_max (int): the time window for checking values that are constant during a
                                            whole day [in minutes]

        Returns:
        -------
            None
        """
        ann_constant: int = 5
        ann_constant_frozen: int = 6
        rh_threshold: float = 95
        ann_constant_max: int = 7

        with pytest.raises(IndexError):
            ConstantDataCheck.constant_data_check(
                constant_data_check_input_empty_df,
                parameter,
                time_window_constant,
                ann_constant,
                ann_constant_frozen,
                rh_threshold,
                time_window_constant_max,
                ann_constant_max,
            )
