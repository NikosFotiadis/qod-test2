import pandas as pd
import pytest
from obc_sqc.model.filling_ignoring_period import FillingIgnoringPeriod
from tests.obc_sqc.fixtures.filling_ignoring_period_fixtures_test import *  # noqa: F403


class TestFillingIgnoringPeriod:
    """Tests the filling_ignoring_period() function in multiple scenarios."""

    @pytest.mark.parametrize(
        "filling_ignoring_period_input_df, filling_ignoring_period_output_df, parameter,",
        [
            (
                variable,
                variable,
                variable,
            )
            for variable in ["temperature", "wind_speed", "wind_direction", "precipitation_accumulated"]
        ],
        indirect=["filling_ignoring_period_input_df", "filling_ignoring_period_output_df"],
    )
    def test_normal_return_success(
        self,
        filling_ignoring_period_input_df: pd.DataFrame,
        filling_ignoring_period_output_df: pd.DataFrame,
        parameter: str,
    ) -> None:
        """Tests filling_ignoring_period() with a sample dataframe.

        Args:
        ----
            filling_ignoring_period_input_df (pd.DataFrame): the dataframe containing input data
            filling_ignoring_period_output_df (pd.DataFrame): the dataframe containing output data
            parameter (str): the name of the examined parameter

        Returns:
        -------
            None
        """
        ignoring_period: int = 60
        data_timestep: int = 16

        filling_ignoring_period_result: pd.DataFrame = FillingIgnoringPeriod.filling_ignoring_period(
            filling_ignoring_period_input_df,
            parameter,
            ignoring_period,
            data_timestep,
        )

        # re-order columns
        filling_ignoring_period_result = filling_ignoring_period_result[filling_ignoring_period_output_df.columns]

        for column1, column2 in zip(
            filling_ignoring_period_result.columns, filling_ignoring_period_output_df.columns, strict=True
        ):
            result: bool = filling_ignoring_period_result[column1].equals(filling_ignoring_period_output_df[column2])
            assert result

    @pytest.mark.parametrize(
        "filling_ignoring_period_input_nan_df, filling_ignoring_period_output_nan_df, parameter,",
        [
            (
                variable,
                variable,
                variable,
            )
            for variable in ["temperature", "wind_speed", "wind_direction", "precipitation_accumulated"]
        ],
        indirect=["filling_ignoring_period_input_nan_df", "filling_ignoring_period_output_nan_df"],
    )
    def test_nans_success(
        self,
        filling_ignoring_period_input_nan_df: pd.DataFrame,
        filling_ignoring_period_output_nan_df: pd.DataFrame,
        parameter: str,
    ) -> None:
        """Tests filling_ignoring_period() with a nan-filled dataframe.

        Args:
        ----
            filling_ignoring_period_input_nan_df (pd.DataFrame): the dataframe containing input data
            filling_ignoring_period_output_nan_df (pd.DataFrame): the dataframe containing output data
            parameter (str): the name of the examined parameter

        Returns:
        -------
            None
        """
        ignoring_period: int = 60
        data_timestep: int = 16

        filling_ignoring_period_result: pd.DataFrame = FillingIgnoringPeriod.filling_ignoring_period(
            filling_ignoring_period_input_nan_df,
            parameter,
            ignoring_period,
            data_timestep,
        )

        # re-order columns
        filling_ignoring_period_result = filling_ignoring_period_result[filling_ignoring_period_output_nan_df.columns]

        filling_ignoring_period_result = filling_ignoring_period_result.fillna(pd.NA)
        filling_ignoring_period_output_nan_df = filling_ignoring_period_output_nan_df.fillna(pd.NA)

        for column1, column2 in zip(
            filling_ignoring_period_result.columns, filling_ignoring_period_output_nan_df.columns, strict=True
        ):
            result: bool = filling_ignoring_period_result[column1].equals(
                filling_ignoring_period_output_nan_df[column2]
            )
            assert result

    @pytest.mark.parametrize(
        "filling_ignoring_period_input_empty_df, filling_ignoring_period_output_empty_df, parameter,",
        [
            (
                variable,
                variable,
                variable,
            )
            for variable in ["temperature", "wind_speed", "wind_direction", "precipitation_accumulated"]
        ],
        indirect=["filling_ignoring_period_input_empty_df", "filling_ignoring_period_output_empty_df"],
    )
    def test_empty_success(
        self,
        filling_ignoring_period_input_empty_df: pd.DataFrame,
        filling_ignoring_period_output_empty_df: pd.DataFrame,
        parameter: str,
    ) -> None:
        """Tests filling_ignoring_period() with an empty dataframe.

        Args:
        ----
            filling_ignoring_period_input_empty_df (pd.DataFrame): the dataframe containing input data
            filling_ignoring_period_output_empty_df (pd.DataFrame): the dataframe containing output data
            parameter (str): the name of the examined parameter

        Returns:
        -------
            None
        """
        ignoring_period: int = 60
        data_timestep: int = 16

        filling_ignoring_period_result: pd.DataFrame = FillingIgnoringPeriod.filling_ignoring_period(
            filling_ignoring_period_input_empty_df,
            parameter,
            ignoring_period,
            data_timestep,
        )

        # re-order columns
        filling_ignoring_period_result = filling_ignoring_period_result[filling_ignoring_period_output_empty_df.columns]

        for column1, column2 in zip(
            filling_ignoring_period_result.columns, filling_ignoring_period_output_empty_df.columns, strict=True
        ):
            result: bool = filling_ignoring_period_result[column1].equals(
                filling_ignoring_period_output_empty_df[column2]
            )
            assert result
