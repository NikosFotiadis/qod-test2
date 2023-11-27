from typing import Any

import numpy as np
import pandas as pd
import pytest

from obc_sqc.model.averaging_utils import AveragingUtils
from tests.obc_sqc.fixtures.average_utils_fixtures_test import *  # noqa: F403


class TestColumnAverageUsingAnnotation:
    """Tests the column_average_using_annotation() function in multiple scenarios."""

    @pytest.mark.parametrize("threshold", np.round(np.arange(0.1, 0.8, 0.1, dtype=np.float64), decimals=1))
    def test_normal_return_float_success(self, threshold: float, sample_df: pd.DataFrame) -> None:  # noqa: PLR6301
        """Tests column_average_using_annotation() with a sample dataframe and a sufficient threshold.

        Args:
        ----
            threshold (float): the threshold used for column_average_using_annotation()
            sample_df (pd.DataFrame): the dataframe containing the data

        Returns:
        -------
            None
        """
        variable: str = "values"
        annotation_column_name: str = "annotation"

        average: float = AveragingUtils.column_average_using_annotation(
            sample_df, variable, threshold, annotation_column_name
        )

        target_average: float = 26.2

        assert average == target_average

    @pytest.mark.parametrize("threshold", np.round(np.arange(0.8, 1.1, 0.1, dtype=np.float64), decimals=1))
    def test_normal_return_nan_success(self, threshold: float, sample_df: pd.DataFrame) -> None:  # noqa: PLR6301
        """Tests column_average_using_annotation() with a sample dataframe and an insufficient threshold.

        Args:
        ----
            threshold (float): the threshold used for column_average_using_annotation()
            sample_df (pd.DataFrame): the dataframe containing the data

        Returns:
        -------
            None
        """
        variable: str = "values"
        annotation_column_name: str = "annotation"

        average: float = AveragingUtils.column_average_using_annotation(
            sample_df, variable, threshold, annotation_column_name
        )

        assert average is np.nan

    @pytest.mark.parametrize("threshold", [0.01, 0.99])
    def test_empty_crash(self, threshold: float, empty_df: pd.DataFrame) -> None:  # noqa: PLR6301
        """Tests column_average_using_annotation() with an empty dataframe.

        Args:
        ----
            threshold (float): the threshold used for column_average_using_annotation()
            empty_df (pd.DataFrame): the dataframe containing the data

        Returns:
        -------
            None
        """
        variable: str = "values"
        annotation_column_name: str = "annotation"

        average: float = AveragingUtils.column_average_using_annotation(
            empty_df, variable, threshold, annotation_column_name
        )

        assert average is np.nan

    @pytest.mark.parametrize("threshold", [0.01, 0.99])
    def test_nans_return_nan_success(self, threshold: float, nan_df: pd.DataFrame) -> None:  # noqa: PLR6301
        """Tests column_average_using_annotation() with a dataframe filled with nans.

        Args:
        ----
            threshold (float): the threshold used for column_average_using_annotation()
            nan_df (pd.DataFrame): the dataframe containing the data

        Returns:
        -------
            None
        """
        variable: str = "values"
        annotation_column_name: str = "annotation"

        average: float = AveragingUtils.column_average_using_annotation(
            nan_df, variable, threshold, annotation_column_name
        )

        assert average is np.nan


class TestColumnWindSpeedAverageUsingAnnotation:
    """Tests the column_wind_speed_average_using_annotation() function in multiple scenarios."""

    @pytest.mark.parametrize("threshold", np.round(np.arange(0, 0.5, 0.2, dtype=np.float64), decimals=1))
    def test_normal_return_float_success(self, threshold: float, sample_wind_df: pd.DataFrame) -> None:  # noqa: PLR6301
        """Tests column_wind_speed_average_using_annotation() with a sample dataframe and a sufficient threshold.

        Args:
        ----
            threshold (float): the threshold used for column_wind_speed_average_using_annotation()
            sample_wind_df (pd.DataFrame): the dataframe containing the data

        Returns:
        -------
            None
        """
        annotation_column_name: str = "annotation"

        average: float = AveragingUtils.column_wind_speed_average_using_annotation(
            sample_wind_df, threshold, annotation_column_name
        )

        target_average: float = 5.0

        assert average == target_average

    @pytest.mark.parametrize("threshold", np.round(np.arange(0.5, 1.1, 0.2, dtype=np.float64), decimals=1))
    def test_normal_return_nan_success(self, threshold: float, sample_wind_df: pd.DataFrame) -> None:  # noqa: PLR6301
        """Tests column_wind_speed_average_using_annotation() with a sample dataframe and an insufficient threshold.

        Args:
        ----
            threshold (float): the threshold used for column_wind_speed_average_using_annotation()
            sample_wind_df (pd.DataFrame): the dataframe containing the data

        Returns:
        -------
            None
        """
        annotation_column_name: str = "annotation"

        average: float = AveragingUtils.column_wind_speed_average_using_annotation(
            sample_wind_df, threshold, annotation_column_name
        )

        assert average is np.nan

    @pytest.mark.parametrize("threshold", [0.01, 0.99])
    def test_empty_crash(self, threshold: float, empty_df: pd.DataFrame) -> None:  # noqa: PLR6301
        """Tests column_wind_speed_average_using_annotation() with an empty dataframe.

        Args:
        ----
            threshold (float): the threshold used for column_wind_speed_average_using_annotation()
            empty_df (pd.DataFrame): the dataframe containing the data

        Returns:
        -------
            None
        """
        annotation_column_name: str = "annotation"

        average: float = AveragingUtils.column_wind_speed_average_using_annotation(
            empty_df, threshold, annotation_column_name
        )

        assert average is np.nan

    @pytest.mark.parametrize("threshold", [0.01, 0.99])
    def test_nans_return_nan_success(self, threshold: float, nan_wind_df: pd.DataFrame) -> None:  # noqa: PLR6301
        """Tests column_wind_speed_average_using_annotation() with a dataframe filled with nans.

        Args:
        ----
            threshold (float): the threshold used for column_wind_speed_average_using_annotation()
            nan_wind_df (pd.DataFrame): the dataframe containing the data

        Returns:
        -------
            None
        """
        annotation_column_name: str = "annotation"

        average: float = AveragingUtils.column_wind_speed_average_using_annotation(
            nan_wind_df, threshold, annotation_column_name
        )

        assert average is np.nan


class TestColumnWindDirectionAverageUsingAnnotation:
    """Tests the column_wind_direction_average_using_annotation() function in multiple scenarios."""

    @pytest.mark.parametrize("threshold", np.round(np.arange(0, 0.5, 0.2, dtype=np.float64), decimals=1))
    def test_normal_return_float_success(self, threshold: float, sample_wind_df: pd.DataFrame) -> None:  # noqa: PLR6301
        """Tests column_wind_direction_average_using_annotation() with a sample dataframe and a sufficient threshold.

        Args:
        ----
            threshold (float): the threshold used for column_wind_direction_average_using_annotation()
            sample_wind_df (pd.DataFrame): the dataframe containing the data

        Returns:
        -------
            None
        """
        annotation_column_name = "annotation"

        average: float = AveragingUtils.column_wind_direction_average_using_annotation(
            sample_wind_df, threshold, annotation_column_name
        )

        assert average == pytest.approx(126.87, abs=1)

    def test_complicated_return_float_success(self, complicated_wind_df: pd.DataFrame) -> None:  # noqa: PLR6301
        """Tests column_wind_direction_average_using_annotation().

            Test is performed using a sample, more complicated, dataframe and a sufficient threshold.

        Args:
        ----
            complicated_wind_df (pd.DataFrame): the dataframe containing the data

        Returns:
        -------
            None
        """
        annotation_column_name: str = "annotation"
        threshold: float = 0

        assert (
            complicated_wind_df.apply(
                lambda row: AveragingUtils.column_wind_direction_average_using_annotation(
                    pd.DataFrame(row).T, threshold, annotation_column_name
                ),
                axis=1,
            )
            == complicated_wind_df["direction"]
        ).all()

    @pytest.mark.parametrize("threshold", np.round(np.arange(0.5, 1.1, 0.2, dtype=np.float64), decimals=1))
    def test_normal_return_nan_success(self, threshold: float, sample_wind_df: pd.DataFrame) -> None:  # noqa: PLR6301
        """Tests column_wind_direction_average_using_annotation().

            Test is performed using a sample dataframe and an insufficient threshold.

        Args:
        ----
            threshold (float): the threshold used for column_wind_direction_average_using_annotation()
            sample_wind_df (pd.DataFrame): the dataframe containing the data

        Returns:
        -------
            None
        """
        annotation_column_name: str = "annotation"

        average: float = AveragingUtils.column_wind_direction_average_using_annotation(
            sample_wind_df, threshold, annotation_column_name
        )

        assert average is np.nan

    @pytest.mark.parametrize("threshold", [0.01, 0.99])
    def test_empty_crash(self, threshold: float, empty_df: pd.DataFrame) -> None:  # noqa: PLR6301
        """Tests column_wind_direction_average_using_annotation() with an empty dataframe.

        Args:
        ----
            threshold (float): the threshold used for column_wind_direction_average_using_annotation()
            empty_df (pd.DataFrame): the dataframe containing the data

        Returns:
        -------
            None
        """
        annotation_column_name: str = "annotation"

        average: float = AveragingUtils.column_wind_direction_average_using_annotation(
            empty_df, threshold, annotation_column_name
        )

        assert average is np.nan

    @pytest.mark.parametrize("threshold", [0.01, 0.99])
    def test_nans_return_nan_success(self, threshold: float, nan_wind_df: pd.DataFrame) -> None:  # noqa: PLR6301
        """Tests column_wind_direction_average_using_annotation() with a dataframe filled with nans.

        Args:
        ----
            threshold (float): the threshold used for column_wind_direction_average_using_annotation()
            nan_wind_df (pd.DataFrame): the dataframe containing the data

        Returns:
        -------
            None
        """
        annotation_column_name = "annotation"

        average: float = AveragingUtils.column_wind_direction_average_using_annotation(
            nan_wind_df, threshold, annotation_column_name
        )

        assert average is np.nan


class TestRowWindSpeedCalculation:
    """Tests the row_wind_speed_calculation() function in multiple scenarios."""

    def test_normal_return_float_success(self, sample_wind_df: pd.DataFrame) -> None:  # noqa: PLR6301
        """Tests row_wind_speed_calculation().

            Test is performed using sample rows of a DataFrame, containing u and v wind components.

        Args:
        ----
            sample_wind_df (pd.DataFrame): the DataFrame containing the data

        Returns:
        -------
            None
        """
        sample_wind_df = sample_wind_df.rename(columns={"wind_u": "u", "wind_v": "v"})

        assert (
            sample_wind_df.apply(AveragingUtils.row_wind_speed_calculation, axis=1) == sample_wind_df["speed"]
        ).all()

    def test_empty_crash(self, empty_df: pd.DataFrame) -> None:  # noqa: PLR6301
        """Tests row_wind_speed_calculation() with empty rows of an empty DataFrame.

        Args:
        ----
            empty_df (pd.DataFrame): the empty DataFrame

        Returns:
        -------
            None
        """

        def check_key_error(row: Any) -> None:
            AveragingUtils.row_wind_speed_calculation(row)

        empty_df.apply(check_key_error, axis=1)  # type: ignore[call-overload]

    def test_nans_return_nan_success(self, nan_wind_df: pd.DataFrame) -> None:  # noqa: PLR6301
        """Tests row_wind_speed_calculation() with rows of a DataFrame, containing only nans.

        Args:
        ----
            nan_wind_df (pd.DataFrame): the DataFrame containing the data

        Returns:
        -------
            None
        """
        nan_wind_df = nan_wind_df.rename(columns={"wind_u": "u", "wind_v": "v"})

        assert (nan_wind_df.apply(AveragingUtils.row_wind_speed_calculation, axis=1).isna()).all()


class TestRowWindDirectionCalculation:
    """Tests the row_wind_direction_calculation() function in multiple scenarios."""

    def test_normal_return_float_success(self, sample_wind_df: pd.DataFrame) -> None:  # noqa: PLR6301
        """Tests row_wind_direction_calculation().

            Test is performed using sample rows of a DataFrame, containing u and v wind components.

        Args:
        ----
            sample_wind_df (pd.DataFrame): the DataFrame containing the data

        Returns:
        -------
            None
        """
        sample_wind_df = sample_wind_df.rename(columns={"wind_u": "u", "wind_v": "v"})

        assert sample_wind_df.apply(AveragingUtils.row_wind_direction_calculation, axis=1).tolist() == pytest.approx(
            sample_wind_df["direction"].astype(np.float64), abs=1
        )

    def test_complicated_return_float_success(self, complicated_wind_df: pd.DataFrame) -> None:  # noqa: PLR6301
        """Tests row_wind_direction_calculation().

            Test is performed using sample, more complicated, rows of a DataFrame, containing
            u and v wind components.

        Args:
        ----
            complicated_wind_df (pd.DataFrame): the DataFrame containing the data

        Returns:
        -------
            None
        """
        complicated_wind_df = complicated_wind_df.rename(columns={"wind_u": "u", "wind_v": "v"})

        assert (
            complicated_wind_df.apply(AveragingUtils.row_wind_direction_calculation, axis=1)
            == complicated_wind_df["direction"]
        ).all()

    def test_empty_crash(self, empty_df: pd.DataFrame) -> None:  # noqa: PLR6301
        """Tests row_wind_direction_calculation() with empty rows of an empty DataFrame.

        Args:
        ----
            empty_df (pd.Series): the empty DataFrame

        Returns:
        -------
            None
        """

        def check_key_error(row: Any) -> None:
            with pytest.raises(KeyError):
                AveragingUtils.row_wind_direction_calculation(row)

        empty_df.apply(check_key_error, axis=1)  # type: ignore[call-overload]

    def test_nans_return_nan_success(self, nan_wind_df: pd.DataFrame) -> None:  # noqa: PLR6301
        """Tests row_wind_direction_calculation() with rows of a DataFrame, containing only nans.

        Args:
        ----
            nan_wind_df (pd.DataFrame): the DataFrame row containing the data

        Returns:
        -------
            None
        """
        nan_wind_df = nan_wind_df.rename(columns={"wind_u": "u", "wind_v": "v"})

        assert (nan_wind_df.apply(AveragingUtils.row_wind_direction_calculation, axis=1).isna()).all()
