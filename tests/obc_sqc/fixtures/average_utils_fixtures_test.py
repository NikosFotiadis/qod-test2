import pytest
import pandas as pd
import numpy as np

from typing import Union


@pytest.fixture
def sample_df() -> pd.DataFrame:
    """Creates a sample DataFrame, containing a column with int values and a column with 0/1 annotations.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    data: dict[str, list[float]] = {"values": [25, 27, 23, 22, 26, 28, 29], "annotation": [0, np.nan, 0, 1, 0, 0, 0]}

    dtypes = {
        "values": "Float64",
        "annotation": "Float64",
    }

    df: pd.DataFrame = pd.DataFrame(data).astype(dtypes)

    return df


@pytest.fixture
def empty_df() -> pd.DataFrame:
    """Creates an empty DataFrame.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    dtypes = {
        "values": "Float64",
        "annotation": "Float64",
    }

    df = pd.DataFrame(columns=["values", "annotation"]).astype(dtype=dtypes)  # noqa: PD901

    return df


@pytest.fixture
def nan_df() -> pd.DataFrame:
    """Creates a sample DataFrame, containing a column with nan values and a column with nan annotations.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    data: dict[str, list[float]] = {"values": [np.nan] * 7, "annotation": [np.nan] * 7}

    dtypes = {
        "values": "Float64",
        "annotation": "Float64",
    }

    df: pd.DataFrame = pd.DataFrame(data).astype(dtypes)

    return df


@pytest.fixture
def sample_wind_df() -> pd.DataFrame:
    """Creates a sample DataFrame for wind.

        The DataFrame contains columns with u and v components, 0/1 annotations, target wind speed
        and target wind direction.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    data: dict[str, Union[np.ndarray, list[int], list[float]]] = {
        "wind_u": np.arange(-40, 40, 4),
        "wind_v": np.arange(30, -30, -3),
        "annotation": [0, 1] * 10,
        "speed": [50, 45, 40, 35, 30, 25, 20, 15, 10, 5, 0, 5, 10, 15, 20, 25, 30, 35, 40, 45],
        "direction": [
            126.87,
            126.87,
            126.87,
            126.87,
            126.87,
            126.87,
            126.87,
            126.87,
            126.87,
            126.87,
            180.0,
            306.87,
            306.87,
            306.87,
            306.87,
            306.87,
            306.87,
            306.87,
            306.87,
            306.87,
        ],
    }

    dtypes = {
        "wind_u": "Float64",
        "wind_v": "Float64",
        "annotation": "Float64",
        "speed": "Float64",
        "direction": "Float64",
    }

    df: pd.DataFrame = pd.DataFrame(data).astype(dtypes)

    return df


@pytest.fixture
def complicated_wind_df() -> pd.DataFrame:
    """Creates a sample DataFrame with edge cases for wind.

        The DataFrame contains columns with u and v components, 0/1 annotations,
        target wind speed and target wind direction.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    data: dict[str, list[int]] = {
        "wind_u": [0, -10, 0, 10],
        "wind_v": [-10, 0, 10, 0],
        "annotation": [0, 0, 0, 0],
        "speed": [10, 10, 10, 10],
        "direction": [180, 90, 180, 270],
    }

    dtypes = {
        "wind_u": "Float64",
        "wind_v": "Float64",
        "annotation": "Float64",
        "speed": "Float64",
        "direction": "Float64",
    }

    df: pd.DataFrame = pd.DataFrame(data).astype(dtypes)

    return df


@pytest.fixture
def empty_wind_df() -> pd.DataFrame:
    """Creates an empty DataFrame.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    dtypes = {
        "wind_u": "Float64",
        "wind_v": "Float64",
        "annotation": "Float64",
        "speed": "Float64",
        "direction": "Float64",
    }

    df = pd.DataFrame(columns=["wind_u", "wind_v", "annotation", "speed", "direction"]).astype(dtypes)  # noqa: PD901

    return df


@pytest.fixture
def nan_wind_df() -> pd.DataFrame:
    """Creates a DataFrame for wind.

        The DataFrame contains columns with u and v components and 0/1 annotations,
        all filled with nans.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    data: dict[str, list[float]] = {"wind_u": [np.nan] * 7, "wind_v": [np.nan] * 7, "annotation": [np.nan] * 7}

    dtypes = {
        "wind_u": "Float64",
        "wind_v": "Float64",
        "annotation": "Float64",
    }

    df: pd.DataFrame = pd.DataFrame(data).astype(dtypes)

    return df
