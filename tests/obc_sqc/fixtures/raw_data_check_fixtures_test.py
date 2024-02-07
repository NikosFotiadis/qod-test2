import pandas as pd
import pytest

from typing import Callable


def get_raw_data_check_input_temperature_df() -> pd.DataFrame:
    """Reads the raw data check input file from the parquet file and returns it.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    df: pd.DataFrame = pd.read_parquet(
        "tests/obc_sqc/fixtures_data/raw_data/input/raw_data_check_input_temperature_df.parquet"
    )

    dtypes: dict[str, str] = {
        "date": "datetime64[ns]",
        "temperature": "Float64",
        "humidity": "Float64",
        "wind_speed": "Float64",
        "wind_direction": "Float64",
        "pressure": "Float64",
        "illuminance": "Float64",
        "precipitation_accumulated": "Float64",
        "model": "object",
        "utc_datetime": "object",
        "ann_obc": "int64",
        "humidity_for_raw_check": "Float64",
        "humidity_consec_filling": "int64",
        "ann_constant": "int64",
        "ann_constant_long": "int64",
        "ann_constant_frozen": "int64",
        "temperature_for_raw_check": "Float64",
        "temperature_consec_filling": "int64",
    }
    df = df.astype(dtypes)

    return df


def get_raw_data_check_input_temperature_nan_df() -> pd.DataFrame:
    """Creates the input dataframe filled with nans and returns it.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    dtypes: dict[str, str] = {
        "date": "float64",
        "temperature": "float64",
        "humidity": "float64",
        "wind_speed": "float64",
        "wind_direction": "float64",
        "pressure": "float64",
        "illuminance": "float64",
        "precipitation_accumulated": "float64",
        "model": "float64",
        "utc_datetime": "float64",
        "ann_obc": "float64",
        "humidity_for_raw_check": "float64",
        "humidity_consec_filling": "float64",
        "ann_constant": "float64",
        "ann_constant_long": "float64",
        "ann_constant_frozen": "float64",
        "temperature_for_raw_check": "float64",
        "temperature_consec_filling": "float64",
    }

    # Create an empty DataFrame with specified columns
    df: pd.DataFrame = pd.DataFrame(index=range(6750), columns=list(dtypes.keys()))

    df = df.astype(dtypes)

    df.loc[:] = pd.NA  # type: ignore [call-overload]

    return df


def get_raw_data_check_input_temperature_empty_df() -> pd.DataFrame:
    """Creates the input empty dataframe and returns it.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    dtypes: dict[str, str] = {
        "date": "datetime64[ns]",
        "temperature": "Float64",
        "humidity": "Float64",
        "wind_speed": "Float64",
        "wind_direction": "Float64",
        "pressure": "Float64",
        "illuminance": "Float64",
        "precipitation_accumulated": "Float64",
        "model": "object",
        "utc_datetime": "object",
        "ann_obc": "int64",
        "humidity_for_raw_check": "Float64",
        "humidity_consec_filling": "int64",
        "ann_constant": "int64",
        "ann_constant_long": "int64",
        "ann_constant_frozen": "int64",
        "temperature_for_raw_check": "Float64",
        "temperature_consec_filling": "int64",
    }

    # Create an empty DataFrame with specified columns
    df: pd.DataFrame = pd.DataFrame(columns=list(dtypes.keys()))

    df = df.astype(dtypes)

    return df


def get_raw_data_check_input_wind_speed_df() -> pd.DataFrame:
    """Reads the raw data check input file from the parquet file and returns it.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    df: pd.DataFrame = pd.read_parquet(
        "tests/obc_sqc/fixtures_data/raw_data/input/raw_data_check_input_wind_speed_df.parquet"
    )

    dtypes: dict[str, str] = {
        "date": "datetime64[ns]",
        "temperature": "Float64",
        "humidity": "Float64",
        "wind_speed": "Float64",
        "wind_direction": "Float64",
        "pressure": "Float64",
        "illuminance": "Float64",
        "precipitation_accumulated": "Float64",
        "model": "object",
        "utc_datetime": "object",
        "ann_obc": "int64",
        "humidity_for_raw_check": "Float64",
        "humidity_consec_filling": "int64",
        "ann_constant": "float64",
        "ann_constant_long": "int64",
        "ann_constant_frozen": "float64",
        "temperature_for_raw_check": "Float64",
        "temperature_consec_filling": "int64",
        "wind_direction_for_raw_check": "Float64",
        "wind_direction_consec_filling": "int64",
        "wind_speed_for_raw_check": "Float64",
        "wind_speed_consec_filling": "int64",
    }
    df = df.astype(dtypes)

    return df


def get_raw_data_check_input_wind_speed_nan_df() -> pd.DataFrame:
    """Creates the input dataframe filled with nans and returns it.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    dtypes: dict[str, str] = {
        "date": "float64",
        "temperature": "float64",
        "humidity": "float64",
        "wind_speed": "float64",
        "wind_direction": "float64",
        "pressure": "float64",
        "illuminance": "float64",
        "precipitation_accumulated": "float64",
        "model": "float64",
        "utc_datetime": "float64",
        "ann_obc": "float64",
        "humidity_for_raw_check": "float64",
        "humidity_consec_filling": "float64",
        "ann_constant": "float64",
        "ann_constant_long": "float64",
        "ann_constant_frozen": "float64",
        "temperature_for_raw_check": "float64",
        "temperature_consec_filling": "float64",
        "wind_direction_for_raw_check": "float64",
        "wind_direction_consec_filling": "float64",
        "wind_speed_for_raw_check": "float64",
        "wind_speed_consec_filling": "float64",
    }

    # Create an empty DataFrame with specified columns
    df: pd.DataFrame = pd.DataFrame(index=range(6750), columns=list(dtypes.keys()))

    df = df.astype(dtypes)

    df.loc[:] = pd.NA  # type: ignore [call-overload]

    return df


def get_raw_data_check_input_wind_speed_empty_df() -> pd.DataFrame:
    """Creates the input empty dataframe and returns it.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    dtypes: dict[str, str] = {
        "date": "datetime64[ns]",
        "temperature": "Float64",
        "humidity": "Float64",
        "wind_speed": "Float64",
        "wind_direction": "Float64",
        "pressure": "Float64",
        "illuminance": "Float64",
        "precipitation_accumulated": "Float64",
        "model": "object",
        "utc_datetime": "object",
        "ann_obc": "int64",
        "humidity_for_raw_check": "Float64",
        "humidity_consec_filling": "int64",
        "ann_constant": "float64",
        "ann_constant_long": "int64",
        "ann_constant_frozen": "float64",
        "temperature_for_raw_check": "Float64",
        "temperature_consec_filling": "int64",
        "wind_direction_for_raw_check": "Float64",
        "wind_direction_consec_filling": "int64",
        "wind_speed_for_raw_check": "Float64",
        "wind_speed_consec_filling": "int64",
    }

    # Create an empty DataFrame with specified columns
    df: pd.DataFrame = pd.DataFrame(columns=list(dtypes.keys()))

    df = df.astype(dtypes)

    return df


def get_raw_data_check_input_wind_direction_df() -> pd.DataFrame:
    """Reads the raw data check input file from the parquet file and returns it.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    df: pd.DataFrame = pd.read_parquet(
        "tests/obc_sqc/fixtures_data/raw_data/input/raw_data_check_input_wind_direction_df.parquet"
    )

    dtypes: dict[str, str] = {
        "date": "datetime64[ns]",
        "temperature": "Float64",
        "humidity": "Float64",
        "wind_speed": "Float64",
        "wind_direction": "Float64",
        "pressure": "Float64",
        "illuminance": "Float64",
        "precipitation_accumulated": "Float64",
        "model": "object",
        "utc_datetime": "object",
        "ann_obc": "int64",
        "humidity_for_raw_check": "Float64",
        "humidity_consec_filling": "int64",
        "ann_constant": "float64",
        "ann_constant_long": "int64",
        "ann_constant_frozen": "float64",
        "temperature_for_raw_check": "Float64",
        "temperature_consec_filling": "int64",
        "wind_direction_for_raw_check": "Float64",
        "wind_direction_consec_filling": "int64",
    }
    df = df.astype(dtypes)

    return df


def get_raw_data_check_input_wind_direction_nan_df() -> pd.DataFrame:
    """Creates the input dataframe filled with nans and returns it.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    dtypes: dict[str, str] = {
        "date": "float64",
        "temperature": "float64",
        "humidity": "float64",
        "wind_speed": "float64",
        "wind_direction": "float64",
        "pressure": "float64",
        "illuminance": "float64",
        "precipitation_accumulated": "float64",
        "model": "float64",
        "utc_datetime": "float64",
        "ann_obc": "float64",
        "humidity_for_raw_check": "float64",
        "humidity_consec_filling": "float64",
        "ann_constant": "float64",
        "ann_constant_long": "float64",
        "ann_constant_frozen": "float64",
        "temperature_for_raw_check": "float64",
        "temperature_consec_filling": "float64",
        "wind_direction_for_raw_check": "float64",
        "wind_direction_consec_filling": "float64",
    }

    # Create an empty DataFrame with specified columns
    df: pd.DataFrame = pd.DataFrame(index=range(6750), columns=list(dtypes.keys()))

    df = df.astype(dtypes)

    df.loc[:] = pd.NA  # type: ignore [call-overload]

    return df


def get_raw_data_check_input_wind_direction_empty_df() -> pd.DataFrame:
    """Creates the input empty dataframe and returns it.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    dtypes: dict[str, str] = {
        "date": "datetime64[ns]",
        "temperature": "Float64",
        "humidity": "Float64",
        "wind_speed": "Float64",
        "wind_direction": "Float64",
        "pressure": "Float64",
        "illuminance": "Float64",
        "precipitation_accumulated": "Float64",
        "model": "object",
        "utc_datetime": "object",
        "ann_obc": "int64",
        "humidity_for_raw_check": "Float64",
        "humidity_consec_filling": "int64",
        "ann_constant": "float64",
        "ann_constant_long": "int64",
        "ann_constant_frozen": "float64",
        "temperature_for_raw_check": "Float64",
        "temperature_consec_filling": "int64",
        "wind_direction_for_raw_check": "Float64",
        "wind_direction_consec_filling": "int64",
    }

    # Create an empty DataFrame with specified columns
    df: pd.DataFrame = pd.DataFrame(columns=list(dtypes.keys()))

    df = df.astype(dtypes)

    return df


def get_raw_data_check_input_precipitation_accumulated_df() -> pd.DataFrame:
    """Reads the raw data check input file from the parquet file and returns it.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    df: pd.DataFrame = pd.read_parquet(
        "tests/obc_sqc/fixtures_data/raw_data/input/raw_data_check_input_precipitation_accumulated_df.parquet"
    )

    dtypes: dict[str, str] = {
        "temperature": "Float64",
        "humidity": "Float64",
        "wind_speed": "Float64",
        "wind_direction": "Float64",
        "pressure": "Float64",
        "illuminance": "Float64",
        "precipitation_accumulated": "Float64",
        "model": "object",
        "utc_datetime": "object",
        "ann_obc": "int64",
        "humidity_for_raw_check": "Float64",
        "humidity_consec_filling": "int64",
        "date": "datetime64[ns]",
        "ann_constant": "int64",
        "ann_constant_long": "int64",
        "ann_constant_frozen": "int64",
        "temperature_for_raw_check": "Float64",
        "temperature_consec_filling": "int64",
        "wind_direction_for_raw_check": "Float64",
        "wind_direction_consec_filling": "int64",
        "wind_speed_for_raw_check": "Float64",
        "wind_speed_consec_filling": "int64",
        "pressure_for_raw_check": "Float64",
        "pressure_consec_filling": "int64",
        "illuminance_for_raw_check": "Float64",
        "illuminance_consec_filling": "int64",
        "precipitation_accumulated_for_raw_check": "Float64",
        "precipitation_accumulated_consec_filling": "int64",
        "precipitation_diff": "Float64",
        "ann_constant_max": "int64",
    }
    df = df.astype(dtypes)

    return df


def get_raw_data_check_input_precipitation_accumulated_nan_df() -> pd.DataFrame:
    """Creates the input dataframe filled with nans and returns it.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    dtypes: dict[str, str] = {
        "temperature": "float64",
        "humidity": "float64",
        "wind_speed": "float64",
        "wind_direction": "float64",
        "pressure": "float64",
        "illuminance": "float64",
        "precipitation_accumulated": "float64",
        "model": "float64",
        "utc_datetime": "float64",
        "ann_obc": "float64",
        "humidity_for_raw_check": "float64",
        "humidity_consec_filling": "float64",
        "date": "float64",
        "ann_constant": "float64",
        "ann_constant_long": "float64",
        "ann_constant_frozen": "float64",
        "temperature_for_raw_check": "float64",
        "temperature_consec_filling": "float64",
        "wind_direction_for_raw_check": "float64",
        "wind_direction_consec_filling": "float64",
        "wind_speed_for_raw_check": "float64",
        "wind_speed_consec_filling": "float64",
        "pressure_for_raw_check": "float64",
        "pressure_consec_filling": "float64",
        "illuminance_for_raw_check": "float64",
        "illuminance_consec_filling": "float64",
        "precipitation_accumulated_for_raw_check": "float64",
        "precipitation_accumulated_consec_filling": "float64",
        "precipitation_diff": "float64",
        "ann_constant_max": "float64",
    }

    # Create an empty DataFrame with specified columns
    df: pd.DataFrame = pd.DataFrame(index=range(6750), columns=list(dtypes.keys()))

    df = df.astype(dtypes)

    df.loc[:] = pd.NA  # type: ignore [call-overload]

    return df


def get_raw_data_check_input_precipitation_accumulated_empty_df() -> pd.DataFrame:
    """Creates the input empty dataframe and returns it.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    dtypes: dict[str, str] = {
        "temperature": "Float64",
        "humidity": "Float64",
        "wind_speed": "Float64",
        "wind_direction": "Float64",
        "pressure": "Float64",
        "illuminance": "Float64",
        "precipitation_accumulated": "Float64",
        "model": "object",
        "utc_datetime": "object",
        "ann_obc": "int64",
        "humidity_for_raw_check": "Float64",
        "humidity_consec_filling": "int64",
        "date": "datetime64[ns]",
        "ann_constant": "int64",
        "ann_constant_long": "int64",
        "ann_constant_frozen": "int64",
        "temperature_for_raw_check": "Float64",
        "temperature_consec_filling": "int64",
        "wind_direction_for_raw_check": "Float64",
        "wind_direction_consec_filling": "int64",
        "wind_speed_for_raw_check": "Float64",
        "wind_speed_consec_filling": "int64",
        "pressure_for_raw_check": "Float64",
        "pressure_consec_filling": "int64",
        "illuminance_for_raw_check": "Float64",
        "illuminance_consec_filling": "int64",
        "precipitation_accumulated_for_raw_check": "Float64",
        "precipitation_accumulated_consec_filling": "int64",
        "precipitation_diff": "Float64",
        "ann_constant_max": "int64",
    }

    # Create an empty DataFrame with specified columns
    df: pd.DataFrame = pd.DataFrame(columns=list(dtypes.keys()))

    df = df.astype(dtypes)

    return df


def get_raw_data_check_output_temperature_df() -> pd.DataFrame:
    """Reads the raw data check output file from the parquet file and returns it.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    df: pd.DataFrame = pd.read_parquet(
        "tests/obc_sqc/fixtures_data/raw_data/output/raw_data_check_output_temperature_df.parquet"
    ).set_index("utc_datetime")

    dtypes: dict[str, str] = {
        "date": "datetime64[ns]",
        "temperature": "Float64",
        "humidity": "Float64",
        "wind_speed": "Float64",
        "wind_direction": "Float64",
        "pressure": "Float64",
        "illuminance": "Float64",
        "precipitation_accumulated": "Float64",
        "model": "object",
        "ann_obc": "int64",
        "humidity_for_raw_check": "Float64",
        "humidity_consec_filling": "int64",
        "ann_constant": "int64",
        "ann_constant_long": "int64",
        "ann_constant_frozen": "int64",
        "temperature_for_raw_check": "Float64",
        "temperature_consec_filling": "int64",
        "rolling_median": "float64",
        "consec_obs_diff_abs": "Float64",
        "median_diff_abs": "Float64",
        "ann_jump_couples": "int64",
        "ann_invalid_datum": "int64",
        "ann_unidentified_spike": "int64",
        "ann_no_datum": "int64",
        "total_raw_annotation": "int64",
        "reward_annotation": "int64",
    }
    df = df.astype(dtypes)

    return df


def get_raw_data_check_output_temperature_empty_df() -> pd.DataFrame:
    """Reads the raw data check output file from the parquet file and returns it.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    df: pd.DataFrame = pd.read_parquet(
        "tests/obc_sqc/fixtures_data/raw_data/output/raw_data_check_output_temperature_empty_df.parquet"
    )

    dtypes: dict[str, str] = {
        "utc_datetime": "datetime64[ns]",
        "date": "datetime64[ns]",
        "temperature": "Float64",
        "humidity": "Float64",
        "wind_speed": "Float64",
        "wind_direction": "Float64",
        "pressure": "Float64",
        "illuminance": "Float64",
        "precipitation_accumulated": "Float64",
        "model": "object",
        "ann_obc": "int64",
        "humidity_for_raw_check": "Float64",
        "humidity_consec_filling": "int64",
        "ann_constant": "int64",
        "ann_constant_long": "int64",
        "ann_constant_frozen": "int64",
        "temperature_for_raw_check": "Float64",
        "temperature_consec_filling": "int64",
        "rolling_median": "float64",
        "consec_obs_diff_abs": "Float64",
        "median_diff_abs": "Float64",
        "ann_jump_couples": "int64",
        "ann_invalid_datum": "int64",
        "ann_unidentified_spike": "int64",
        "ann_no_datum": "int64",
        "total_raw_annotation": "int64",
        "reward_annotation": "int64",
    }
    df = df.astype(dtypes)

    df = df.set_index("utc_datetime")
    return df


def get_raw_data_check_output_wind_speed_df() -> pd.DataFrame:
    """Reads the raw data check output file from the parquet file and returns it.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    df: pd.DataFrame = pd.read_parquet(
        "tests/obc_sqc/fixtures_data/raw_data/output/raw_data_check_output_wind_speed_df.parquet"
    ).set_index("utc_datetime")

    dtypes: dict[str, str] = {
        "date": "datetime64[ns]",
        "temperature": "Float64",
        "humidity": "Float64",
        "wind_speed": "Float64",
        "wind_direction": "Float64",
        "pressure": "Float64",
        "illuminance": "Float64",
        "precipitation_accumulated": "Float64",
        "model": "object",
        "ann_obc": "int64",
        "humidity_for_raw_check": "Float64",
        "humidity_consec_filling": "int64",
        "ann_constant": "float64",
        "ann_constant_long": "int64",
        "ann_constant_frozen": "float64",
        "temperature_for_raw_check": "Float64",
        "temperature_consec_filling": "int64",
        "wind_direction_for_raw_check": "Float64",
        "wind_direction_consec_filling": "int64",
        "wind_speed_for_raw_check": "Float64",
        "wind_speed_consec_filling": "int64",
        "rolling_median": "float64",
        "consec_obs_diff_abs": "Float64",
        "median_diff_abs": "Float64",
        "ann_jump_couples": "int64",
        "ann_invalid_datum": "int64",
        "ann_unidentified_spike": "int64",
        "ann_no_datum": "int64",
        "total_raw_annotation": "int64",
        "reward_annotation": "int64",
    }
    df = df.astype(dtypes)

    return df


def get_raw_data_check_output_wind_speed_empty_df() -> pd.DataFrame:
    """Reads the raw data check output file from the parquet file and returns it.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    df: pd.DataFrame = pd.read_parquet(
        "tests/obc_sqc/fixtures_data/raw_data/output/raw_data_check_output_wind_speed_empty_df.parquet"
    )

    dtypes: dict[str, str] = {
        "utc_datetime": "datetime64[ns]",
        "date": "datetime64[ns]",
        "temperature": "Float64",
        "humidity": "Float64",
        "wind_speed": "Float64",
        "wind_direction": "Float64",
        "pressure": "Float64",
        "illuminance": "Float64",
        "precipitation_accumulated": "Float64",
        "model": "object",
        "ann_obc": "int64",
        "humidity_for_raw_check": "Float64",
        "humidity_consec_filling": "int64",
        "ann_constant": "float64",
        "ann_constant_long": "int64",
        "ann_constant_frozen": "float64",
        "temperature_for_raw_check": "Float64",
        "temperature_consec_filling": "int64",
        "wind_direction_for_raw_check": "Float64",
        "wind_direction_consec_filling": "int64",
        "wind_speed_for_raw_check": "Float64",
        "wind_speed_consec_filling": "int64",
        "rolling_median": "float64",
        "consec_obs_diff_abs": "Float64",
        "median_diff_abs": "Float64",
        "ann_jump_couples": "int64",
        "ann_invalid_datum": "int64",
        "ann_unidentified_spike": "int64",
        "ann_no_datum": "int64",
        "total_raw_annotation": "int64",
        "reward_annotation": "int64",
    }
    df = df.astype(dtypes)

    df = df.set_index("utc_datetime")

    return df


def get_raw_data_check_output_wind_direction_df() -> pd.DataFrame:
    """Reads the raw data check output file from the parquet file and returns it.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    df: pd.DataFrame = pd.read_parquet(
        "tests/obc_sqc/fixtures_data/raw_data/output/raw_data_check_output_wind_direction_df.parquet"
    ).set_index("utc_datetime")

    dtypes: dict[str, str] = {
        "date": "datetime64[ns]",
        "temperature": "Float64",
        "humidity": "Float64",
        "wind_speed": "Float64",
        "wind_direction": "Float64",
        "pressure": "Float64",
        "illuminance": "Float64",
        "precipitation_accumulated": "Float64",
        "model": "object",
        "ann_obc": "int64",
        "humidity_for_raw_check": "Float64",
        "humidity_consec_filling": "int64",
        "ann_constant": "float64",
        "ann_constant_long": "int64",
        "ann_constant_frozen": "float64",
        "temperature_for_raw_check": "Float64",
        "temperature_consec_filling": "int64",
        "wind_direction_for_raw_check": "Float64",
        "wind_direction_consec_filling": "int64",
        "rolling_median": "float64",
        "consec_obs_diff_abs": "float64",
        "median_diff_abs": "float64",
        "ann_jump_couples": "float64",
        "ann_invalid_datum": "float64",
        "ann_unidentified_spike": "float64",
        "ann_no_datum": "int64",
        "total_raw_annotation": "int64",
        "reward_annotation": "int64",
    }
    df = df.astype(dtypes)

    return df


def get_raw_data_check_output_wind_direction_nan_df() -> pd.DataFrame:
    """Reads the raw data check output file from the parquet file and returns it.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    df: pd.DataFrame = pd.read_parquet(
        "tests/obc_sqc/fixtures_data/raw_data/output/raw_data_check_output_wind_direction_nan_df.parquet"
    ).set_index("utc_datetime")

    dtypes: dict[str, str] = {
        "date": "object",
        "temperature": "object",
        "humidity": "object",
        "wind_speed": "object",
        "wind_direction": "object",
        "pressure": "object",
        "illuminance": "object",
        "precipitation_accumulated": "object",
        "model": "object",
        "ann_obc": "object",
        "humidity_for_raw_check": "object",
        "humidity_consec_filling": "object",
        "ann_constant": "object",
        "ann_constant_long": "object",
        "ann_constant_frozen": "object",
        "temperature_for_raw_check": "object",
        "temperature_consec_filling": "object",
        "wind_direction_for_raw_check": "object",
        "wind_direction_consec_filling": "object",
        "rolling_median": "float64",
        "consec_obs_diff_abs": "float64",
        "median_diff_abs": "float64",
        "ann_jump_couples": "float64",
        "ann_invalid_datum": "float64",
        "ann_unidentified_spike": "float64",
        "ann_no_datum": "int64",
        "total_raw_annotation": "int64",
        "reward_annotation": "int64",
    }

    df = df.astype(dtypes)

    df = df.fillna(pd.NA)

    return df


def get_raw_data_check_output_wind_direction_empty_df() -> pd.DataFrame:
    """Reads the raw data check output file from the parquet file and returns it.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    df: pd.DataFrame = pd.read_parquet(
        "tests/obc_sqc/fixtures_data/raw_data/output/raw_data_check_output_wind_direction_empty_df.parquet"
    )

    dtypes: dict[str, str] = {
        "utc_datetime": "datetime64[ns]",
        "date": "datetime64[ns]",
        "temperature": "Float64",
        "humidity": "Float64",
        "wind_speed": "Float64",
        "wind_direction": "Float64",
        "pressure": "Float64",
        "illuminance": "Float64",
        "precipitation_accumulated": "Float64",
        "model": "object",
        "ann_obc": "int64",
        "humidity_for_raw_check": "Float64",
        "humidity_consec_filling": "int64",
        "ann_constant": "float64",
        "ann_constant_long": "int64",
        "ann_constant_frozen": "float64",
        "temperature_for_raw_check": "Float64",
        "temperature_consec_filling": "int64",
        "wind_direction_for_raw_check": "Float64",
        "wind_direction_consec_filling": "int64",
        "rolling_median": "float64",
        "consec_obs_diff_abs": "float64",
        "median_diff_abs": "float64",
        "ann_jump_couples": "float64",
        "ann_invalid_datum": "float64",
        "ann_unidentified_spike": "float64",
        "ann_no_datum": "int64",
        "total_raw_annotation": "int64",
        "reward_annotation": "int64",
    }
    df = df.astype(dtypes)

    df = df.set_index("utc_datetime")

    return df


def get_raw_data_check_output_precipitation_accumulated_df() -> pd.DataFrame:
    """Reads the raw data check output file from the parquet file and returns it.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    df: pd.DataFrame = pd.read_parquet(
        "tests/obc_sqc/fixtures_data/raw_data/output/raw_data_check_output_precipitation_accumulated_df.parquet"
    ).set_index("utc_datetime")

    dtypes: dict[str, str] = {
        "temperature": "Float64",
        "humidity": "Float64",
        "wind_speed": "Float64",
        "wind_direction": "Float64",
        "pressure": "Float64",
        "illuminance": "Float64",
        "precipitation_accumulated": "Float64",
        "model": "object",
        "ann_obc": "int64",
        "humidity_for_raw_check": "Float64",
        "humidity_consec_filling": "int64",
        "date": "datetime64[ns]",
        "ann_constant": "int64",
        "ann_constant_long": "int64",
        "ann_constant_frozen": "int64",
        "temperature_for_raw_check": "Float64",
        "temperature_consec_filling": "int64",
        "wind_direction_for_raw_check": "Float64",
        "wind_direction_consec_filling": "int64",
        "wind_speed_for_raw_check": "Float64",
        "wind_speed_consec_filling": "int64",
        "pressure_for_raw_check": "Float64",
        "pressure_consec_filling": "int64",
        "illuminance_for_raw_check": "Float64",
        "illuminance_consec_filling": "int64",
        "precipitation_accumulated_for_raw_check": "Float64",
        "precipitation_accumulated_consec_filling": "int64",
        "precipitation_diff": "Float64",
        "ann_constant_max": "int64",
        "rolling_median": "float64",
        "consec_obs_diff_abs": "float64",
        "median_diff_abs": "float64",
        "ann_jump_couples": "float64",
        "ann_invalid_datum": "float64",
        "ann_unidentified_spike": "float64",
        "ann_no_datum": "int64",
        "total_raw_annotation": "int64",
        "reward_annotation": "int64",
    }
    df = df.astype(dtypes)

    return df


def get_raw_data_check_output_precipitation_accumulated_nan_df() -> pd.DataFrame:
    """Reads the raw data check output file from the parquet file and returns it.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    df: pd.DataFrame = pd.read_parquet(
        "tests/obc_sqc/fixtures_data/raw_data/output/raw_data_check_output_precipitation_accumulated_nan_df.parquet"
    ).set_index("utc_datetime")

    dtypes: dict[str, str] = {
        "temperature": "object",
        "humidity": "object",
        "wind_speed": "object",
        "wind_direction": "object",
        "pressure": "object",
        "illuminance": "object",
        "precipitation_accumulated": "object",
        "model": "object",
        "ann_obc": "object",
        "humidity_for_raw_check": "object",
        "humidity_consec_filling": "object",
        "date": "object",
        "ann_constant": "object",
        "ann_constant_long": "object",
        "ann_constant_frozen": "object",
        "temperature_for_raw_check": "object",
        "temperature_consec_filling": "object",
        "wind_direction_for_raw_check": "object",
        "wind_direction_consec_filling": "object",
        "wind_speed_for_raw_check": "object",
        "wind_speed_consec_filling": "object",
        "pressure_for_raw_check": "object",
        "pressure_consec_filling": "object",
        "illuminance_for_raw_check": "object",
        "illuminance_consec_filling": "object",
        "precipitation_accumulated_for_raw_check": "object",
        "precipitation_accumulated_consec_filling": "object",
        "precipitation_diff": "object",
        "ann_constant_max": "object",
        "rolling_median": "float64",
        "consec_obs_diff_abs": "float64",
        "median_diff_abs": "float64",
        "ann_jump_couples": "float64",
        "ann_invalid_datum": "float64",
        "ann_unidentified_spike": "float64",
        "ann_no_datum": "int64",
        "total_raw_annotation": "int64",
    }

    df = df.astype(dtypes)

    df = df.fillna(pd.NA)

    return df


def get_raw_data_check_output_precipitation_accumulated_empty_df() -> pd.DataFrame:
    """Reads the raw data check output file from the parquet file and returns it.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    df: pd.DataFrame = pd.read_parquet(
        "tests/obc_sqc/fixtures_data/raw_data/output/raw_data_check_output_precipitation_accumulated_empty_df.parquet"
    )

    dtypes: dict[str, str] = {
        "utc_datetime": "datetime64[ns]",
        "temperature": "Float64",
        "humidity": "Float64",
        "wind_speed": "Float64",
        "wind_direction": "Float64",
        "pressure": "Float64",
        "illuminance": "Float64",
        "precipitation_accumulated": "Float64",
        "model": "object",
        "ann_obc": "int64",
        "humidity_for_raw_check": "Float64",
        "humidity_consec_filling": "int64",
        "date": "datetime64[ns]",
        "ann_constant": "int64",
        "ann_constant_long": "int64",
        "ann_constant_frozen": "int64",
        "temperature_for_raw_check": "Float64",
        "temperature_consec_filling": "int64",
        "wind_direction_for_raw_check": "Float64",
        "wind_direction_consec_filling": "int64",
        "wind_speed_for_raw_check": "Float64",
        "wind_speed_consec_filling": "int64",
        "pressure_for_raw_check": "Float64",
        "pressure_consec_filling": "int64",
        "illuminance_for_raw_check": "Float64",
        "illuminance_consec_filling": "int64",
        "precipitation_accumulated_for_raw_check": "Float64",
        "precipitation_accumulated_consec_filling": "int64",
        "precipitation_diff": "Float64",
        "ann_constant_max": "int64",
        "rolling_median": "float64",
        "consec_obs_diff_abs": "float64",
        "median_diff_abs": "float64",
        "ann_jump_couples": "float64",
        "ann_invalid_datum": "float64",
        "ann_unidentified_spike": "float64",
        "ann_no_datum": "int64",
        "total_raw_annotation": "int64",
        "reward_annotation": "int64",
    }
    df = df.astype(dtypes)

    df = df.set_index("utc_datetime")

    return df


@pytest.fixture
def raw_data_check_input_df(request: pytest.FixtureRequest) -> pd.DataFrame:
    """Calls the appropriate function to create the dataframe, according to the parameter name.

    Args:
    ----
        request (pytest.FixtureRequest): A request providing information on the executing test function

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    param_value: str = request.param

    functions_dict: dict[str, Callable] = {
        "temperature": get_raw_data_check_input_temperature_df,
        "wind_speed": get_raw_data_check_input_wind_speed_df,
        "wind_direction": get_raw_data_check_input_wind_direction_df,
        "precipitation_accumulated": get_raw_data_check_input_precipitation_accumulated_df,
    }

    if param_value in functions_dict:
        df: pd.DataFrame = functions_dict[param_value]()
    else:
        raise RuntimeError()

    return df


@pytest.fixture
def raw_data_check_input_nan_df(request: pytest.FixtureRequest) -> pd.DataFrame:
    """Calls the appropriate function to create the dataframe, according to the parameter name.

    Args:
    ----
        request (pytest.FixtureRequest): A request providing information on the executing test function

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    param_value: str = request.param

    functions_dict: dict[str, Callable] = {
        "temperature": get_raw_data_check_input_temperature_nan_df,
        "wind_speed": get_raw_data_check_input_wind_speed_nan_df,
        "wind_direction": get_raw_data_check_input_wind_direction_nan_df,
        "precipitation_accumulated": get_raw_data_check_input_precipitation_accumulated_nan_df,
    }

    if param_value in functions_dict:
        df: pd.DataFrame = functions_dict[param_value]()
    else:
        raise RuntimeError()

    return df


@pytest.fixture
def raw_data_check_input_empty_df(request: pytest.FixtureRequest) -> pd.DataFrame:
    """Calls the appropriate function to create the dataframe, according to the parameter name.

    Args:
    ----
        request (pytest.FixtureRequest): A request providing information on the executing test function

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    param_value: str = request.param

    functions_dict: dict[str, Callable] = {
        "temperature": get_raw_data_check_input_temperature_empty_df,
        "wind_speed": get_raw_data_check_input_wind_speed_empty_df,
        "wind_direction": get_raw_data_check_input_wind_direction_empty_df,
        "precipitation_accumulated": get_raw_data_check_input_precipitation_accumulated_empty_df,
    }

    if param_value in functions_dict:
        df: pd.DataFrame = functions_dict[param_value]()
    else:
        raise RuntimeError()

    return df


@pytest.fixture
def raw_data_check_output_df(request: pytest.FixtureRequest) -> pd.DataFrame:
    """Calls the appropriate function to create the dataframe, according to the parameter name.

    Args:
    ----
        request (pytest.FixtureRequest): A request providing information on the executing test function

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    param_value: str = request.param

    functions_dict: dict[str, Callable] = {
        "temperature": get_raw_data_check_output_temperature_df,
        "wind_speed": get_raw_data_check_output_wind_speed_df,
        "wind_direction": get_raw_data_check_output_wind_direction_df,
        "precipitation_accumulated": get_raw_data_check_output_precipitation_accumulated_df,
    }

    if param_value in functions_dict:
        df: pd.DataFrame = functions_dict[param_value]()
    else:
        raise RuntimeError()

    return df


@pytest.fixture
def raw_data_check_output_nan_df(request: pytest.FixtureRequest) -> pd.DataFrame:
    """Calls the appropriate function to create the dataframe, according to the parameter name.

    Args:
    ----
        request (pytest.FixtureRequest): A request providing information on the executing test function

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    param_value: str = request.param

    functions_dict: dict[str, Callable] = {
        "wind_direction": get_raw_data_check_output_wind_direction_nan_df,
        "precipitation_accumulated": get_raw_data_check_output_precipitation_accumulated_nan_df,
    }

    if param_value in functions_dict:
        df: pd.DataFrame = functions_dict[param_value]()
    else:
        raise RuntimeError()

    return df


@pytest.fixture
def raw_data_check_output_empty_df(request: pytest.FixtureRequest) -> pd.DataFrame:
    """Calls the appropriate function to create the dataframe, according to the parameter name.

    Args:
    ----
        request (pytest.FixtureRequest): A request providing information on the executing test function

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    param_value: str = request.param

    functions_dict: dict[str, Callable] = {
        "temperature": get_raw_data_check_output_temperature_empty_df,
        "wind_speed": get_raw_data_check_output_wind_speed_empty_df,
        "wind_direction": get_raw_data_check_output_wind_direction_empty_df,
        "precipitation_accumulated": get_raw_data_check_output_precipitation_accumulated_empty_df,
    }

    if param_value in functions_dict:
        df: pd.DataFrame = functions_dict[param_value]()
    else:
        raise RuntimeError()

    return df
