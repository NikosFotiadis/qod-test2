import pandas as pd
import numpy as np
import pytest


def get_time_normalized_temperature_df() -> pd.DataFrame:
    """Reads the time normalized dataframe from the parquet file and returns it.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    df: pd.DataFrame = pd.read_parquet(
        "tests/obc_sqc/fixtures_data/time_normalized/time_normalized_temperature_df.parquet"
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
        "ann_obc": "int64",
        "humidity_for_raw_check": "Float64",
        "humidity_consec_filling": "int64",
        "date": "datetime64[ns]",
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
        "annotation": "object",
    }
    df = df.astype(dtypes)

    return df


def get_time_normalized_temperature_nan_df() -> pd.DataFrame:
    """Reads the time normalized dataframe from the parquet file and returns it.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    # Specify the datetime range for the index
    start_date: str = "2023-10-29 18:00:00"
    end_date: str = "2023-10-30 23:59:44"
    date_range: pd.DatetimeIndex = pd.date_range(start=start_date, end=end_date, freq="16S")

    dtypes: dict[str, str] = {
        "temperature": "float64",
        "humidity": "float64",
        "wind_speed": "float64",
        "wind_direction": "float64",
        "pressure": "float64",
        "illuminance": "float64",
        "precipitation_accumulated": "float64",
        "model": "float64",
        "ann_obc": "float64",
        "humidity_for_raw_check": "float64",
        "humidity_consec_filling": "float64",
        "date": "float64",
        "ann_constant": "float64",
        "ann_constant_long": "float64",
        "ann_constant_frozen": "float64",
        "temperature_for_raw_check": "float64",
        "temperature_consec_filling": "float64",
        "rolling_median": "float64",
        "consec_obs_diff_abs": "float64",
        "median_diff_abs": "float64",
        "ann_jump_couples": "float64",
        "ann_invalid_datum": "float64",
        "ann_unidentified_spike": "float64",
        "ann_no_datum": "float64",
        "total_raw_annotation": "float64",
        "reward_annotation": "float64",
        "annotation": "float64",
    }

    # Create an empty DataFrame with specified columns
    df: pd.DataFrame = pd.DataFrame(index=date_range, columns=list(dtypes.keys()))

    df = df.astype(dtypes)

    df.loc[:] = pd.NA  # type: ignore [call-overload]

    return df


def get_time_normalized_temperature_empty_df() -> pd.DataFrame:
    """Creates the empty minute averaged dataframe and returns it.

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
        "ann_obc": "int64",
        "humidity_for_raw_check": "Float64",
        "humidity_consec_filling": "int64",
        "date": "datetime64[ns]",
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
        "annotation": "object",
    }

    # Create an empty DataFrame with specified columns
    df: pd.DataFrame = pd.DataFrame(columns=list(dtypes.keys()), index=pd.DatetimeIndex([]))

    df = df.astype(dtypes)

    return df


def get_time_normalized_wind_speed_df() -> pd.DataFrame:
    """Reads the time normalized dataframe from the parquet file and returns it.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    df: pd.DataFrame = pd.read_parquet(
        "tests/obc_sqc/fixtures_data/time_normalized/time_normalized_wind_speed_df.parquet"
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
        "ann_obc": "int64",
        "humidity_for_raw_check": "Float64",
        "humidity_consec_filling": "int64",
        "date": "datetime64[ns]",
        "ann_constant": "int64",
        "ann_constant_long": "int64",
        "ann_constant_frozen": "int64",
        "temperature_for_raw_check": "Float64",
        "temperature_consec_filling": "int64",
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
        "annotation": "object",
    }
    df = df.astype(dtypes)

    return df


def get_time_normalized_wind_speed_nan_df() -> pd.DataFrame:
    """Reads the time normalized dataframe from the parquet file and returns it.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    # Specify the datetime range for the index
    start_date: str = "2023-10-29 18:00:00"
    end_date: str = "2023-10-30 23:59:44"
    date_range: pd.DatetimeIndex = pd.date_range(start=start_date, end=end_date, freq="16S")

    dtypes: dict[str, str] = {
        "temperature": "float64",
        "humidity": "float64",
        "wind_speed": "float64",
        "wind_direction": "float64",
        "pressure": "float64",
        "illuminance": "float64",
        "precipitation_accumulated": "float64",
        "model": "float64",
        "ann_obc": "float64",
        "humidity_for_raw_check": "float64",
        "humidity_consec_filling": "float64",
        "date": "float64",
        "ann_constant": "float64",
        "ann_constant_long": "float64",
        "ann_constant_frozen": "float64",
        "temperature_for_raw_check": "float64",
        "temperature_consec_filling": "float64",
        "wind_speed_for_raw_check": "float64",
        "wind_speed_consec_filling": "float64",
        "rolling_median": "float64",
        "consec_obs_diff_abs": "float64",
        "median_diff_abs": "float64",
        "ann_jump_couples": "float64",
        "ann_invalid_datum": "float64",
        "ann_unidentified_spike": "float64",
        "ann_no_datum": "float64",
        "total_raw_annotation": "float64",
        "reward_annotation": "float64",
        "annotation": "float64",
    }

    # Create an empty DataFrame with specified columns
    df: pd.DataFrame = pd.DataFrame(index=date_range, columns=list(dtypes.keys()), data=np.nan)

    df = df.astype(dtypes)

    df.loc[:] = pd.NA  # type: ignore [call-overload]

    return df


def get_time_normalized_wind_speed_empty_df() -> pd.DataFrame:
    """Creates the empty minute averaged dataframe and returns it.

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
        "ann_obc": "int64",
        "humidity_for_raw_check": "Float64",
        "humidity_consec_filling": "int64",
        "date": "datetime64[ns]",
        "ann_constant": "int64",
        "ann_constant_long": "int64",
        "ann_constant_frozen": "int64",
        "temperature_for_raw_check": "Float64",
        "temperature_consec_filling": "int64",
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
        "annotation": "object",
    }

    # Create an empty DataFrame with specified columns
    df: pd.DataFrame = pd.DataFrame(columns=list(dtypes.keys()), index=pd.DatetimeIndex([]))

    df = df.astype(dtypes)

    return df


def get_time_normalized_wind_direction_df() -> pd.DataFrame:
    """Reads the time normalized dataframe from the parquet file and returns it.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    df: pd.DataFrame = pd.read_parquet(
        "tests/obc_sqc/fixtures_data/time_normalized/time_normalized_wind_direction_df.parquet"
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
        "ann_obc": "int64",
        "humidity_for_raw_check": "Float64",
        "humidity_consec_filling": "int64",
        "date": "datetime64[ns]",
        "ann_constant": "int64",
        "ann_constant_long": "int64",
        "ann_constant_frozen": "int64",
        "temperature_for_raw_check": "Float64",
        "temperature_consec_filling": "int64",
        "wind_speed_for_raw_check": "Float64",
        "wind_speed_consec_filling": "int64",
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
        "annotation": "object",
    }
    df = df.astype(dtypes)

    return df


def get_time_normalized_wind_direction_nan_df() -> pd.DataFrame:
    """Reads the time normalized dataframe from the parquet file and returns it.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    # Specify the datetime range for the index
    start_date: str = "2023-10-29 18:00:00"
    end_date: str = "2023-10-30 23:59:44"
    date_range: pd.DatetimeIndex = pd.date_range(start=start_date, end=end_date, freq="16S")

    dtypes: dict[str, str] = {
        "temperature": "float64",
        "humidity": "float64",
        "wind_speed": "float64",
        "wind_direction": "float64",
        "pressure": "float64",
        "illuminance": "float64",
        "precipitation_accumulated": "float64",
        "model": "float64",
        "ann_obc": "float64",
        "humidity_for_raw_check": "float64",
        "humidity_consec_filling": "float64",
        "date": "float64",
        "ann_constant": "float64",
        "ann_constant_long": "float64",
        "ann_constant_frozen": "float64",
        "temperature_for_raw_check": "float64",
        "temperature_consec_filling": "float64",
        "wind_speed_for_raw_check": "float64",
        "wind_speed_consec_filling": "float64",
        "wind_direction_for_raw_check": "float64",
        "wind_direction_consec_filling": "float64",
        "rolling_median": "float64",
        "consec_obs_diff_abs": "float64",
        "median_diff_abs": "float64",
        "ann_jump_couples": "float64",
        "ann_invalid_datum": "float64",
        "ann_unidentified_spike": "float64",
        "ann_no_datum": "float64",
        "total_raw_annotation": "float64",
        "reward_annotation": "float64",
        "annotation": "float64",
    }

    # Create an empty DataFrame with specified columns
    df: pd.DataFrame = pd.DataFrame(index=date_range, columns=list(dtypes.keys()), data=np.nan)

    df = df.astype(dtypes)

    df.loc[:] = pd.NA  # type: ignore [call-overload]

    return df


def get_time_normalized_wind_direction_empty_df() -> pd.DataFrame:
    """Creates the empty minute averaged dataframe and returns it.

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
        "ann_obc": "int64",
        "humidity_for_raw_check": "Float64",
        "humidity_consec_filling": "int64",
        "date": "datetime64[ns]",
        "ann_constant": "int64",
        "ann_constant_long": "int64",
        "ann_constant_frozen": "int64",
        "temperature_for_raw_check": "Float64",
        "temperature_consec_filling": "int64",
        "wind_speed_for_raw_check": "Float64",
        "wind_speed_consec_filling": "int64",
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
        "annotation": "object",
    }

    # Create an empty DataFrame with specified columns
    df: pd.DataFrame = pd.DataFrame(columns=list(dtypes.keys()), index=pd.DatetimeIndex([]))

    df = df.astype(dtypes)

    return df


def get_time_normalized_precipitation_accumulated_df() -> pd.DataFrame:
    """Reads the time normalized dataframe from the parquet file and returns it.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    df: pd.DataFrame = pd.read_parquet(
        "tests/obc_sqc/fixtures_data/time_normalized/time_normalized_precipitation_accumulated_df.parquet"
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
        "ann_obc": "int64",
        "humidity_for_raw_check": "Float64",
        "humidity_consec_filling": "int64",
        "date": "datetime64[ns]",
        "ann_constant": "int64",
        "ann_constant_long": "int64",
        "ann_constant_frozen": "int64",
        "temperature_for_raw_check": "Float64",
        "temperature_consec_filling": "int64",
        "wind_speed_for_raw_check": "Float64",
        "wind_speed_consec_filling": "int64",
        "wind_direction_for_raw_check": "Float64",
        "wind_direction_consec_filling": "int64",
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
        "annotation": "object",
    }
    df = df.astype(dtypes)

    return df


def get_time_normalized_precipitation_accumulated_nan_df() -> pd.DataFrame:
    """Reads the time normalized dataframe from the parquet file and returns it.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    # Specify the datetime range for the index
    start_date: str = "2023-10-29 18:00:00"
    end_date: str = "2023-10-30 23:59:44"
    date_range: pd.DatetimeIndex = pd.date_range(start=start_date, end=end_date, freq="16S")

    dtypes: dict[str, str] = {
        "temperature": "float64",
        "humidity": "float64",
        "wind_speed": "float64",
        "wind_direction": "float64",
        "pressure": "float64",
        "illuminance": "float64",
        "precipitation_accumulated": "float64",
        "model": "float64",
        "ann_obc": "float64",
        "humidity_for_raw_check": "float64",
        "humidity_consec_filling": "float64",
        "date": "float64",
        "ann_constant": "float64",
        "ann_constant_long": "float64",
        "ann_constant_frozen": "float64",
        "temperature_for_raw_check": "float64",
        "temperature_consec_filling": "float64",
        "wind_speed_for_raw_check": "float64",
        "wind_speed_consec_filling": "float64",
        "wind_direction_for_raw_check": "float64",
        "wind_direction_consec_filling": "float64",
        "pressure_for_raw_check": "float64",
        "pressure_consec_filling": "float64",
        "illuminance_for_raw_check": "float64",
        "illuminance_consec_filling": "float64",
        "precipitation_accumulated_for_raw_check": "float64",
        "precipitation_accumulated_consec_filling": "float64",
        "precipitation_diff": "float64",
        "ann_constant_max": "float64",
        "rolling_median": "float64",
        "consec_obs_diff_abs": "float64",
        "median_diff_abs": "float64",
        "ann_jump_couples": "float64",
        "ann_invalid_datum": "float64",
        "ann_unidentified_spike": "float64",
        "ann_no_datum": "float64",
        "total_raw_annotation": "float64",
        "reward_annotation": "float64",
        "annotation": "float64",
    }

    # Create an empty DataFrame with specified columns
    df: pd.DataFrame = pd.DataFrame(index=date_range, columns=list(dtypes.keys()), data=np.nan)

    df = df.astype(dtypes)

    df.loc[:] = pd.NA  # type: ignore [call-overload]

    return df


def get_time_normalized_precipitation_accumulated_empty_df() -> pd.DataFrame:
    """Creates the empty minute averaged dataframe and returns it.

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
        "ann_obc": "int64",
        "humidity_for_raw_check": "Float64",
        "humidity_consec_filling": "int64",
        "date": "datetime64[ns]",
        "ann_constant": "int64",
        "ann_constant_long": "int64",
        "ann_constant_frozen": "int64",
        "temperature_for_raw_check": "Float64",
        "temperature_consec_filling": "int64",
        "wind_speed_for_raw_check": "Float64",
        "wind_speed_consec_filling": "int64",
        "wind_direction_for_raw_check": "Float64",
        "wind_direction_consec_filling": "int64",
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
        "annotation": "object",
    }

    # Create an empty DataFrame with specified columns
    df: pd.DataFrame = pd.DataFrame(columns=list(dtypes.keys()), index=pd.DatetimeIndex([]))

    df = df.astype(dtypes)

    return df


def get_minute_averaging_temperature_df() -> pd.DataFrame:
    """Reads the minute averaged dataframe from the parquet file and returns it.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    df: pd.DataFrame = pd.read_parquet(
        "tests/obc_sqc/fixtures_data/minute_averaging/minute_averaging_temperature_df.parquet"
    ).set_index("utc_datetime")
    df["annotation"] = df["annotation"].fillna("")

    dtypes: dict[str, str] = {
        "temperature_avg": "object",
        "temperature_avg_corrected": "float64",
        "faulty_rewards": "int64",
        "num_faulty": "int64",
        "num_total_slots": "float64",
        "valid_percentage": "float64",
        "valid_percentage_rewards": "float64",
        "rolling_median": "float64",
        "diff_abs": "object",
        "median_diff_abs": "object",
        "ann_jump_couples": "int64",
        "ann_invalid_datum": "int64",
        "ann_unidentified_change": "int64",
        "ann_all_from_raw": "int64",
        "ann_all_from_raw_rewards": "int64",
        "ann_total": "int64",
        "ann_total_rewards": "int64",
        "annotation": "object",
    }
    df = df.astype(dtypes)

    return df


def get_minute_averaging_wind_speed_df() -> pd.DataFrame:
    """Reads the minute averaged dataframe from the parquet file and returns it.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    df: pd.DataFrame = pd.read_parquet(
        "tests/obc_sqc/fixtures_data/minute_averaging/minute_averaging_wind_speed_df.parquet"
    ).set_index("utc_datetime")
    df["annotation"] = df["annotation"].fillna("")

    dtypes: dict[str, str] = {
        "wind_direction_avg": "float64",
        "wind_speed_avg": "object",
        "u": "object",
        "v": "object",
        "num_faulty": "int64",
        "num_total_slots": "float64",
        "valid_percentage": "float64",
        "valid_percentage_rewards": "float64",
        "faulty_rewards": "int64",
        "wind_spd_avg_corrected": "float64",
        "wind_dir_avg_corrected": "float64",
        "rolling_median": "float64",
        "diff_abs": "object",
        "median_diff_abs": "object",
        "ann_jump_couples": "int64",
        "ann_invalid_datum": "int64",
        "ann_unidentified_change": "int64",
        "ann_all_from_raw": "int64",
        "ann_all_from_raw_rewards": "int64",
        "ann_total": "int64",
        "ann_total_rewards": "int64",
        "annotation": "object",
    }
    df = df.astype(dtypes)

    return df


def get_minute_averaging_wind_direction_df() -> pd.DataFrame:
    """Reads the minute averaged dataframe from the parquet file and returns it.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    df: pd.DataFrame = pd.read_parquet(
        "tests/obc_sqc/fixtures_data/minute_averaging/minute_averaging_wind_direction_df.parquet"
    ).set_index("utc_datetime")
    df["annotation"] = df["annotation"].fillna("")

    dtypes: dict[str, str] = {
        "wind_direction_avg": "float64",
        "wind_speed_avg": "object",
        "u": "object",
        "v": "object",
        "num_faulty": "int64",
        "num_total_slots": "float64",
        "valid_percentage": "float64",
        "valid_percentage_rewards": "float64",
        "faulty_rewards": "int64",
        "wind_spd_avg_corrected": "float64",
        "wind_dir_avg_corrected": "float64",
        "rolling_median_minute": "float64",
        "diff_abs": "float64",
        "median_diff_abs": "float64",
        "ann_jump_couples": "float64",
        "ann_invalid_datum": "float64",
        "ann_unidentified_change": "float64",
        "ann_all_from_raw": "int64",
        "ann_all_from_raw_rewards": "int64",
        "ann_total": "int64",
        "ann_total_rewards": "int64",
        "annotation": "object",
    }
    df = df.astype(dtypes)

    return df


def get_minute_averaging_precipitation_accumulated_df() -> pd.DataFrame:
    """Reads the minute averaged dataframe from the parquet file and returns it.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    df: pd.DataFrame = pd.read_parquet(
        "tests/obc_sqc/fixtures_data/minute_averaging/minute_averaging_precipitation_accumulated_df.parquet"
    ).set_index("utc_datetime")
    df["annotation"] = df["annotation"].fillna("")

    dtypes: dict[str, str] = {
        "precipitation_accumulated_avg": "float64",
        "precipitation_accumulated_avg_corrected": "float64",
        "faulty_rewards": "int64",
        "num_faulty": "int64",
        "num_total_slots": "float64",
        "valid_percentage": "float64",
        "valid_percentage_rewards": "float64",
        "rolling_median_minute": "float64",
        "diff_abs": "float64",
        "median_diff_abs": "float64",
        "ann_jump_couples": "float64",
        "ann_invalid_datum": "float64",
        "ann_unidentified_change": "float64",
        "ann_all_from_raw": "int64",
        "ann_all_from_raw_rewards": "int64",
        "ann_total": "int64",
        "ann_total_rewards": "int64",
        "annotation": "object",
    }
    df = df.astype(dtypes)

    return df


@pytest.fixture
def time_normalized_df(request: pytest.FixtureRequest) -> pd.DataFrame:
    """Calls the appropriate function to create the dataframe, according to the parameter name.

    Args:
    ----
        request (pytest.FixtureRequest): A request providing information on the executing test function

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    param_value: str = request.param

    functions_dict = {
        "temperature": get_time_normalized_temperature_df,
        "wind_speed": get_time_normalized_wind_speed_df,
        "wind_direction": get_time_normalized_wind_direction_df,
        "precipitation_accumulated": get_time_normalized_precipitation_accumulated_df,
    }

    if param_value in functions_dict:
        df: pd.DataFrame = functions_dict[param_value]()
    else:
        raise RuntimeError()

    return df


@pytest.fixture
def time_normalized_nan_df(request: pytest.FixtureRequest) -> pd.DataFrame:
    """Calls the appropriate function to create the dataframe, according to the parameter name.

    Args:
    ----
        request (pytest.FixtureRequest): A request providing information on the executing test function

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    param_value: str = request.param

    functions_dict = {
        "temperature": get_time_normalized_temperature_nan_df,
        "wind_speed": get_time_normalized_wind_speed_nan_df,
        "wind_direction": get_time_normalized_wind_direction_nan_df,
        "precipitation_accumulated": get_time_normalized_precipitation_accumulated_nan_df,
    }

    if param_value in functions_dict:
        df: pd.DataFrame = functions_dict[param_value]()
    else:
        raise RuntimeError()

    return df


@pytest.fixture
def time_normalized_empty_df(request: pytest.FixtureRequest) -> pd.DataFrame:
    """Calls the appropriate function to create the dataframe, according to the parameter name.

    Args:
    ----
        request (pytest.FixtureRequest): A request providing information on the executing test function

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    param_value: str = request.param

    functions_dict = {
        "temperature": get_time_normalized_temperature_empty_df,
        "wind_speed": get_time_normalized_wind_speed_empty_df,
        "wind_direction": get_time_normalized_wind_direction_empty_df,
        "precipitation_accumulated": get_time_normalized_precipitation_accumulated_empty_df,
    }

    if param_value in functions_dict:
        df: pd.DataFrame = functions_dict[param_value]()
    else:
        raise RuntimeError()

    return df


@pytest.fixture
def minute_averaging_df(request: pytest.FixtureRequest) -> pd.DataFrame:
    """Calls the appropriate function to create the dataframe, according to the parameter name.

    Args:
    ----
        request (pytest.FixtureRequest): A request providing information on the executing test function

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    param_value: str = request.param

    functions_dict = {
        "temperature": get_minute_averaging_temperature_df,
        "wind_speed": get_minute_averaging_wind_speed_df,
        "wind_direction": get_minute_averaging_wind_direction_df,
        "precipitation_accumulated": get_minute_averaging_precipitation_accumulated_df,
    }

    if param_value in functions_dict:
        df: pd.DataFrame = functions_dict[param_value]()
    else:
        raise RuntimeError()

    return df
