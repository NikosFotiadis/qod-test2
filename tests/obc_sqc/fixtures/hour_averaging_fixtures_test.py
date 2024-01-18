import pandas as pd
import numpy as np
import pytest


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


def get_minute_averaging_temperature_nan_df() -> pd.DataFrame:
    """Reads the nan-filled minute averaged dataframe from the parquet file and returns it.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    # Specify the datetime range for the index
    start_date: str = "2023-10-30 00:00:00"
    end_date: str = "2023-10-30 23:59:00"
    date_range: pd.DatetimeIndex = pd.date_range(start=start_date, end=end_date, freq="T")

    # Specify the column names
    columns: list[str] = [
        "temperature_avg",
        "temperature_avg_corrected",
        "faulty_rewards",
        "num_faulty",
        "num_total_slots",
        "valid_percentage",
        "valid_percentage_rewards",
        "rolling_median",
        "diff_abs",
        "median_diff_abs",
        "ann_jump_couples",
        "ann_invalid_datum",
        "ann_unidentified_change",
        "ann_all_from_raw",
        "ann_all_from_raw_rewards",
        "ann_total",
        "ann_total_rewards",
        "annotation",
    ]

    # Create an empty DataFrame with specified columns
    df: pd.DataFrame = pd.DataFrame(index=date_range, columns=columns, data=np.nan)

    dtypes: dict[str, str] = {
        "temperature_avg": "float64",
        "temperature_avg_corrected": "float64",
        "faulty_rewards": "float64",
        "num_faulty": "float64",
        "num_total_slots": "float64",
        "valid_percentage": "float64",
        "valid_percentage_rewards": "float64",
        "rolling_median": "float64",
        "diff_abs": "float64",
        "median_diff_abs": "float64",
        "ann_jump_couples": "float64",
        "ann_invalid_datum": "float64",
        "ann_unidentified_change": "float64",
        "ann_all_from_raw": "float64",
        "ann_all_from_raw_rewards": "float64",
        "ann_total": "float64",
        "ann_total_rewards": "float64",
        "annotation": "float64",
    }
    df = df.astype(dtypes)

    return df


def get_minute_averaging_temperature_empty_df() -> pd.DataFrame:
    """Creates the empty minute averaged dataframe and returns it.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    # Specify the column names
    columns: list[str] = [
        "temperature_avg",
        "temperature_avg_corrected",
        "faulty_rewards",
        "num_faulty",
        "num_total_slots",
        "valid_percentage",
        "valid_percentage_rewards",
        "rolling_median",
        "diff_abs",
        "median_diff_abs",
        "ann_jump_couples",
        "ann_invalid_datum",
        "ann_unidentified_change",
        "ann_all_from_raw",
        "ann_all_from_raw_rewards",
        "ann_total",
        "ann_total_rewards",
        "annotation",
    ]

    # Create an empty DataFrame with specified columns
    df: pd.DataFrame = pd.DataFrame(columns=columns, index=pd.DatetimeIndex([]))

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


def get_hour_averaging_temperature_df(threshold: str) -> pd.DataFrame:
    """Reads the hourly averaged dataframe from the parquet file and returns it.

    Args:
    ----
        threshold (str): the threshold suffix of the file to be read

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    df: pd.DataFrame = pd.read_parquet(
        f"tests/obc_sqc/fixtures_data/hour_averaging/hour_averaging_temperature_df_threshold_{threshold}.parquet"
    ).set_index("utc_datetime")
    df["annotation"] = df["annotation"].fillna("")

    dtypes: dict[str, str] = {
        "temperature_avg": "float64",
        "num_time_slots": "int64",
        "num_hourly_faulty": "int64",
        "num_hourly_faulty_rewards": "int64",
        "temperature_avg_corrected": "float64",
        "valid_percentage": "float64",
        "ann_total_hour": "int64",
        "valid_percentage_rewards": "float64",
        "ann_total_hour_rewards": "int64",
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


def get_minute_averaging_wind_speed_nan_df() -> pd.DataFrame:
    """Reads the nan-filled minute averaged dataframe from the parquet file and returns it.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    # Specify the datetime range for the index
    start_date: str = "2023-10-30 00:00:00"
    end_date: str = "2023-10-30 23:59:00"
    date_range: pd.DatetimeIndex = pd.date_range(start=start_date, end=end_date, freq="T")

    # Specify the column names
    columns: list[str] = [
        "wind_direction_avg",
        "wind_speed_avg",
        "u",
        "v",
        "num_faulty",
        "num_total_slots",
        "valid_percentage",
        "valid_percentage_rewards",
        "faulty_rewards",
        "wind_spd_avg_corrected",
        "wind_dir_avg_corrected",
        "rolling_median",
        "diff_abs",
        "median_diff_abs",
        "ann_jump_couples",
        "ann_invalid_datum",
        "ann_unidentified_change",
        "ann_all_from_raw",
        "ann_all_from_raw_rewards",
        "ann_total",
        "ann_total_rewards",
        "annotation",
    ]

    # Create an empty DataFrame with specified columns
    df: pd.DataFrame = pd.DataFrame(index=date_range, columns=columns, data=np.nan)

    dtypes: dict[str, str] = {
        "wind_direction_avg": "float64",
        "wind_speed_avg": "float64",
        "u": "float64",
        "v": "float64",
        "num_faulty": "float64",
        "num_total_slots": "float64",
        "valid_percentage": "float64",
        "valid_percentage_rewards": "float64",
        "faulty_rewards": "float64",
        "wind_spd_avg_corrected": "float64",
        "wind_dir_avg_corrected": "float64",
        "rolling_median": "float64",
        "diff_abs": "float64",
        "median_diff_abs": "float64",
        "ann_jump_couples": "float64",
        "ann_invalid_datum": "float64",
        "ann_unidentified_change": "float64",
        "ann_all_from_raw": "float64",
        "ann_all_from_raw_rewards": "float64",
        "ann_total": "float64",
        "ann_total_rewards": "float64",
        "annotation": "float64",
    }
    df = df.astype(dtypes)

    return df


def get_minute_averaging_wind_speed_empty_df() -> pd.DataFrame:
    """Creates the empty minute averaged dataframe and returns it.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    # Specify the column names
    columns: list[str] = [
        "wind_direction_avg",
        "wind_speed_avg",
        "u",
        "v",
        "num_faulty",
        "num_total_slots",
        "valid_percentage",
        "valid_percentage_rewards",
        "faulty_rewards",
        "wind_spd_avg_corrected",
        "wind_dir_avg_corrected",
        "rolling_median",
        "diff_abs",
        "median_diff_abs",
        "ann_jump_couples",
        "ann_invalid_datum",
        "ann_unidentified_change",
        "ann_all_from_raw",
        "ann_all_from_raw_rewards",
        "ann_total",
        "ann_total_rewards",
        "annotation",
    ]

    # Create an empty DataFrame with specified columns
    df: pd.DataFrame = pd.DataFrame(columns=columns, index=pd.DatetimeIndex([]))

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


def get_hour_averaging_wind_speed_df(threshold: str) -> pd.DataFrame:
    """Reads the hourly averaged dataframe from the parquet file and returns it.

    Args:
    ----
        threshold (str): the threshold suffix of the file to be read

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    df: pd.DataFrame = pd.read_parquet(
        f"tests/obc_sqc/fixtures_data/hour_averaging/hour_averaging_wind_speed_df_threshold_{threshold}.parquet"
    ).set_index("utc_datetime")
    df["annotation"] = df["annotation"].fillna("")

    dtypes: dict[str, str] = {
        "wind_direction_avg": "Float64",
        "wind_speed_avg": "Float64",
        "u": "float64",
        "v": "float64",
        "num_time_slots": "int64",
        "num_hourly_faulty": "int64",
        "num_hourly_faulty_rewards": "int64",
        "wind_spd_avg_corrected": "float64",
        "wind_dir_avg_corrected": "float64",
        "valid_percentage": "float64",
        "ann_total_hour": "int64",
        "valid_percentage_rewards": "float64",
        "ann_total_hour_rewards": "int64",
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


def get_minute_averaging_wind_direction_nan_df() -> pd.DataFrame:
    """Reads the nan-filled minute averaged dataframe from the parquet file and returns it.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    # Specify the datetime range for the index
    start_date: str = "2023-10-30 00:00:00"
    end_date: str = "2023-10-30 23:59:00"
    date_range: pd.DatetimeIndex = pd.date_range(start=start_date, end=end_date, freq="T")

    # Specify the column names
    columns: list[str] = [
        "wind_direction_avg",
        "wind_speed_avg",
        "u",
        "v",
        "num_faulty",
        "num_total_slots",
        "valid_percentage",
        "valid_percentage_rewards",
        "faulty_rewards",
        "wind_spd_avg_corrected",
        "wind_dir_avg_corrected",
        "rolling_median_minute",
        "diff_abs",
        "median_diff_abs",
        "ann_jump_couples",
        "ann_invalid_datum",
        "ann_unidentified_change",
        "ann_all_from_raw",
        "ann_all_from_raw_rewards",
        "ann_total",
        "ann_total_rewards",
        "annotation",
    ]

    # Create an empty DataFrame with specified columns
    df: pd.DataFrame = pd.DataFrame(index=date_range, columns=columns, data=np.nan)

    dtypes: dict[str, str] = {
        "wind_direction_avg": "float64",
        "wind_speed_avg": "float64",
        "u": "float64",
        "v": "float64",
        "num_faulty": "float64",
        "num_total_slots": "float64",
        "valid_percentage": "float64",
        "valid_percentage_rewards": "float64",
        "faulty_rewards": "float64",
        "wind_spd_avg_corrected": "float64",
        "wind_dir_avg_corrected": "float64",
        "rolling_median_minute": "float64",
        "diff_abs": "float64",
        "median_diff_abs": "float64",
        "ann_jump_couples": "float64",
        "ann_invalid_datum": "float64",
        "ann_unidentified_change": "float64",
        "ann_all_from_raw": "float64",
        "ann_all_from_raw_rewards": "float64",
        "ann_total": "float64",
        "ann_total_rewards": "float64",
        "annotation": "float64",
    }
    df = df.astype(dtypes)

    return df


def get_minute_averaging_wind_direction_empty_df() -> pd.DataFrame:
    """Creates the empty minute averaged dataframe and returns it.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    # Specify the column names
    columns: list[str] = [
        "wind_direction_avg",
        "wind_speed_avg",
        "u",
        "v",
        "num_faulty",
        "num_total_slots",
        "valid_percentage",
        "valid_percentage_rewards",
        "faulty_rewards",
        "wind_spd_avg_corrected",
        "wind_dir_avg_corrected",
        "rolling_median_minute",
        "diff_abs",
        "median_diff_abs",
        "ann_jump_couples",
        "ann_invalid_datum",
        "ann_unidentified_change",
        "ann_all_from_raw",
        "ann_all_from_raw_rewards",
        "ann_total",
        "ann_total_rewards",
        "annotation",
    ]

    # Create an empty DataFrame with specified columns
    df: pd.DataFrame = pd.DataFrame(columns=columns, index=pd.DatetimeIndex([]))

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


def get_hour_averaging_wind_direction_df(threshold: str) -> pd.DataFrame:
    """Reads the hourly averaged dataframe from the parquet file and returns it.

    Args:
    ----
        threshold (str): the threshold suffix of the file to be read

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    df: pd.DataFrame = pd.read_parquet(
        f"tests/obc_sqc/fixtures_data/hour_averaging/hour_averaging_wind_direction_df_threshold_{threshold}.parquet"
    ).set_index("utc_datetime")
    df["annotation"] = df["annotation"].fillna("")

    dtypes: dict[str, str] = {
        "wind_direction_avg": "Float64",
        "wind_speed_avg": "Float64",
        "u": "float64",
        "v": "float64",
        "num_time_slots": "int64",
        "num_hourly_faulty": "int64",
        "num_hourly_faulty_rewards": "int64",
        "wind_spd_avg_corrected": "float64",
        "wind_dir_avg_corrected": "float64",
        "valid_percentage": "float64",
        "ann_total_hour": "int64",
        "valid_percentage_rewards": "float64",
        "ann_total_hour_rewards": "int64",
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


def get_minute_averaging_precipitation_accumulated_nan_df() -> pd.DataFrame:
    """Reads the nan-filled minute averaged dataframe from the parquet file and returns it.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    # Specify the datetime range for the index
    start_date: str = "2023-10-30 00:00:00"
    end_date: str = "2023-10-30 23:59:00"
    date_range: pd.DatetimeIndex = pd.date_range(start=start_date, end=end_date, freq="T")

    # Specify the column names
    columns: list[str] = [
        "precipitation_accumulated_avg",
        "precipitation_accumulated_avg_corrected",
        "faulty_rewards",
        "num_faulty",
        "num_total_slots",
        "valid_percentage",
        "valid_percentage_rewards",
        "rolling_median_minute",
        "diff_abs",
        "median_diff_abs",
        "ann_jump_couples",
        "ann_invalid_datum",
        "ann_unidentified_change",
        "ann_all_from_raw",
        "ann_all_from_raw_rewards",
        "ann_total",
        "ann_total_rewards",
        "annotation",
    ]

    # Create an empty DataFrame with specified columns
    df: pd.DataFrame = pd.DataFrame(index=date_range, columns=columns, data=np.nan)

    dtypes: dict[str, str] = {
        "precipitation_accumulated_avg": "float64",
        "precipitation_accumulated_avg_corrected": "float64",
        "faulty_rewards": "float64",
        "num_faulty": "float64",
        "num_total_slots": "float64",
        "valid_percentage": "float64",
        "valid_percentage_rewards": "float64",
        "rolling_median_minute": "float64",
        "diff_abs": "float64",
        "median_diff_abs": "float64",
        "ann_jump_couples": "float64",
        "ann_invalid_datum": "float64",
        "ann_unidentified_change": "float64",
        "ann_all_from_raw": "float64",
        "ann_all_from_raw_rewards": "float64",
        "ann_total": "float64",
        "ann_total_rewards": "float64",
        "annotation": "float64",
    }
    df = df.astype(dtypes)

    return df


def get_minute_averaging_precipitation_accumulated_empty_df() -> pd.DataFrame:
    """Creates the empty minute averaged dataframe and returns it.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    # Specify the column names
    columns: list[str] = [
        "precipitation_accumulated_avg",
        "precipitation_accumulated_avg_corrected",
        "faulty_rewards",
        "num_faulty",
        "num_total_slots",
        "valid_percentage",
        "valid_percentage_rewards",
        "rolling_median_minute",
        "diff_abs",
        "median_diff_abs",
        "ann_jump_couples",
        "ann_invalid_datum",
        "ann_unidentified_change",
        "ann_all_from_raw",
        "ann_all_from_raw_rewards",
        "ann_total",
        "ann_total_rewards",
        "annotation",
    ]

    # Create an empty DataFrame with specified columns
    df: pd.DataFrame = pd.DataFrame(columns=columns, index=pd.DatetimeIndex([]))

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


def get_hour_averaging_precipitation_accumulated_df(threshold: str) -> pd.DataFrame:
    """Reads the hourly averaged dataframe from the parquet file and returns it.

    Args:
    ----
        threshold (str): the threshold suffix of the file to be read

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    df: pd.DataFrame = pd.read_parquet(
        f"tests/obc_sqc/fixtures_data/hour_averaging/hour_averaging_precipitation_accumulated_df_threshold_{threshold}.parquet"
    ).set_index("utc_datetime")
    df["annotation"] = df["annotation"].fillna("")

    dtypes: dict[str, str] = {
        "precipitation_accumulated_avg": "float64",
        "num_time_slots": "int64",
        "num_hourly_faulty": "int64",
        "num_hourly_faulty_rewards": "int64",
        "valid_percentage": "float64",
        "ann_total_hour": "int64",
        "valid_percentage_rewards": "float64",
        "ann_total_hour_rewards": "int64",
        "annotation": "object",
    }
    df = df.astype(dtypes)

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

    df: pd.DataFrame
    if param_value == "temperature":
        df = get_minute_averaging_temperature_df()
    elif param_value == "wind_speed":
        df = get_minute_averaging_wind_speed_df()
    elif param_value == "wind_direction":
        df = get_minute_averaging_wind_direction_df()
    elif param_value == "precipitation_accumulated":
        df = get_minute_averaging_precipitation_accumulated_df()
    else:
        raise RuntimeError()

    return df


@pytest.fixture
def minute_averaging_nan_df(request: pytest.FixtureRequest) -> pd.DataFrame:
    """Calls the appropriate function to create the nans dataframe, according to the parameter name.

    Args:
    ----
        request (pytest.FixtureRequest): A request providing information on the executing test function

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    param_value: str = request.param

    df: pd.DataFrame
    if param_value == "temperature":
        df = get_minute_averaging_temperature_nan_df()
    elif param_value == "wind_speed":
        df = get_minute_averaging_wind_speed_nan_df()
    elif param_value == "wind_direction":
        df = get_minute_averaging_wind_direction_nan_df()
    elif param_value == "precipitation_accumulated":
        df = get_minute_averaging_precipitation_accumulated_nan_df()
    else:
        raise RuntimeError()

    return df


@pytest.fixture
def minute_averaging_empty_df(request: pytest.FixtureRequest) -> pd.DataFrame:
    """Calls the appropriate function to create the empty dataframe, according to the parameter name.

    Args:
    ----
        request (pytest.FixtureRequest): A request providing information on the executing test function

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    param_value: str = request.param

    df: pd.DataFrame
    if param_value == "temperature":
        df = get_minute_averaging_temperature_empty_df()
    elif param_value == "wind_speed":
        df = get_minute_averaging_wind_speed_empty_df()
    elif param_value == "wind_direction":
        df = get_minute_averaging_wind_direction_empty_df()
    elif param_value == "precipitation_accumulated":
        df = get_minute_averaging_precipitation_accumulated_empty_df()
    else:
        raise RuntimeError()

    return df


@pytest.fixture
def hour_averaging_df(request: pytest.FixtureRequest) -> pd.DataFrame:
    """Calls the appropriate function to create the dataframe, according to the parameter name.

    Args:
    ----
        request (pytest.FixtureRequest): A request providing information on the executing test function

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    param_value: str = request.getfixturevalue("parameter")
    threshold: float = request.getfixturevalue("availability_threshold")
    formatted_threshold: str = str(threshold).replace(".", "_")

    df: pd.DataFrame
    if param_value == "temperature":
        df = get_hour_averaging_temperature_df(formatted_threshold)
    elif param_value == "wind_speed":
        df = get_hour_averaging_wind_speed_df(formatted_threshold)
    elif param_value == "wind_direction":
        df = get_hour_averaging_wind_direction_df(formatted_threshold)
    elif param_value == "precipitation_accumulated":
        df = get_hour_averaging_precipitation_accumulated_df(formatted_threshold)
    else:
        raise RuntimeError()

    return df


@pytest.fixture
def hour_averaging_precipitation_accumulated_empty_df() -> pd.DataFrame:
    """Creates the empty hour averaged dataframe and returns it.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    # Specify the column names
    columns: list[str] = [
        "precipitation_accumulated_avg",
        "num_time_slots",
        "num_hourly_faulty",
        "num_hourly_faulty_rewards",
        "valid_percentage",
        "ann_total_hour",
        "valid_percentage_rewards",
        "ann_total_hour_rewards",
        "annotation",
    ]

    # Create an empty DataFrame with specified columns
    df: pd.DataFrame = pd.DataFrame(columns=columns, index=pd.DatetimeIndex([]))

    dtypes: dict[str, str] = {
        "precipitation_accumulated_avg": "float64",
        "num_time_slots": "int64",
        "num_hourly_faulty": "int64",
        "num_hourly_faulty_rewards": "int64",
        "valid_percentage": "float64",
        "ann_total_hour": "int64",
        "valid_percentage_rewards": "float64",
        "ann_total_hour_rewards": "int64",
        "annotation": "object",
    }
    df = df.astype(dtypes)

    return df
