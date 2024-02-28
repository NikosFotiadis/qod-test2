import pandas as pd
import pytest

from typing import Callable


def get_fnl_raw_process_input_temperature_df() -> pd.DataFrame:
    """Reads the raw data input dataframe from the parquet file and returns it.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    df: pd.DataFrame = pd.read_parquet(
        "tests/obc_sqc/fixtures_data/annotation_utils/error_codes_hourly/input/fnl_raw_process/fnl_raw_process_input_temperature_df.parquet"
    )

    dtypes: dict[str, str] = {
        "utc_datetime": "datetime64[ns]",
        "temperature": "Float64",
        "rolling_median": "float64",
        "consec_obs_diff_abs": "Float64",
        "median_diff_abs": "Float64",
        "ann_obc": "int64",
        "ann_jump_couples": "int64",
        "ann_invalid_datum": "int64",
        "ann_unidentified_spike": "int64",
        "ann_no_datum": "int64",
        "ann_constant": "int64",
        "ann_constant_long": "int64",
        "ann_constant_frozen": "int64",
        "total_raw_annotation": "int64",
        "reward_annotation": "int64",
        "annotation": "object",
    }
    df = df.astype(dtypes)

    return df


def get_fnl_raw_process_input_temperature_nan_df() -> pd.DataFrame:
    """Creates the input dataframe filled with nans and returns it.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    dtypes: dict[str, str] = {
        "utc_datetime": "object",
        "temperature": "object",
        "rolling_median": "object",
        "consec_obs_diff_abs": "object",
        "median_diff_abs": "object",
        "ann_obc": "object",
        "ann_jump_couples": "object",
        "ann_invalid_datum": "object",
        "ann_unidentified_spike": "object",
        "ann_no_datum": "object",
        "ann_constant": "object",
        "ann_constant_long": "object",
        "ann_constant_frozen": "object",
        "total_raw_annotation": "object",
        "reward_annotation": "object",
        "annotation": "object",
    }

    # Create an empty DataFrame with specified columns
    df: pd.DataFrame = pd.DataFrame(index=range(6750), columns=list(dtypes.keys()))

    df = df.astype(dtypes)

    df.loc[:] = pd.NA  # type: ignore [call-overload]

    return df


def get_fnl_raw_process_input_temperature_empty_df() -> pd.DataFrame:
    """Creates the empty input dataframe and returns it.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    dtypes: dict[str, str] = {
        "utc_datetime": "datetime64[ns]",
        "temperature": "Float64",
        "rolling_median": "float64",
        "consec_obs_diff_abs": "Float64",
        "median_diff_abs": "Float64",
        "ann_obc": "int64",
        "ann_jump_couples": "int64",
        "ann_invalid_datum": "int64",
        "ann_unidentified_spike": "int64",
        "ann_no_datum": "int64",
        "ann_constant": "int64",
        "ann_constant_long": "int64",
        "ann_constant_frozen": "int64",
        "total_raw_annotation": "int64",
        "reward_annotation": "int64",
        "annotation": "object",
    }

    # Create an empty DataFrame with specified columns
    df: pd.DataFrame = pd.DataFrame(columns=list(dtypes.keys()))

    df = df.astype(dtypes)

    return df


def get_fnl_raw_process_input_wind_speed_df() -> pd.DataFrame:
    """Reads the raw data input dataframe from the parquet file and returns it.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    df: pd.DataFrame = pd.read_parquet(
        "tests/obc_sqc/fixtures_data/annotation_utils/error_codes_hourly/input/fnl_raw_process/fnl_raw_process_input_wind_speed_df.parquet"
    )

    dtypes: dict[str, str] = {
        "utc_datetime": "datetime64[ns]",
        "wind_speed": "Float64",
        "rolling_median": "float64",
        "consec_obs_diff_abs": "Float64",
        "median_diff_abs": "Float64",
        "ann_obc": "int64",
        "ann_jump_couples": "int64",
        "ann_invalid_datum": "int64",
        "ann_unidentified_spike": "int64",
        "ann_no_datum": "int64",
        "ann_constant": "float64",
        "ann_constant_long": "int64",
        "ann_constant_frozen": "float64",
        "total_raw_annotation": "int64",
        "reward_annotation": "int64",
        "annotation": "object",
    }
    df = df.astype(dtypes)

    return df


def get_fnl_raw_process_input_wind_speed_nan_df() -> pd.DataFrame:
    """Creates the input dataframe filled with nans and returns it.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    dtypes: dict[str, str] = {
        "utc_datetime": "object",
        "wind_speed": "object",
        "rolling_median": "object",
        "consec_obs_diff_abs": "object",
        "median_diff_abs": "object",
        "ann_obc": "object",
        "ann_jump_couples": "object",
        "ann_invalid_datum": "object",
        "ann_unidentified_spike": "object",
        "ann_no_datum": "object",
        "ann_constant": "object",
        "ann_constant_long": "object",
        "ann_constant_frozen": "object",
        "total_raw_annotation": "object",
        "reward_annotation": "object",
        "annotation": "object",
    }

    # Create an empty DataFrame with specified columns
    df: pd.DataFrame = pd.DataFrame(index=range(6750), columns=list(dtypes.keys()))

    df = df.astype(dtypes)

    df.loc[:] = pd.NA  # type: ignore [call-overload]

    return df


def get_fnl_raw_process_input_wind_speed_empty_df() -> pd.DataFrame:
    """Creates the empty input dataframe and returns it.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    dtypes: dict[str, str] = {
        "utc_datetime": "datetime64[ns]",
        "wind_speed": "Float64",
        "rolling_median": "float64",
        "consec_obs_diff_abs": "Float64",
        "median_diff_abs": "Float64",
        "ann_obc": "int64",
        "ann_jump_couples": "int64",
        "ann_invalid_datum": "int64",
        "ann_unidentified_spike": "int64",
        "ann_no_datum": "int64",
        "ann_constant": "float64",
        "ann_constant_long": "int64",
        "ann_constant_frozen": "float64",
        "total_raw_annotation": "int64",
        "reward_annotation": "int64",
        "annotation": "object",
    }

    # Create an empty DataFrame with specified columns
    df: pd.DataFrame = pd.DataFrame(columns=list(dtypes.keys()))

    df = df.astype(dtypes)

    return df


def get_fnl_raw_process_input_wind_direction_df() -> pd.DataFrame:
    """Reads the raw data input dataframe from the parquet file and returns it.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    df: pd.DataFrame = pd.read_parquet(
        "tests/obc_sqc/fixtures_data/annotation_utils/error_codes_hourly/input/fnl_raw_process/fnl_raw_process_input_wind_direction_df.parquet"
    )

    dtypes: dict[str, str] = {
        "utc_datetime": "datetime64[ns]",
        "wind_direction": "Float64",
        "rolling_median": "float64",
        "consec_obs_diff_abs": "float64",
        "median_diff_abs": "float64",
        "ann_obc": "int64",
        "ann_jump_couples": "float64",
        "ann_invalid_datum": "float64",
        "ann_unidentified_spike": "float64",
        "ann_no_datum": "int64",
        "ann_constant": "float64",
        "ann_constant_long": "int64",
        "ann_constant_frozen": "float64",
        "total_raw_annotation": "int64",
        "reward_annotation": "int64",
        "annotation": "object",
    }
    df = df.astype(dtypes)

    return df


def get_fnl_raw_process_input_wind_direction_nan_df() -> pd.DataFrame:
    """Creates the input dataframe filled with nans and returns it.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    dtypes: dict[str, str] = {
        "utc_datetime": "object",
        "wind_direction": "object",
        "rolling_median": "object",
        "consec_obs_diff_abs": "object",
        "median_diff_abs": "object",
        "ann_obc": "object",
        "ann_jump_couples": "object",
        "ann_invalid_datum": "object",
        "ann_unidentified_spike": "object",
        "ann_no_datum": "object",
        "ann_constant": "object",
        "ann_constant_long": "object",
        "ann_constant_frozen": "object",
        "total_raw_annotation": "object",
        "reward_annotation": "object",
        "annotation": "object",
    }

    # Create an empty DataFrame with specified columns
    df: pd.DataFrame = pd.DataFrame(index=range(6750), columns=list(dtypes.keys()))

    df = df.astype(dtypes)

    df.loc[:] = pd.NA  # type: ignore [call-overload]

    return df


def get_fnl_raw_process_input_wind_direction_empty_df() -> pd.DataFrame:
    """Creates the empty input dataframe and returns it.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    dtypes: dict[str, str] = {
        "utc_datetime": "datetime64[ns]",
        "wind_direction": "Float64",
        "rolling_median": "float64",
        "consec_obs_diff_abs": "float64",
        "median_diff_abs": "float64",
        "ann_obc": "int64",
        "ann_jump_couples": "float64",
        "ann_invalid_datum": "float64",
        "ann_unidentified_spike": "float64",
        "ann_no_datum": "int64",
        "ann_constant": "float64",
        "ann_constant_long": "int64",
        "ann_constant_frozen": "float64",
        "total_raw_annotation": "int64",
        "reward_annotation": "int64",
        "annotation": "object",
    }

    # Create an empty DataFrame with specified columns
    df: pd.DataFrame = pd.DataFrame(columns=list(dtypes.keys()))

    df = df.astype(dtypes)

    return df


def get_fnl_raw_process_input_precipitation_accumulated_df() -> pd.DataFrame:
    """Reads the raw data input dataframe from the parquet file and returns it.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    df: pd.DataFrame = pd.read_parquet(
        "tests/obc_sqc/fixtures_data/annotation_utils/error_codes_hourly/input/fnl_raw_process/fnl_raw_process_input_precipitation_accumulated_df.parquet"
    )

    dtypes: dict[str, str] = {
        "utc_datetime": "datetime64[ns]",
        "precipitation_accumulated": "Float64",
        "rolling_median": "float64",
        "consec_obs_diff_abs": "float64",
        "median_diff_abs": "float64",
        "ann_obc": "int64",
        "ann_jump_couples": "float64",
        "ann_invalid_datum": "float64",
        "ann_unidentified_spike": "float64",
        "ann_no_datum": "int64",
        "ann_constant": "int64",
        "ann_constant_long": "int64",
        "ann_constant_frozen": "int64",
        "total_raw_annotation": "int64",
        "reward_annotation": "int64",
        "annotation": "object",
    }
    df = df.astype(dtypes)

    return df


def get_fnl_raw_process_input_precipitation_accumulated_nan_df() -> pd.DataFrame:
    """Creates the input dataframe filled with nans and returns it.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    dtypes: dict[str, str] = {
        "utc_datetime": "object",
        "precipitation_accumulated": "object",
        "rolling_median": "object",
        "consec_obs_diff_abs": "object",
        "median_diff_abs": "object",
        "ann_obc": "object",
        "ann_jump_couples": "object",
        "ann_invalid_datum": "object",
        "ann_unidentified_spike": "object",
        "ann_no_datum": "object",
        "ann_constant": "object",
        "ann_constant_long": "object",
        "ann_constant_frozen": "object",
        "total_raw_annotation": "object",
        "reward_annotation": "object",
        "annotation": "object",
    }

    # Create an empty DataFrame with specified columns
    df: pd.DataFrame = pd.DataFrame(index=range(6750), columns=list(dtypes.keys()))

    df = df.astype(dtypes)

    df.loc[:] = pd.NA  # type: ignore [call-overload]

    return df


def get_fnl_raw_process_input_precipitation_accumulated_empty_df() -> pd.DataFrame:
    """Creates the empty input dataframe and returns it.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    dtypes: dict[str, str] = {
        "utc_datetime": "datetime64[ns]",
        "precipitation_accumulated": "Float64",
        "rolling_median": "float64",
        "consec_obs_diff_abs": "float64",
        "median_diff_abs": "float64",
        "ann_obc": "int64",
        "ann_jump_couples": "float64",
        "ann_invalid_datum": "float64",
        "ann_unidentified_spike": "float64",
        "ann_no_datum": "int64",
        "ann_constant": "int64",
        "ann_constant_long": "int64",
        "ann_constant_frozen": "int64",
        "total_raw_annotation": "int64",
        "reward_annotation": "int64",
        "annotation": "object",
    }

    # Create an empty DataFrame with specified columns
    df: pd.DataFrame = pd.DataFrame(columns=list(dtypes.keys()))

    df = df.astype(dtypes)

    return df


def get_minute_averaging_input_temperature_df() -> pd.DataFrame:
    """Reads the minute averaged dataframe from the parquet file and returns it.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    df: pd.DataFrame = pd.read_parquet(
        "tests/obc_sqc/fixtures_data/annotation_utils/error_codes_hourly/input/minute_averaging/minute_averaging_input_temperature_df.parquet"
    )

    dtypes: dict[str, str] = {
        "utc_datetime": "datetime64[ns]",
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

    df = df.set_index("utc_datetime")

    return df


def get_minute_averaging_input_temperature_nan_df() -> pd.DataFrame:
    """Creates the input dataframe filled with nans and returns it.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    dtypes: dict[str, str] = {
        "utc_datetime": "datetime64[ns]",
        "temperature_avg": "object",
        "temperature_avg_corrected": "object",
        "faulty_rewards": "object",
        "num_faulty": "object",
        "num_total_slots": "object",
        "valid_percentage": "object",
        "valid_percentage_rewards": "object",
        "rolling_median": "object",
        "diff_abs": "object",
        "median_diff_abs": "object",
        "ann_jump_couples": "object",
        "ann_invalid_datum": "object",
        "ann_unidentified_change": "object",
        "ann_all_from_raw": "object",
        "ann_all_from_raw_rewards": "object",
        "ann_total": "object",
        "ann_total_rewards": "object",
        "annotation": "object",
    }

    # Create an empty DataFrame with specified columns
    df: pd.DataFrame = pd.DataFrame(columns=list(dtypes.keys()))

    df = df.astype(dtypes).set_index("utc_datetime")

    df.loc[:] = pd.NA  # type: ignore [call-overload]

    return df


def get_minute_averaging_input_temperature_empty_df() -> pd.DataFrame:
    """Creates the empty input dataframe and returns it.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    dtypes: dict[str, str] = {
        "utc_datetime": "datetime64[ns]",
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

    # Create an empty DataFrame with specified columns
    df: pd.DataFrame = pd.DataFrame(columns=list(dtypes.keys()))

    df = df.astype(dtypes).set_index("utc_datetime")

    return df


def get_minute_averaging_input_wind_speed_df() -> pd.DataFrame:
    """Reads the minute averaged dataframe from the parquet file and returns it.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    df: pd.DataFrame = pd.read_parquet(
        "tests/obc_sqc/fixtures_data/annotation_utils/error_codes_hourly/input/minute_averaging/minute_averaging_input_wind_speed_df.parquet"
    )

    dtypes: dict[str, str] = {
        "utc_datetime": "datetime64[ns]",
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

    df = df.set_index("utc_datetime")

    return df


def get_minute_averaging_input_wind_speed_nan_df() -> pd.DataFrame:
    """Creates the input dataframe filled with nans and returns it.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    dtypes: dict[str, str] = {
        "utc_datetime": "datetime64[ns]",
        "wind_direction_avg": "object",
        "wind_speed_avg": "object",
        "u": "object",
        "v": "object",
        "num_faulty": "object",
        "num_total_slots": "object",
        "valid_percentage": "object",
        "valid_percentage_rewards": "object",
        "faulty_rewards": "object",
        "wind_spd_avg_corrected": "object",
        "wind_dir_avg_corrected": "object",
        "rolling_median": "object",
        "diff_abs": "object",
        "median_diff_abs": "object",
        "ann_jump_couples": "object",
        "ann_invalid_datum": "object",
        "ann_unidentified_change": "object",
        "ann_all_from_raw": "object",
        "ann_all_from_raw_rewards": "object",
        "ann_total": "object",
        "ann_total_rewards": "object",
        "annotation": "object",
    }

    # Create an empty DataFrame with specified columns
    df: pd.DataFrame = pd.DataFrame(columns=list(dtypes.keys()))

    df = df.astype(dtypes).set_index("utc_datetime")

    df.loc[:] = pd.NA  # type: ignore [call-overload]

    return df


def get_minute_averaging_input_wind_speed_empty_df() -> pd.DataFrame:
    """Creates the empty input dataframe and returns it.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    dtypes: dict[str, str] = {
        "utc_datetime": "datetime64[ns]",
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

    # Create an empty DataFrame with specified columns
    df: pd.DataFrame = pd.DataFrame(columns=list(dtypes.keys()))

    df = df.astype(dtypes).set_index("utc_datetime")

    return df


def get_minute_averaging_input_wind_direction_df() -> pd.DataFrame:
    """Reads the minute averaged dataframe from the parquet file and returns it.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    df: pd.DataFrame = pd.read_parquet(
        "tests/obc_sqc/fixtures_data/annotation_utils/error_codes_hourly/input/minute_averaging/minute_averaging_input_wind_direction_df.parquet"
    )

    dtypes: dict[str, str] = {
        "utc_datetime": "datetime64[ns]",
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

    df = df.set_index("utc_datetime")

    return df


def get_minute_averaging_input_wind_direction_nan_df() -> pd.DataFrame:
    """Creates the input dataframe filled with nans and returns it.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    dtypes: dict[str, str] = {
        "utc_datetime": "datetime64[ns]",
        "wind_direction_avg": "object",
        "wind_speed_avg": "object",
        "u": "object",
        "v": "object",
        "num_faulty": "object",
        "num_total_slots": "object",
        "valid_percentage": "object",
        "valid_percentage_rewards": "object",
        "faulty_rewards": "object",
        "wind_spd_avg_corrected": "object",
        "wind_dir_avg_corrected": "object",
        "rolling_median_minute": "object",
        "diff_abs": "object",
        "median_diff_abs": "object",
        "ann_jump_couples": "object",
        "ann_invalid_datum": "object",
        "ann_unidentified_change": "object",
        "ann_all_from_raw": "object",
        "ann_all_from_raw_rewards": "object",
        "ann_total": "object",
        "ann_total_rewards": "object",
        "annotation": "object",
    }

    # Create an empty DataFrame with specified columns
    df: pd.DataFrame = pd.DataFrame(columns=list(dtypes.keys()))

    df = df.astype(dtypes).set_index("utc_datetime")

    df.loc[:] = pd.NA  # type: ignore [call-overload]

    return df


def get_minute_averaging_input_wind_direction_empty_df() -> pd.DataFrame:
    """Creates the empty input dataframe and returns it.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    dtypes: dict[str, str] = {
        "utc_datetime": "datetime64[ns]",
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

    # Create an empty DataFrame with specified columns
    df: pd.DataFrame = pd.DataFrame(columns=list(dtypes.keys()))

    df = df.astype(dtypes).set_index("utc_datetime")

    df.loc[:] = pd.NA  # type: ignore [call-overload]

    return df


def get_minute_averaging_input_precipitation_accumulated_df() -> pd.DataFrame:
    """Reads the minute averaged dataframe from the parquet file and returns it.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    df: pd.DataFrame = pd.read_parquet(
        "tests/obc_sqc/fixtures_data/annotation_utils/error_codes_hourly/input/minute_averaging/minute_averaging_input_precipitation_accumulated_df.parquet"
    )

    dtypes: dict[str, str] = {
        "utc_datetime": "datetime64[ns]",
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

    df = df.set_index("utc_datetime")

    return df


def get_minute_averaging_input_precipitation_accumulated_nan_df() -> pd.DataFrame:
    """Creates the input dataframe filled with nans and returns it.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    dtypes: dict[str, str] = {
        "utc_datetime": "datetime64[ns]",
        "precipitation_accumulated_avg": "object",
        "precipitation_accumulated_avg_corrected": "object",
        "faulty_rewards": "object",
        "num_faulty": "object",
        "num_total_slots": "object",
        "valid_percentage": "object",
        "valid_percentage_rewards": "object",
        "rolling_median_minute": "object",
        "diff_abs": "object",
        "median_diff_abs": "object",
        "ann_jump_couples": "object",
        "ann_invalid_datum": "object",
        "ann_unidentified_change": "object",
        "ann_all_from_raw": "object",
        "ann_all_from_raw_rewards": "object",
        "ann_total": "object",
        "ann_total_rewards": "object",
        "annotation": "object",
    }

    # Create an empty DataFrame with specified columns
    df: pd.DataFrame = pd.DataFrame(columns=list(dtypes.keys()))

    df = df.astype(dtypes).set_index("utc_datetime")

    df.loc[:] = pd.NA  # type: ignore [call-overload]

    return df


def get_minute_averaging_input_precipitation_accumulated_empty_df() -> pd.DataFrame:
    """Creates the empty input dataframe and returns it.

    Args:
    ----
        None

    Returns:
    -------
        pd.DataFrame: the created DataFrame
    """
    dtypes: dict[str, str] = {
        "utc_datetime": "datetime64[ns]",
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

    # Create an empty DataFrame with specified columns
    df: pd.DataFrame = pd.DataFrame(columns=list(dtypes.keys()))

    df = df.astype(dtypes).set_index("utc_datetime")

    return df


def get_annotations_output_temperature_series() -> pd.Series:
    """Reads the annotations dataframe from the parquet file and returns it as series.

    Args:
    ----
        None

    Returns:
    -------
        pd.Series: the created Series
    """
    df: pd.DataFrame = pd.read_parquet(
        "tests/obc_sqc/fixtures_data/annotation_utils/error_codes_hourly/output/annotations_output_temperature_df.parquet"
    )

    dtypes: dict[str, str] = {
        "utc_datetime": "datetime64[ns]",
        "annotations": "object",
    }
    df = df.astype(dtypes)

    df = df.set_index("utc_datetime")

    series: pd.Series = df["annotations"].rename_axis().rename(None)

    return series


def get_annotations_output_temperature_nan_series() -> pd.Series:
    """Reads the annotations dataframe from the parquet file and returns it as series.

    Args:
    ----
        None

    Returns:
    -------
        pd.Series: the created Series
    """
    df: pd.DataFrame = pd.read_parquet(
        "tests/obc_sqc/fixtures_data/annotation_utils/error_codes_hourly/output/annotations_output_temperature_nan_df.parquet"
    )

    dtypes: dict[str, str] = {
        "utc_datetime": "datetime64[ns]",
        "annotations": "object",
    }
    df = df.astype(dtypes)

    df = df.set_index("utc_datetime")

    series: pd.Series = df["annotations"].rename_axis().rename(None)

    return series


def get_annotations_output_temperature_empty_series() -> pd.Series:
    """Reads the annotations dataframe from the parquet file and returns it as series.

    Args:
    ----
        None

    Returns:
    -------
        pd.Series: the created Series
    """
    df: pd.DataFrame = pd.read_parquet(
        "tests/obc_sqc/fixtures_data/annotation_utils/error_codes_hourly/output/annotations_output_temperature_empty_df.parquet"
    )

    dtypes: dict[str, str] = {
        "utc_datetime": "datetime64[ns]",
        "annotations": "object",
    }
    df = df.astype(dtypes)

    df = df.set_index("utc_datetime")

    series: pd.Series = df["annotations"].rename_axis().rename(None)

    return series


def get_annotations_output_wind_speed_series() -> pd.Series:
    """Reads the annotations dataframe from the parquet file and returns it as series.

    Args:
    ----
        None

    Returns:
    -------
        pd.Series: the created Series
    """
    df: pd.DataFrame = pd.read_parquet(
        "tests/obc_sqc/fixtures_data/annotation_utils/error_codes_hourly/output/annotations_output_wind_speed_df.parquet"
    )

    dtypes: dict[str, str] = {
        "utc_datetime": "datetime64[ns]",
        "annotations": "object",
    }
    df = df.astype(dtypes)

    df = df.set_index("utc_datetime")

    series: pd.Series = df["annotations"].rename_axis().rename(None)

    return series


def get_annotations_output_wind_speed_nan_series() -> pd.Series:
    """Reads the annotations dataframe from the parquet file and returns it as series.

    Args:
    ----
        None

    Returns:
    -------
        pd.Series: the created Series
    """
    df: pd.DataFrame = pd.read_parquet(
        "tests/obc_sqc/fixtures_data/annotation_utils/error_codes_hourly/output/annotations_output_wind_speed_nan_df.parquet"
    )

    dtypes: dict[str, str] = {
        "utc_datetime": "datetime64[ns]",
        "annotations": "object",
    }
    df = df.astype(dtypes)

    df = df.set_index("utc_datetime")

    series: pd.Series = df["annotations"].rename_axis().rename(None)

    return series


def get_annotations_output_wind_speed_empty_series() -> pd.Series:
    """Reads the annotations dataframe from the parquet file and returns it as series.

    Args:
    ----
        None

    Returns:
    -------
        pd.Series: the created Series
    """
    df: pd.DataFrame = pd.read_parquet(
        "tests/obc_sqc/fixtures_data/annotation_utils/error_codes_hourly/output/annotations_output_wind_speed_empty_df.parquet"
    )

    dtypes: dict[str, str] = {
        "utc_datetime": "datetime64[ns]",
        "annotations": "object",
    }
    df = df.astype(dtypes)

    df = df.set_index("utc_datetime")

    series: pd.Series = df["annotations"].rename_axis().rename(None)

    return series


def get_annotations_output_wind_direction_series() -> pd.Series:
    """Reads the annotations dataframe from the parquet file and returns it as series.

    Args:
    ----
        None

    Returns:
    -------
        pd.Series: the created Series
    """
    df: pd.DataFrame = pd.read_parquet(
        "tests/obc_sqc/fixtures_data/annotation_utils/error_codes_hourly/output/annotations_output_wind_direction_df.parquet"
    )

    dtypes: dict[str, str] = {
        "utc_datetime": "datetime64[ns]",
        "annotations": "object",
    }
    df = df.astype(dtypes)

    df = df.set_index("utc_datetime")

    series: pd.Series = df["annotations"].rename_axis().rename(None)

    return series


def get_annotations_output_wind_direction_nan_series() -> pd.Series:
    """Reads the annotations dataframe from the parquet file and returns it as series.

    Args:
    ----
        None

    Returns:
    -------
        pd.Series: the created Series
    """
    df: pd.DataFrame = pd.read_parquet(
        "tests/obc_sqc/fixtures_data/annotation_utils/error_codes_hourly/output/annotations_output_wind_direction_nan_df.parquet"
    )

    dtypes: dict[str, str] = {
        "utc_datetime": "datetime64[ns]",
        "annotations": "object",
    }
    df = df.astype(dtypes)

    df = df.set_index("utc_datetime")

    series: pd.Series = df["annotations"].rename_axis().rename(None)

    return series


def get_annotations_output_wind_direction_empty_series() -> pd.Series:
    """Reads the annotations dataframe from the parquet file and returns it as series.

    Args:
    ----
        None

    Returns:
    -------
        pd.Series: the created Series
    """
    df: pd.DataFrame = pd.read_parquet(
        "tests/obc_sqc/fixtures_data/annotation_utils/error_codes_hourly/output/annotations_output_temperature_empty_df.parquet"
    )

    dtypes: dict[str, str] = {
        "utc_datetime": "datetime64[ns]",
        "annotations": "object",
    }
    df = df.astype(dtypes)

    df = df.set_index("utc_datetime")

    series: pd.Series = df["annotations"].rename_axis().rename(None)

    return series


def get_annotations_output_precipitation_accumulated_series() -> pd.Series:
    """Reads the annotations dataframe from the parquet file and returns it as series.

    Args:
    ----
        None

    Returns:
    -------
        pd.Series: the created Series
    """
    df: pd.DataFrame = pd.read_parquet(
        "tests/obc_sqc/fixtures_data/annotation_utils/error_codes_hourly/output/annotations_output_precipitation_accumulated_df.parquet"
    )

    dtypes: dict[str, str] = {
        "utc_datetime": "datetime64[ns]",
        "annotations": "object",
    }
    df = df.astype(dtypes)

    df = df.set_index("utc_datetime")

    series: pd.Series = df["annotations"].rename_axis().rename(None)

    return series


def get_annotations_output_precipitation_accumulated_nan_series() -> pd.Series:
    """Reads the annotations dataframe from the parquet file and returns it as series.

    Args:
    ----
        None

    Returns:
    -------
        pd.Series: the created Series
    """
    df: pd.DataFrame = pd.read_parquet(
        "tests/obc_sqc/fixtures_data/annotation_utils/error_codes_hourly/output/annotations_output_precipitation_accumulated_nan_df.parquet"
    )

    dtypes: dict[str, str] = {
        "utc_datetime": "datetime64[ns]",
        "annotations": "object",
    }
    df = df.astype(dtypes)

    df = df.set_index("utc_datetime")

    series: pd.Series = df["annotations"].rename_axis().rename(None)

    return series


def get_annotations_output_precipitation_accumulated_empty_series() -> pd.Series:
    """Reads the annotations dataframe from the parquet file and returns it as series.

    Args:
    ----
        None

    Returns:
    -------
        pd.Series: the created Series
    """
    df: pd.DataFrame = pd.read_parquet(
        "tests/obc_sqc/fixtures_data/annotation_utils/error_codes_hourly/output/annotations_output_precipitation_accumulated_empty_df.parquet"
    )

    dtypes: dict[str, str] = {
        "utc_datetime": "datetime64[ns]",
        "annotations": "object",
    }
    df = df.astype(dtypes)

    df = df.set_index("utc_datetime")

    series: pd.Series = df["annotations"].rename_axis().rename(None)

    return series


@pytest.fixture
def fnl_raw_process_input_df(request: pytest.FixtureRequest) -> pd.DataFrame:
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
        "temperature": get_fnl_raw_process_input_temperature_df,
        "wind_speed": get_fnl_raw_process_input_wind_speed_df,
        "wind_direction": get_fnl_raw_process_input_wind_direction_df,
        "precipitation_accumulated": get_fnl_raw_process_input_precipitation_accumulated_df,
    }

    if param_value in functions_dict:
        df: pd.DataFrame = functions_dict[param_value]()
    else:
        raise RuntimeError()

    return df


@pytest.fixture
def fnl_raw_process_input_nan_df(request: pytest.FixtureRequest) -> pd.DataFrame:
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
        "temperature": get_fnl_raw_process_input_temperature_nan_df,
        "wind_speed": get_fnl_raw_process_input_wind_speed_nan_df,
        "wind_direction": get_fnl_raw_process_input_wind_direction_nan_df,
        "precipitation_accumulated": get_fnl_raw_process_input_precipitation_accumulated_nan_df,
    }

    if param_value in functions_dict:
        df: pd.DataFrame = functions_dict[param_value]()
    else:
        raise RuntimeError()

    return df


@pytest.fixture
def fnl_raw_process_input_empty_df(request: pytest.FixtureRequest) -> pd.DataFrame:
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
        "temperature": get_fnl_raw_process_input_temperature_empty_df,
        "wind_speed": get_fnl_raw_process_input_wind_speed_empty_df,
        "wind_direction": get_fnl_raw_process_input_wind_direction_empty_df,
        "precipitation_accumulated": get_fnl_raw_process_input_precipitation_accumulated_empty_df,
    }

    if param_value in functions_dict:
        df: pd.DataFrame = functions_dict[param_value]()
    else:
        raise RuntimeError()

    return df


@pytest.fixture
def minute_averaging_input_df(request: pytest.FixtureRequest) -> pd.DataFrame:
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
        "temperature": get_minute_averaging_input_temperature_df,
        "wind_speed": get_minute_averaging_input_wind_speed_df,
        "wind_direction": get_minute_averaging_input_wind_direction_df,
        "precipitation_accumulated": get_minute_averaging_input_precipitation_accumulated_df,
    }

    if param_value in functions_dict:
        df: pd.DataFrame = functions_dict[param_value]()
    else:
        raise RuntimeError()

    return df


@pytest.fixture
def minute_averaging_input_nan_df(request: pytest.FixtureRequest) -> pd.DataFrame:
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
        "temperature": get_minute_averaging_input_temperature_nan_df,
        "wind_speed": get_minute_averaging_input_wind_speed_nan_df,
        "wind_direction": get_minute_averaging_input_wind_direction_nan_df,
        "precipitation_accumulated": get_minute_averaging_input_precipitation_accumulated_nan_df,
    }

    if param_value in functions_dict:
        df: pd.DataFrame = functions_dict[param_value]()
    else:
        raise RuntimeError()

    return df


@pytest.fixture
def minute_averaging_input_empty_df(request: pytest.FixtureRequest) -> pd.DataFrame:
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
        "temperature": get_minute_averaging_input_temperature_empty_df,
        "wind_speed": get_minute_averaging_input_wind_speed_empty_df,
        "wind_direction": get_minute_averaging_input_wind_direction_empty_df,
        "precipitation_accumulated": get_minute_averaging_input_precipitation_accumulated_empty_df,
    }

    if param_value in functions_dict:
        df: pd.DataFrame = functions_dict[param_value]()
    else:
        raise RuntimeError()

    return df


@pytest.fixture
def annotations_output_series(request: pytest.FixtureRequest) -> pd.Series:
    """Calls the appropriate function to create the series, according to the parameter name.

    Args:
    ----
        request (pytest.FixtureRequest): A request providing information on the executing test function

    Returns:
    -------
        pd.Series: the created Series
    """
    param_value: str = request.param

    functions_dict: dict[str, Callable] = {
        "temperature": get_annotations_output_temperature_series,
        "wind_speed": get_annotations_output_wind_speed_series,
        "wind_direction": get_annotations_output_wind_direction_series,
        "precipitation_accumulated": get_annotations_output_precipitation_accumulated_series,
    }

    if param_value in functions_dict:
        series: pd.Series = functions_dict[param_value]()
    else:
        raise RuntimeError()

    return series


@pytest.fixture
def annotations_output_nan_series(request: pytest.FixtureRequest) -> pd.Series:
    """Calls the appropriate function to create the series, according to the parameter name.

    Args:
    ----
        request (pytest.FixtureRequest): A request providing information on the executing test function

    Returns:
    -------
        pd.Series: the created Series
    """
    param_value: str = request.param

    functions_dict: dict[str, Callable] = {
        "temperature": get_annotations_output_temperature_nan_series,
        "wind_speed": get_annotations_output_wind_speed_nan_series,
        "wind_direction": get_annotations_output_wind_direction_nan_series,
        "precipitation_accumulated": get_annotations_output_precipitation_accumulated_nan_series,
    }

    if param_value in functions_dict:
        series: pd.Series = functions_dict[param_value]()
    else:
        raise RuntimeError()

    return series


@pytest.fixture
def annotations_output_empty_series(request: pytest.FixtureRequest) -> pd.Series:
    """Calls the appropriate function to create the series, according to the parameter name.

    Args:
    ----
        request (pytest.FixtureRequest): A request providing information on the executing test function

    Returns:
    -------
        pd.Series: the created Series
    """
    param_value: str = request.param

    functions_dict: dict[str, Callable] = {
        "temperature": get_annotations_output_temperature_empty_series,
        "wind_speed": get_annotations_output_wind_speed_empty_series,
        "wind_direction": get_annotations_output_wind_direction_empty_series,
        "precipitation_accumulated": get_annotations_output_precipitation_accumulated_empty_series,
    }

    if param_value in functions_dict:
        series: pd.Series = functions_dict[param_value]()
    else:
        raise RuntimeError()

    return series
