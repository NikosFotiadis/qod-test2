from __future__ import annotations

import numpy as np
import pandas as pd


class RawDataCheck:
    """Checks in raw data."""

    @staticmethod
    def raw_data_suspicious_check(
        fnl_df: pd.DataFrame,
        parameter: str,
        control_threshold: float,
        minimum_timestep: int,
        time_window: int,
        availability_threshold_median: float,
        ann_unident_spk: int,
        ann_no_datum: int,
        ann_invalid_datum: int,
    ) -> pd.DataFrame:
        """Detects faulty observations based on WMO criteria.

        Furthermore, it annotates as faulty the observation which is unavailable or has the greatest difference
        from the 10-minute median in case of a significant jump within a couple of raw data.
        As an example, if we have a dataset of temperatures [10,10,10.1,50,10.1,50,50,50,10.2, 10.2,nan,10.2],
        then e.g., the difference between the 3rd and 4th elements is >2, but the difference of the
        4th element from the 10-min median is larger, and thus the 4th element is faulty. Also 6th, 7th and 8th
        elements will be considered as faulty because  they are equal to the latest faulty measurement. In addition,
        the nan value will be annotated as faulty/invalid. It should be noted that if median cannot be calculated due to
        significant amount of unavailable data, then all the suspicious for spike/dip values will be annotated
            as faulty.
        fnl_df (df): the output of time_normalisation_dataframe def, which is a dataframe with a fixed temporal
        resolution parameter (str): the parameter that this def looks into, e.g., temperature, humidity, wind speed etc.
        control_threshold (float): the threshold to check for jumps in a parameter
        minimum_timestep (int): the timestep of the reframed raw data (e.g., 16sec) [in seconds]
        time_window (int): the time window for rolling median [in minutes]
        availability_threshold_median (float): the availability threshold, e.g., we are able to calculate median
            only if <67%/75% of timeslots within a certain period are available
        ann_unident_spk (int): annotation where no median has been calculated
        ann_no_datum (int): annotation where no datum is available
        ann_invalid_datum (int): annotation where the datum exists, but it's not valid

        result (df): a df with the
            a. parameter under investigation,
            b. rolling median,
            c. abs difference between consecutive observations,
            d. abs difference between measurement and last 10-min median,
            e. annotation for couples of invalid jump, invalid datum (the one obs from a couple),
                not available median, not available datum, constant data, unidentified spike
                (for both all- and -reward faulty data)
            f. text annotations for all reasons that a datum is faulty
        """
        # Sort the DataFrame by date
        fnl_df = fnl_df.sort_values("utc_datetime")

        # Set the date column as the index
        if fnl_df.index.name is None:
            # Ensure that UTC datetime is of type 'datetime64[ns]', otherwise the 'rolling' fails downstream
            fnl_df["utc_datetime"] = pd.to_datetime(fnl_df["utc_datetime"])
            fnl_df = fnl_df.set_index("utc_datetime")

        # We exclude the parameters of wind direction and precipitation.
        # No hump check can be applied for them.
        if parameter not in {"wind_direction", "precipitation_accumulated"}:
            # Calculating the 10-min rolling median
            rolling_median: pd.Series = fnl_df[f"{parameter}_for_raw_check"].rolling(f"{time_window}min").median()

            # Getting the number of available observations within the time_window
            available_observations: pd.Series = (
                fnl_df[f"{parameter}_for_raw_check"].rolling(f"{time_window}min").count()
            )

            # Calculating the total number of possible observations within the time
            # window (e.g., 10 minutes / 16 seconds)
            possible_observations: float = time_window * 60 / minimum_timestep

            # Finding the percentage of the available observations
            percentage_available: pd.Series = available_observations / possible_observations

            # Assign np.nan to rolling median where percentage of available observations is
            # less than the availability_threshold_median (e.g., 67%)
            rolling_median[percentage_available < availability_threshold_median] = np.nan

            # Create a new column in the df with the rolling median values
            fnl_df["rolling_median"] = rolling_median

            # Get the absolute difference between consecutive observations
            fnl_df["consec_obs_diff_abs"] = fnl_df[f"{parameter}_for_raw_check"].diff().abs()

            # Finding the absolute difference between an observation and the median of the last x minutes
            fnl_df["median_diff_abs"] = (fnl_df[f"{parameter}_for_raw_check"] - fnl_df["rolling_median"]).abs()

            # Annotate both the values of a couple of observations with 1 and finally
            # annotate which values have a larger difference from median
            # Create 4 columns to use them for annotation
            fnl_df["ann_jump_couples"] = 0  # for annotating both observations in case of an invalid jump

            # for annotating only one of the observations of a couple with an invalid jump
            fnl_df["ann_invalid_datum"] = 0

            # if median cannot be calculated and there are suspicious for spike/dip values
            fnl_df["ann_unidentified_spike"] = 0

            # if there is no datum in a certain timeslot
            fnl_df["ann_no_datum"] = 0

            temp: pd.Series = fnl_df[f"{parameter}_for_raw_check"]

            temp_diff: pd.Series = temp.diff().abs()

            fnl_df["ann_jump_couples"] = 0
            fnl_df["ann_invalid_datum"] = 0

            # check if difference is larger than the defined threshold
            mask_diff_larger_than_threshold: pd.Series = temp_diff > control_threshold
            fnl_df.loc[mask_diff_larger_than_threshold, "ann_jump_couples"] = 1
            fnl_df.loc[mask_diff_larger_than_threshold.shift(-1).fillna(False), "ann_jump_couples"] = 1

            # In case the difference is large, check which abs(value-median)
            # of the couple of observations is larger and annotate
            mask_prev_val_bigger_than_curr: pd.Series = fnl_df["median_diff_abs"].shift(1) > fnl_df["median_diff_abs"]
            mask_curr_val_bigger_than_prev: pd.Series = fnl_df["median_diff_abs"] > fnl_df["median_diff_abs"].shift(1)

            fnl_df.loc[
                (mask_diff_larger_than_threshold & mask_prev_val_bigger_than_curr).shift(-1).fillna(False),
                "ann_invalid_datum",
            ] = ann_invalid_datum
            fnl_df.loc[
                (mask_diff_larger_than_threshold & mask_curr_val_bigger_than_prev),
                "ann_invalid_datum",
            ] = ann_invalid_datum

            # Annotating as invalid, observations that are equal to a previous invalid value
            mask_invalid_equal_to_prev: pd.Series = (temp == temp.shift(1)) & (
                fnl_df["ann_invalid_datum"].shift(1) == ann_invalid_datum
            )
            fnl_df.loc[mask_invalid_equal_to_prev, "ann_invalid_datum"] = ann_invalid_datum

            no_median_mask: pd.Series = fnl_df["rolling_median"].isna()
            ann_jump_couples_mask: pd.Series = fnl_df["ann_jump_couples"] == 1
            combined_mask: pd.Series = no_median_mask & ann_jump_couples_mask

            # Annotate for 'ann_unidentified_spike' where 'median_diff_abs' is null, but there are unexpected spikes
            fnl_df.loc[combined_mask, "ann_unidentified_spike"] = ann_unident_spk

        # As this series of checks is not for wind direction, all the
        # following columns are created just for facilitating the algorithm
        else:
            fnl_df = fnl_df.reset_index()
            fnl_df["rolling_median"] = np.nan
            fnl_df["consec_obs_diff_abs"] = np.nan
            fnl_df["median_diff_abs"] = np.nan
            fnl_df["ann_jump_couples"] = np.nan
            fnl_df["ann_invalid_datum"] = np.nan
            fnl_df["ann_unidentified_spike"] = np.nan
            fnl_df["ann_no_datum"] = 0

        # Create a boolean mask where missing values are marked as True. So, unavailable data are annotated.
        no_observation_mask: pd.Series = fnl_df[parameter].isna()
        fnl_df.loc[no_observation_mask, "ann_no_datum"] = ann_no_datum

        # Make a new column where all faulty elements (for any reason)
        # detected in previous processes are annotated with 1.
        # Thus, we merge all faults in a new column.
        fnl_df["total_raw_annotation"] = np.where(
            (fnl_df["ann_obc"] > 0)
            | (fnl_df["ann_invalid_datum"] > 0)
            | (fnl_df["ann_unidentified_spike"] > 0)
            | (fnl_df["ann_no_datum"] > 0)
            | (fnl_df["ann_constant_frozen"] > 0)
            | (fnl_df["ann_constant"] > 0)
            | (fnl_df["ann_constant_long"] > 0),
            1,
            0,
        )

        # Make a new column where only reward-faulty elements are annotated with 1.
        fnl_df["reward_annotation"] = np.where(
            (fnl_df["ann_obc"] > 0)
            | (fnl_df["ann_invalid_datum"] > 0)
            | (fnl_df["ann_unidentified_spike"] > 0)
            | (fnl_df["ann_no_datum"] > 0)
            | (fnl_df["ann_constant_frozen"] > 0)
            | (fnl_df["ann_constant"] > 0)
            | (fnl_df["ann_constant_long"] > 0),
            1,
            0,
        )

        if "utc_datetime" in fnl_df.columns:
            fnl_df = fnl_df.set_index("utc_datetime")

        return fnl_df
