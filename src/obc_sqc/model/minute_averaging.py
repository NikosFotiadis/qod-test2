from __future__ import annotations

import numpy as np
import pandas as pd

from obc_sqc.model.annotation_utils import AnnotationUtils
from obc_sqc.model.averaging_utils import AveragingUtils


class MinuteAveraging:  # noqa: D101
    group_label: str = "left"

    @staticmethod
    def average_in_minute(  # noqa: PLR0912, PLR0913, PLR0915, C901
        fnl_df,
        parameter,
        averaging_period,
        availability_threshold,
        time_window_median,
        control_threshold,
        ann_invalid_datum,
        pr_int,
        pr_hourly_availability,
        preprocess_time_window,
        station_type,
    ):
        """This def detects faulty x-minute averages based on WMO criteria and annotates as faulty the average value
        which is unavailable or has the greatest difference from the 10-minute median in case of a significant jump
        within a couple of raw data. As an example, if we have a dataset of 1-minute average temperatures [10,10,
        10.1,50,10.1,50,50,50,10.2,10.2,nan,10.2], then e.g., the difference between the 3rd and 4th elements is >3,
        but the difference of the 4th element from the 10-min median is larger, and thus the 4th element is faulty.
        Also 6th, 7th and 8th elements will be considered as faulty because they are equal to the latest faulty
        measurement. In addition, the nan value will be annotated as faulty/invalid. It should be noted that the
        first 10min data are considered as invalid and no decision can be taken as median cannot be calculated.
        However, if only 10min data are missing from an entire hour, rewards should be finally awarded. Note that the
        average of e.g. 01:10:00 is the average of values between 01:09:01-01:10:00.

        fnl_df (df): the output of time_normalisation_dataframe def,
            which is a dataframe with a fixed temporal resolution
        parameter (str): the parameter that this def looks into, e.g., temperature, humidity, wind speed etc.
        averaging_period (): the desired period for averaging, e.g., we want to calculate 2-minute averages
        availability_threshold (float): the availability threshold, e.g., if <67% of timeslots within a certain period
            is available, averaging or rewarding is not possible [x out of 1]
        time_window_median (): the time window for rolling median [in minutes]
        control_threshold (int): the threshold to check for jumps in a parameter
        time_window_constant (int): the time window for rolling window checking constant values within it [in minutes]
        preprocess_time_window (int): the time window between start_timestamp and the first timestamp of the current day
            [in minutes]

        result (df): a df with the
            a. averaged parameter under investigation,
            b. rolling median,
            c. abs difference between consecutive observations,
            d. abs difference between measurement and last 10-min median,
            e. annotation for couples of invalid jump, invalid datum (the one obs from a couple),
                not available median, not available datum, constant data, (for both all- and -reward faulty data)
            f. text annotations for all reasons that a datum is faulty
        """  # noqa: D202, D205

        delim: str = ","
        fnl_df = fnl_df.reset_index().rename(columns={"index": "utc_datetime"})

        # convert time to datetime format
        fnl_df["utc_datetime"] = pd.to_datetime(fnl_df["utc_datetime"])

        if parameter == "precipitation_accumulated" and station_type != "WS1000":
            availability_threshold = pr_hourly_availability

        # In case of wind spd/dir, we need to apply vector average
        if parameter == "wind_speed" or parameter == "wind_direction":  # noqa: PLR1714
            # Calculate the u and v components of the wind in the raw dataset
            fnl_df["wind_v"] = -1 * fnl_df["wind_speed"] * np.cos(fnl_df["wind_direction"] * np.pi / 180.0)
            fnl_df["wind_u"] = -1 * fnl_df["wind_speed"] * np.sin(fnl_df["wind_direction"] * np.pi / 180.0)

            # Group by minute and calculate average of wind components.
            # It is important that we firstly average the u and v components of the wind
            minute_averaging = fnl_df.groupby(
                pd.Grouper(key="utc_datetime", label=MinuteAveraging.group_label, freq=f"{averaging_period}min")
            ).agg(
                num_time_slots=(
                    f"{parameter}",
                    "count",
                ),  # this counts the elements having a value
                num_nan_values=(
                    parameter,
                    lambda x: x.isna().sum(),
                ),  # this counts the elements having nan
                u=("wind_u", "mean"),  # this averages the u component of the wind
                v=("wind_v", "mean"),  # this averages the v component of the wind
                num_faulty=(
                    "total_raw_annotation",
                    "sum",
                ),  # this counts the faulty observations
                faulty_rewards=(
                    "reward_annotation",
                    "sum",
                ),  # this counts only the faulty obs for rewards
                annotation=(  # this merges all text annotations in one string
                    "annotation",
                    lambda x: (
                        delim.join(set(delim.join(x.fillna("").values).split(delim))).lstrip(delim)
                        if any(x.str.contains(delim))
                        else delim.join(x.fillna("")).lstrip(delim)
                    ),
                ),
            )

            # We put both columns in the dfs for wind dir and wind speed, as we
            # 'll need both for hourly averaging of wind spd and dir
            minute_averaging.insert(
                1,
                "wind_speed_avg",
                minute_averaging.apply(AveragingUtils.row_wind_speed_calculation, axis=1),
            )
            minute_averaging.insert(
                1,
                "wind_direction_avg",
                minute_averaging.apply(AveragingUtils.row_wind_direction_calculation, axis=1),
            )

            minute_averaging["u"] = minute_averaging["u"].round(2)
            minute_averaging["v"] = minute_averaging["v"].round(2)
            minute_averaging.loc[minute_averaging["wind_speed_avg"].notna(), "wind_speed_avg"] = minute_averaging[
                minute_averaging["wind_speed_avg"].notna()
            ].round(2)
            minute_averaging.loc[minute_averaging["wind_direction_avg"].notna(), "wind_direction_avg"] = (
                minute_averaging[minute_averaging["wind_direction_avg"].notna()].round(2)
            )

            minute_averaging["wind_spd_avg_corrected"] = fnl_df.groupby(
                pd.Grouper(key="utc_datetime", label=MinuteAveraging.group_label, freq=f"{averaging_period}min")
            ).apply(
                AveragingUtils.column_wind_speed_average_using_annotation,
                availability_threshold=availability_threshold,
                annotation_col="total_raw_annotation",
            )
            minute_averaging["wind_dir_avg_corrected"] = fnl_df.groupby(
                pd.Grouper(key="utc_datetime", label=MinuteAveraging.group_label, freq=f"{averaging_period}min")
            ).apply(
                AveragingUtils.column_wind_direction_average_using_annotation,
                availability_threshold=availability_threshold,
                annotation_col="total_raw_annotation",
            )

        # for all the other variables we calculate a simple average
        else:
            # in order to give a certain name in the column of the parameter average
            parameter_avg_name = parameter + "_avg"

            if parameter == "precipitation_accumulated":
                # Group data by x-minute intervals and perform aggregations
                minute_averaging = fnl_df.groupby(
                    pd.Grouper(key="utc_datetime", label=MinuteAveraging.group_label, freq=f"{averaging_period}min")
                ).agg(
                    num_time_slots=(f"{parameter}", "count"),
                    num_nan_values=(f"{parameter}", lambda x: x.isna().sum()),
                    **{
                        parameter_avg_name: (
                            "precipitation_diff",
                            lambda x: np.sum(x[(x <= averaging_period * 60 * pr_int) & (x > 0)]),
                        )
                    },
                    # For precipitation, we sum instead of averaging
                    num_faulty=("total_raw_annotation", "sum"),
                    faulty_rewards=("reward_annotation", "sum"),
                    annotation=(
                        "annotation",
                        lambda x: (
                            delim.join(set(delim.join(x.fillna("").values).split(delim))).lstrip(delim)
                            if any(x.str.contains(delim))
                            else delim.join(x.fillna("")).lstrip(delim)
                        ),
                    ),
                )

                minute_averaging.insert(
                    loc=3,
                    column=f"{parameter}_avg_corrected",
                    value=minute_averaging[f"{parameter_avg_name}"],
                )

            else:
                # group data by x-minute intervals and count number of time slots, NaN values,
                # and calculate the x-minute average temperature
                minute_averaging = fnl_df.groupby(
                    pd.Grouper(key="utc_datetime", label=MinuteAveraging.group_label, freq=f"{averaging_period}min")
                ).agg(
                    num_time_slots=(
                        f"{parameter}",
                        "count",
                    ),  # this counts the elements having a value
                    num_nan_values=(
                        parameter,
                        lambda x: x.isna().sum(),
                    ),  # this counts the elements having nan
                    **{parameter_avg_name: (parameter, "mean")},  # this counts the simple average of the parameter
                    num_faulty=(
                        "total_raw_annotation",
                        "sum",
                    ),  # this counts the faulty observations
                    faulty_rewards=(
                        "reward_annotation",
                        "sum",
                    ),  # this counts only the faulty obs for rewards
                    annotation=(
                        "annotation",  # this merges all text annotations in one string
                        lambda x: (
                            delim.join(set(delim.join(x.fillna("").values).split(delim))).lstrip(delim)
                            if any(x.str.contains(delim))
                            else delim.join(x.fillna("")).lstrip(delim)
                        ),
                    ),
                )

                minute_averaging[f"{parameter_avg_name}"] = minute_averaging[f"{parameter_avg_name}"].round(2)

                # calculate average after removing faulty measurements
                # and only if >availability_threshold of the data is available
                corr_avg = fnl_df.groupby(
                    pd.Grouper(key="utc_datetime", label=MinuteAveraging.group_label, freq=f"{averaging_period}min")
                ).apply(
                    AveragingUtils.column_average_using_annotation,
                    column=parameter,
                    availability_threshold=availability_threshold,
                    annotation_col="total_raw_annotation",
                )
                minute_averaging.insert(loc=3, column=f"{parameter}_avg_corrected", value=corr_avg)
                minute_averaging[f"{parameter}_avg_corrected"] = minute_averaging[f"{parameter}_avg_corrected"].round(2)

        # Finding jumps between consecutive minute averages
        if parameter != "wind_direction" and parameter != "precipitation_accumulated":  # noqa: PLR1714
            # calculate the rolling median with the time_window_median
            rolling_median = (
                minute_averaging[f"{parameter}_avg"].astype("Float64").rolling(f"{time_window_median}min").median()
            )

            # Then we only keep median values if >availability_threshold of the data is available
            # calculate the number of available observations within the window
            available_observations = (
                minute_averaging[f"{parameter}_avg"].astype("Float64").rolling(f"{time_window_median}min").count()
            )

            # calculate the total number of possible observations within the window (10 minutes / 16 seconds)
            possible_observations = time_window_median / averaging_period

            # calculate the percentage of available observations
            percentage_available = available_observations / possible_observations

            # assign np.nan to rolling median where percentage of available observations is less
            # than availability_threshold (e.g. 67%)
            rolling_median[percentage_available < availability_threshold] = np.nan

            # create a new column in the DataFrame with the rolling median values
            # we create new columns
            minute_averaging["rolling_median"] = rolling_median.round(2)

            # abs difference between consecutive averages
            minute_averaging["diff_abs"] = minute_averaging[f"{parameter}_avg"].astype("Float64").diff().abs().round(2)

            # abs difference between an obs and the x-min median
            minute_averaging["median_diff_abs"] = (
                (minute_averaging[f"{parameter}_avg"] - minute_averaging["rolling_median"])
                .astype("Float64")
                .abs()
                .round(2)
            )

            # creating a new column for annotating invalid jumps between consecutive averages
            minute_averaging["ann_jump_couples"] = 0

            # creating a new column for annotating unavailable averages
            minute_averaging["ann_invalid_datum"] = 0

            temp = minute_averaging[f"{parameter}_avg"]
            minute_averaging.reset_index(inplace=True)  # noqa: PD002

            # check if difference is larger than the defined threshold
            for i in range(1, len(temp)):
                prev_val = minute_averaging.loc[i - 1, "median_diff_abs"] if i > 0 else 0
                curr_val = minute_averaging.loc[i, "median_diff_abs"]
                prev_or_cur_val_is_missing: bool = prev_val is pd.NA or curr_val is pd.NA

                # check if difference of consecutive average values is larger than control_threshold
                diff = abs(temp[i] - temp[i - 1]) if temp[i] is not pd.NA and temp[i - 1] is not pd.NA else pd.NA

                if not prev_or_cur_val_is_missing:
                    if diff > control_threshold:
                        if diff > control_threshold:
                            minute_averaging.loc[i, "ann_jump_couples"] = 1
                            minute_averaging.loc[i - 1, "ann_jump_couples"] = 1

                            # in case the difference is large,
                            # check which abs(value-median) of the couple of observations
                            # is larger and annotate
                            # Warning! if median is not available, fnl_df['ann_invalid_datum']=0. However,
                            # later we will annotate all elements with not available median as invalid
                            if prev_val > curr_val:
                                minute_averaging.loc[i - 1, "ann_invalid_datum"] = ann_invalid_datum
                            elif curr_val > prev_val:
                                minute_averaging.loc[i, "ann_invalid_datum"] = ann_invalid_datum

                else:
                    minute_averaging.loc[i, "ann_jump_couples"] = 0
                    minute_averaging.loc[i, "ann_invalid_datum"] = 0

                # we also annotate a value as invalid if it's equal to a previous faulty measurement
                if not prev_or_cur_val_is_missing:
                    if temp[i] == temp[i - 1] and minute_averaging["ann_invalid_datum"][i - 1] == ann_invalid_datum:
                        minute_averaging.loc[i, "ann_invalid_datum"] = ann_invalid_datum

            minute_averaging.set_index("utc_datetime", inplace=True)  # noqa: PD002

        # As this series of checks is not for wind direction, all the following columns are created just for
        # facilitating the algorithm
        else:
            minute_averaging["rolling_median_minute"] = np.nan
            minute_averaging["diff_abs"] = np.nan
            minute_averaging["median_diff_abs"] = np.nan
            minute_averaging["ann_jump_couples"] = np.nan
            minute_averaging["ann_invalid_datum"] = np.nan

        # Annotating all faulty observations
        minute_averaging["num_total_slots"] = minute_averaging["num_time_slots"] + minute_averaging["num_nan_values"]
        minute_averaging = minute_averaging.drop(columns=["num_time_slots", "num_nan_values"])

        # annotating with 1 if the availability of data within the averaging_period is < availability_threshold
        minute_averaging["ann_all_from_raw"] = minute_averaging.apply(
            lambda x: (
                1 if (x["num_total_slots"] - x["num_faulty"]) / (x["num_total_slots"]) < availability_threshold else 0
            ),
            axis=1,
        )

        # calculate the percentage of valid/available data within averaging_period
        minute_averaging["valid_percentage"] = minute_averaging.apply(
            lambda x: ((x["num_total_slots"] - x["num_faulty"]) * 100) / x["num_total_slots"],
            axis=1,
        )

        minute_averaging["valid_percentage"] = minute_averaging["valid_percentage"].round(2)

        # Annotating only reward-faulty observations
        # annotating with 1 if the availability of data within the averaging_period is < availability_threshold
        minute_averaging["ann_all_from_raw_rewards"] = minute_averaging.apply(
            lambda x: (
                1
                if (x["num_total_slots"] - x["faulty_rewards"]) / (x["num_total_slots"]) < availability_threshold
                else 0
            ),
            axis=1,
        )

        # calculate the percentage of valid/available data within averaging_period
        val_perc_rew = minute_averaging.apply(
            lambda x: ((x["num_total_slots"] - x["faulty_rewards"]) * 100) / x["num_total_slots"],
            axis=1,
        )

        minute_averaging.insert(loc=5, column="valid_percentage_rewards", value=val_perc_rew)
        minute_averaging["valid_percentage_rewards"] = minute_averaging["valid_percentage_rewards"].round(2)

        # Add text annotation when jump in minute level exists and if there was no jump in raw level
        minute_averaging = minute_averaging.apply(
            AnnotationUtils.update_ann_text,
            args=(
                "SPIKES",
                "ann_invalid_datum",
            ),
            axis=1,
        )

        # Add text annotation when average could not be calculated in minute level due to unavailability of data
        minute_averaging = minute_averaging.apply(
            AnnotationUtils.update_ann_text,
            args=(
                "NO_DATA",
                "ann_all_from_raw",
            ),
            axis=1,
        )

        # Make a new column where all faulty elements (for any reason)
        # detected in previous processes are annotated with 1
        minute_averaging["ann_total"] = np.where(
            (minute_averaging["ann_all_from_raw"] > 0) | (minute_averaging["ann_invalid_datum"] > 0),
            1,
            0,
        )

        # Make a new column where reward-faulty elements detected in previous processes are annotated with 1
        minute_averaging["ann_total_rewards"] = np.where(
            (minute_averaging["ann_all_from_raw_rewards"] > 0) | (minute_averaging["ann_invalid_datum"] > 0),
            1,
            0,
        )

        # Rearranging location of some columns for improving the readability of csv
        col2move = minute_averaging.pop("num_faulty")  # remove column 'C' and store it as a series

        # insert the series as column 'C' at index 0
        minute_averaging.insert(loc=4, column="num_faulty", value=col2move)

        # remove column 'C' and store it as a series
        col2move = minute_averaging.pop("num_total_slots")

        # insert the series as column 'C' at index 0
        minute_averaging.insert(loc=5, column="num_total_slots", value=col2move)

        col2move = minute_averaging.pop("valid_percentage")  # remove column 'C' and store it as a series

        # insert the series as column 'C' at index 0
        minute_averaging.insert(loc=6, column="valid_percentage", value=col2move)

        # remove column 'C' and store it as a series
        col2move = minute_averaging.pop("annotation")

        # insert the series as column 'C' at index 0
        minute_averaging.insert(len(minute_averaging.columns), "annotation", col2move)

        # Removing the first 'time_window_median' minutes of data in order to exclude the extra
        # time period required only for median calculation
        cutoff_time = minute_averaging.index[0] + pd.Timedelta(minutes=preprocess_time_window - 1)
        minute_averaging = minute_averaging[minute_averaging.index > cutoff_time]

        return fnl_df, minute_averaging
