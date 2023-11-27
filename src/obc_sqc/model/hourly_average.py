from __future__ import annotations

import numpy as np
import pandas as pd

from obc_sqc.model.averaging_utils import AveragingUtils


class HourlyAveraging:  # noqa: D101
    group_label: str = "left"

    @staticmethod
    def average_in_hour(minute_averaging, fnl_timeslot, availability_threshold, parameter):
        """This def calculates the hourly averages of a parameter and the percentage of available data within an
        hour. Then, it annotates as invalid the hourly average if the percentage of available data was lower than the
        given availability_threshold. Note that the average of e.g. 01:00:00 is the average of values between
        00:00:01-01:00:00.

        minute_averaging (df): the output of average_in_minute def
        fnl_timeslot (int): the length of the final timeframe resolution in [minutes], e.g., if we want to give rewards
            for each hour then the value is 60 minutes
        availability_threshold (float): the availability threshold, e.g., if <67% of timeslots within a certain period
            is available, averaging or rewarding is not possible [x out of 1]
        parameter (str): the parameter that this def looks into, e.g., temperature, humidity, wind speed etc.

        result (df):
            a df with the a. averaged parameter under investigation,
            b. number of total timeslots within an hour,
            c. number of faulty slots within an hour,
            d. the percentage of valid data,
            e. numeric annotation (for both all- and -reward faulty data) and
            f. text annotation of each hourly slot
        """  # noqa: D205
        delim: str = ","

        # FIXME bandage
        minute_averaging = minute_averaging.reset_index(names=["utc_datetime"])
        minute_averaging["date"] = minute_averaging["utc_datetime"]

        # group data by x-minute intervals and count number of time slots,
        # NaN values, and calculate the 2-minute average temperature

        # In case of wind, we need to apply vector average
        if parameter == "wind_speed" or parameter == "wind_direction":  # noqa: PLR1714
            # calculate the u and v components of the wind in the raw dataset
            minute_averaging["wind_v"] = (
                -1
                * minute_averaging["wind_speed_avg"]
                * np.cos(minute_averaging["wind_direction_avg"].astype("Float64") * np.pi / 180.0)
            )
            minute_averaging["wind_u"] = (
                -1
                * minute_averaging["wind_speed_avg"]
                * np.sin(minute_averaging["wind_direction_avg"].astype("Float64") * np.pi / 180.0)
            )

            # Group by minute and calculate average of wind components
            # It is important that we firstly average the u and v components of the wind
            hour_averaging = minute_averaging.groupby(
                pd.Grouper(key="utc_datetime", label=HourlyAveraging.group_label, freq=f"{fnl_timeslot}min")
            ).agg(
                u=("wind_u", "mean"),  # this averages the u component of the wind
                v=("wind_v", "mean"),  # this averages the v component of the wind
                num_time_slots=(
                    "utc_datetime",
                    "count",
                ),  # this counts the elements having a value
                num_hourly_faulty=(
                    "ann_total",
                    "sum",
                ),  # find the total faulty elements within an hour
                num_hourly_faulty_rewards=(
                    "ann_total_rewards",
                    "sum",
                ),  # find the total reward-faulty elements within an hour
                annotation=(
                    "annotation",
                    lambda x: delim.join(s for s in set(x.str.split(delim).sum()) if s != ""),  # noqa: PLC1901
                ),
            )  # this merges all text annotation in one string

            # We put both columns in the dfs for wind dir and wind speed,
            # as we 'll need both for hourly averaging of wind spd and dir
            hour_averaging.insert(
                0,
                "wind_speed_avg",
                hour_averaging.apply(AveragingUtils.row_wind_speed_calculation, axis=1),
            )
            hour_averaging.insert(
                0,
                "wind_direction_avg",
                hour_averaging.apply(AveragingUtils.row_wind_direction_calculation, axis=1),
            )

            hour_averaging["u"] = hour_averaging["u"].round(2)
            hour_averaging["v"] = hour_averaging["v"].round(2)
            hour_averaging["wind_speed_avg"] = hour_averaging["wind_speed_avg"].astype("Float64").round(2)
            hour_averaging["wind_direction_avg"] = hour_averaging["wind_direction_avg"].astype("Float64").round(2)

            hour_averaging["wind_spd_avg_corrected"] = minute_averaging.groupby(
                pd.Grouper(key="utc_datetime", label=HourlyAveraging.group_label, freq=f"{fnl_timeslot}min")
            ).apply(
                AveragingUtils.column_wind_speed_average_using_annotation,
                availability_threshold=availability_threshold,
                annotation_col="ann_total",
            )
            hour_averaging["wind_dir_avg_corrected"] = minute_averaging.groupby(
                pd.Grouper(key="utc_datetime", label=HourlyAveraging.group_label, freq=f"{fnl_timeslot}min")
            ).apply(
                AveragingUtils.column_wind_direction_average_using_annotation,
                availability_threshold=availability_threshold,
                annotation_col="ann_total",
            )

        # For the rest of parameters we calculate the simple average
        else:
            parameter_avg_name = parameter + "_avg"

            if parameter == "precipitation_accumulated":
                hour_averaging = minute_averaging.groupby(
                    pd.Grouper(key="date", label=HourlyAveraging.group_label, freq=f"{fnl_timeslot}min")
                ).agg(
                    **{parameter_avg_name: (f"{parameter}_avg", "sum")},  # this counts the sum of the parameter
                    num_time_slots=(
                        "utc_datetime",
                        "count",
                    ),  # this counts the elements within the selected period e.g., 60minutes
                    num_hourly_faulty=(
                        "ann_total",
                        "sum",
                    ),  # find the total faulty elements within an hour
                    num_hourly_faulty_rewards=(
                        "ann_total_rewards",
                        "sum",
                    ),  # find the total reward-faulty elements within an hour
                    annotation=(
                        "annotation",
                        lambda x: delim.join(s for s in set(x.str.split(delim).sum()) if s != ""),  # noqa: PLC1901
                    ),
                )  # this merges all text annotation in one string

            else:
                hour_averaging = minute_averaging.groupby(
                    pd.Grouper(key="utc_datetime", label=HourlyAveraging.group_label, freq=f"{fnl_timeslot}min")
                ).agg(
                    **{parameter_avg_name: (f"{parameter}_avg", "mean")},
                    # this counts the simple average of the parameter
                    num_time_slots=(
                        "utc_datetime",
                        "count",
                    ),  # this counts the elements within the selected period e.g., 60minutes
                    num_hourly_faulty=(
                        "ann_total",
                        "sum",
                    ),  # find the total faulty elements within an hour
                    num_hourly_faulty_rewards=(
                        "ann_total_rewards",
                        "sum",
                    ),  # find the total reward-faulty elements within an hour
                    annotation=(
                        "annotation",
                        lambda x: delim.join(s for s in set(x.str.split(delim).sum()) if s != ""),  # noqa: PLC1901
                    ),
                )  # this merges all text annotation in one string

                hour_averaging[f"{parameter_avg_name}"] = hour_averaging[f"{parameter_avg_name}"].round(2)

                # calculating the average excluding faulty values
                hour_averaging[f"{parameter}_avg_corrected"] = minute_averaging.groupby(
                    pd.Grouper(key="utc_datetime", freq=f"{fnl_timeslot}min")
                ).apply(
                    AveragingUtils.column_average_using_annotation,
                    column=f"{parameter}_avg_corrected",
                    availability_threshold=availability_threshold,
                    annotation_col="ann_total",
                )
                hour_averaging[f"{parameter}_avg_corrected"] = hour_averaging[f"{parameter}_avg_corrected"].round(2)

        # calculate the percentage of valid observations per hour
        hour_averaging["valid_percentage"] = hour_averaging.apply(
            lambda x: ((x["num_time_slots"] - x["num_hourly_faulty"]) * 100) / x["num_time_slots"],
            axis=1,
        )
        hour_averaging["valid_percentage"] = hour_averaging["valid_percentage"].round(2)

        # annotate an hourly timeslot with 1 if <availability_threshold of the data is available.
        # Otherwise, annotate with 0.
        hour_averaging["ann_total_hour"] = hour_averaging.apply(
            lambda x: (
                1
                if (x["num_time_slots"] - x["num_hourly_faulty"]) / (x["num_time_slots"]) < availability_threshold
                else 0
            ),
            axis=1,
        )

        # calculate the percentage of valid observations per hour
        hour_averaging["valid_percentage_rewards"] = hour_averaging.apply(
            lambda x: ((x["num_time_slots"] - x["num_hourly_faulty_rewards"]) * 100) / x["num_time_slots"],
            axis=1,
        )
        hour_averaging["valid_percentage_rewards"] = hour_averaging["valid_percentage_rewards"].round(2)

        # annotate an hourly timeslot with 1 if <availability_threshold of the data is available.
        # Otherwise, annotate with 0.
        hour_averaging["ann_total_hour_rewards"] = hour_averaging.apply(
            lambda x: (
                1
                if (x["num_time_slots"] - x["num_hourly_faulty_rewards"]) / (x["num_time_slots"])
                < availability_threshold
                else 0
            ),
            axis=1,
        )

        # Rearranging location of some columns for improving the readability of csv
        col2move = hour_averaging.pop("annotation")
        hour_averaging.insert(len(hour_averaging.columns), "annotation", col2move)

        return hour_averaging
