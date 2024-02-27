from __future__ import annotations

import argparse
import datetime
import logging
import sys
import time

import awswrangler as wr
import pandas as pd

from obc_sqc.model.obc_sqc_driver import ObcSqcCheck
from obc_sqc.schema.schema import SchemaDefinitions

logger = logging.getLogger("obc_sqc")
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.DEBUG)


def main():
    """The algo requires an input a timeseries in csv with raw data of parameters of 'temperature',
    'humidity', 'wind_speed', 'wind_direction', 'pressure' and 'illuminance'. The following are conducted:
     - create a new timeframe with fixed time interval
     - check for constant data
     - check for jumps and availability in raw level (a)
     - check for jumps and availability in minute level (b)
     - check for availability in hourly level (c)
     - export results from a, b and c into csvs
    """  # noqa: D202, D205, D400, D415

    time.time()

    parser = argparse.ArgumentParser(description="OBC SQC Direct Inference")

    parser.add_argument("--device_id", help="Device ID", required=True)
    parser.add_argument("--date", help="Input date formatted as %Y-%m-%d", required=True)

    (k_args, unknown_args) = parser.parse_known_args(sys.argv[1:])

    args = vars(k_args)

    # Convert start and end dates to datetime
    input_date: datetime = datetime.datetime.strptime(args["date"], "%Y-%m-%d")
    starting_date = input_date - pd.Timedelta(hours=6)
    end_date = input_date + pd.Timedelta(hours=23, minutes=59, seconds=59)

    # QoD object/model/classifier
    qod_model = ObcSqcCheck()

    # Partition pruning
    required_columns: list[str] = SchemaDefinitions.qod_input_schema().keys()
    wr_df: pd.DataFrame = (
        wr.s3.read_parquet(
            f"s3://wxm-lake/device_data/by_device_date/device_id={args['device_id']}/",
            dataset=True,
            columns=required_columns,
            partition_filter=lambda x: (str(starting_date.date()) <= x["date"] <= str(end_date.date())),
        )
        .drop(columns=["device_id", "date"])
        .astype(SchemaDefinitions.qod_input_schema())
        .drop_duplicates()
    )

    # In-memory filtering
    df_with_schema: pd.DataFrame = wr_df[
        (wr_df["utc_datetime"] >= str(starting_date)) & (wr_df["utc_datetime"] <= str(end_date))
    ].reset_index(drop=True)

    result_df: pd.DataFrame = qod_model.run(df_with_schema)
    result_df.to_csv(f"fnl.csv", index=True)
    pd.set_option("display.max_rows", 500)
    pd.set_option("display.max_columns", 500)
    pd.set_option("display.width", 1000)
    pd.set_option("display.expand_frame_repr", False)
    logger.info(result_df.head(24))


if __name__ == "__main__":
    main()
