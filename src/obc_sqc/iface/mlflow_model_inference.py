from __future__ import annotations

import argparse
import datetime
import logging
import sys

import awswrangler as wr
import mlflow
import pandas as pd
from obc_sqc.schema.schema import SchemaDefinitions

logger = logging.getLogger("obc_sqc")
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.DEBUG)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="OBC SQC MLFlow inference")

    parser.add_argument("--device_id", help="Device ID", type=str, required=True)
    parser.add_argument("--date", type=str, help="Input date", required=True)
    parser.add_argument("--model_version", help="MLFlow model version.", type=str, default="Production")

    (
        k_args,
        unknown_args,
    ) = parser.parse_known_args(sys.argv[1:])

    args = vars(k_args)

    model_version: str = args["model_version"]
    loaded_model = mlflow.pyfunc.load_model(
        model_uri=f"models:/obc_sqc/{model_version}",
    )

    # Convert start and end dates to datetime
    input_date: datetime = datetime.datetime.strptime(args["date"], "%Y-%m-%d")
    starting_date = input_date - pd.Timedelta(hours=6)
    end_date = input_date + pd.Timedelta(hours=23, minutes=59, seconds=59)

    qod_input_schema = {
        "temperature": "Float64",
        "humidity": "Float64",
        "wind_speed": "Float64",
        "wind_direction": "Float64",
        "pressure": "Float64",
        "illuminance": "Float64",
        "precipitation_accumulated": "Float64",
        "model": str,
        "utc_datetime": str,
    }

    wr_df: pd.DataFrame = (
        wr.s3.read_parquet(
            f"s3://wxm-lake/device_data/by_device_date/device_id={args['device_id']}/",
            dataset=True,
            columns=list(qod_input_schema.keys()),
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

    pd.set_option("display.max_rows", 500)
    pd.set_option("display.max_columns", 500)
    pd.set_option("display.width", 1000)
    pd.set_option("display.expand_frame_repr", False)
    pd.set_option("max_colwidth", 99999)

    logger.info(df_with_schema.head())

    prediction = loaded_model.predict(df_with_schema, params={"device_id": args["device_id"]})

    logger.info(prediction.head(24))
