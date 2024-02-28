from __future__ import annotations

import datetime
import os
import socket
import traceback
from typing import Any

import mlflow
import pandas as pd

from obc_sqc.model.obc_sqc_driver import ObcSqcCheck
from obc_sqc.schema.schema import SchemaDefinitions
import awswrangler as wr
import opensearchpy
import logging

logger = logging.getLogger("obc_sqc")


class ObcSqcCheckWrapper(mlflow.pyfunc.PythonModel):  # noqa: D101
    def setup_os_logging(self) -> None:
        cur_date: str = str(datetime.datetime.now().date())
        self.es_index: str = f"mlflow-qod-logs-{cur_date}"
        self.opensearch_client: opensearchpy.OpenSearch = wr.opensearch.connect(
            host=os.environ["ES_QOD_HOST"], username=os.environ["ES_QOD_USER"], password=os.environ["ES_QOD_PASSWORD"]
        )

    def predict(self, context, model_input: pd.DataFrame, params: dict[str, Any] | None = None) -> pd.DataFrame:
        log_to_opensearch: bool = params["log_to_opensearch"]
        device_id: str = params["device_id"]
        cur_date: str = str(datetime.datetime.now().date())
        proc_ts_utc: str = f"{datetime.datetime.now().replace(microsecond=0).isoformat()}.000Z"

        try:
            # Log to an OS index
            if log_to_opensearch:
                self.setup_os_logging()

            # Inference
            model: ObcSqcCheck = ObcSqcCheck()
            result: pd.DataFrame = model.run(model_input)
            result_score: float = result["qod_score"].iloc[0]

            doc_info: dict = {
                "@timestamp": proc_ts_utc,
                "hostname": socket.gethostname(),
                "device_id": device_id,
                "date": cur_date,
                "score": result_score,
                "status": "success",
            }

            if log_to_opensearch:
                wr.opensearch.index_documents(self.opensearch_client, documents=[doc_info], index=self.es_index)
            else:
                logger.info(doc_info)

            return result
        except Exception as _:
            traceback.print_exc()
            var = traceback.format_exc()

            doc_info: dict = {
                "@timestamp": proc_ts_utc,
                "hostname": socket.gethostname(),
                "device_id": device_id,
                "date": cur_date,
                "exception": var,
                "status": "failure",
            }

            if log_to_opensearch:
                wr.opensearch.index_documents(self.opensearch_client, documents=[doc_info], index=self.es_index)
            else:
                logger.error(doc_info)

            return pd.DataFrame(columns=SchemaDefinitions.mlflow_obc_sqc_schema().keys()).astype(
                SchemaDefinitions.mlflow_obc_sqc_schema()
            )
        finally:
            if log_to_opensearch:
                self.opensearch_client.close()
