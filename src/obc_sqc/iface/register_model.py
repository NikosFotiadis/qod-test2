from __future__ import annotations

import mlflow

from obc_sqc.iface.model_wrapper import ObcSqcCheckWrapper
from obc_sqc.schema.schema import SchemaDefinitions


def main():  # noqa: D103
    # Model parametrization should be done here
    qod_model: mlflow.pyfunc.PythonModel = ObcSqcCheckWrapper()

    # MLFlow settngs
    mlflow.autolog()
    mlflow.set_experiment("obc_sqc")

    signature = SchemaDefinitions.mlflow_signature()

    # Start an MLFlow run
    with mlflow.start_run():
        model_info = mlflow.pyfunc.log_model(
            python_model=qod_model,
            code_path=["src/obc_sqc"],
            artifact_path="",
            signature=signature,
        )
        print(f"Model info: {model_info}")

        cur_model_uri: str = model_info.model_uri
        model_reg_result = mlflow.register_model(cur_model_uri, "obc_sqc", tags={"project": "qod", "version": "1.0.6"})
        print(f"New model version: {model_reg_result.version}")


if __name__ == "__main__":
    main()
