# Databricks notebook source
from typing import List
import mlflow
mlflow.set_registry_uri("databricks-uc")

# COMMAND ----------

import telnetlib

def predict(model_input: List[str]) -> List[str]:
    def telnet_model(host, port):

        try:
        # Create a Telnet object and establish a connection
            telnet = telnetlib.Telnet(host, int(port), 30)
            telnet.close()
            return f"Connection to {host} on port 443 succeeded."
        except Exception as e:
            return f"Connection to {host} on port 443 failed: {e}"

    result = telnet_model(model_input[0], model_input[1])
    print(result)

    return [result]


with mlflow.start_run():
    model_info = mlflow.pyfunc.log_model(
        artifact_path="model", python_model=predict, input_example=["www.google.com", "443"], registered_model_name="aso_catalog.default.telnet_model"
    )


loaded_model = mlflow.pyfunc.load_model(model_uri=model_info.model_uri)
print(loaded_model.predict(["www.google.com", "443"]))
