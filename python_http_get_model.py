# Databricks notebook source
from typing import List
import mlflow
mlflow.set_registry_uri("databricks-uc")

# COMMAND ----------

import os
import requests
import subprocess


def predict(model_input: List[str]) -> List[str]:
    def check_endpoint(endpoint_url):

        response = requests.get(endpoint_url)

        print("Status Code:", response.status_code)
        return f"Status Code:, {response.status_code}"

    result = check_endpoint(model_input[0])
    print(result)

    return [result]


with mlflow.start_run():
    model_info = mlflow.pyfunc.log_model(
        artifact_path="model", python_model=predict, input_example=["https://www.google.com"], registered_model_name="aso_fe_catalog.default.http_get"
    )


loaded_model = mlflow.pyfunc.load_model(model_uri=model_info.model_uri)
print(loaded_model.predict(["https://www.google.com"]))
