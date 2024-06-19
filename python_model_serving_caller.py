# Databricks notebook source
from typing import List
import mlflow
mlflow.set_registry_uri("databricks-uc")

# COMMAND ----------

import os
import requests
import subprocess

# COMMAND ----------

def predict(model_input: List[str]) -> List[str]:
    def call_databricks(endpoint_url):

        headers = {'Authorization': f'Bearer {os.environ.get("DATABRICKS_TOKEN")}', 'Content-Type': 'application/json'}
        response = requests.get(f'{endpoint_url}/api/2.0/clusters/list', headers=headers)

        print("Response:", response.text)
        return f"Response:, {response.text}"

    result = call_databricks(model_input[0])
    print(result)

    return [result]


with mlflow.start_run():
    model_info = mlflow.pyfunc.log_model(
        artifact_path="model", python_model=predict, input_example=["https://dbc-32d8ea4d-e303.cloud.databricks.com"], registered_model_name="aso_catalog.default.databricks_api_caller"
    )


loaded_model = mlflow.pyfunc.load_model(model_uri=model_info.model_uri)
print(loaded_model.predict(["https://dbc-32d8ea4d-e303.cloud.databricks.com"]))
