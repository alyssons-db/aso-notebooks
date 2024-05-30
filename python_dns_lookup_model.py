# Databricks notebook source
from typing import List
import mlflow
mlflow.set_registry_uri("databricks-uc")

# COMMAND ----------

!pip install dnspython

# COMMAND ----------

import socket

def predict(model_input: List[str]) -> List[str]:
    def dns_query(domain):
        try:
            ip_address = socket.gethostbyname(domain)
        
        except socket.gaierror:
            return "Invalid domain name or DNS lookup failed"

        print(f"The IP address for {domain} is {ip_address}")
        return f"The IP address for {domain} is {ip_address}"

    result = dns_query(model_input[0])
    print(result)

    return [result]


with mlflow.start_run():
    model_info = mlflow.pyfunc.log_model(
        artifact_path="model", python_model=predict, input_example=["www.google.com"], registered_model_name="aso_default.default.dns_lookup"
    )


loaded_model = mlflow.pyfunc.load_model(model_uri=model_info.model_uri)
print(loaded_model.predict(["www.google.com"]))
