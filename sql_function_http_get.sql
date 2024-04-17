-- Databricks notebook source
use catalog aso_fe_catalog;
use default;

-- COMMAND ----------

CREATE OR REPLACE FUNCTION aso_http_get(url STRING)
RETURNS STRING
LANGUAGE PYTHON
AS $$
  import requests
  response = requests.get(url)
  return response.text
$$

-- COMMAND ----------

SELECT aso_http_get('https://www.google.com') AS response;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC url = 'https://www.google.com'
-- MAGIC
-- MAGIC import requests
-- MAGIC response = requests.get(url)
-- MAGIC display(response.text)
