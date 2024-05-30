-- Databricks notebook source
use catalog aso_catalog;
use default;

-- COMMAND ----------

CREATE OR REPLACE FUNCTION aso_dns_lookup(url STRING)
RETURNS STRING
LANGUAGE PYTHON
AS $$
  import socket
  try:
    ip_address = socket.gethostbyname_ex(url)
        
  except socket.gaierror:
    return "Invalid domain name or DNS lookup failed"
    
  return f"The IP address for {url} is {ip_address}"

$$

-- COMMAND ----------

SELECT aso_dns_lookup('310f35649dd0414787bc915304e266fd.serving.cloud.databricks.com') AS response;
