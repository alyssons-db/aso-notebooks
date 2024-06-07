-- Databricks notebook source
-- MAGIC %md
-- MAGIC Set Catalog and Schema

-- COMMAND ----------

use catalog aso_catalog;
use default;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create Python UDF for SQL

-- COMMAND ----------

CREATE OR REPLACE FUNCTION aso_conn_test(host STRING, port INTEGER)
RETURNS STRING
LANGUAGE PYTHON
AS $$
    import telnetlib
    try:
        # Create a Telnet object and establish a connection
        telnet = telnetlib.Telnet(host, port, 30)
        telnet.close()
        return f"Connection to {host} on port {port} succeeded."
    except Exception as e:
        return f"Connection to {host} on port {port} failed: {e}"
$$

-- COMMAND ----------

SELECT aso_conn_test('google.com', 443) AS response;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Test calling the Python function directly 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import socket
-- MAGIC
-- MAGIC def test_port_connection(host, port, timeout=3):
-- MAGIC     # Create a socket object
-- MAGIC     sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
-- MAGIC     sock.settimeout(timeout)  # Set a timeout on blocking socket operations
-- MAGIC
-- MAGIC     try:
-- MAGIC         # Attempt to connect to the specified host and port
-- MAGIC         result = sock.connect_ex((host, port))
-- MAGIC         if result == 0:
-- MAGIC             return f"Port {port} on {host} is open."
-- MAGIC         else:
-- MAGIC             return f"Port {port} on {host} is closed."
-- MAGIC     except socket.error as e:
-- MAGIC         return f"Failed to connect to {host} on port {port}: {str(e)}"
-- MAGIC     finally:
-- MAGIC         sock.close()  # Ensure the socket is closed
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC host = 'www.google.com'
-- MAGIC port = 443
-- MAGIC result = test_port_connection(host, port)
-- MAGIC print(result)
-- MAGIC
