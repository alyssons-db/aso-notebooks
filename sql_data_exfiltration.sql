-- Databricks notebook source
DECLARE OR REPLACE VARIABLE pg_user STRING;
DECLARE OR REPLACE VARIABLE pg_pass STRING;

-- COMMAND ----------

SET VARIABLE pg_user = (SELECT secret('postgres', 'user'));
SET VARIABLE pg_pass = (SELECT secret('postgres', 'pass'));

-- COMMAND ----------

CREATE TABLE postres_data
USING postgresql
OPTIONS (
  dbtable 'exfil_data',
  host 'aso-postgres.cwyrldeexig2.ap-southeast-2.rds.amazonaws.com',
  port '5432',
  database 'postgres',
  user pg_user,
  password pg_pass
) AS
SELECT * FROM aso_fe_catalog.default.trips
