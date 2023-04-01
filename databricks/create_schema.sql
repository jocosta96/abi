-- Databricks notebook source
-- MAGIC %md
-- MAGIC [doc](https://docs.databricks.com/sql/language-manual/sql-ref-syntax-ddl-create-schema.html)

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS 10_bronze
COMMENT 'raw zone' 
LOCATION '/mnt/10_bronze'

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS 20_silver
COMMENT 'refined zone' 
LOCATION '/mnt/20_silver'

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS 30_gold
COMMENT 'trusted zone' 
LOCATION '/mnt/30_gold'

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS metadata
COMMENT 'etl metadata'

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS metadata.landing (
    name     string
  , path     string
  , type     string
  , sep      string
)

USING DELTA
LOCATION '/mnt/dls/metadata/landing'

-- COMMAND ----------


