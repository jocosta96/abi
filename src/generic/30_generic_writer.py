# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

dbutils.widgets.text('tb_name', '')
tb_name = dbutils.widgets.get('tb_name')

# COMMAND ----------

sdf_20 = spark.read.table(f'20_silver.{tb_name}')

# COMMAND ----------

(
    sdf_20.write.format("delta")
      .mode('overwrite')
      .option('delta.logRetentionDuration', f'interval 7 days')
      .option('delta.autoOptimize.autoCompact', 'true')
      .option('path', f'/mnt/30_gold/{tb_name}')
      .option('comment', f'{tb_name}')
      .saveAsTable(f'30_gold.{tb_name}')
)
