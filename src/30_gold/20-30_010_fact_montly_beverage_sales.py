# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

list_id_cols = [
      'montly_calendar_id'
    , 'bottler_origin_id'
    , 'brand_id'
    , 'package_id'
    , 'trade_channel_id'
    , 'channel_group_id'
]

# COMMAND ----------

sdf_20 = spark.read.table('20_silver.fact_beverage_sales')

# COMMAND ----------

sdf_30 = sdf_20.groupBy(list_id_cols).agg(F.sum('volume').alias('volume'))

# COMMAND ----------

(
    sdf_30.write.format("delta")
      .mode('overwrite')
      .option('delta.logRetentionDuration', f'interval 7 days')
      .option('delta.autoOptimize.autoCompact', 'true')
      .option('path', '/mnt/30_gold/fact_montly_beverage_sales')
      .option('comment', f'fact_montly_beverage_sales')
      .saveAsTable(f'30_gold.fact_montly_beverage_sales')
)
