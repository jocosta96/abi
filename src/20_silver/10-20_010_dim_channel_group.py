# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

# MAGIC %run ../generic/merge_with_index

# COMMAND ----------

sdf_10 = spark.read.table('10_bronze.beverage_sales')

# COMMAND ----------

sdf_tc = sdf_10.select(
    
    F.trim(F.col('CHNL_GROUP')).alias('channel_group_desc')
    
).distinct()

# COMMAND ----------

merge_with_model(
      sdf=sdf_tc
    , layer='20_silver'
    , tb_name='dim_channel_group'
    , tb_keys=['channel_group_desc']
    , idx_name='channel_group_id'
)
